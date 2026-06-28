//! [`SequencerInboxReader`]: fetch `SequencerBatchDelivered` logs from an L1 RPC and
//! resolve each batch's payload.

use alloy_consensus::Transaction as _;
use alloy_primitives::{Address, B256};
use alloy_provider::Provider;
use alloy_rpc_types_eth::{Filter, Log};
use alloy_sol_types::SolEvent;

use arb_reth_derive::batch::{
    data_location, parse_sequencer_batch_delivered, SequencerBatchDeliveredData,
};

use crate::contracts::SequencerBatchDelivered;
use crate::{extract_calldata_payload, L1Error};

pub use crate::contracts::SEQUENCER_INBOX_MAINNET;

/// A `SequencerBatchDelivered` event with its payload resolved from L1.
#[derive(Debug, Clone)]
pub struct DeliveredBatch {
    /// `batchSequenceNumber` (indexed topic 1).
    pub sequence_number: u64,
    /// `beforeAcc` (indexed topic 2): inbox accumulator before this batch.
    pub before_acc: B256,
    /// `afterAcc` (indexed topic 3): inbox accumulator after this batch.
    pub after_acc: B256,
    /// Decoded non-indexed event fields (timeBounds, delayed cursor, dataLocation).
    pub event: SequencerBatchDeliveredData,
    /// The batch payload, located per `event.data_location`.
    pub payload: BatchPayload,
}

/// Where a batch's bytes came from.
#[derive(Debug, Clone)]
pub enum BatchPayload {
    /// `dataLocation == TxInput`: the header-flagged payload from tx calldata.
    Calldata(Vec<u8>),
    /// `dataLocation == Blob`: the tx's blob versioned hashes plus the L1 block they
    /// were posted in (used to derive the beacon slot for sidecar retrieval).
    Blob { versioned_hashes: Vec<B256>, block_number: u64 },
    /// `dataLocation == NoData`: an empty (delayed-only) batch.
    None,
}

/// Reads batches from an L1 `SequencerInbox` over a [`Provider`].
#[derive(Debug, Clone)]
pub struct SequencerInboxReader<P> {
    provider: P,
    address: Address,
}

impl<P: Provider> SequencerInboxReader<P> {
    /// Reader for an arbitrary `SequencerInbox` address (use for testnets/L3s).
    pub fn new(provider: P, address: Address) -> Self {
        Self { provider, address }
    }

    /// Reader pinned to the Arbitrum One `SequencerInbox`.
    pub fn mainnet(provider: P) -> Self {
        Self::new(provider, SEQUENCER_INBOX_MAINNET)
    }

    /// The configured `SequencerInbox` address.
    pub fn address(&self) -> Address {
        self.address
    }

    /// Fetch all `SequencerBatchDelivered` logs in the inclusive L1 block range.
    pub async fn batch_logs(&self, from_block: u64, to_block: u64) -> Result<Vec<Log>, L1Error> {
        let filter = Filter::new()
            .address(self.address)
            .event_signature(SequencerBatchDelivered::SIGNATURE_HASH)
            .from_block(from_block)
            .to_block(to_block);
        self.provider.get_logs(&filter).await.map_err(|e| L1Error::Rpc(e.to_string()))
    }

    /// Resolve one `SequencerBatchDelivered` log into a [`DeliveredBatch`], fetching
    /// the posting transaction when the payload lives in calldata or blobs.
    pub async fn resolve_batch(&self, log: &Log) -> Result<DeliveredBatch, L1Error> {
        let topics = log.inner.data.topics();
        if topics.len() < 4 {
            return Err(L1Error::Missing("indexed batch topics"));
        }
        // Indexed topics are right-aligned big-endian; the seq number fits in u64.
        let sequence_number = u64::from_be_bytes(topics[1].0[24..32].try_into().unwrap());
        let before_acc = topics[2];
        let after_acc = topics[3];

        let event = parse_sequencer_batch_delivered(&log.inner.data.data).map_err(L1Error::Batch)?;

        let payload = match event.data_location {
            data_location::TX_INPUT => {
                let tx = self.fetch_posting_tx(log).await?;
                BatchPayload::Calldata(extract_calldata_payload(tx.input().as_ref())?)
            }
            data_location::BLOB_HASHES => {
                let tx = self.fetch_posting_tx(log).await?;
                let versioned_hashes =
                    tx.blob_versioned_hashes().map(<[B256]>::to_vec).unwrap_or_default();
                let block_number = log.block_number.ok_or(L1Error::Missing("log block_number"))?;
                BatchPayload::Blob { versioned_hashes, block_number }
            }
            data_location::NO_DATA => BatchPayload::None,
            other => return Err(L1Error::UnsupportedDataLocation(other)),
        };

        Ok(DeliveredBatch { sequence_number, before_acc, after_acc, event, payload })
    }

    /// Resolve a blob batch's payload: derive the beacon slot from the posting
    /// block's timestamp, fetch the matching sidecars, and field-element decode them
    /// into the brotli-flagged batch payload (feed to [`crate::decode_payload_messages`]).
    pub async fn resolve_blob_payload(
        &self,
        block_number: u64,
        versioned_hashes: &[B256],
        beacon: &crate::BeaconClient,
    ) -> Result<Vec<u8>, L1Error> {
        let block = self
            .provider
            .get_block_by_number(alloy_eips::BlockNumberOrTag::Number(block_number))
            .await
            .map_err(|e| L1Error::Rpc(e.to_string()))?
            .ok_or(L1Error::Missing("L1 block"))?;
        let slot = beacon.slot_for_timestamp(block.header.timestamp);
        beacon.blob_batch_payload(slot, versioned_hashes).await
    }

    async fn fetch_posting_tx(&self, log: &Log) -> Result<impl alloy_consensus::Transaction, L1Error> {
        let tx_hash = log.transaction_hash.ok_or(L1Error::Missing("log transaction_hash"))?;
        self.provider
            .get_transaction_by_hash(tx_hash)
            .await
            .map_err(|e| L1Error::Rpc(e.to_string()))?
            .ok_or(L1Error::Missing("posting transaction"))
    }
}
