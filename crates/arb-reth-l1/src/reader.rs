//! [`SequencerInboxReader`]: fetch `SequencerBatchDelivered` logs from an L1 RPC and
//! resolve each batch's payload.

use alloy_consensus::Transaction as _;
use alloy_primitives::{Address, B256, U256};
use alloy_provider::Provider;
use alloy_rpc_types_eth::{Filter, Log};
use alloy_sol_types::SolEvent;

use arb_reth_derive::batch::{
    data_location, parse_sequencer_batch_delivered, SequencerBatchDeliveredData,
};

use crate::contracts::{SequencerBatchData, SequencerBatchDelivered};
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
    /// The header-flagged batch payload as inline bytes. Sourced from the posting tx
    /// calldata (`dataLocation == TxInput`) or from a `SequencerBatchData` event
    /// (`dataLocation == SeparateBatchEvent`); both yield the same payload shape, so
    /// downstream decode and `serialize_batch` treat them identically.
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

    /// Find the L1 block in which the batch with `sequence_number` was delivered,
    /// scanning forward from `from_block` in `window`-sized `getLogs` ranges. Returns
    /// `None` if the batch is not seen within a bounded number of windows.
    ///
    /// This resolves a snapshot's L1 resume point from chain state instead of trusting a
    /// hardcoded block: a genesis snapshot resumes at batch 0, whose delivery block this
    /// looks up anchored at the `SequencerInbox` deploy block (batch 0 lands in the first
    /// window). The bounded sweep avoids an unbounded scan when the batch can't be found.
    pub async fn delivery_block_of_batch(
        &self,
        sequence_number: u64,
        from_block: u64,
        window: u64,
    ) -> Result<Option<u64>, L1Error> {
        const MAX_WINDOWS: u64 = 64;
        let mut from = from_block;
        for _ in 0..MAX_WINDOWS {
            let to = from + window - 1;
            for log in self.batch_logs(from, to).await? {
                let topics = log.inner.data.topics();
                if topics.len() < 4 {
                    continue;
                }
                let seq = u64::from_be_bytes(topics[1].0[24..32].try_into().unwrap());
                if seq == sequence_number {
                    return Ok(Some(log.block_number.ok_or(L1Error::Missing("log block_number"))?));
                }
            }
            from = to + 1;
        }
        Ok(None)
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
            data_location::SEPARATE_BATCH_EVENT => {
                BatchPayload::Calldata(self.fetch_separate_batch_payload(log, sequence_number).await?)
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

    /// Resolve a `SeparateBatchEvent` batch's payload: find the `SequencerBatchData`
    /// event the `SequencerInbox` emitted in the same L1 block for this batch's sequence
    /// number, and decode its `bytes data` (the header-flagged payload). Mirrors Nitro's
    /// `getSequencerBatchData` separate-event path: filter by emitter address +
    /// `SequencerBatchData` topic-0 + the sequence number as the indexed topic-1, expect
    /// exactly one match.
    async fn fetch_separate_batch_payload(
        &self,
        log: &Log,
        sequence_number: u64,
    ) -> Result<Vec<u8>, L1Error> {
        let block_number = log.block_number.ok_or(L1Error::Missing("log block_number"))?;
        // Sequence number as the right-aligned 32-byte indexed topic (a uint256).
        let seq_topic = B256::from(U256::from(sequence_number).to_be_bytes::<32>());
        let filter = Filter::new()
            .address(self.address)
            .event_signature(SequencerBatchData::SIGNATURE_HASH)
            .topic1(seq_topic)
            .from_block(block_number)
            .to_block(block_number);
        let logs =
            self.provider.get_logs(&filter).await.map_err(|e| L1Error::Rpc(e.to_string()))?;
        let mut matched = logs.into_iter();
        let data_log = matched.next().ok_or(L1Error::Missing("SequencerBatchData event"))?;
        if matched.next().is_some() {
            return Err(L1Error::Missing("unique SequencerBatchData event"));
        }
        decode_event_bytes(&data_log.inner.data.data)
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

/// Decode a single ABI-encoded dynamic `bytes` argument from event data (offset word,
/// length word, then the body). Used for `SequencerBatchData(uint256, bytes)`'s `data`.
fn decode_event_bytes(data: &[u8]) -> Result<Vec<u8>, L1Error> {
    if data.len() < 64 {
        return Err(L1Error::Missing("SequencerBatchData head"));
    }
    let len: usize = U256::from_be_slice(&data[32..64])
        .try_into()
        .map_err(|_| L1Error::Missing("SequencerBatchData length overflow"))?;
    data.get(64..64 + len)
        .map(<[u8]>::to_vec)
        .ok_or(L1Error::Missing("SequencerBatchData body"))
}

#[cfg(test)]
mod tests {
    use super::decode_event_bytes;
    use alloy_primitives::U256;

    /// ABI-encode a single dynamic `bytes` argument: offset word (0x20), length word,
    /// then the body zero-padded to a 32-byte boundary.
    fn abi_bytes(payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&U256::from(32u64).to_be_bytes::<32>());
        out.extend_from_slice(&U256::from(payload.len()).to_be_bytes::<32>());
        out.extend_from_slice(payload);
        let pad = (32 - payload.len() % 32) % 32;
        out.extend(std::iter::repeat_n(0u8, pad));
        out
    }

    #[test]
    fn decodes_empty_bytes() {
        // Mirrors Arbitrum One batch 0's on-chain SequencerBatchData (empty payload).
        assert_eq!(decode_event_bytes(&abi_bytes(&[])).unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn decodes_nonempty_bytes_with_padding() {
        // A header-flagged payload whose length is not a multiple of 32 (exercises the
        // length word and the trailing zero-padding the body sits in).
        let payload: &[u8] = b"\x00brotli-flagged-batch-payload";
        assert_eq!(decode_event_bytes(&abi_bytes(payload)).unwrap(), payload);
    }

    #[test]
    fn rejects_head_shorter_than_two_words() {
        assert!(decode_event_bytes(&[0u8; 40]).is_err());
    }

    #[test]
    fn rejects_body_shorter_than_declared_length() {
        // Length word claims 32 bytes but no body follows.
        let mut data = U256::from(32u64).to_be_bytes::<32>().to_vec();
        data.extend_from_slice(&U256::from(32u64).to_be_bytes::<32>());
        assert!(decode_event_bytes(&data).is_err());
    }
}
