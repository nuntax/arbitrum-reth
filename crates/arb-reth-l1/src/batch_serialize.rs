//! Batch serialization for `BatchPostingReport` (kind 13) gas accounting.
//!
//! A batch posting report records `keccak256(serialized)` of the batch it reports,
//! and ArbOS charges the poster using `BatchDataStats { length, non_zeros }` over
//! those same `serialized` bytes. The report body carries neither the stats nor the
//! data, so an L1-derivation node must rebuild `serialized` itself (mirrors Nitro
//! `arbnode/mel/extraction` `SerializeBatch` + `arbostypes.GetDataStats`).
//!
//! `serialized` = 40-byte big-endian timeBounds header
//! (`minTimestamp,maxTimestamp,minL1Block,maxL1Block,afterDelayedMessages`) followed
//! by the batch data, which depends on the data location:
//! * calldata: the `addSequencerL2BatchFromOrigin` `data` argument;
//! * blob: the `0x50` blob-hashes flag byte followed by the blob versioned hashes
//!   (the on-chain footprint, NOT the decompressed blob payload);
//! * none: nothing.

use alloy_primitives::{keccak256, B256};
use arb_reth_derive::batch::flag;
use arbitrum_alloy_sequencer::sequencer::feed::BatchDataStats;

use crate::reader::{BatchPayload, DeliveredBatch};

/// Rebuild the serialized batch bytes a `BatchPostingReport` hashes over.
pub fn serialize_batch(batch: &DeliveredBatch) -> Vec<u8> {
    let h = batch.event.batch_header();
    let mut out = Vec::new();
    for v in [
        h.min_timestamp,
        h.max_timestamp,
        h.min_l1_block,
        h.max_l1_block,
        h.after_delayed_messages,
    ] {
        out.extend_from_slice(&v.to_be_bytes());
    }
    match &batch.payload {
        BatchPayload::Calldata(d) => out.extend_from_slice(d),
        BatchPayload::Blob { versioned_hashes, .. } => {
            out.push(flag::BLOB_HASHES);
            for vh in versioned_hashes {
                out.extend_from_slice(vh.as_slice());
            }
        }
        BatchPayload::None => {}
    }
    out
}

/// `keccak256(serialize_batch(batch))`; equals the report body's `data_hash`.
pub fn batch_data_hash(batch: &DeliveredBatch) -> B256 {
    keccak256(serialize_batch(batch))
}

/// The L1 calldata stats ArbOS charges the batch poster for.
pub fn batch_data_stats(serialized: &[u8]) -> BatchDataStats {
    BatchDataStats {
        length: serialized.len() as u64,
        non_zeros: serialized.iter().filter(|&&b| b != 0).count() as u64,
    }
}

/// Extract the `data_hash` field (`keccak256(serialized)`) from a `BatchPostingReport`
/// message body: fixed-width `[timestamp(32), poster(20), data_hash(32), batchNum(32),
/// l1BaseFee(32), extraGas(8)?]`.
pub fn report_data_hash(body: &[u8]) -> Option<B256> {
    body.get(52..84).map(B256::from_slice)
}

/// Extract the reported batch's sequence number (`batchNum`, big-endian word at
/// `[84..116]`) from a `BatchPostingReport` body.
pub fn report_batch_num(body: &[u8]) -> Option<u64> {
    let word = body.get(84..116)?;
    Some(u64::from_be_bytes(word[24..32].try_into().unwrap()))
}
