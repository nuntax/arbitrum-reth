//! Assemble ordered batches + delayed messages into the feed-message stream the
//! driver executes.
//!
//! Each batch decodes (via the multiplexer) into an ordered list of derived messages
//! (sequencer L2 messages interleaved with the delayed messages it consumes); each
//! becomes one block. `BatchPostingReport` (kind 13) messages additionally need the
//! `BatchDataStats` of the batch they report on, matched by `data_hash`. Reports are
//! posted in the same L1 tx as their batch and consumed by a later batch, so the
//! reported batch is always seen before its report; we accumulate
//! `data_hash -> stats` as batches are processed.

use std::collections::BTreeMap;

use alloy_primitives::B256;
use arb_reth_derive::delayed::DelayedSource;
use arbitrum_alloy_sequencer::sequencer::feed::{BatchDataStats, BroadcastFeedMessage};

use arb_reth_derive::message::DerivedMessage;

use crate::batch_serialize::{batch_data_hash, batch_data_stats, report_data_hash, serialize_batch};
use crate::reader::DeliveredBatch;
use crate::{decode_payload_messages, derived_to_feed_message, derived_to_feed_message_with_stats, L1Error};

/// L1 message kind for a batch posting report.
pub const KIND_BATCH_POSTING_REPORT: u8 = 13;

/// Convert one derived message to a feed message, attaching batch stats when it is a
/// `BatchPostingReport` (looked up by the report's `data_hash`).
pub fn derived_to_feed(
    m: &DerivedMessage,
    seq: u64,
    report_stats: &BTreeMap<B256, BatchDataStats>,
) -> Result<BroadcastFeedMessage, L1Error> {
    if m.header.kind == KIND_BATCH_POSTING_REPORT {
        let dh =
            report_data_hash(&m.l2_msg).ok_or(L1Error::Missing("batch posting report data_hash"))?;
        let stats = report_stats
            .get(&dh)
            .cloned()
            .ok_or(L1Error::Missing("batch posting report stats (batch not seen)"))?;
        Ok(derived_to_feed_message_with_stats(m, seq, Some(stats)))
    } else {
        Ok(derived_to_feed_message(m, seq))
    }
}

/// Decode one batch's resolved payload into feed messages, numbered from `seq_start`.
///
/// `payload` is the brotli-flagged batch payload (calldata `data`, or the
/// beacon-decoded blob payload). `before_delayed_count` is the delayed cursor before
/// this batch. `report_stats` maps batch `data_hash` to stats for any
/// `BatchPostingReport` consumed here.
pub fn batch_to_feed_messages(
    batch: &DeliveredBatch,
    payload: &[u8],
    before_delayed_count: u64,
    delayed: &dyn DelayedSource,
    report_stats: &BTreeMap<B256, BatchDataStats>,
    seq_start: u64,
) -> Result<Vec<BroadcastFeedMessage>, L1Error> {
    let header = batch.event.batch_header();
    let msgs = decode_payload_messages(&header, payload, before_delayed_count, delayed)?;

    let mut out = Vec::with_capacity(msgs.len());
    for (i, m) in msgs.iter().enumerate() {
        out.push(derived_to_feed(m, seq_start + i as u64, report_stats)?);
    }
    Ok(out)
}

/// Assemble an in-order run of resolved batches into one feed-message stream.
///
/// `resolved` is `(batch, resolved_payload)` in ascending batch order;
/// `start_delayed_count` is the delayed cursor before the first batch. Threads the
/// delayed cursor (`before` = previous batch's `afterDelayedMessagesRead`) and
/// accumulates batch stats so reports resolve.
pub fn assemble_feed_messages(
    resolved: &[(DeliveredBatch, Vec<u8>)],
    delayed: &dyn DelayedSource,
    start_delayed_count: u64,
) -> Result<Vec<BroadcastFeedMessage>, L1Error> {
    assemble_feed_messages_with_seed(resolved, delayed, start_delayed_count, BTreeMap::new())
}

/// As [`assemble_feed_messages`], but with a pre-seeded `data_hash -> stats` map.
///
/// A `BatchPostingReport` consumed in this run can report on a batch posted *before* the
/// run (reports are delayed messages read by a later batch), so its stats are not
/// recoverable from `resolved` alone. The catch-up runtime fetches those out-of-range
/// batches and seeds them here. In-range batch stats are still accumulated as batches
/// are processed, so the seed only needs to cover reports for batches outside `resolved`.
pub fn assemble_feed_messages_with_seed(
    resolved: &[(DeliveredBatch, Vec<u8>)],
    delayed: &dyn DelayedSource,
    start_delayed_count: u64,
    seed_stats: BTreeMap<B256, BatchDataStats>,
) -> Result<Vec<BroadcastFeedMessage>, L1Error> {
    let mut report_stats: BTreeMap<B256, BatchDataStats> = seed_stats;
    let mut before = start_delayed_count;
    let mut seq = 0u64;
    let mut out = Vec::new();

    for (batch, payload) in resolved {
        report_stats.insert(batch_data_hash(batch), batch_data_stats(&serialize_batch(batch)));
        let after = batch.event.after_delayed_messages_read;

        let msgs = batch_to_feed_messages(batch, payload, before, delayed, &report_stats, seq)?;
        seq += msgs.len() as u64;
        out.extend(msgs);
        before = after;
    }
    Ok(out)
}
