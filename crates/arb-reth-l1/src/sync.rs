//! Range derivation: turn an L1 block range into the ordered feed-message stream the
//! driver executes.
//!
//! This is the orchestration the catch-up runtime drives: fetch a range of
//! `SequencerBatchDelivered` logs, resolve each batch's payload (calldata inline, blob
//! via beacon), reconstruct the delayed messages those batches consume, and run the
//! pure [`assemble_feed_messages`] pipeline over them.
//!
//! Delayed-message coverage is the subtle part. A batch can consume delayed messages
//! that were delivered to the inbox in L1 blocks well before the batch's own block (a
//! deposit can sit in the delayed inbox indefinitely before a batch reads it). So the
//! delayed scan cannot be the batch range; [`fetch_delayed_map_covering`] walks L1
//! blocks backward from the range end until every delayed index the batches consume is
//! in hand.

use std::collections::{BTreeMap, BTreeSet};

use alloy_primitives::B256;
use alloy_provider::Provider;
use arb_reth_derive::delayed::{DelayedMap, DelayedMessage, NoDelayed};
use arb_sequencer_network::sequencer::feed::{BatchDataStats, BroadcastFeedMessage};

use crate::assemble::KIND_BATCH_POSTING_REPORT;
use crate::beacon::BeaconClient;
use crate::delayed::DelayedInboxReader;
use crate::reader::{BatchPayload, DeliveredBatch, SequencerInboxReader};
use crate::{
    assemble_feed_messages_with_seed, batch_data_hash, batch_data_stats, report_batch_num,
    report_data_hash, serialize_batch, L1Error,
};

/// Result of deriving one L1 block range.
#[derive(Debug)]
pub struct DerivedRange {
    /// Feed messages in execution order, ready for the driver.
    pub messages: Vec<BroadcastFeedMessage>,
    /// Delayed cursor after the last batch in the range (= the last batch's
    /// `afterDelayedMessagesRead`); the start cursor for the next range. Unchanged from
    /// the input when the range contained no batches.
    pub next_delayed_count: u64,
    /// Number of batches found in the range.
    pub batches: usize,
}

/// Default backward-scan window (in L1 blocks) for delayed-message coverage. Sized to
/// stay under typical `eth_getLogs` range caps (Alchemy/most providers reject ranges
/// far larger than this).
pub const DEFAULT_DELAYED_WINDOW: u64 = 10_000;

/// Fetch and resolve every batch in the inclusive L1 block range, in ascending batch
/// order, pairing each with its resolved payload bytes.
///
/// Blob batches require `beacon`; a blob batch with no beacon client is an error.
pub async fn resolve_batches<P: Provider>(
    seq_reader: &SequencerInboxReader<P>,
    beacon: Option<&BeaconClient>,
    from_block: u64,
    to_block: u64,
) -> Result<Vec<(DeliveredBatch, Vec<u8>)>, L1Error> {
    let mut logs = seq_reader.batch_logs(from_block, to_block).await?;
    // getLogs is ordered already, but pin it: assemble relies on ascending batch order.
    logs.sort_by_key(|l| (l.block_number.unwrap_or(0), l.log_index.unwrap_or(0)));

    let mut out = Vec::with_capacity(logs.len());
    for log in &logs {
        let batch = seq_reader.resolve_batch(log).await?;
        let payload = match &batch.payload {
            BatchPayload::Calldata(p) => p.clone(),
            BatchPayload::None => Vec::new(),
            BatchPayload::Blob { versioned_hashes, block_number } => {
                let beacon = beacon.ok_or(L1Error::Missing("beacon client for blob batch"))?;
                seq_reader.resolve_blob_payload(*block_number, versioned_hashes, beacon).await?
            }
        };
        out.push((batch, payload));
    }
    Ok(out)
}

/// Build a [`DelayedMap`] covering every delayed index in `[need_from, need_to)`.
///
/// Walks L1 blocks backward from `to_block` in `window`-sized chunks, accumulating
/// reconstructed delayed messages until all required indices are present (or block 0 is
/// reached). Returns `Missing` if the floor is hit without full coverage. An empty
/// required range returns an empty map without any RPC calls.
pub async fn fetch_delayed_map_covering<P: Provider>(
    delayed_reader: &DelayedInboxReader<P>,
    to_block: u64,
    need_from: u64,
    need_to: u64,
    window: u64,
) -> Result<DelayedMap, L1Error> {
    if need_to <= need_from {
        return Ok(DelayedMap::default());
    }
    let window = window.max(1);

    let mut acc: std::collections::BTreeMap<u64, DelayedMessage> = std::collections::BTreeMap::new();
    let mut hi = to_block;
    loop {
        let lo = hi.saturating_sub(window - 1);
        for m in delayed_reader.fetch_delayed(lo, hi).await? {
            acc.insert(m.inbox_seq_num, m);
        }

        // Covered once the lowest required index is in hand: indices are delivered in
        // ascending order, so seeing `need_from` means everything above it up to the
        // range end was captured by the windows already scanned.
        if acc.contains_key(&need_from) || (need_from..need_to).all(|i| acc.contains_key(&i)) {
            break;
        }
        if lo == 0 {
            return Err(L1Error::Missing("delayed messages: scan reached block 0 without coverage"));
        }
        hi = lo - 1;
    }

    for i in need_from..need_to {
        if !acc.contains_key(&i) {
            return Err(L1Error::Missing("delayed message index not found in scan"));
        }
    }
    Ok(DelayedMap::from_messages(acc.into_values()))
}

/// Derive one L1 block range end to end: resolve its batches, reconstruct the delayed
/// messages they consume, and assemble the feed-message stream.
///
/// `start_delayed_count` is the delayed cursor before the first batch in the range
/// (the previous range's [`DerivedRange::next_delayed_count`], or the resume point's
/// `delayedMessagesRead`). `window` bounds the backward delayed scan; pass
/// [`DEFAULT_DELAYED_WINDOW`] unless the provider needs a smaller `getLogs` range.
pub async fn derive_range<P: Provider>(
    seq_reader: &SequencerInboxReader<P>,
    delayed_reader: &DelayedInboxReader<P>,
    beacon: Option<&BeaconClient>,
    from_block: u64,
    to_block: u64,
    start_delayed_count: u64,
    window: u64,
) -> Result<DerivedRange, L1Error> {
    let resolved = resolve_batches(seq_reader, beacon, from_block, to_block).await?;
    if resolved.is_empty() {
        return Ok(DerivedRange { messages: Vec::new(), next_delayed_count: start_delayed_count, batches: 0 });
    }

    let next_delayed_count = resolved.last().unwrap().0.event.after_delayed_messages_read;
    let batches = resolved.len();

    // No delayed messages consumed across the range: skip the (expensive) delayed scan.
    if next_delayed_count <= start_delayed_count {
        let messages = assemble_feed_messages_with_seed(&resolved, &NoDelayed, start_delayed_count, BTreeMap::new())?;
        return Ok(DerivedRange { messages, next_delayed_count, batches });
    }

    let delayed = fetch_delayed_map_covering(
        delayed_reader,
        to_block,
        start_delayed_count,
        next_delayed_count,
        window,
    )
    .await?;

    // Seed stats for BatchPostingReports that report on batches posted *before* this
    // range. A report is a delayed message delivered in the same L1 block (and tx) as
    // the batch it reports; ArbOS fills its stats from that batch (matched by
    // keccak256(serialized) == data_hash). Reports for in-range batches are covered by
    // `assemble_feed_messages` itself, so they are skipped here.
    let in_range: BTreeSet<B256> = resolved.iter().map(|(b, _)| batch_data_hash(b)).collect();
    let seed_stats =
        seed_report_stats(seq_reader, &delayed, &in_range).await?;

    let messages =
        assemble_feed_messages_with_seed(&resolved, &delayed, start_delayed_count, seed_stats)?;
    Ok(DerivedRange { messages, next_delayed_count, batches })
}

/// Build the `data_hash -> stats` seed for any `BatchPostingReport` in `delayed` whose
/// reported batch lies outside the current range. Each report names its batch's
/// sequence number and was delivered in that batch's L1 block, so the batch is fetched
/// from `report.block_number` and verified by `keccak256(serialized) == data_hash`.
async fn seed_report_stats<P: Provider>(
    seq_reader: &SequencerInboxReader<P>,
    delayed: &DelayedMap,
    in_range: &BTreeSet<B256>,
) -> Result<BTreeMap<B256, BatchDataStats>, L1Error> {
    let mut seed: BTreeMap<B256, BatchDataStats> = BTreeMap::new();
    for m in delayed.0.values() {
        if m.kind != KIND_BATCH_POSTING_REPORT {
            continue;
        }
        let dh = report_data_hash(&m.data).ok_or(L1Error::Missing("report data_hash"))?;
        if in_range.contains(&dh) || seed.contains_key(&dh) {
            continue;
        }
        let seq = report_batch_num(&m.data).ok_or(L1Error::Missing("report batch num"))?;
        seed.insert(dh, stats_for_reported_batch(seq_reader, m.block_number, seq, dh).await?);
    }
    Ok(seed)
}

/// Fetch the batch with sequence number `seq_num` from L1 block `l1_block`, serialize
/// it, and return its [`BatchDataStats`] after verifying `keccak256(serialized)` equals
/// the report's `data_hash`.
async fn stats_for_reported_batch<P: Provider>(
    seq_reader: &SequencerInboxReader<P>,
    l1_block: u64,
    seq_num: u64,
    data_hash: B256,
) -> Result<BatchDataStats, L1Error> {
    let logs = seq_reader.batch_logs(l1_block, l1_block).await?;
    for log in &logs {
        let topics = log.inner.data.topics();
        let s = topics
            .get(1)
            .map(|t| u64::from_be_bytes(t.0[24..32].try_into().unwrap()));
        if s != Some(seq_num) {
            continue;
        }
        let batch = seq_reader.resolve_batch(log).await?;
        if batch_data_hash(&batch) != data_hash {
            return Err(L1Error::Missing("reported batch data_hash mismatch"));
        }
        return Ok(batch_data_stats(&serialize_batch(&batch)));
    }
    Err(L1Error::Missing("reported batch not found at its delivery block"))
}
