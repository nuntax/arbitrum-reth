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
    derive_from_resolved(
        seq_reader,
        delayed_reader,
        resolved,
        to_block,
        start_delayed_count,
        window,
    )
    .await
}

/// `data_hash -> stats` for batches already resolved from L1, persistent across windows so a
/// `BatchPostingReport` naming an earlier window's batch is served from memory instead of
/// re-fetched. Threaded by the catch-up runtime; see [`derive_from_resolved_cached`].
pub type ReportStatsCache = BTreeMap<B256, BatchDataStats>;

/// Forward-carried delayed-message cache, threaded across consecutive windows so the
/// `getLogs` cost is paid once per L1 range rather than re-paid on every consuming window.
///
/// The original derive path re-scanned the delayed inbox backward from each consuming
/// window's `to_block`, re-fetching heavily overlapping ranges (costly on a provider that
/// caps the `getLogs` span). This cache instead scans each L1 range exactly once: it extends
/// forward as the sync advances (picking up deliveries since the last scan) and only walks
/// backward for the rare deposit delivered before the earliest scan.
///
/// Invariant: the scanned L1 interval `[scanned_lo, scanned_hi]` is contiguous. It extends
/// forward from `scanned_hi` or backward from `scanned_lo`, never leaving a hole, so a block's
/// delayed logs are fetched at most once over the whole sync.
#[derive(Debug, Default)]
pub struct DelayedCache {
    msgs: BTreeMap<u64, DelayedMessage>,
    scanned_lo: Option<u64>,
    scanned_hi: Option<u64>,
}

impl DelayedCache {
    /// A fresh, empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    fn covers(&self, need_from: u64, need_to: u64) -> bool {
        (need_from..need_to).all(|i| self.msgs.contains_key(&i))
    }

    /// Scan `[lo, hi]` for delayed messages in `window`-sized `getLogs` chunks (providers cap
    /// the getLogs span), folding them into the cache and extending the scanned interval.
    async fn scan_range<P: Provider>(
        &mut self,
        reader: &DelayedInboxReader<P>,
        lo: u64,
        hi: u64,
        window: u64,
    ) -> Result<(), L1Error> {
        let window = window.max(1);
        let mut chunk_hi = hi;
        loop {
            let chunk_lo = chunk_hi.saturating_sub(window - 1).max(lo);
            for m in reader.fetch_delayed(chunk_lo, chunk_hi).await? {
                self.msgs.insert(m.inbox_seq_num, m);
            }
            if chunk_lo == lo {
                break;
            }
            chunk_hi = chunk_lo - 1;
        }
        self.scanned_lo = Some(self.scanned_lo.map_or(lo, |x| x.min(lo)));
        self.scanned_hi = Some(self.scanned_hi.map_or(hi, |x| x.max(hi)));
        Ok(())
    }

    /// Ensure delayed indices `[need_from, need_to)` are cached and return them as a
    /// [`DelayedMap`], fetching only L1 ranges not scanned before. Consumed entries
    /// (`< need_from`) are pruned afterward to bound memory.
    ///
    /// Delayed messages are delivered in ascending index order at L1 blocks no later than the
    /// batch that consumes them, so extending forward to `to_block` captures everything a
    /// window needs; the backward walk only fires for a deposit that predates the first scan.
    pub async fn cover<P: Provider>(
        &mut self,
        reader: &DelayedInboxReader<P>,
        to_block: u64,
        need_from: u64,
        need_to: u64,
        window: u64,
    ) -> Result<DelayedMap, L1Error> {
        if need_to <= need_from {
            return Ok(DelayedMap::default());
        }
        let window = window.max(1);

        if !self.covers(need_from, need_to) {
            // Extend forward over the gap since the last scan (each block scanned once).
            match self.scanned_hi {
                Some(hi) if hi < to_block => self.scan_range(reader, hi + 1, to_block, window).await?,
                Some(_) => {}
                None => {
                    self.scan_range(reader, to_block.saturating_sub(window - 1), to_block, window).await?
                }
            }
            // Still missing a low index: a deposit delivered before the earliest scan.
            while !self.covers(need_from, need_to) {
                let lo = self.scanned_lo.expect("scanned after forward extend");
                if lo == 0 {
                    return Err(L1Error::Missing(
                        "delayed messages: scan reached block 0 without coverage",
                    ));
                }
                let next_hi = lo - 1;
                let next_lo = next_hi.saturating_sub(window - 1);
                self.scan_range(reader, next_lo, next_hi, window).await?;
            }
        }

        let map = DelayedMap::from_messages(
            (need_from..need_to).map(|i| self.msgs.get(&i).expect("covered above").clone()),
        );
        self.msgs.retain(|&i, _| i >= need_from);
        Ok(map)
    }
}

/// The delayed-message + assembly tail of [`derive_range`], split out so the catch-up
/// runtime can prefetch [`resolve_batches`] for many L1 windows concurrently (it is the
/// main `getLogs`/blob RPC cost and is independent of the delayed cursor) and then run
/// this ordered tail sequentially, threading `start_delayed_count` window-to-window.
///
/// This is the one-shot form: it allocates fresh caches, so its behavior is identical to
/// deriving the window in isolation. The catch-up runtime uses [`derive_from_resolved_cached`]
/// with caches threaded across windows so the delayed scan and report-stat fetches are paid
/// once per L1 range instead of re-paid per window.
///
/// `resolved` is one window's [`resolve_batches`] output; `to_block` is that window's end.
pub async fn derive_from_resolved<P: Provider>(
    seq_reader: &SequencerInboxReader<P>,
    delayed_reader: &DelayedInboxReader<P>,
    resolved: Vec<(DeliveredBatch, Vec<u8>)>,
    to_block: u64,
    start_delayed_count: u64,
    window: u64,
) -> Result<DerivedRange, L1Error> {
    let mut delayed_cache = DelayedCache::new();
    let mut report_cache = ReportStatsCache::new();
    derive_from_resolved_cached(
        seq_reader,
        delayed_reader,
        resolved,
        to_block,
        start_delayed_count,
        window,
        &mut delayed_cache,
        &mut report_cache,
    )
    .await
}

/// As [`derive_from_resolved`], but with the [`DelayedCache`] and [`ReportStatsCache`] threaded
/// in by the caller so they persist across consecutive windows. This is the CU-efficient path:
/// each L1 range's delayed logs are fetched once (forward-carried), and a `BatchPostingReport`
/// naming an already-resolved batch is served from `report_cache` instead of being re-fetched.
pub async fn derive_from_resolved_cached<P: Provider>(
    seq_reader: &SequencerInboxReader<P>,
    delayed_reader: &DelayedInboxReader<P>,
    resolved: Vec<(DeliveredBatch, Vec<u8>)>,
    to_block: u64,
    start_delayed_count: u64,
    window: u64,
    delayed_cache: &mut DelayedCache,
    report_cache: &mut ReportStatsCache,
) -> Result<DerivedRange, L1Error> {
    if resolved.is_empty() {
        return Ok(DerivedRange { messages: Vec::new(), next_delayed_count: start_delayed_count, batches: 0 });
    }

    let next_delayed_count = resolved.last().unwrap().0.event.after_delayed_messages_read;
    let batches = resolved.len();

    // Record each in-range batch's stats by data hash (local, no RPC) so a later window's
    // BatchPostingReport naming one of them hits `report_cache` instead of re-fetching L1.
    // (A report is delivered in its batch's L1 block, so it lands out-of-range one window
    // later.) Reports for in-range batches are covered by `assemble_feed_messages` itself,
    // so `in_range` still excludes them from `seed_report_stats`.
    let in_range: BTreeSet<B256> = resolved
        .iter()
        .map(|(b, _)| {
            let dh = batch_data_hash(b);
            report_cache.entry(dh).or_insert_with(|| batch_data_stats(&serialize_batch(b)));
            dh
        })
        .collect();

    // No delayed messages consumed across the range: skip the (expensive) delayed scan.
    if next_delayed_count <= start_delayed_count {
        let messages = assemble_feed_messages_with_seed(&resolved, &NoDelayed, start_delayed_count, BTreeMap::new())?;
        return Ok(DerivedRange { messages, next_delayed_count, batches });
    }

    let delayed = delayed_cache
        .cover(delayed_reader, to_block, start_delayed_count, next_delayed_count, window)
        .await?;

    let seed_stats = seed_report_stats(seq_reader, &delayed, &in_range, report_cache).await?;

    let messages =
        assemble_feed_messages_with_seed(&resolved, &delayed, start_delayed_count, seed_stats)?;
    Ok(DerivedRange { messages, next_delayed_count, batches })
}

/// Build the `data_hash -> stats` seed for any `BatchPostingReport` in `delayed` whose
/// reported batch lies outside the current range. Served from `report_cache` when the batch was
/// resolved in an earlier window; otherwise the batch is fetched from `report.block_number`
/// (verified by `keccak256(serialized) == data_hash`) and the result is cached.
async fn seed_report_stats<P: Provider>(
    seq_reader: &SequencerInboxReader<P>,
    delayed: &DelayedMap,
    in_range: &BTreeSet<B256>,
    report_cache: &mut ReportStatsCache,
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
        if let Some(stats) = report_cache.get(&dh) {
            seed.insert(dh, stats.clone());
            continue;
        }
        let seq = report_batch_num(&m.data).ok_or(L1Error::Missing("report batch num"))?;
        let stats = stats_for_reported_batch(seq_reader, m.block_number, seq, dh).await?;
        report_cache.insert(dh, stats.clone());
        seed.insert(dh, stats);
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
