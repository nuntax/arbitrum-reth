//! The L1-derivation catch-up runtime.
//!
//! Drives trustless sync. Walks L1 block ranges with [`arb_reth_l1::sync::derive_range`]
//! (SequencerInbox batches + delayed inbox + blob sidecars), and pushes each derived
//! [`BroadcastFeedMessage`] into the same feed channel the `ArbEngineDriver` drains, so
//! derived blocks execute through the same path the driver uses. It follows the L1 head
//! once caught up.
//!
//! ## Resuming
//!
//! `start_block` / `start_delayed_count` are the L1 block and `delayedMessagesRead` the resume
//! point was built from, and `start_l2_block` is the L2 block that point sits after. As the runtime
//! consumes each L1 window it records an [`L1ResumeCheckpoint`](crate::resume), once the window's
//! blocks are durable, so a later restart resumes from the last checkpoint instead of Nitro
//! genesis. On a resume whose checkpoint predates the durable tip (persistence outran the last
//! written checkpoint), the first re-derived blocks reproduce ones already on disk; they are
//! numbered absolutely from `start_l2_block` and dropped up to `db_tip_l2`, so the driver only ever
//! sees `db_tip_l2 + 1` onward. The very first sync of a genesis snapshot has no checkpoint yet, so
//! the caller supplies the genesis start point.
//!
//! ## ArbOS version across upgrades
//!
//! This runtime does not carry an ArbOS version. The version advances per block downstream:
//! `produce()` (engine.rs) derives the message-parse version from the parent header's encoded
//! ArbOS version, and the per-block start-block internal tx applies scheduled ArbOS state
//! upgrades (`upgrade_arbos_version`, arb_revm `internal_tx.rs`) when due. So a catch-up across
//! an upgrade boundary is wired, though not yet validated against a real mainnet upgrade crossing.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::Duration;

use alloy_primitives::Address;
use alloy_provider::{Provider, ProviderBuilder};
use arb_reth_l1::sync::{
    derive_from_resolved_cached, resolve_batches, DelayedCache, ReportStatsCache,
    DEFAULT_DELAYED_WINDOW,
};
use arb_reth_l1::{BeaconClient, DelayedInboxReader, DeliveredBatch, SequencerInboxReader};
use arb_sequencer_network::sequencer::feed::BroadcastFeedMessage;
use eyre::{eyre, Context as _};
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

use crate::resume::{L1ResumeCheckpoint, L1ResumeLog};

/// Configuration for the L1-derivation catch-up runtime.
#[derive(Debug, Clone)]
pub struct L1SyncConfig {
    /// L1 execution-layer JSON-RPC endpoint (archive, with historical `getLogs`).
    pub l1_rpc: String,
    /// L1 beacon (consensus-layer) REST endpoint for blob sidecars. Required to derive
    /// post-Dencun blob batches; calldata-era ranges work without it.
    pub l1_beacon: Option<String>,
    /// `SequencerInbox` contract address.
    pub sequencer_inbox: Address,
    /// `Bridge` contract address (delayed-inbox metadata source).
    pub bridge: Address,
    /// First L1 block to derive from (the resume point's batch boundary).
    pub start_block: u64,
    /// Last L1 block to derive (inclusive). `None` follows the head indefinitely.
    pub end_block: Option<u64>,
    /// Delayed cursor before `start_block` (the L2 tip's `delayedMessagesRead`).
    pub start_delayed_count: u64,
    /// Absolute L2 block number that `start_block` resumes *after*: the block preceding the first
    /// message derived from `start_block`. Derived messages are numbered `start_l2_block + 1, +2,
    /// …` so already-present blocks can be recognized and skipped. For a genesis start this is the
    /// Nitro genesis block; for a checkpoint resume it is the checkpoint's `l2_block`.
    pub start_l2_block: u64,
    /// Current durable L2 tip. Re-derived blocks with number `<= db_tip_l2` are already in the DB,
    /// so they are dropped rather than re-sent to the driver (which would produce them again). The
    /// first message sent is always block `db_tip_l2 + 1`.
    pub db_tip_l2: u64,
    /// Where to persist the [`L1ResumeCheckpoint`] as sync advances (`None` disables checkpointing).
    pub checkpoint_path: Option<PathBuf>,
    /// L1 blocks per `derive_range` call (bounds `getLogs` range per request).
    pub batch_window: u64,
    /// Backward-scan window for delayed-message coverage.
    pub delayed_window: u64,
    /// Stay this many blocks behind the L1 head (reorg safety margin).
    pub confirmations: u64,
    /// Poll interval when caught up to the safe head.
    pub poll_interval: Duration,
    /// How many `resolve_batches` window fetches to keep in flight concurrently (prefetch
    /// depth). `resolve_batches` is the dominant `getLogs`/blob RPC cost and is independent of
    /// the delayed cursor, so prefetching overlaps its latency during catch-up. 1 = serial.
    pub prefetch_windows: u64,
}

impl L1SyncConfig {
    /// Mainnet defaults: Arbitrum One `SequencerInbox`/`Bridge`, 1k-block windows, 8
    /// confirmations, 12s polling. `l1_rpc` and the resume point must still be set.
    pub fn mainnet(l1_rpc: String, start_block: u64, start_delayed_count: u64) -> Self {
        Self {
            l1_rpc,
            l1_beacon: None,
            sequencer_inbox: arb_reth_l1::SEQUENCER_INBOX_MAINNET,
            bridge: arb_reth_l1::BRIDGE_MAINNET,
            start_block,
            end_block: None,
            start_delayed_count,
            start_l2_block: 0,
            db_tip_l2: 0,
            checkpoint_path: None,
            batch_window: 1_000,
            delayed_window: DEFAULT_DELAYED_WINDOW,
            confirmations: 8,
            poll_interval: Duration::from_secs(12),
            prefetch_windows: 6,
        }
    }
}

/// Run the catch-up runtime: derive L1 ranges and push feed messages into `feed_tx`
/// until `end_block` is reached (or forever, following the head, when `end_block` is
/// `None`). Returns when the range is exhausted or the channel closes.
///
/// `persisted_tip` reports the current durable L2 tip (the persisted DB head, NOT the in-memory
/// canonical head); it gates checkpoint writes so a resume point is only recorded once its blocks
/// are on disk. It is polled once per consumed window, so it must be cheap.
pub async fn run_l1_sync<F>(
    cfg: L1SyncConfig,
    feed_tx: Sender<BroadcastFeedMessage>,
    persisted_tip: F,
) -> eyre::Result<()>
where
    F: Fn() -> u64 + Send,
{
    let provider = ProviderBuilder::new()
        .connect_http(cfg.l1_rpc.parse().wrap_err("invalid --l1-rpc URL")?);
    let seq_reader = SequencerInboxReader::new(provider.clone(), cfg.sequencer_inbox);
    let delayed_reader = DelayedInboxReader::new(provider.clone(), cfg.bridge);
    let beacon = cfg.l1_beacon.as_ref().map(|u| BeaconClient::new(u.clone()));

    let prefetch = cfg.prefetch_windows.max(1) as usize;

    // The ORDERED tail (delayed-map + assembly) threads these window-to-window.
    let mut delayed = cfg.start_delayed_count;
    // Forward-carried caches so the delayed scan and report-stat fetches (the dominant
    // `getLogs` cost) are paid once per L1 range instead of re-paid on every window.
    let mut delayed_cache = DelayedCache::new();
    let mut report_cache = ReportStatsCache::new();
    let mut consume_cursor = cfg.start_block;
    // The prefetcher runs ahead: the next L1 window to spawn a `resolve_batches` for.
    let mut spawn_cursor = cfg.start_block;
    let mut safe_head: u64 = 0;

    // Absolute L2 numbering + resume bookkeeping. `next_l2` is the block number the next derived
    // message produces; blocks `<= db_tip_l2` are already persisted and get dropped.
    let mut next_l2 = cfg.start_l2_block + 1;
    let db_tip_l2 = cfg.db_tip_l2;
    // Window boundaries awaiting durability before they can be appended to the resume log.
    // Ascending in both `l1_block` and `l2_block`; drained front-to-back as `persisted_tip` rises.
    let mut pending_ckpt: VecDeque<L1ResumeCheckpoint> = VecDeque::new();
    // The persisted resume log, seeded from the existing file so `record`'s history survives across
    // restarts (and the rewind tool can find an old-enough boundary).
    let mut resume_log = cfg
        .checkpoint_path
        .as_deref()
        .and_then(L1ResumeLog::load)
        .unwrap_or_default();

    // In-flight `resolve_batches` tasks, kept in ascending window order (FIFO). Each is
    // independent of the delayed cursor, so up to `prefetch` run concurrently, overlapping
    // their `getLogs`/blob RPC latency; we consume them in order and run the delayed tail.
    type Inflight = (u64, u64, JoinHandle<Result<Vec<(DeliveredBatch, Vec<u8>)>, arb_reth_l1::L1Error>>);
    let mut inflight: VecDeque<Inflight> = VecDeque::new();

    loop {
        if cfg.end_block.is_some_and(|end| consume_cursor > end) {
            break;
        }

        // Refresh the safe head only when the prefetcher has caught up to it (avoids a
        // head query per window during bulk catch-up).
        if spawn_cursor > safe_head {
            let head = provider
                .get_block_number()
                .await
                .map_err(|e| eyre!("L1 get_block_number: {e}"))?;
            safe_head = head.saturating_sub(cfg.confirmations);
        }

        // Fill the prefetch pipeline: spawn concurrent `resolve_batches` for the next windows.
        while inflight.len() < prefetch && spawn_cursor <= safe_head {
            if cfg.end_block.is_some_and(|end| spawn_cursor > end) {
                break;
            }
            let mut to = (spawn_cursor + cfg.batch_window.max(1) - 1).min(safe_head);
            if let Some(end) = cfg.end_block {
                to = to.min(end);
            }
            let (s, b, from) = (seq_reader.clone(), beacon.clone(), spawn_cursor);
            let handle = tokio::spawn(async move { resolve_batches(&s, b.as_ref(), from, to).await });
            inflight.push_back((from, to, handle));
            spawn_cursor = to + 1;
        }

        // Nothing in flight and nothing new to spawn: caught up to the safe head. Wait.
        let Some((from, to, handle)) = inflight.pop_front() else {
            tokio::time::sleep(cfg.poll_interval).await;
            continue;
        };

        // Await the oldest prefetched window (in order), then run the ordered delayed tail.
        let resolved = handle
            .await
            .map_err(|e| eyre!("resolve_batches task [{from}, {to}] failed: {e}"))??;
        let derived = derive_from_resolved_cached(
            &seq_reader,
            &delayed_reader,
            resolved,
            to,
            delayed,
            cfg.delayed_window,
            &mut delayed_cache,
            &mut report_cache,
        )
        .await
        .wrap_err_with(|| format!("derive_from_resolved [{from}, {to}]"))?;

        if derived.batches > 0 {
            tracing::info!(
                target: "arb-reth::l1-sync",
                from, to, batches = derived.batches,
                messages = derived.messages.len(), next_delayed = derived.next_delayed_count,
                inflight = inflight.len(),
                "derived L1 range",
            );
        }

        // Number each derived message with its absolute L2 block, then either send it or, when it
        // reproduces a block already on disk (a resume that started before the DB tip), drop it.
        // The `next_l2` counter advances for dropped blocks too: they are real blocks in the chain.
        let mut skipped = 0u64;
        for mut msg in derived.messages {
            let bn = next_l2;
            next_l2 += 1;
            if bn <= db_tip_l2 {
                skipped += 1;
                continue;
            }
            msg.sequence_number = bn;
            if feed_tx.send(msg).await.is_err() {
                tracing::warn!(target: "arb-reth::l1-sync", "feed channel closed; stopping L1 sync");
                return Ok(());
            }
        }
        if skipped > 0 {
            tracing::debug!(
                target: "arb-reth::l1-sync",
                from, to, skipped, resumed_at = db_tip_l2 + 1,
                "dropped already-persisted blocks on resume",
            );
        }

        delayed = derived.next_delayed_count;
        consume_cursor = to + 1;

        // Queue this window boundary and flush any whose L2 blocks are now durable. Appending only
        // persisted boundaries keeps the log recoverable: after a crash the DB tip is always at or
        // beyond the last-logged `l2_block`.
        if cfg.checkpoint_path.is_some() {
            pending_ckpt.push_back(L1ResumeCheckpoint {
                l1_block: consume_cursor,
                delayed_count: delayed,
                l2_block: next_l2 - 1,
            });
            maybe_write_checkpoint(
                cfg.checkpoint_path.as_deref(),
                &mut resume_log,
                &mut pending_ckpt,
                persisted_tip(),
            );
        }
    }

    tracing::info!(target: "arb-reth::l1-sync", final_block = consume_cursor.saturating_sub(1), "L1 sync reached end block");
    Ok(())
}

/// Append every durable window boundary to the resume log and rewrite it.
///
/// Moves each queued boundary with `l2_block <= persisted` (they are ascending) into the in-memory
/// log, then rewrites the whole (bounded) log once. A save failure is logged, not fatal: the
/// boundaries stay in the in-memory log, so the next window's save re-persists them, with no
/// re-queue (which would double-record).
fn maybe_write_checkpoint(
    path: Option<&std::path::Path>,
    log: &mut L1ResumeLog,
    pending: &mut VecDeque<L1ResumeCheckpoint>,
    persisted: u64,
) {
    let Some(path) = path else { return };
    let mut newest: Option<L1ResumeCheckpoint> = None;
    while pending.front().is_some_and(|cp| cp.l2_block <= persisted) {
        let cp = pending.pop_front().unwrap();
        log.record(cp);
        newest = Some(cp);
    }
    let Some(newest) = newest else { return };
    match log.save(path) {
        Ok(()) => tracing::debug!(
            target: "arb-reth::l1-sync",
            l1_block = newest.l1_block, delayed = newest.delayed_count, l2_block = newest.l2_block,
            "wrote L1 resume checkpoint",
        ),
        Err(e) => tracing::warn!(
            target: "arb-reth::l1-sync", err = %e,
            "failed to write L1 resume log; will retry on the next window",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cp(l1_block: u64, l2_block: u64) -> L1ResumeCheckpoint {
        L1ResumeCheckpoint { l1_block, delayed_count: 0, l2_block }
    }

    /// The gate only appends boundaries whose L2 blocks are durable, always advances the log's
    /// newest entry to the FURTHEST such boundary, and leaves not-yet-persisted boundaries queued.
    #[test]
    fn checkpoint_gate_appends_durable_boundaries() {
        let dir = reth_db::test_utils::tempdir_path();
        let path = L1ResumeLog::path_in(&dir);
        let mut log = L1ResumeLog::default();
        let mut pending: VecDeque<L1ResumeCheckpoint> =
            [cp(100, 10), cp(200, 20), cp(300, 30)].into_iter().collect();

        // Nothing persisted past block 5 → no boundary is safe to append yet.
        maybe_write_checkpoint(Some(&path), &mut log, &mut pending, 5);
        assert_eq!(L1ResumeLog::load(&path), None);
        assert_eq!(pending.len(), 3, "no boundary consumed");

        // Durable tip at 20 → boundaries 10 and 20 are safe; both logged, 30 stays queued.
        maybe_write_checkpoint(Some(&path), &mut log, &mut pending, 20);
        let loaded = L1ResumeLog::load(&path).expect("log written");
        assert_eq!(loaded.resume_for(u64::MAX), Some(cp(200, 20)), "newest logged is 20");
        assert_eq!(loaded.checkpoints, vec![cp(100, 10), cp(200, 20)]);
        assert_eq!(pending, [cp(300, 30)].into_iter().collect::<VecDeque<_>>());

        // Durable tip past 30 → final boundary flushes.
        maybe_write_checkpoint(Some(&path), &mut log, &mut pending, 99);
        assert_eq!(L1ResumeLog::load(&path).unwrap().resume_for(u64::MAX), Some(cp(300, 30)));
        assert!(pending.is_empty());
    }

    /// An empty window (no new blocks) still advances `l1_block` at the same `l2_block`, so resume
    /// skips barren L1 ranges instead of re-scanning them.
    #[test]
    fn checkpoint_gate_advances_l1_over_empty_windows() {
        let dir = reth_db::test_utils::tempdir_path();
        let path = L1ResumeLog::path_in(&dir);
        let mut log = L1ResumeLog::default();
        // Two windows produced block 10; the next two windows held no batches (l2 stays 10).
        let mut pending: VecDeque<L1ResumeCheckpoint> =
            [cp(100, 10), cp(200, 10), cp(300, 10)].into_iter().collect();

        maybe_write_checkpoint(Some(&path), &mut log, &mut pending, 10);
        let loaded = L1ResumeLog::load(&path).expect("log written");
        assert_eq!(
            loaded.checkpoints,
            vec![cp(300, 10)],
            "l1_block advances past empty ranges while l2_block is unchanged",
        );
        assert!(pending.is_empty());
    }
}
