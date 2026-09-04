//! Persistent L1-derivation resume checkpoints.
//!
//! The node's L2 blocks are durable in MDBX, and the driver already boots its tip from the DB
//! head. The one piece of sync state that is NOT recoverable from the L2 chain alone is the
//! L1-derivation cursor: which L1 block / delayed-inbox count the next batch should be read from.
//! Without it, a restart can only re-derive from Nitro genesis.
//!
//! This module persists that cursor to `<datadir>/arb-l1-resume.json` as the node syncs, at
//! batch/window boundaries whose L2 blocks are durably persisted. A restart reads it back and
//! resumes derivation from there (the file-based equivalent of Nitro's `arbitrumdata` mapping,
//! for the common single-writer case).
//!
//! ## Why dense recent history plus sparse historical history
//!
//! The file keeps the latest [`RECENT_CHECKPOINTS`] boundaries at full resolution. Older entries
//! are compacted to the earliest boundary in each [`HISTORICAL_CHECKPOINT_L2_INTERVAL`]-block L2
//! bucket. A consumer can therefore pick a boundary at or below an arbitrarily old L2 block
//! without retaining every derivation window. Two cases need that:
//!
//! * **Rewind** (`arb-rewind`) unwinds the DB to some block `N-1` after a divergence; it truncates
//!   the log to boundaries `<= N-1` so the next start resumes from a batch boundary at or below the
//!   new tip instead of the (now-removed) poisoned tip.
//! * **Crash** under MDBX `SafeNoSync`, where a power-loss can roll the DB tip back behind the last
//!   synced checkpoint; the loader falls back to an earlier boundary instead of failing.
//!
//! ## Crash safety
//!
//! A boundary is only appended once its L2 blocks are durably persisted (`db_tip >= l2_block`), and
//! the whole log is rewritten atomically (temp file + rename). So after a crash the on-disk DB tip
//! is at or beyond the newest logged `l2_block` (barring a `SafeNoSync` rollback, handled above),
//! and derivation resumes at a clean batch boundary; the L1-sync runtime drops any re-derived
//! blocks it already has.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// File name (under the data directory) of the L1-derivation resume log.
pub const RESUME_FILE_NAME: &str = "arb-l1-resume.json";

/// How many recent derivation boundaries remain at full resolution.
pub const RECENT_CHECKPOINTS: usize = 128;

/// L2 span represented by one compacted historical checkpoint.
///
/// The earliest boundary in each bucket is retained. It is therefore safe to resume any target
/// in that bucket from the retained boundary, re-deriving at most roughly this many existing L2
/// blocks before reaching the target again.
pub const HISTORICAL_CHECKPOINT_L2_INTERVAL: u64 = 100_000;

/// A durable L1-derivation resume point, recorded at a batch/window boundary.
///
/// All three fields describe the SAME instant in the derivation: after consuming every batch up to
/// (and including) L1 block `l1_block - 1`, the delayed cursor is `delayed_count` and the L2 chain
/// has reached block `l2_block`. Resuming derivation from `l1_block` with `delayed_count` therefore
/// produces block `l2_block + 1` next.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct L1ResumeCheckpoint {
    /// Next L1 block to derive from (the consumed window's `to + 1`).
    pub l1_block: u64,
    /// Delayed-inbox cursor (`delayedMessagesRead`) at this boundary: the `before` count the next
    /// range's assembly starts from.
    pub delayed_count: u64,
    /// Absolute L2 block number reached at this boundary (durably persisted when written).
    pub l2_block: u64,
}

/// An ascending log of recent and compacted historical [`L1ResumeCheckpoint`]s, persisted as
/// `arb-l1-resume.json`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct L1ResumeLog {
    /// Boundaries in ascending `l2_block` order, with compacted history followed by a dense tail.
    pub checkpoints: Vec<L1ResumeCheckpoint>,
}

impl L1ResumeLog {
    /// Path of the resume log within `datadir`.
    pub fn path_in(datadir: &Path) -> PathBuf {
        datadir.join(RESUME_FILE_NAME)
    }

    /// Read the log from `path`, returning `None` if it is absent or unparseable (a torn or stale
    /// file is treated as "no checkpoint" rather than a fatal error). Also accepts a legacy file
    /// holding a bare [`L1ResumeCheckpoint`] object, wrapping it into a one-entry log.
    pub fn load(path: &Path) -> Option<Self> {
        let bytes = std::fs::read(path).ok()?;
        if let Ok(log) = serde_json::from_slice::<Self>(&bytes) {
            return Some(log);
        }
        serde_json::from_slice::<L1ResumeCheckpoint>(&bytes)
            .ok()
            .map(|cp| Self { checkpoints: vec![cp] })
    }

    /// Atomically persist to `path` (write a sibling temp file, then rename over `path`) so a crash
    /// mid-write can never leave a partially-written log.
    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        let tmp = path.with_extension("json.tmp");
        let bytes = serde_json::to_vec(self).expect("L1ResumeLog serializes");
        std::fs::write(&tmp, bytes)?;
        std::fs::rename(&tmp, path)
    }

    /// The newest boundary at or below `l2_block`, i.e. the furthest safe point to resume a chain
    /// whose durable tip is `l2_block`. `None` if every boundary is above it (rolled back further
    /// than the log reaches).
    pub fn resume_for(&self, l2_block: u64) -> Option<L1ResumeCheckpoint> {
        self.checkpoints.iter().rev().find(|cp| cp.l2_block <= l2_block).copied()
    }

    /// Append a boundary, keeping the log ascending and deduplicated by `l2_block`. Boundaries
    /// arrive in ascending order during sync; a repeat `l2_block` (an empty window that only
    /// advanced `l1_block`) replaces the prior entry so the newest `l1_block` wins.
    ///
    /// The recent tail remains dense. Older entries retain the earliest boundary in each fixed L2
    /// bucket so a deep rewind always has a safe starting point before its target.
    pub fn record(&mut self, cp: L1ResumeCheckpoint) {
        if self.checkpoints.last().is_some_and(|last| last.l2_block == cp.l2_block) {
            *self.checkpoints.last_mut().unwrap() = cp;
        } else {
            self.checkpoints.push(cp);
        }
        self.compact_history();
    }

    fn compact_history(&mut self) {
        let Some(recent_start) = self.checkpoints.len().checked_sub(RECENT_CHECKPOINTS) else {
            return;
        };
        if recent_start == 0 {
            return;
        }

        let mut compacted = Vec::with_capacity(self.checkpoints.len());
        let mut last_bucket = None;
        for checkpoint in self.checkpoints[..recent_start].iter().copied() {
            let bucket = checkpoint.l2_block / HISTORICAL_CHECKPOINT_L2_INTERVAL;
            if last_bucket != Some(bucket) {
                compacted.push(checkpoint);
                last_bucket = Some(bucket);
            }
        }
        compacted.extend_from_slice(&self.checkpoints[recent_start..]);
        self.checkpoints = compacted;
    }

    /// Drop every boundary above `l2_block` (used by rewind after unwinding the DB to a new tip).
    /// Returns the newest surviving boundary, or `None` if the rewind target predates the log.
    pub fn truncate_to(&mut self, l2_block: u64) -> Option<L1ResumeCheckpoint> {
        self.checkpoints.retain(|cp| cp.l2_block <= l2_block);
        self.checkpoints.last().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cp(l1: u64, l2: u64) -> L1ResumeCheckpoint {
        L1ResumeCheckpoint { l1_block: l1, delayed_count: 0, l2_block: l2 }
    }

    #[test]
    fn round_trips_through_disk() {
        let dir = reth_db::test_utils::tempdir_path();
        let path = L1ResumeLog::path_in(&dir);
        assert_eq!(L1ResumeLog::load(&path), None, "absent file loads as None");

        let mut log = L1ResumeLog::default();
        log.record(cp(100, 10));
        log.record(cp(200, 20));
        log.save(&path).expect("save");
        assert_eq!(L1ResumeLog::load(&path), Some(log));
    }

    #[test]
    fn resume_for_picks_newest_at_or_below() {
        let mut log = L1ResumeLog::default();
        log.record(cp(100, 10));
        log.record(cp(200, 20));
        log.record(cp(300, 30));
        assert_eq!(log.resume_for(25), Some(cp(200, 20)), "newest boundary <= tip");
        assert_eq!(log.resume_for(30), Some(cp(300, 30)), "exact match");
        assert_eq!(log.resume_for(5), None, "tip below every boundary");
    }

    #[test]
    fn record_dedupes_empty_windows() {
        let mut log = L1ResumeLog::default();
        log.record(cp(100, 10));
        // Empty windows: same l2, advancing l1, so the newest l1 must win, no duplicate l2 entry.
        log.record(cp(200, 10));
        log.record(cp(300, 10));
        assert_eq!(log.checkpoints, vec![cp(300, 10)]);
    }

    #[test]
    fn record_retains_sparse_history_and_dense_recent_tail() {
        let mut log = L1ResumeLog::default();
        for l2 in (0..1_000_000).step_by(1_000) {
            log.record(cp(l2 + 10, l2));
        }

        // Old buckets retain their earliest boundary instead of being discarded.
        assert_eq!(log.resume_for(150_050), Some(cp(100_010, 100_000)));
        assert_eq!(log.resume_for(550_050), Some(cp(500_010, 500_000)));

        // The newest 128 entries remain available at their original resolution.
        assert_eq!(log.resume_for(950_050), Some(cp(950_010, 950_000)));
        assert_eq!(log.checkpoints.last().copied(), Some(cp(999_010, 999_000)));

        // Ten historical buckets plus the dense tail is tiny compared with every input boundary.
        assert!(log.checkpoints.len() <= 10 + RECENT_CHECKPOINTS);
    }

    #[test]
    fn truncate_to_drops_boundaries_above_target() {
        let mut log = L1ResumeLog::default();
        log.record(cp(100, 10));
        log.record(cp(200, 20));
        log.record(cp(300, 30));
        assert_eq!(log.truncate_to(20), Some(cp(200, 20)), "newest surviving boundary");
        assert_eq!(log.checkpoints, vec![cp(100, 10), cp(200, 20)]);
        assert_eq!(log.truncate_to(5), None, "target predates the log");
        assert!(log.checkpoints.is_empty());
    }
}
