//! Replica replay parity monitor (experimental).
//!
//! `arb-reth node` can already replay a live Arbitrum chain: L1 derivation performs the
//! historical catch-up and `--feed-url` follows the sequencer feed at the tip. What that
//! leaves open operationally is *trust*: how do we know the replica's replayed chain is the
//! same chain the canonical (Nitro) node serves, block for block, root for root?
//!
//! This crate is that missing boundary. It treats both nodes as opaque JSON-RPC head
//! sources and drives a small state machine:
//!
//! ```text
//! Syncing ──(lag <= threshold)──> InSync ──(hash/root mismatch)──> Diverged
//!    ^                               │
//!    └───────(lag grows)─────────────┘
//! ```
//!
//! - **Syncing**: the replica is behind the canonical head by more than the configured
//!   threshold (initial import / L1 catch-up). Only progress is reported.
//! - **InSync**: the replica is at (or near) the tip. Every poll compares block hash and
//!   state root at a confirmation-depth-adjusted height.
//! - **Diverged**: a compared block mismatched. The monitor walks back to find the first
//!   divergent height so a rewind target is immediately known.
//!
//! The monitor is deliberately RPC-only. It has no reth or ArbOS dependencies, so it works
//! against any local/remote pair and cannot bias the comparison by sharing code with the
//! execution path it is validating.

use serde::Serialize;
use std::fmt;

pub mod rpc;

/// Identity of a block as far as parity is concerned.
///
/// Hash equality implies header equality (the state root, receipts root, etc. are all
/// committed in the hash), but we carry the state root separately so a divergence report
/// can say *which* of the two mismatched: a hash-only mismatch with equal roots points at
/// header encoding, while a root mismatch points at execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BlockIdentity {
    pub number: u64,
    /// 0x-prefixed block hash.
    pub hash: String,
    /// 0x-prefixed state root.
    pub state_root: String,
}

/// A chain head that can be polled over some transport (JSON-RPC in production, mocks in
/// tests).
pub trait HeadSource: Send + Sync {
    /// Latest block number.
    fn head_number(&self) -> impl Future<Output = eyre::Result<u64>> + Send;
    /// Block identity at `number`, or `None` if the source does not have it (pruned or
    /// not yet produced).
    fn block_identity(
        &self,
        number: u64,
    ) -> impl Future<Output = eyre::Result<Option<BlockIdentity>>> + Send;
}

/// Monitor tuning. Defaults are conservative; see field docs.
#[derive(Debug, Clone)]
pub struct MonitorConfig {
    /// Compare at `min(local_head, canonical_head) - confirm_depth` instead of the raw
    /// tip. The sequencer feed and the canonical RPC race each other at the tip; a couple
    /// of blocks of slack avoids flagging a transient "replica is ahead of the RPC"
    /// window as a mismatch.
    pub confirm_depth: u64,
    /// Lag (canonical head minus local head) above which the replica is considered
    /// `Syncing` rather than `InSync`, and comparison is skipped.
    pub sync_lag_threshold: u64,
    /// On mismatch, walk back at most this many blocks looking for the last matching
    /// ancestor. Divergence deeper than this is reported with `first_diverged: None`.
    pub max_walkback: u64,
}

impl Default for MonitorConfig {
    fn default() -> Self {
        Self {
            confirm_depth: 2,
            sync_lag_threshold: 8,
            max_walkback: 256,
        }
    }
}

/// Result of a single poll.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ReplicaStatus {
    /// Replica is catching up (initial import or L1 derivation); no comparison performed.
    Syncing {
        local_head: u64,
        canonical_head: u64,
        lag: u64,
    },
    /// Replica is at the tip and the checked block matched.
    InSync {
        checked: BlockIdentity,
        local_head: u64,
        canonical_head: u64,
        lag: u64,
    },
    /// The checked block mismatched.
    Diverged {
        /// Height where the mismatch was observed.
        checked_number: u64,
        local: BlockIdentity,
        canonical: BlockIdentity,
        /// First height at which the chains disagree, if found within `max_walkback`.
        /// `first_diverged - 1` is the rewind target.
        first_diverged: Option<u64>,
    },
    /// One side has no block at the comparison height (pruned history or an empty
    /// datadir). Not a divergence verdict.
    Unavailable {
        number: u64,
        local_missing: bool,
        canonical_missing: bool,
    },
}

impl fmt::Display for ReplicaStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Syncing {
                local_head,
                canonical_head,
                lag,
            } => {
                write!(
                    f,
                    "syncing local={local_head} canonical={canonical_head} lag={lag}"
                )
            }
            Self::InSync { checked, lag, .. } => {
                write!(
                    f,
                    "in-sync checked=#{} hash={} lag={lag}",
                    checked.number, checked.hash
                )
            }
            Self::Diverged {
                checked_number,
                first_diverged,
                ..
            } => {
                write!(
                    f,
                    "DIVERGED at #{checked_number} first_diverged={first_diverged:?}"
                )
            }
            Self::Unavailable {
                number,
                local_missing,
                canonical_missing,
            } => write!(
                f,
                "block #{number} unavailable local_missing={local_missing} canonical_missing={canonical_missing}"
            ),
        }
    }
}

impl ReplicaStatus {
    pub fn is_diverged(&self) -> bool {
        matches!(self, Self::Diverged { .. })
    }
}

/// Compares a local replica against a canonical upstream.
pub struct ParityMonitor<L, C> {
    local: L,
    canonical: C,
    cfg: MonitorConfig,
}

impl<L: HeadSource, C: HeadSource> ParityMonitor<L, C> {
    pub fn new(local: L, canonical: C, cfg: MonitorConfig) -> Self {
        Self {
            local,
            canonical,
            cfg,
        }
    }

    /// Run one comparison cycle. Errors are transport errors only; a semantic mismatch is
    /// a `Diverged` status, not an error.
    pub async fn poll_once(&self) -> eyre::Result<ReplicaStatus> {
        let (local_head, canonical_head) =
            tokio::try_join!(self.local.head_number(), self.canonical.head_number())?;
        let lag = canonical_head.saturating_sub(local_head);

        if lag > self.cfg.sync_lag_threshold {
            return Ok(ReplicaStatus::Syncing {
                local_head,
                canonical_head,
                lag,
            });
        }

        let check = local_head
            .min(canonical_head)
            .saturating_sub(self.cfg.confirm_depth);
        let (local, canonical) = tokio::try_join!(
            self.local.block_identity(check),
            self.canonical.block_identity(check)
        )?;
        let (local, canonical) = match (local, canonical) {
            (Some(l), Some(c)) => (l, c),
            (l, c) => {
                return Ok(ReplicaStatus::Unavailable {
                    number: check,
                    local_missing: l.is_none(),
                    canonical_missing: c.is_none(),
                });
            }
        };

        if local == canonical {
            return Ok(ReplicaStatus::InSync {
                checked: local,
                local_head,
                canonical_head,
                lag,
            });
        }

        let first_diverged = self.find_first_divergence(check).await?;
        Ok(ReplicaStatus::Diverged {
            checked_number: check,
            local,
            canonical,
            first_diverged,
        })
    }

    /// Walk back from `mismatch` (exclusive of block 0) looking for the last height where
    /// both sides agree. Linear scan bounded by `max_walkback`: divergence in a live
    /// replica is expected near the tip (feed reorg, ArbOS version skew on recent
    /// blocks), so a bounded linear walk is simpler than bisection and touches the same
    /// handful of blocks in the common case.
    async fn find_first_divergence(&self, mismatch: u64) -> eyre::Result<Option<u64>> {
        let floor = mismatch.saturating_sub(self.cfg.max_walkback);
        let mut first_bad = mismatch;
        while first_bad > floor && first_bad > 0 {
            let parent = first_bad - 1;
            let (l, c) = tokio::try_join!(
                self.local.block_identity(parent),
                self.canonical.block_identity(parent)
            )?;
            match (l, c) {
                (Some(l), Some(c)) if l == c => return Ok(Some(first_bad)),
                (Some(_), Some(_)) => first_bad = parent,
                // History gap on either side: cannot localize further.
                _ => return Ok(None),
            }
        }
        // Walked to the floor without finding agreement.
        Ok(if first_bad == 0 { Some(0) } else { None })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// Deterministic in-memory chain: block hash/root derived from (tag, number), with
    /// per-height overrides to simulate divergence.
    struct MockChain {
        tag: &'static str,
        head: u64,
        overrides: BTreeMap<u64, &'static str>,
        missing_below: u64,
    }

    impl MockChain {
        fn new(tag: &'static str, head: u64) -> Self {
            Self {
                tag,
                head,
                overrides: BTreeMap::new(),
                missing_below: 0,
            }
        }

        fn ident(&self, n: u64) -> BlockIdentity {
            let tag = self.overrides.get(&n).copied().unwrap_or(self.tag);
            BlockIdentity {
                number: n,
                hash: format!("0x{tag}-hash-{n}"),
                state_root: format!("0x{tag}-root-{n}"),
            }
        }
    }

    impl HeadSource for MockChain {
        async fn head_number(&self) -> eyre::Result<u64> {
            Ok(self.head)
        }

        async fn block_identity(&self, number: u64) -> eyre::Result<Option<BlockIdentity>> {
            if number > self.head || number < self.missing_below {
                return Ok(None);
            }
            Ok(Some(self.ident(number)))
        }
    }

    fn cfg() -> MonitorConfig {
        MonitorConfig {
            confirm_depth: 2,
            sync_lag_threshold: 8,
            max_walkback: 64,
        }
    }

    #[tokio::test]
    async fn reports_syncing_while_behind() {
        let monitor = ParityMonitor::new(MockChain::new("a", 100), MockChain::new("a", 500), cfg());
        let status = monitor.poll_once().await.unwrap();
        assert_eq!(
            status,
            ReplicaStatus::Syncing {
                local_head: 100,
                canonical_head: 500,
                lag: 400
            }
        );
    }

    #[tokio::test]
    async fn in_sync_when_identities_match() {
        let monitor = ParityMonitor::new(MockChain::new("a", 500), MockChain::new("a", 503), cfg());
        let status = monitor.poll_once().await.unwrap();
        // check height = min(500, 503) - confirm_depth = 498.
        match status {
            ReplicaStatus::InSync { checked, lag, .. } => {
                assert_eq!(checked.number, 498);
                assert_eq!(lag, 3);
            }
            other => panic!("expected InSync, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn divergence_is_localized_to_first_bad_block() {
        let mut local = MockChain::new("a", 500);
        // Local chain forked off at 490: blocks 490.. have different identities.
        for n in 490..=500 {
            local.overrides.insert(n, "fork");
        }
        let monitor = ParityMonitor::new(local, MockChain::new("a", 500), cfg());
        let status = monitor.poll_once().await.unwrap();
        match status {
            ReplicaStatus::Diverged {
                checked_number,
                first_diverged,
                local,
                canonical,
            } => {
                assert_eq!(checked_number, 498);
                assert_eq!(first_diverged, Some(490));
                assert_ne!(local, canonical);
            }
            other => panic!("expected Diverged, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn divergence_deeper_than_walkback_is_unlocalized() {
        let mut local = MockChain::new("a", 500);
        for n in 0..=500 {
            local.overrides.insert(n, "fork");
        }
        let monitor = ParityMonitor::new(local, MockChain::new("a", 500), cfg());
        let status = monitor.poll_once().await.unwrap();
        match status {
            ReplicaStatus::Diverged { first_diverged, .. } => assert_eq!(first_diverged, None),
            other => panic!("expected Diverged, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn pruned_history_is_unavailable_not_diverged() {
        let mut local = MockChain::new("a", 500);
        local.missing_below = 499;
        let monitor = ParityMonitor::new(local, MockChain::new("a", 500), cfg());
        let status = monitor.poll_once().await.unwrap();
        assert_eq!(
            status,
            ReplicaStatus::Unavailable {
                number: 498,
                local_missing: true,
                canonical_missing: false
            }
        );
    }

    #[tokio::test]
    async fn replica_ahead_of_canonical_still_compares() {
        // Feed can put the replica ahead of a lagging canonical RPC; compare at the
        // canonical head minus depth rather than declaring lag.
        let monitor = ParityMonitor::new(MockChain::new("a", 505), MockChain::new("a", 500), cfg());
        let status = monitor.poll_once().await.unwrap();
        match status {
            ReplicaStatus::InSync { checked, lag, .. } => {
                assert_eq!(checked.number, 498);
                assert_eq!(lag, 0);
            }
            other => panic!("expected InSync, got {other:?}"),
        }
    }
}
