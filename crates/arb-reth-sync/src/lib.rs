//! `arb-reth-sync`: the L1-derivation catch-up runtime and its resumable checkpoint log.
//!
//! Split out of `arb-reth-node`. `l1_sync` walks L1 windows through `arb-reth-l1`'s readers into a
//! feed-message channel; `resume` persists an ascending log of `arb-l1-resume.json` checkpoints so a
//! restart continues from the last durably-persisted L2 tip. The node depends on this crate and
//! spawns `run_l1_sync` as a feed producer; the fetch/derive primitives live in `arb-reth-l1`.

pub mod l1_sync;
pub mod resume;

pub use l1_sync::{run_l1_sync, L1SyncConfig};
pub use resume::{L1ResumeCheckpoint, L1ResumeLog};
