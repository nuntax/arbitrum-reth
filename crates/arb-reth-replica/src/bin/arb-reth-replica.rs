//! Replica parity monitor CLI (experimental).
//!
//! Pair this with an `arb-reth node` that replays a live chain:
//!
//! ```sh
//! arb-reth node --datadir /data/orbit \
//!   --chain-info chaininfo.json --genesis genesis.json \
//!   --l1-rpc $L1_RPC --feed-url $SEQUENCER_FEED \
//!   --http --http.port 8545 &
//!
//! arb-reth-replica \
//!   --local-rpc http://127.0.0.1:8545 \
//!   --canonical-rpc https://<canonical-nitro-endpoint> \
//!   --status-file /data/replica-status.json
//! ```
//!
//! The monitor polls both heads, reports sync progress while the replica catches up, and
//! once at the tip verifies block hash + state root parity every interval. On divergence
//! it localizes the first bad height (a rewind target for `arb-reth rewind`) and, with
//! `--exit-on-divergence`, exits non-zero so a supervisor can act.

use arb_reth_replica::{MonitorConfig, ParityMonitor, ReplicaStatus, rpc::RpcHeadSource};
use clap::Parser;
use std::path::PathBuf;
use tracing::{error, info, warn};

#[derive(Parser, Debug)]
#[command(
    name = "arb-reth-replica",
    about = "Parity monitor for an arb-reth replica"
)]
struct Args {
    /// HTTP JSON-RPC of the local arb-reth replica.
    #[arg(long)]
    local_rpc: String,
    /// HTTP JSON-RPC of the canonical upstream (e.g. the hosted Nitro endpoint).
    #[arg(long)]
    canonical_rpc: String,
    /// Seconds between polls.
    #[arg(long, default_value_t = 5)]
    poll_interval: u64,
    /// Compare this many blocks behind the tip to avoid transient tip races.
    #[arg(long, default_value_t = 2)]
    confirm_depth: u64,
    /// Lag above which the replica is reported as syncing and comparison is skipped.
    #[arg(long, default_value_t = 8)]
    sync_lag_threshold: u64,
    /// Maximum blocks to walk back when localizing a divergence.
    #[arg(long, default_value_t = 256)]
    max_walkback: u64,
    /// Write the latest status as JSON to this file after every poll.
    #[arg(long)]
    status_file: Option<PathBuf>,
    /// Exit with a non-zero status as soon as a divergence is confirmed.
    #[arg(long, default_value_t = false)]
    exit_on_divergence: bool,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    let monitor = ParityMonitor::new(
        RpcHeadSource::new(&args.local_rpc, "local")?,
        RpcHeadSource::new(&args.canonical_rpc, "canonical")?,
        MonitorConfig {
            confirm_depth: args.confirm_depth,
            sync_lag_threshold: args.sync_lag_threshold,
            max_walkback: args.max_walkback,
        },
    );

    let mut interval = tokio::time::interval(std::time::Duration::from_secs(args.poll_interval));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        interval.tick().await;
        let status = match monitor.poll_once().await {
            Ok(status) => status,
            Err(err) => {
                // Transport-level failure (endpoint down, etc). Not a parity verdict;
                // keep polling.
                warn!(%err, "poll failed");
                continue;
            }
        };

        match &status {
            ReplicaStatus::Diverged { .. } => error!(%status, "parity check failed"),
            _ => info!(%status, "parity check"),
        }

        if let Some(path) = &args.status_file
            && let Err(err) = std::fs::write(path, serde_json::to_vec_pretty(&status)?)
        {
            warn!(%err, path = %path.display(), "failed to write status file");
        }

        if status.is_diverged() && args.exit_on_divergence {
            eyre::bail!("replica diverged from canonical chain: {status}");
        }
    }
}
