//! `arb-reth` — runnable entrypoint for the no-engine Arbitrum node (D.3b.4).
//!
//! This wires the CLI to the [`ArbLauncher`] custom `LaunchNode` (D.3b): it opens
//! an on-disk MDBX database under the data directory, boots reth's `LaunchContext`
//! provider/blockchain-db stack (NO engine — see `launcher.rs`), spawns the
//! `ArbChainDriver` block producer, and optionally serves the `eth_*` JSON-RPC API.
//!
//! ## What this is (and isn't) yet
//!
//! The sequencer-feed channel is created but left empty by default. Stage F
//! (L1-inbox derivation / live feed following) is what fills it for production use.
//!
//! With `--replay-feed <NDJSON>` the binary reads a file of
//! `BroadcastFeedMessage` JSON objects (one per line) and pushes them all into the
//! feed channel immediately after launch, then **keeps the feed channel open** so the
//! driver can drain it while RPC queries remain servable. The held sender keeps the
//! node alive until SIGTERM. This lets a user run a finite replay and then inspect
//! the produced blocks via JSON-RPC.
//!
//! With `--chain <PATH>` an Arbitrum chain-config JSON is parsed to produce a real
//! ArbOS genesis allocation instead of the MAINNET placeholder.

#![allow(missing_docs)]

use std::{fs, net::IpAddr, path::PathBuf};

use arb_reth_node::{
    arb_chain_spec, arbos_init_from_chain_config_json, ArbNode, ARB_ONE_CHAIN_ID,
};
use arb_reth_node::launcher::ArbLauncher;
use arb_sequencer_network::sequencer::feed::BroadcastFeedMessage;
use clap::Parser;
use reth_chainspec::MAINNET;
use reth_cli_runner::{CliContext, CliRunner};
use reth_db::{init_db, mdbx::DatabaseArguments, ClientVersion};
use reth_node_builder::{LaunchContext, LaunchNode, NodeBuilder, NodeConfig};
use reth_node_core::{
    args::DatadirArgs,
    dirs::{DataDirPath, MaybePlatformPath},
};
use reth_tracing::tracing::info;
use reth_tracing::{RethTracer, Tracer};

/// Stack-probe shim for x86_64. wasmer's vm crate (pulled in transitively via
/// arb_revm's Stylus support) references the LLVM `__rust_probestack` intrinsic
/// that recent `compiler-builtins` no longer exports; defining an empty function
/// here satisfies the linker. No-op on aarch64.
///
/// # Safety
///
/// Defined for the linker only; never called from Rust.
#[cfg(target_arch = "x86_64")]
#[no_mangle]
pub unsafe extern "C" fn __rust_probestack() {}

/// `arb-reth` — standalone no-engine Arbitrum (ArbOS-on-reth) node.
#[derive(Debug, Parser)]
#[command(name = "arb-reth", about = "Standalone no-engine Arbitrum (ArbOS-on-reth) node")]
struct Args {
    /// Data directory for the node's database and static files.
    /// Defaults to the platform-specific reth data directory for this chain.
    #[arg(long, value_name = "PATH")]
    datadir: Option<PathBuf>,

    /// Enable the `eth_*` JSON-RPC HTTP server.
    #[arg(long = "http")]
    http: bool,

    /// HTTP-RPC server bind address.
    #[arg(long = "http.addr", default_value = "127.0.0.1")]
    http_addr: IpAddr,

    /// HTTP-RPC server port.
    #[arg(long = "http.port", default_value_t = 8545)]
    http_port: u16,

    /// Flush produced blocks to disk every N blocks (1 = every block).
    #[arg(long, default_value_t = 1)]
    persistence_threshold: u64,

    /// Arbitrum execution chain id used by the block driver.
    #[arg(long, default_value_t = ARB_ONE_CHAIN_ID)]
    chain_id: u64,

    /// Path to an Arbitrum chain-config JSON file (the `ChainConfig` Go format:
    /// `{"chainId":..., "arbitrum":{...}}`). When provided, the node boots with a
    /// real ArbOS genesis allocation instead of the mainnet placeholder.
    #[arg(long = "chain", value_name = "PATH")]
    chain_config: Option<PathBuf>,

    /// Path to an NDJSON replay-feed file (one `BroadcastFeedMessage` JSON per line).
    /// After launch all messages are pushed into the feed channel so the block driver
    /// processes them, then the node stays alive for RPC inspection.
    ///
    /// Sender lifecycle: after pushing all messages the original sender is kept alive
    /// (not dropped) so the driver does NOT exit — the node serves RPC until SIGTERM.
    /// This lets you replay a finite file and then query the produced blocks.
    #[arg(long = "replay-feed", value_name = "PATH")]
    replay_feed: Option<PathBuf>,

    /// ArbOS format version to tag replay-feed messages with (default 0).
    #[arg(long = "replay-version", default_value_t = 0)]
    replay_version: u8,
}

fn main() -> eyre::Result<()> {
    // Idiomatic reth tracing; guard is held for the process lifetime.
    let _guard = RethTracer::new().init()?;

    let args = Args::parse();

    // reth's runtime + ctrl-c harness: runs `run(..)` until it resolves or the
    // process receives SIGINT/SIGTERM, then drives spawned tasks to shutdown.
    let runner = CliRunner::try_default_runtime()?;
    runner.run_command_until_exit(|ctx| run(ctx, args))
}

async fn run(ctx: CliContext, args: Args) -> eyre::Result<()> {
    let task_executor = ctx.task_executor;

    // Chain spec — either a real ArbOS genesis (from --chain) or the mainnet
    // placeholder. The block driver uses the chain id for execution; the chain spec's
    // primary role here is the genesis allocation and fork schedule.
    //
    // When --chain is provided the chain id is derived from the chain config JSON so
    // that eth_chainId and the driver both see the same value. The --chain-id CLI arg
    // is used only when --chain is not given.
    let (chain_spec, effective_chain_id) = match &args.chain_config {
        Some(path) => {
            let json = fs::read(path)
                .map_err(|e| eyre::eyre!("failed to read chain config file {:?}: {}", path, e))?;
            let init = arbos_init_from_chain_config_json(&json)?;
            let derived_chain_id = init.chain_id.to::<u64>();
            info!(
                target: "arb-reth",
                chain_id = derived_chain_id,
                arbos_version = init.initial_arbos_version,
                "loaded ArbOS genesis from chain config"
            );
            let spec = std::sync::Arc::new(arb_chain_spec(&init)?);
            (spec, derived_chain_id)
        }
        None => (MAINNET.clone(), args.chain_id),
    };

    // Resolve the data directory and node config.
    let datadir_args = match args.datadir {
        Some(path) => {
            DatadirArgs { datadir: MaybePlatformPath::<DataDirPath>::from(path), ..Default::default() }
        }
        None => DatadirArgs::default(),
    };
    let config = NodeConfig::new(chain_spec).with_datadir_args(datadir_args);
    let data_dir = config.datadir();

    // Open the on-disk MDBX database (same recipe as reth's `node` command).
    let db_path = data_dir.db();
    info!(target: "arb-reth", path = ?db_path, "opening database");
    let db = init_db(db_path, DatabaseArguments::new(ClientVersion::default()))?;

    // Build the `NodeBuilderWithComponents` our custom launcher consumes.
    let node_builder = NodeBuilder::new(config).with_database(db).node(ArbNode);

    // Sequencer-feed channel. The held sender keeps the driver parked (and the
    // node alive) until SIGTERM. When --replay-feed is set we push all messages
    // via a spawned task and then keep the sender alive for RPC inspection.
    let (feed_tx, feed_rx) = tokio::sync::mpsc::channel::<(BroadcastFeedMessage, u8)>(4096);

    let rpc_addr = args.http.then(|| (args.http_addr, args.http_port).into());

    let launcher = ArbLauncher {
        ctx: LaunchContext::new(task_executor.clone(), data_dir),
        chain_id: effective_chain_id,
        persistence_threshold: args.persistence_threshold,
        messages: feed_rx,
        rpc_addr,
    };

    let handle = launcher.launch_node(node_builder).await?;

    match handle.http_url() {
        Some(url) => info!(target: "arb-reth", %url, "arb-reth node started; eth_* RPC serving"),
        None => info!(target: "arb-reth", "arb-reth node started (RPC disabled; pass --http to enable)"),
    }

    // Replay-feed: push all messages from the NDJSON file via a spawned task,
    // then keep the sender alive so the driver does not exit after draining.
    //
    // Sender lifecycle choice: we KEEP the feed_tx alive (held by the async block
    // below) so the node stays up for RPC queries after all messages are pushed.
    // Users can then `curl` the produced blocks and shut down with SIGTERM.
    if let Some(feed_path) = args.replay_feed {
        let tx = feed_tx.clone();
        let version = args.replay_version;
        task_executor.spawn_task(async move {
            let content = match fs::read_to_string(&feed_path) {
                Ok(c) => c,
                Err(e) => {
                    reth_tracing::tracing::error!(
                        target: "arb-reth",
                        path = ?feed_path,
                        err = %e,
                        "failed to read replay-feed file"
                    );
                    return;
                }
            };

            let mut pushed = 0usize;
            for line in content.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                match serde_json::from_str::<BroadcastFeedMessage>(line) {
                    Ok(msg) => {
                        if tx.send((msg, version)).await.is_err() {
                            reth_tracing::tracing::warn!(
                                target: "arb-reth",
                                "feed channel closed before replay finished"
                            );
                            break;
                        }
                        pushed += 1;
                    }
                    Err(e) => {
                        reth_tracing::tracing::warn!(
                            target: "arb-reth",
                            err = %e,
                            "skipping malformed replay-feed line"
                        );
                    }
                }
            }
            info!(target: "arb-reth", pushed, "replay-feed push complete; node remains up for RPC");
            // `tx` (clone) is dropped here; the original feed_tx below keeps the channel open.
        });
    }

    // Keep the feed sender alive across the await so the driver task parks on the
    // channel instead of flushing and exiting immediately. Stage F will hand this
    // sender to the derivation pipeline.
    let _feed_tx = feed_tx;

    // Block until the driver task exits (it won't, while the sender is held) or
    // until `run_command_until_exit` cancels us on SIGINT/SIGTERM.
    handle.wait_for_node_exit().await
}
