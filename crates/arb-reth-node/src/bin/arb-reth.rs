//! `arb-reth`: runnable entrypoint for the Arbitrum engine-tree node.
//!
//! This wires the CLI to the [`ArbLauncher`] custom `LaunchNode`: it opens an on-disk MDBX
//! database under the data directory, boots reth's `LaunchContext` provider/blockchain-db
//! stack, spawns the `ArbEngineDriver` block producer (which drives reth's engine tree; see
//! `launcher.rs`), and optionally serves the `eth_*` JSON-RPC API.
//!
//! ## Feed sources
//!
//! The sequencer-feed channel is created but left empty by default. L1-inbox derivation
//! (`--l1-rpc`, below) or `--replay-feed` fills it.
//!
//! With `--replay-feed <NDJSON>` the binary reads a file of
//! `BroadcastFeedMessage` JSON objects (one per line) and pushes them all into the
//! feed channel immediately after launch, then keeps the feed channel open so the
//! driver can drain it while RPC queries remain servable. The held sender keeps the
//! node alive until SIGTERM. This lets a user run a finite replay and then inspect
//! the produced blocks via JSON-RPC.
//!
//! With `--chain <PATH>` an Arbitrum chain-config JSON is parsed to produce a real
//! ArbOS genesis allocation instead of the MAINNET placeholder.

#![allow(missing_docs)]

use std::{fs, net::IpAddr, path::PathBuf};

use alloy_primitives::Address;
use alloy_provider::{Provider, ProviderBuilder};
use arb_reth_l1::{DelayedInboxReader, SequencerInboxReader};
use arb_reth_node::{
    arb_chain_spec, arbos_init_from_chain_config_json, arbos_init_from_parsed, ArbNode,
    ARB_ONE_CHAIN_ID, L1ResumeLog,
};
use arb_sequencer_network::init_message::parse_init_message_from_body;
use arb_reth_node::launcher::ArbLauncher;
use reth_provider::BlockNumReader;
use arb_sequencer_network::sequencer::feed::BroadcastFeedMessage;
use clap::Parser;
use reth_chainspec::MAINNET;
use reth_cli_runner::{CliContext, CliRunner};
use reth_db::{init_db, mdbx::DatabaseArguments, mdbx::SyncMode, ClientVersion};
use reth_node_builder::{LaunchContext, LaunchNode, NodeBuilder, NodeConfig};
use reth_node_core::{
    args::DatadirArgs,
    dirs::{DataDirPath, MaybePlatformPath},
};
use reth_tracing::tracing::info;
use reth_tracing::{RethTracer, Tracer};

/// Stack-probe shim for x86_64: wasmer references `__rust_probestack` which recent
/// `compiler-builtins` no longer exports; this satisfies the linker. No-op on aarch64.
///
/// # Safety
///
/// Defined for the linker only; never called from Rust.
#[cfg(target_arch = "x86_64")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn __rust_probestack() {}

/// `arb-reth`: standalone no-engine Arbitrum (ArbOS-on-reth) node.
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

    /// Engine-tree persistence threshold: persist once the canonical tip is this many blocks
    /// ahead of the last persisted block (larger = bigger, less frequent commit batches).
    #[arg(long, default_value_t = 2)]
    persistence_threshold: u64,

    /// Engine-tree memory buffer target: keep this many recent blocks in memory before flushing.
    #[arg(long = "memory-buffer-target", default_value_t = 0)]
    memory_buffer_target: u64,

    /// Engine-tree backpressure threshold: stall block production once this many blocks are
    /// unpersisted (bounds memory; larger = production runs further ahead of the disk).
    #[arg(long = "persistence-backpressure", default_value_t = 16)]
    persistence_backpressure: u64,

    /// Use the DEPRECATED legacy parent-state read path (`state_by_block_hash(parent)`) instead of
    /// the ring overlay. The ring overlay is ON by default: it reads parent state from a driver-held
    /// ring of just-executed blocks over the immune latest provider, eliminating the torn-read hazard
    /// so deep buffers stay parity-safe. Only pass this to A/B or debug the legacy path.
    #[arg(long = "no-ring-overlay", default_value_t = false)]
    no_ring_overlay: bool,

    /// DEPRECATED no-op: the ring overlay is now the default. Accepted for backward compatibility
    /// (older invocations pass `--ring-overlay`); it has no effect. Use `--no-ring-overlay` to opt out.
    #[arg(long = "ring-overlay", default_value_t = false, hide = true)]
    ring_overlay_compat: bool,

    /// Open MDBX in `SafeNoSync` durability mode: skip the per-commit fsync during bulk
    /// historical sync. Each block still commits to MDBX (so the parent state is visible to the
    /// child), but the OS flushes lazily, cutting ~50ms fsync latency off every block. Stays
    /// crash-consistent (MDBX rolls back to the last synced meta page on restart); the only loss
    /// on a crash is a suffix of recently-produced blocks, which the L1 derivation re-produces.
    /// Not for a node expected to be durable across power loss without re-sync.
    #[arg(long = "no-fsync", default_value_t = false)]
    no_fsync: bool,

    /// Arbitrum execution chain id used by the block driver.
    #[arg(long, default_value_t = ARB_ONE_CHAIN_ID)]
    chain_id: u64,

    /// Path to an Arbitrum chain-config JSON file (the `ChainConfig` Go format:
    /// `{"chainId":..., "arbitrum":{...}}`). When provided, the node boots with a
    /// real ArbOS genesis allocation instead of the mainnet placeholder.
    #[arg(long = "chain", value_name = "PATH")]
    chain_config: Option<PathBuf>,

    /// Initial L1 base fee (wei) baked into the ArbOS genesis when booting from --chain.
    /// Defaults to Nitro's `DefaultInitialL1BaseFee` of 50 GWei. This value is part of the
    /// genesis state, so a chain created with a different initial base fee (a nitro-testnode
    /// commonly uses a tiny value) needs this set to reproduce its genesis root.
    #[arg(long = "initial-l1-base-fee", value_name = "WEI")]
    initial_l1_base_fee: Option<u128>,

    /// Path to an NDJSON replay-feed file (one `BroadcastFeedMessage` JSON per line).
    /// After launch all messages are pushed into the feed channel so the block driver
    /// processes them, then the node stays alive for RPC inspection.
    ///
    /// Sender lifecycle: after pushing all messages the original sender is kept alive
    /// (not dropped) so the driver does not exit; the node serves RPC until SIGTERM.
    /// This lets you replay a finite file and then query the produced blocks.
    #[arg(long = "replay-feed", value_name = "PATH")]
    replay_feed: Option<PathBuf>,


    /// L1 execution-layer RPC endpoint. When set, the node runs trustless L1-derivation
    /// catch-up: it reads SequencerInbox batches + the delayed inbox and feeds the
    /// derived messages to the block driver. Requires an archive endpoint (historical
    /// `getLogs`).
    #[arg(long = "l1-rpc", value_name = "URL")]
    l1_rpc: Option<String>,

    /// L1 beacon (consensus-layer) REST endpoint for blob sidecars. Required to derive
    /// post-Dencun blob batches; calldata-era ranges work without it.
    #[arg(long = "l1-beacon", value_name = "URL")]
    l1_beacon: Option<String>,

    /// First L1 block to derive from. Optional override: normally the resume point comes from the
    /// persisted `arb-l1-resume.json` checkpoint (updated as the node syncs), or, on the first sync
    /// of a genesis snapshot, from the chain (batch 0's delivery block). Pass this only to force a
    /// start block: it must be the batch boundary the current L2 tip was built from.
    #[arg(long = "l1-start-block")]
    l1_start_block: Option<u64>,

    /// Last L1 block to derive (inclusive). Omit to follow the L1 head indefinitely.
    #[arg(long = "l1-end-block")]
    l1_end_block: Option<u64>,

    /// Concurrent L1 `resolve_batches` prefetch depth during catch-up (overlaps getLogs/blob
    /// RPC latency). Higher = faster catch-up until the L1 provider rate-limits. 1 = serial.
    #[arg(long = "l1-prefetch", default_value_t = 6)]
    l1_prefetch: u64,

    /// Delayed cursor before the start block. Optional override: defaults to the snapshot tip
    /// header's nonce (the L2 tip's `delayedMessagesRead`), so it need not be supplied.
    #[arg(long = "l1-start-delayed")]
    l1_start_delayed: Option<u64>,

    /// `SequencerInbox` contract address on L1. This and --l1-bridge are one rollup deployment:
    /// set both to target a custom chain (a nitro-testnode or an Orbit chain), or neither to use
    /// the built-in Arbitrum One deployment. Setting only one is an error.
    #[arg(long = "l1-sequencer-inbox", value_name = "ADDR")]
    l1_sequencer_inbox: Option<Address>,

    /// `Bridge` contract address on L1 (delayed-inbox metadata source). Paired with
    /// --l1-sequencer-inbox; see its help for the set-together rule.
    #[arg(long = "l1-bridge", value_name = "ADDR")]
    l1_bridge: Option<Address>,

    /// L1 block the rollup was deployed at, used as the anchor for reading batch 0 and the
    /// Initialize message (Nitro's `DeployedAt`). Defaults to the Arbitrum One deploy height when
    /// targeting Arbitrum One, or block 0 for a custom deployment.
    #[arg(long = "l1-inbox-deploy-block")]
    l1_inbox_deploy_block: Option<u64>,

    /// L2 block the chain's genesis sits at, the L2-numbering anchor on a no-checkpoint genesis
    /// sync. Defaults to the Arbitrum One Nitro genesis (22207817) when targeting Arbitrum One, or
    /// block 0 for a custom deployment (a fresh chain).
    #[arg(long = "l2-genesis-block")]
    l2_genesis_block: Option<u64>,

    /// Boot on a snapshot-imported datadir: path to the `reth-export --mode blocks` head stream
    /// (`H <num> <hash> <headerRLP>`). The node builds its chain spec from that head header so the
    /// genesis-hash check accepts the imported DB, and resumes from the snapshot's head block.
    /// Use with `--datadir <imported-dir>` (do not pass `--chain`).
    #[arg(long = "snapshot-head", value_name = "PATH")]
    snapshot_head: Option<PathBuf>,
}

fn main() -> eyre::Result<()> {
    // Idiomatic reth tracing; guard is held for the process lifetime.
    let _guard = RethTracer::new().init()?;

    let args = Args::parse();

    let runner = CliRunner::try_default_runtime()?;
    runner.run_command_until_exit(|ctx| run(ctx, args))
}

/// The L1 rollup deployment arb-reth reads from: the contract addresses plus the L1 block the
/// rollup was deployed at, resolved as one coherent set the way Nitro resolves its
/// `RollupAddresses` from chain info (`chaininfo.GetRollupAddressesConfig`). The addresses always
/// travel together; you do not mix one chain's inbox with another's bridge.
struct RollupDeployment {
    sequencer_inbox: Address,
    bridge: Address,
    /// L1 block the rollup was deployed at; the anchor for reading batch 0 and the Initialize
    /// message. Nitro's `RollupAddresses.DeployedAt`.
    deployed_at: u64,
    /// L2 block the chain's genesis sits at: 0 for a fresh chain, the Nitro-migration block for
    /// Arbitrum One. Nitro's `ArbitrumChainParams.GenesisBlockNum`.
    l2_genesis_block: u64,
}

/// Resolve the rollup deployment from the CLI, with Nitro-like set/unset semantics:
///
/// - Neither `--l1-sequencer-inbox` nor `--l1-bridge` set: the built-in Arbitrum One deployment,
///   like Nitro resolving chain-id 42161 from its embedded chain info. `deployed_at` and the L2
///   genesis default to Arbitrum One's heights.
/// - Both set: a custom rollup. Since the addresses are one deployment, `deployed_at` and the L2
///   genesis default to a fresh chain (block 0), not Arbitrum One's heights. Either can still be
///   overridden explicitly.
/// - Exactly one set: rejected, rather than pairing a custom address with an Arbitrum One one.
fn resolve_rollup_deployment(args: &Args) -> eyre::Result<RollupDeployment> {
    match (args.l1_sequencer_inbox, args.l1_bridge) {
        (None, None) => Ok(RollupDeployment {
            sequencer_inbox: arb_reth_l1::SEQUENCER_INBOX_MAINNET,
            bridge: arb_reth_l1::BRIDGE_MAINNET,
            deployed_at: args
                .l1_inbox_deploy_block
                .unwrap_or(arb_reth_l1::SEQUENCER_INBOX_DEPLOY_BLOCK_MAINNET),
            l2_genesis_block: args.l2_genesis_block.unwrap_or(arb_reth_l1::NITRO_GENESIS_BLOCK_MAINNET),
        }),
        (Some(sequencer_inbox), Some(bridge)) => Ok(RollupDeployment {
            sequencer_inbox,
            bridge,
            deployed_at: args.l1_inbox_deploy_block.unwrap_or(0),
            l2_genesis_block: args.l2_genesis_block.unwrap_or(0),
        }),
        _ => Err(eyre::eyre!(
            "--l1-sequencer-inbox and --l1-bridge are one rollup deployment and must be set \
             together: set both for a custom chain, or neither for Arbitrum One"
        )),
    }
}

/// Build the genesis chain spec from the chain's Initialize message on L1, the way Nitro
/// bootstraps a fresh chain. The Initialize message is delayed-inbox message 0; it carries the
/// chain id, the serialized chain config, and the initial L1 base fee (version 1), so none of
/// those need to be supplied by hand. Used for fresh chains (a nitro-testnode or a new Orbit
/// chain) that start at L2 block 0; Arbitrum One instead boots from a snapshot, because its
/// genesis is the classic-state migration block, not an Initialize message.
async fn derive_genesis_from_l1(
    l1_rpc: &str,
    bridge: Address,
    from_block: u64,
    base_fee_override: Option<u128>,
) -> eyre::Result<(std::sync::Arc<reth_chainspec::ChainSpec>, u64)> {
    let provider = ProviderBuilder::new()
        .connect_http(l1_rpc.parse().map_err(|e| eyre::eyre!("invalid --l1-rpc URL: {e}"))?);
    let head = provider
        .get_block_number()
        .await
        .map_err(|e| eyre::eyre!("l1 get_block_number: {e}"))?;
    let reader = DelayedInboxReader::new(provider, bridge);
    let msgs = reader
        .fetch_delayed(from_block, head)
        .await
        .map_err(|e| eyre::eyre!("fetch delayed messages for L1 genesis: {e}"))?;
    let init = msgs
        .iter()
        .find(|m| m.inbox_seq_num == 0)
        .ok_or_else(|| eyre::eyre!("no delayed message 0 (Initialize) in L1 blocks {from_block}..={head}"))?;
    let parsed = parse_init_message_from_body(init.kind, &init.data)
        .map_err(|e| eyre::eyre!("parse Initialize message: {e}"))?;
    let mut arbos_init = arbos_init_from_parsed(&parsed)?;
    // The Initialize message carries the base fee; an explicit flag still wins if passed.
    if let Some(fee) = base_fee_override {
        arbos_init.initial_l1_base_fee = alloy_primitives::U256::from(fee);
    }
    let chain_id = arbos_init.chain_id.to::<u64>();
    let spec = std::sync::Arc::new(arb_chain_spec(&arbos_init)?);
    Ok((spec, chain_id))
}

async fn run(ctx: CliContext, args: Args) -> eyre::Result<()> {
    let task_executor = ctx.task_executor;

    // Resolve the rollup addresses + deploy/genesis anchors as one set up front, so a
    // half-specified custom deployment fails fast rather than mid-boot.
    let rollup = resolve_rollup_deployment(&args)?;

    // --snapshot-head: boot on an imported snapshot DB by building the chain spec from its head
    // header (so reth's genesis-hash check accepts the DB). Takes precedence over --chain.
    // When --chain is provided the chain id is derived from the JSON so eth_chainId and the
    // driver agree. When not provided, the mainnet placeholder is used with --chain-id.
    // `snapshot_delayed` carries the L2 tip's `delayedMessagesRead` (the header nonce) so the
    // L1-sync delayed cursor defaults to it without a manual flag.
    let mut snapshot_delayed: Option<u64> = None;
    let (chain_spec, effective_chain_id) = match (&args.snapshot_head, &args.chain_config) {
        (Some(head_path), _) => {
            let (num, hash, header) = arb_reth_node::read_head_header(head_path)?;
            snapshot_delayed = Some(u64::from_be_bytes(header.nonce.0));
            info!(
                target: "arb-reth",
                head_block = num, %hash, chain_id = args.chain_id,
                delayed_messages_read = snapshot_delayed.unwrap(),
                "booting on snapshot head header",
            );
            (arb_reth_node::arb_chain_spec_with_header(args.chain_id, header, hash), args.chain_id)
        }
        (None, Some(path)) => {
            let json = fs::read(path)
                .map_err(|e| eyre::eyre!("failed to read chain config file {:?}: {}", path, e))?;
            let mut init = arbos_init_from_chain_config_json(&json)?;
            if let Some(fee) = args.initial_l1_base_fee {
                init.initial_l1_base_fee = alloy_primitives::U256::from(fee);
            }
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
        (None, None) => match &args.l1_rpc {
            // No genesis file given but an L1 is: bootstrap genesis from the chain's Initialize
            // message on that L1 (chain id + config + base fee all come from it). This is the
            // zero-config path for a fresh chain like a nitro-testnode.
            Some(l1_rpc) => {
                let (spec, cid) = derive_genesis_from_l1(
                    l1_rpc,
                    rollup.bridge,
                    rollup.deployed_at,
                    args.initial_l1_base_fee,
                )
                .await?;
                info!(
                    target: "arb-reth",
                    chain_id = cid,
                    "bootstrapped ArbOS genesis from the L1 Initialize message",
                );
                (spec, cid)
            }
            None => (MAINNET.clone(), args.chain_id),
        },
    };

    let datadir_args = match args.datadir {
        Some(path) => {
            DatadirArgs { datadir: MaybePlatformPath::<DataDirPath>::from(path), ..Default::default() }
        }
        None => DatadirArgs::default(),
    };
    let config = NodeConfig::new(chain_spec).with_datadir_args(datadir_args);
    let data_dir = config.datadir();

    // Resolve the L1-derivation resume log path before `data_dir` is moved into the launcher.
    let resume_checkpoint_path = L1ResumeLog::path_in(data_dir.data_dir());

    let db_path = data_dir.db();
    info!(target: "arb-reth", path = ?db_path, no_fsync = args.no_fsync, "opening database");
    let mut db_args = DatabaseArguments::new(ClientVersion::default());
    if args.no_fsync {
        db_args = db_args.with_sync_mode(Some(SyncMode::SafeNoSync));
    }
    let db = init_db(db_path, db_args)?;

    let node_builder = NodeBuilder::new(config).with_database(db).node(ArbNode);

    // The held sender keeps the driver parked (and the node alive) until SIGTERM.
    let (feed_tx, feed_rx) = tokio::sync::mpsc::channel::<BroadcastFeedMessage>(4096);

    let rpc_addr = args.http.then(|| (args.http_addr, args.http_port).into());

    if args.ring_overlay_compat {
        tracing::warn!(
            "--ring-overlay is deprecated and now a no-op: the ring overlay is on by default \
             (pass --no-ring-overlay to use the deprecated legacy path)"
        );
    }
    let launcher = ArbLauncher {
        ctx: LaunchContext::new(task_executor.clone(), data_dir),
        chain_id: effective_chain_id,
        tuning: arb_reth_node::ArbEngineTuning {
            persistence_threshold: args.persistence_threshold,
            memory_block_buffer_target: args.memory_buffer_target,
            persistence_backpressure_threshold: args.persistence_backpressure,
            ring_overlay: !args.no_ring_overlay,
        },
        messages: feed_rx,
        rpc_addr,
    };

    let handle = launcher.launch_node(node_builder).await?;

    match handle.http_url() {
        Some(url) => info!(target: "arb-reth", %url, "arb-reth node started; eth_* RPC serving"),
        None => info!(target: "arb-reth", "arb-reth node started (RPC disabled; pass --http to enable)"),
    }

    if let Some(feed_path) = args.replay_feed {
        let tx = feed_tx.clone();
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
                        if tx.send(msg).await.is_err() {
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
            // tx (clone) is dropped here; the original feed_tx below keeps the channel open.
        });
    }

    // Trustless L1-derivation catch-up. Runs as a feed producer on the
    // same channel the driver drains, so derived blocks execute through the validated
    // STF path. The held sender keeps the node alive even after a bounded run finishes.
    if let Some(l1_rpc) = args.l1_rpc {
        // The current durable L2 tip (`last_block_number` = the persisted DB head, not the
        // in-memory canonical head). The driver already boots its production tip from this block
        // (via reth's `lookup_head`), so L1 derivation must resume so that its first NEW block is
        // `db_tip + 1`. Every block at or below `db_tip` that gets re-derived is dropped downstream.
        let db_tip = handle.provider.last_block_number()?;

        // The rollup addresses and genesis anchors, resolved as one set (Arbitrum One by default,
        // or a custom deployment when the addresses are supplied together).
        let RollupDeployment { sequencer_inbox, bridge, deployed_at: inbox_deploy_block, l2_genesis_block } =
            rollup;

        // The resume log lives in the data directory and is updated as sync advances, so a restart
        // lifts off where it stopped instead of re-deriving from genesis.
        let checkpoint_path = resume_checkpoint_path;
        let resume_log = L1ResumeLog::load(&checkpoint_path);

        // Resolve the L1 derivation resume point: (start_block, start_delayed, start_l2_block).
        // `start_l2_block` is the L2 block the start point sits *after*; derived blocks are numbered
        // from it so already-present ones can be dropped. Precedence: an explicit --l1-start-block
        // override, else the persisted checkpoint, else the genesis-snapshot bootstrap.
        let (start_block, start_delayed, start_l2_block) = if let Some(b) = args.l1_start_block {
            // Manual override: the operator asserts `b` is the batch boundary the tip was built
            // from, so the next derived block is `db_tip + 1`.
            let delayed = args.l1_start_delayed.or(snapshot_delayed).unwrap_or(0);
            info!(target: "arb-reth", l1_block = b, delayed, l2_block = db_tip, "L1 resume point: --l1-start-block override");
            (b, delayed, db_tip)
        } else if let Some(log) = &resume_log {
            // Persisted log: resume from the newest boundary at or below the durable tip. Boundaries
            // are only logged once their blocks are durable, so normally that is the newest entry.
            // If every boundary is ABOVE the tip (e.g. a `SafeNoSync` power-loss rolled the DB back
            // further than the log reaches), refuse rather than silently leave a gap.
            match log.resume_for(db_tip) {
                Some(cp) => {
                    info!(
                        target: "arb-reth",
                        l1_block = cp.l1_block, delayed = cp.delayed_count, l2_block = cp.l2_block, db_tip,
                        "L1 resume point: persisted checkpoint",
                    );
                    (cp.l1_block, cp.delayed_count, cp.l2_block)
                }
                None => {
                    return Err(eyre::eyre!(
                        "resume log at {} has no boundary at or below the durable L2 tip ({db_tip}); \
                         the database was rolled back further than the log reaches; reset the \
                         datadir and re-sync (or delete the log)",
                        checkpoint_path.display(),
                    ));
                }
            }
        } else {
            // No checkpoint: re-derive from Nitro genesis (batch 0), anchoring the L2 numbering at
            // genesis. For a fresh genesis DB this is the normal bootstrap (nothing is skipped). For
            // a DB that advanced past genesis but has no checkpoint (a rewound DB, or one synced by
            // a build predating the resume log) the L1-sync runtime re-derives from genesis and
            // DROPS every block <= db_tip (derivation only, no re-execution), producing just the new
            // tail. Slower to start than a checkpoint resume, but always correct and self-healing;
            // the first window past db_tip writes a fresh checkpoint so later restarts are fast.
            if db_tip != l2_genesis_block {
                info!(
                    target: "arb-reth", db_tip,
                    genesis = l2_genesis_block,
                    "no resume checkpoint; re-deriving from genesis and skipping already-present blocks",
                );
            }
            // Resolve batch 0's delivery block on-chain (anchored at the SequencerInbox deploy
            // block) rather than assuming a literal.
            let provider = ProviderBuilder::new().connect_http(
                l1_rpc.parse().map_err(|e| eyre::eyre!("invalid --l1-rpc URL: {e}"))?,
            );
            let reader = SequencerInboxReader::new(provider, sequencer_inbox);
            let block = reader
                .delivery_block_of_batch(0, inbox_deploy_block, 1_000)
                .await
                .map_err(|e| eyre::eyre!("resolve batch 0 delivery block: {e}"))?
                .ok_or_else(|| eyre::eyre!("batch 0 not found near the SequencerInbox deploy block"))?;
            // Genesis delayed cursor is 0; anchor L2 numbering at genesis so the skip threshold
            // (db_tip) lines up with the absolute block numbers derivation produces.
            let delayed = args.l1_start_delayed.unwrap_or(0);
            info!(target: "arb-reth", batch = 0, l1_block = block, delayed, "L1 resume point: genesis (batch 0)");
            (block, delayed, l2_genesis_block)
        };

        let mut sync_cfg = arb_reth_node::L1SyncConfig::mainnet(l1_rpc, start_block, start_delayed);
        sync_cfg.sequencer_inbox = sequencer_inbox;
        sync_cfg.bridge = bridge;
        sync_cfg.l1_beacon = args.l1_beacon;
        sync_cfg.end_block = args.l1_end_block;
        sync_cfg.prefetch_windows = args.l1_prefetch;
        sync_cfg.start_l2_block = start_l2_block;
        sync_cfg.db_tip_l2 = db_tip;
        sync_cfg.checkpoint_path = Some(checkpoint_path);

        // Read the durable L2 tip on demand so checkpoint writes only advance past blocks that are
        // actually on disk (`last_block_number`, not the in-memory canonical head).
        let tip_provider = handle.provider.clone();
        let persisted_tip = move || tip_provider.last_block_number().unwrap_or(0);

        let tx = feed_tx.clone();
        task_executor.spawn_task(async move {
            if let Err(e) = arb_reth_node::run_l1_sync(sync_cfg, tx, persisted_tip).await {
                reth_tracing::tracing::error!(target: "arb-reth", err = %e, "L1 sync failed");
            }
        });
        info!(target: "arb-reth", start_block, start_delayed, start_l2_block, db_tip, "L1-derivation catch-up started");
    }

    // Hold feed_tx alive so the driver parks on the channel rather than exiting.
    let _feed_tx = feed_tx;
    handle.wait_for_node_exit().await
}
