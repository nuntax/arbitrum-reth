//! `arb-reth node`: runnable entrypoint for the Arbitrum engine-tree node.
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
//! With `--arb-chain-config <PATH>` an Arbitrum chain-config JSON is parsed to produce a real
//! ArbOS genesis allocation instead of the MAINNET placeholder.

use std::{
    fs,
    path::PathBuf,
    sync::Arc,
};

use crate::launcher::ArbLauncher;
use crate::metrics::FeedLatencyTracker;
use crate::{
    ARB_ONE_CHAIN_ID, ArbNode, L1ResumeLog, arb_chain_spec, arbos_init_from_chain_config_json,
    arbos_init_from_parsed,
};
use alloy_primitives::Address;
use alloy_provider::{Provider, ProviderBuilder};
use arb_reth_l1::{DelayedInboxReader, SequencerInboxReader};
use arbitrum_alloy_sequencer::init_message::parse_init_message_from_body;
use arbitrum_alloy_sequencer::sequencer::feed::{BroadcastFeedMessage, Root};
use clap::Args;
use reth_chainspec::{ChainSpec, MAINNET};
use reth_cli::chainspec::ChainSpecParser;
use reth_cli_commands::{launcher::FnLauncher, node::NodeCommand};
use reth_cli_runner::CliContext;
use reth_db::{DatabaseEnv, mdbx::SyncMode};
use reth_node_builder::{LaunchContext, NodeBuilder, WithLaunchContext};
use reth_provider::BlockNumReader;
use reth_tracing::tracing::info;

/// Reth chain-spec parser for the standalone Arbitrum node.
///
/// `arb-one` is the placeholder specification used until an Arbitrum bootstrap source resolves
/// the actual chain state. ArbOS genesis must come from `--arb-chain-config`, an Orbit
/// `--chain-info`/`--genesis` pair, or `--snapshot-head`; a generic Ethereum genesis does not
/// contain the ArbOS system state this node requires.
#[derive(Debug, Clone, Default)]
pub struct ArbChainSpecParser;

impl ChainSpecParser for ArbChainSpecParser {
    type ChainSpec = ChainSpec;

    const SUPPORTED_CHAINS: &'static [&'static str] = &["arb-one"];

    fn parse(value: &str) -> eyre::Result<Arc<Self::ChainSpec>> {
        match value {
            "arb-one" => Ok(MAINNET.clone()),
            _ => Err(eyre::eyre!(
                "unsupported Arbitrum chain spec; use --arb-chain-config, --chain-info with --genesis, or --snapshot-head"
            )),
        }
    }

    fn help_message() -> String {
        "Reth chain-spec identity for this standalone Arbitrum node.\n\
         \n\
         `arb-one` is the only built-in placeholder. Supply ArbOS state separately with \
         `--arb-chain-config`, `--chain-info` plus `--genesis`, or `--snapshot-head`.\n\
         \n\
         Built-in chains:\n    arb-one"
            .to_owned()
    }
}

/// Arbitrum-specific arguments appended to Reth's native [`NodeCommand`].
///
/// Generic node concerns such as the datadir, RPC, metrics, database durability, pruning, and
/// engine-tree tuning deliberately live in Reth's command. This type only describes the inputs
/// that cannot exist on an L1 node.
#[derive(Debug, Args)]
pub struct ArbNodeArgs {
    /// Compatibility alias for `--db.sync-mode safe-no-sync`.
    ///
    /// New invocations should use Reth's database flag directly. Keeping this temporarily avoids
    /// breaking existing operator scripts while the CLI migrates.
    #[arg(long = "no-fsync", default_value_t = false)]
    no_fsync: bool,

    /// Arbitrum execution chain id used by the block driver.
    #[arg(long, default_value_t = ARB_ONE_CHAIN_ID)]
    chain_id: u64,

    /// Path to a Nitro `chaininfo.json` (array of chains: chain-id, parent-chain-id, chain-config,
    /// and the rollup deployment addresses). With `--genesis`, boots an Orbit chain end to end: the
    /// chain spec + prealloc come from the genesis file and the L1 rollup addresses (sequencer
    /// inbox, bridge, deployed-at) come from here. Must be given together with `--genesis`.
    #[arg(long = "chain-info", value_name = "PATH")]
    chain_info: Option<PathBuf>,

    /// Path to a Nitro `genesis.json` (geth-style `alloc` + `arbOSInit.initialL1BaseFee` +
    /// `serializedChainConfig`). Supplies the Orbit chain's genesis state (prealloc contracts +
    /// funded accounts) layered under the ArbOS init state. Must be given together with
    /// `--chain-info`.
    #[arg(long = "genesis", value_name = "PATH")]
    genesis_json: Option<PathBuf>,

    /// Path to an Arbitrum chain-config JSON file (the Go `ChainConfig` format).
    ///
    /// This replaced the old `--chain` meaning. `--chain` is now the native Reth chain-spec
    /// argument, while this flag specifically requests ArbOS genesis construction.
    #[arg(long = "arb-chain-config", alias = "chain-config", value_name = "PATH")]
    chain_config: Option<PathBuf>,

    /// Initial L1 base fee (wei) baked into the ArbOS genesis when booting from
    /// `--arb-chain-config`.
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

    /// Live sequencer-feed relay to follow, e.g. `ws://127.0.0.1:9642` (a nitro-testnode) or
    /// `wss://arb1.arbitrum.io/feed` (Arbitrum One). The node connects, streams each feed frame's
    /// `BroadcastFeedMessage`s into the block driver in real time, and reconnects on drop. This is
    /// the low-latency path: it follows the sequencer directly rather than waiting for L1 batches.
    ///
    /// The relay is a TIP source, not history: its backlog starts at a recent sequence, so this
    /// cannot sync a chain from scratch. Reach the tip via `--l1-rpc` derivation (or a snapshot),
    /// then let the feed ride it. The feed and derivation MAY run together: the driver reconciles by
    /// message sequence (drop already-applied, buffer feed-ahead, drain as the gap fills), so
    /// derivation fills the confirmed prefix while the feed rides the tip. The follower requests our
    /// current tip's sequence on connect. Use `--no-l1-derive` to run the feed as the sole producer
    /// (e.g. resuming an already-synced datadir). Not handled: a feed vs L1 content disagreement
    /// (feed publishes a block L1 later contradicts) — L1 is authoritative and the reorg/resequence
    /// that Nitro does is future work; on an honest sequencer the two never disagree.
    #[arg(long = "feed-url", value_name = "URL")]
    feed_url: Option<String>,

    /// Skip the L1-derivation catch-up loop, making `--feed-url` the sole block source. Genesis is
    /// still bootstrapped from `--l1-rpc` (chain id, spec, initial L1 base fee). Use this to follow a
    /// chain purely through its sequencer feed: the driver applies each feed message as the next
    /// block, so derivation must not also produce (both feed the one channel and would double-apply).
    #[arg(long = "no-l1-derive")]
    no_l1_derive: bool,

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

    /// Max `eth_getLogs` block span per request. Set to your provider's cap when it rejects wide
    /// ranges (e.g. `--l1-getlogs-range 10` for Alchemy's free tier). Bounds every L1 log scan:
    /// the batch window, the delayed-message scan, and the startup batch-0 lookup. Omit to keep the
    /// defaults (1k batch / 10k delayed), which suit an unmetered archive endpoint. Smaller = many
    /// more requests, so slower catch-up.
    #[arg(long = "l1-getlogs-range", value_name = "BLOCKS")]
    l1_getlogs_range: Option<u64>,

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
    /// Use with `--datadir <imported-dir>`; no separate Reth chain spec is needed.
    #[arg(long = "snapshot-head", value_name = "PATH")]
    snapshot_head: Option<PathBuf>,

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
fn resolve_rollup_deployment(args: &ArbNodeArgs) -> eyre::Result<RollupDeployment> {
    match (args.l1_sequencer_inbox, args.l1_bridge) {
        (None, None) => Ok(RollupDeployment {
            sequencer_inbox: arb_reth_l1::SEQUENCER_INBOX_MAINNET,
            bridge: arb_reth_l1::BRIDGE_MAINNET,
            deployed_at: args
                .l1_inbox_deploy_block
                .unwrap_or(arb_reth_l1::SEQUENCER_INBOX_DEPLOY_BLOCK_MAINNET),
            l2_genesis_block: args
                .l2_genesis_block
                .unwrap_or(arb_reth_l1::NITRO_GENESIS_BLOCK_MAINNET),
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
    let provider = ProviderBuilder::new().connect_http(
        l1_rpc
            .parse()
            .map_err(|e| eyre::eyre!("invalid --l1-rpc URL: {e}"))?,
    );
    let head = provider
        .get_block_number()
        .await
        .map_err(|e| eyre::eyre!("l1 get_block_number: {e}"))?;
    let reader = DelayedInboxReader::new(provider, bridge);
    let msgs = reader
        .fetch_delayed(from_block, head)
        .await
        .map_err(|e| eyre::eyre!("fetch delayed messages for L1 genesis: {e}"))?;
    let init = msgs.iter().find(|m| m.inbox_seq_num == 0).ok_or_else(|| {
        eyre::eyre!("no delayed message 0 (Initialize) in L1 blocks {from_block}..={head}")
    })?;
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

struct NodeBootstrap {
    chain_spec: Arc<ChainSpec>,
    chain_id: u64,
    rollup: RollupDeployment,
    snapshot_delayed: Option<u64>,
}

/// Resolve Arbitrum's state-derived chain specification before Reth opens the database.
///
/// Reth's native node command owns generic configuration and database setup. Arbitrum's chain
/// specification is different: a snapshot header, Nitro genesis, or the L1 Initialize message
/// can be the source of truth, so it has to be resolved before handing the command to Reth.
async fn resolve_bootstrap(
    args: &ArbNodeArgs,
    fallback_chain_spec: Arc<ChainSpec>,
) -> eyre::Result<NodeBootstrap> {

    // --chain-info boots an Orbit chain. Highest precedence; supplies BOTH the chain spec and the
    // rollup deployment (L1 addresses). With --genesis the chain spec + prealloc come from the
    // genesis file (byte-exact, for a chain that ships a custom genesis like Robinhood mainnet);
    // without it the chain config comes from the chaininfo entry (ArbOS-init-only genesis, e.g. a
    // testnet with "no custom genesis"). `--genesis` alone (no chaininfo) has no rollup addresses.
    let orbit = match (&args.chain_info, &args.genesis_json) {
        (Some(ci), genesis_opt) => {
            let ci_json = fs::read(ci).map_err(|e| eyre::eyre!("read chain-info {ci:?}: {e}"))?;
            let (spec, init, info) = match genesis_opt {
                Some(g) => {
                    let g_json = fs::read(g).map_err(|e| eyre::eyre!("read genesis {g:?}: {e}"))?;
                    crate::orbit_chain_from_files(&ci_json, &g_json)?
                }
                None => crate::orbit_chain_from_chain_info(
                    &ci_json,
                    args.initial_l1_base_fee.map(alloy_primitives::U256::from),
                )?,
            };
            Some((std::sync::Arc::new(spec), init, info))
        }
        (None, Some(_)) => {
            return Err(eyre::eyre!(
                "--genesis requires --chain-info (the rollup addresses live there)"
            ));
        }
        (None, None) => None,
    };

    // Resolve the rollup addresses + deploy/genesis anchors as one set up front, so a
    // half-specified custom deployment fails fast rather than mid-boot. An Orbit boot takes them
    // straight from the chaininfo file.
    let rollup = match &orbit {
        Some((_, init, info)) => RollupDeployment {
            sequencer_inbox: info.rollup.sequencer_inbox,
            bridge: info.rollup.bridge,
            deployed_at: info.rollup.deployed_at,
            l2_genesis_block: init.genesis_block_number,
        },
        None => resolve_rollup_deployment(args)?,
    };

    // --snapshot-head: boot on an imported snapshot DB by building the chain spec from its head
    // header (so reth's genesis-hash check accepts the DB). It takes precedence over the
    // `arb-one` placeholder spec. The header anchors both eth_chainId and the driver.
    // `snapshot_delayed` carries the L2 tip's `delayedMessagesRead` (the header nonce) so the
    // L1-sync delayed cursor defaults to it without a manual flag.
    let mut snapshot_delayed: Option<u64> = None;
    let (chain_spec, effective_chain_id) = match (&orbit, &args.snapshot_head, &args.chain_config) {
        (Some((spec, init, info)), _, _) => {
            info!(
                target: "arb-reth",
                chain_id = init.chain_id.to::<u64>(),
                arbos_version = init.initial_arbos_version,
                chain_name = %info.chain_name,
                parent_chain_id = info.parent_chain_id,
                sequencer_inbox = %info.rollup.sequencer_inbox,
                deployed_at = info.rollup.deployed_at,
                "booting Orbit chain from chaininfo + genesis files",
            );
            (spec.clone(), init.chain_id.to::<u64>())
        }
        (None, Some(head_path), _) => {
            let (num, hash, header) = crate::read_head_header(head_path)?;
            snapshot_delayed = Some(u64::from_be_bytes(header.nonce.0));
            info!(
                target: "arb-reth",
                head_block = num, %hash, chain_id = args.chain_id,
                delayed_messages_read = snapshot_delayed.unwrap(),
                "booting on snapshot head header",
            );
            (
                crate::arb_chain_spec_with_header(args.chain_id, header, hash),
                args.chain_id,
            )
        }
        (None, None, Some(path)) => {
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
        (None, None, None) => match &args.l1_rpc {
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
            None => (fallback_chain_spec, args.chain_id),
        },
    };

    Ok(NodeBootstrap {
        chain_spec,
        chain_id: effective_chain_id,
        rollup,
        snapshot_delayed,
    })
}

/// Launch the native Reth node command with Arbitrum's derived chain state.
pub async fn run(
    ctx: CliContext,
    mut command: NodeCommand<ArbChainSpecParser, ArbNodeArgs>,
) -> eyre::Result<()> {
    validate_standalone_components(&command)?;

    if command.ext.no_fsync {
        match command.db.sync_mode {
            Some(SyncMode::SafeNoSync) | None => {
                command.db.sync_mode = Some(SyncMode::SafeNoSync);
            }
            Some(mode) => {
                return Err(eyre::eyre!(
                    "--no-fsync conflicts with --db.sync-mode {mode:?}; use one durability mode"
                ));
            }
        }
    }

    let bootstrap = resolve_bootstrap(&command.ext, command.chain.clone()).await?;
    command.chain = bootstrap.chain_spec.clone();

    command
        .execute(
            ctx,
            FnLauncher::new::<ArbChainSpecParser, ArbNodeArgs>(move |builder, args| async move {
                launch(builder, args, bootstrap).await
            }),
        )
        .await
}

/// Reject native Reth option groups that have no component in this standalone topology.
///
/// `NodeCommand` owns the common node configuration, but Arbitrum derives blocks from L1/feed
/// messages. It deliberately has no dev miner, devp2p network, transaction pool, generic payload
/// builder, staged-pipeline debugger, ERA importer, or revmc-JIT wiring. Failing here is safer
/// than accepting an option that the no-op component would ignore.
fn validate_standalone_components(
    command: &NodeCommand<ArbChainSpecParser, ArbNodeArgs>,
) -> eyre::Result<()> {
    if command.network != Default::default() {
        return Err(eyre::eyre!(
            "network options are unsupported: arb-reth derives Arbitrum messages from L1 and the sequencer feed, not devp2p"
        ));
    }
    if command.txpool != Default::default() {
        return Err(eyre::eyre!(
            "txpool options are unsupported: the standalone node has no transaction pool"
        ));
    }
    if command.builder != Default::default() {
        return Err(eyre::eyre!(
            "payload-builder options are unsupported: ArbOS messages are built by the Arbitrum engine driver"
        ));
    }
    if command.debug != Default::default() {
        return Err(eyre::eyre!(
            "debug node options are unsupported by the standalone Arbitrum launcher"
        ));
    }
    if command.dev != Default::default() {
        return Err(eyre::eyre!(
            "dev mode is unsupported: ArbOS blocks must be derived from Arbitrum messages"
        ));
    }
    if command.era.enabled
        || command.era.source.path.is_some()
        || command.era.source.url.is_some()
    {
        return Err(eyre::eyre!(
            "ERA import is unsupported: it imports Ethereum block bodies rather than deriving ArbOS blocks"
        ));
    }
    if command.jit != Default::default() {
        return Err(eyre::eyre!(
            "--jit options are not wired to arb-revm yet"
        ));
    }

    Ok(())
}

async fn launch(
    mut builder: WithLaunchContext<NodeBuilder<DatabaseEnv, ChainSpec>>,
    args: ArbNodeArgs,
    bootstrap: NodeBootstrap,
) -> eyre::Result<()> {
    let task_executor = builder.task_executor().clone();
    let NodeBootstrap {
        chain_id: effective_chain_id,
        rollup,
        snapshot_delayed,
        ..
    } = bootstrap;

    // Resolve the pruning configuration from Reth's native `--prune.*` / `--full` arguments.
    // It is passed both to the provider factory and the engine-tree pruner so static-file writes
    // follow the same segment policy.
    let prune_config = builder
        .config()
        .pruning
        .prune_config(builder.config().chain.as_ref());
    match &prune_config {
        Some(pc) => info!(
            target: "arb-reth",
            segments = ?pc.segments,
            block_interval = pc.block_interval,
            minimum_pruning_distance = pc.minimum_pruning_distance,
            "history pruning enabled",
        ),
        None => info!(target: "arb-reth", "archive node (no pruning configured; keeping all history)"),
    }
    let data_dir = builder.config().datadir();

    // Resolve the L1-derivation resume log path before `data_dir` is moved into the launcher.
    let resume_checkpoint_path = L1ResumeLog::path_in(data_dir.data_dir());

    // The held sender keeps the driver parked (and the node alive) until SIGTERM.
    let (feed_tx, feed_rx) = tokio::sync::mpsc::channel::<BroadcastFeedMessage>(4096);
    // Only live WebSocket messages carry an ingress timestamp. L1-derived and replay messages
    // still drive the same engine callback, but have no sample to record.
    let feed_latency = args.feed_url.as_ref().map(|_| FeedLatencyTracker::new());

    // Preserve the complete native tree configuration before `ArbNode` changes the builder's
    // type-state to its custom no-op add-ons. The driver consumes this exact configuration below.
    let tuning = crate::ArbEngineTuning::from_tree_config(builder.config().tree_config());

    // ArbOS derives blocks internally, rather than receiving Engine API commands from a consensus
    // client. Keep the native public RPC settings but never expose the unusable authenticated
    // Engine API server.
    builder.config_mut().rpc.disable_auth_server = true;
    let node_builder = builder.node(ArbNode);

    let launcher = ArbLauncher {
        ctx: LaunchContext::new(task_executor.clone(), data_dir),
        chain_id: effective_chain_id,
        genesis_block: rollup.l2_genesis_block,
        tuning,
        prune_config,
        messages: feed_rx,
        feed_latency: feed_latency.clone(),
    };

    let handle = node_builder.launch_with(launcher).await?;

    match handle.http_url() {
        Some(url) => info!(target: "arb-reth", %url, "arb-reth node started; eth_* RPC serving"),
        None => {
            info!(target: "arb-reth", "arb-reth node started (HTTP RPC disabled; pass --http to enable it)")
        }
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

    // Live sequencer-feed follower (--feed-url): a reconnecting websocket source that streams each
    // feed frame's messages into the same channel the driver drains. Like --replay-feed but from the
    // network and unbounded in time; the low-latency path that rides the sequencer tip directly.
    if let Some(feed_url) = args.feed_url.clone() {
        let tx = feed_tx.clone();
        let feed_latency = feed_latency.expect("feed latency tracker exists with --feed-url");
        // Ask the relay to start at our tip's next message index (block - genesis + 1). The relay is
        // a bounded tip backlog: if this predates what it holds it just streams its current backlog,
        // and the driver's sequence guard dedups/buffers regardless, so this is an optimization.
        let feed_genesis_block = rollup.l2_genesis_block;
        let feed_start_seq = handle
            .provider
            .last_block_number()
            .unwrap_or(feed_genesis_block)
            .saturating_sub(feed_genesis_block)
            + 1;
        task_executor.spawn_task(async move {
            use futures_util::StreamExt;
            use tokio_tungstenite::tungstenite::Message;
            use tokio_tungstenite::tungstenite::client::IntoClientRequest;
            let mut next_seq = feed_start_seq;
            loop {
                // Resume from where we last were (advances as messages arrive) via the Arbitrum
                // start-sequence header on the websocket upgrade.
                let request = match feed_url.as_str().into_client_request() {
                    Ok(mut req) => {
                        if let Ok(val) = next_seq.to_string().parse() {
                            req.headers_mut().insert("Arbitrum-Requested-Sequence-Number", val);
                        }
                        req
                    }
                    Err(e) => {
                        reth_tracing::tracing::error!(target: "arb-reth", url = %feed_url, err = %e, "feed: invalid url; follower stopping");
                        return;
                    }
                };
                match tokio_tungstenite::connect_async(request).await {
                    Ok((mut ws, _)) => {
                        info!(target: "arb-reth", url = %feed_url, from_seq = next_seq, "feed: connected to sequencer feed");
                        let mut pushed = 0usize;
                        while let Some(frame) = ws.next().await {
                            // This is the ingress edge: take the timestamp before converting or
                            // parsing the WebSocket frame so the metric includes that work too.
                            let frame_received_at = std::time::Instant::now();
                            let text = match frame {
                                Ok(Message::Text(t)) => t.as_str().to_owned(),
                                Ok(Message::Binary(b)) => match core::str::from_utf8(b.as_ref()) {
                                    Ok(s) => s.to_owned(),
                                    Err(_) => continue,
                                },
                                Ok(Message::Close(_)) => break,
                                // ping/pong/frame: nothing to decode.
                                Ok(_) => continue,
                                Err(e) => {
                                    reth_tracing::tracing::warn!(target: "arb-reth", err = %e, "feed: websocket error");
                                    break;
                                }
                            };
                            // Each frame is a feed `Root` { version, messages: [BroadcastFeedMessage] }.
                            // Other frames (e.g. confirmed-sequence-number notices) carry no messages
                            // and are skipped.
                            match serde_json::from_str::<Root>(&text) {
                                Ok(root) => {
                                    let ready_for_channel_at = std::time::Instant::now();
                                    for msg in root.messages.into_iter().flatten() {
                                        next_seq = msg.sequence_number + 1;
                                        feed_latency.record_frame_arrival(
                                            msg.sequence_number,
                                            frame_received_at,
                                        );
                                        feed_latency.record_ready_for_channel(
                                            msg.sequence_number,
                                            ready_for_channel_at,
                                        );
                                        if tx.send(msg).await.is_err() {
                                            reth_tracing::tracing::warn!(target: "arb-reth", "feed channel closed; stopping feed follower");
                                            return;
                                        }
                                        pushed += 1;
                                    }
                                }
                                Err(e) => {
                                    reth_tracing::tracing::debug!(target: "arb-reth", err = %e, "feed: skipping unparsed frame");
                                }
                            }
                        }
                        reth_tracing::tracing::warn!(target: "arb-reth", pushed, "feed: disconnected; reconnecting in 2s");
                    }
                    Err(e) => {
                        reth_tracing::tracing::warn!(target: "arb-reth", url = %feed_url, err = %e, "feed: connect failed; retrying in 2s");
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        });
    }

    // Trustless L1-derivation catch-up. Runs as a feed producer on the
    // same channel the driver drains, so derived blocks execute through the validated
    // STF path. The held sender keeps the node alive even after a bounded run finishes.
    // Skipped under --no-l1-derive so --feed-url is the sole producer (genesis already bootstrapped
    // from --l1-rpc above; the derivation loop and the feed must not both feed the channel).
    if let Some(l1_rpc) = args.l1_rpc.filter(|_| !args.no_l1_derive) {
        // The current durable L2 tip (`last_block_number` = the persisted DB head, not the
        // in-memory canonical head). The driver already boots its production tip from this block
        // (via reth's `lookup_head`), so L1 derivation must resume so that its first NEW block is
        // `db_tip + 1`. Every block at or below `db_tip` that gets re-derived is dropped downstream.
        let db_tip = handle.provider.last_block_number()?;

        // The rollup addresses and genesis anchors, resolved as one set (Arbitrum One by default,
        // or a custom deployment when the addresses are supplied together).
        let RollupDeployment {
            sequencer_inbox,
            bridge,
            deployed_at: inbox_deploy_block,
            l2_genesis_block,
        } = rollup;

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
                l1_rpc
                    .parse()
                    .map_err(|e| eyre::eyre!("invalid --l1-rpc URL: {e}"))?,
            );
            let reader = SequencerInboxReader::new(provider, sequencer_inbox);
            let block = reader
                .delivery_block_of_batch(
                    0,
                    inbox_deploy_block,
                    args.l1_getlogs_range.map(|n| n.max(1)).unwrap_or(1_000),
                )
                .await
                .map_err(|e| eyre::eyre!("resolve batch 0 delivery block: {e}"))?
                .ok_or_else(|| {
                    eyre::eyre!("batch 0 not found near the SequencerInbox deploy block")
                })?;
            // Genesis delayed cursor is 0; anchor L2 numbering at genesis so the skip threshold
            // (db_tip) lines up with the absolute block numbers derivation produces.
            let delayed = args.l1_start_delayed.unwrap_or(0);
            info!(target: "arb-reth", batch = 0, l1_block = block, delayed, "L1 resume point: genesis (batch 0)");
            (block, delayed, l2_genesis_block)
        };

        let mut sync_cfg = crate::L1SyncConfig::mainnet(l1_rpc, start_block, start_delayed);
        sync_cfg.sequencer_inbox = sequencer_inbox;
        sync_cfg.bridge = bridge;
        sync_cfg.l1_beacon = args.l1_beacon;
        sync_cfg.end_block = args.l1_end_block;
        sync_cfg.prefetch_windows = args.l1_prefetch;
        // Cap every getLogs span to the provider's limit when set (free-tier friendly).
        if let Some(n) = args.l1_getlogs_range {
            let n = n.max(1);
            sync_cfg.batch_window = n;
            sync_cfg.delayed_window = n;
        }
        sync_cfg.start_l2_block = start_l2_block;
        sync_cfg.db_tip_l2 = db_tip;
        // Messages are numbered by message index (block - genesis_block) for the driver's
        // sequence-reconciliation; without this a non-zero genesis (Arbitrum One) mis-numbers every
        // derived block and the driver applies none.
        sync_cfg.genesis_block = l2_genesis_block;
        sync_cfg.checkpoint_path = Some(checkpoint_path);

        // Read the durable L2 tip on demand so checkpoint writes only advance past blocks that are
        // actually on disk (`last_block_number`, not the in-memory canonical head).
        let tip_provider = handle.provider.clone();
        let persisted_tip = move || tip_provider.last_block_number().unwrap_or(0);

        let tx = feed_tx.clone();
        task_executor.spawn_task(async move {
            if let Err(e) = crate::run_l1_sync(sync_cfg, tx, persisted_tip).await {
                // `?e` (Debug) prints the full eyre cause chain, not just the top context, so a
                // derivation failure shows the real underlying error (batch/message decode, etc.).
                reth_tracing::tracing::error!(target: "arb-reth", err = ?e, "L1 sync failed");
            }
        });
        info!(target: "arb-reth", start_block, start_delayed, start_l2_block, db_tip, "L1-derivation catch-up started");
    }

    // Hold feed_tx alive so the driver parks on the channel rather than exiting.
    let _feed_tx = feed_tx;
    handle.wait_for_node_exit().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{Parser, Subcommand};

    #[derive(Debug, Parser)]
    struct TestCli {
        #[command(subcommand)]
        command: TestCommand,
    }

    #[derive(Debug, Subcommand)]
    enum TestCommand {
        Node(Box<NodeCommand<ArbChainSpecParser, ArbNodeArgs>>),
    }

    #[test]
    fn native_node_arguments_and_arb_extension_parse_together() {
        let TestCommand::Node(command) = TestCli::try_parse_from([
            "arb-reth",
            "node",
            "--datadir",
            "/tmp/arb-reth-node",
            "--metrics",
            "127.0.0.1:9001",
            "--http",
            "--http.port",
            "8547",
            "--ws",
            "--ws.port",
            "8548",
            "--db.sync-mode",
            "safe-no-sync",
            "--engine.persistence-threshold",
            "128",
            "--engine.memory-block-buffer-target",
            "64",
            "--engine.persistence-backpressure-threshold",
            "512",
            "--full",
            "--arb-chain-config",
            "/tmp/chain-config.json",
            "--feed-url",
            "wss://feed.example",
        ])
        .expect("native and Arbitrum arguments should parse")
        .command;

        assert_eq!(command.engine.persistence_threshold, 128);
        assert_eq!(command.engine.memory_block_buffer_target, 64);
        assert_eq!(command.engine.persistence_backpressure_threshold(), 512);
        assert_eq!(command.db.sync_mode, Some(SyncMode::SafeNoSync));
        assert!(command.pruning.full);
        assert_eq!(
            command.ext.chain_config,
            Some(PathBuf::from("/tmp/chain-config.json"))
        );
        assert_eq!(command.ext.feed_url.as_deref(), Some("wss://feed.example"));
    }

    #[test]
    fn generic_genesis_does_not_replace_arbos_bootstrap() {
        let err = TestCli::try_parse_from([
            "arb-reth",
            "node",
            "--chain",
            "/tmp/ethereum-genesis.json",
        ])
        .expect_err("a generic Ethereum genesis is not an ArbOS bootstrap input");

        assert!(err.to_string().contains("unsupported Arbitrum chain spec"));
    }

    #[test]
    fn noop_component_options_are_rejected() {
        let TestCommand::Node(command) = TestCli::try_parse_from([
            "arb-reth",
            "node",
            "--disable-discovery",
        ])
        .expect("the native parser should accept its standard flag")
        .command;

        let err = validate_standalone_components(&command)
            .expect_err("a no-op network setting must not be silently accepted");
        assert!(err.to_string().contains("network options are unsupported"));
    }
}
