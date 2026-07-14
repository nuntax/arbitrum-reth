//! `ArbLauncher`, a custom `LaunchNode` for the Arbitrum engine-tree node.
//!
//! Mirrors reth's `EngineNodeLauncher::launch_node` type-state chain but stops after
//! `.with_components(...)` (no pipeline, no consensus-engine orchestrator, no RpcAddOns;
//! AddOns = ()). After standing up the provider stack it extracts `ProviderFactory` +
//! `BlockchainProvider`, spawns reth's engine tree via [`ArbEngineDriver::spawn`], and runs a
//! background task that calls `driver.advance()` per feed message (produce → InsertExecutedBlock
//! → ForkchoiceUpdated); the tree owns async persistence and the in-memory overlay.
//!
//! Deadlock rule: never hold a read provider across a `provider_rw()`/`save_blocks()` call.

use core::{future::Future, pin::Pin};
use std::net::SocketAddr;

use crate::metrics::FeedLatencyTracker;
use alloy_consensus::Header;
use arbitrum_alloy_consensus::reth::ArbPrimitives;
use arbitrum_alloy_sequencer::sequencer::feed::BroadcastFeedMessage;
use eyre::eyre;
use reth_chain_state::CanonicalInMemoryState;
use reth_db::{Database, database_metrics::DatabaseMetrics};
use reth_evm::ConfigureEvm;
use reth_node_api::{AddOnsContext, FullNodeTypes, NodeAddOns, NodeTypes, NodeTypesWithDBAdapter};
use reth_node_builder::hooks::NodeHooks;
use reth_node_builder::{
    AddOns, LaunchContext, LaunchNode, Node, NodeBuilderWithComponents, NodeComponents,
    NodeComponentsBuilder, NodeTypesAdapter, RethFullAdapter,
};
use reth_primitives_traits::SealedHeader;
use reth_provider::{
    BalProvider, BlockNumReader, BlockReader, ChangeSetReader, DatabaseProviderFactory,
    HashedPostStateProvider, ProviderFactory, StateProviderFactory, StateReader,
    StorageChangeSetReader,
    providers::{BlockchainProvider, NodeTypesForProvider, ProviderNodeTypes},
};
use reth_rpc_builder::RpcServerHandle;
use reth_storage_api::{
    HeaderProvider, MetadataProvider, MetadataWriter, PruneCheckpointReader, StageCheckpointReader,
    StorageSettingsCache,
};
use reth_tasks::TaskExecutor;
use reth_trie_db::ChangesetCache;
use tokio::sync::oneshot;

use arbitrum_alloy_consensus::{ArbReceiptEnvelope, reth::ArbBlock};

use arb_reth_engine::{ArbEngineDriver, ArbEngineTuning};

/// Handle returned by `ArbLauncher` after the node has been launched.
///
/// Generic over the provider type `P` so the concrete `BlockchainProvider<...>` type flows
/// through without a transmute.
pub struct ArbNodeHandle<P> {
    /// The blockchain provider: cloneable and queryable.
    pub provider: P,
    exit_rx: oneshot::Receiver<eyre::Result<()>>,
    /// Running RPC server handle. Dropping this shuts down the HTTP server.
    pub rpc_handle: Option<RpcServerHandle>,
}

impl<P> ArbNodeHandle<P> {
    /// Wait for the driver task to exit, returning its result.
    pub async fn wait_for_node_exit(self) -> eyre::Result<()> {
        self.exit_rx.await?
    }

    /// Returns the HTTP URL of the running RPC server, or `None` if RPC was not enabled.
    pub fn http_url(&self) -> Option<String> {
        self.rpc_handle.as_ref()?.http_url()
    }
}

/// A custom `LaunchNode` for the no-engine Arbitrum node.
///
/// Reuses reth's `LaunchContext` type-state chain for DB/provider/blockchain-db/task
/// infrastructure but skips the sync pipeline and consensus-engine orchestrator. Spawns
/// an [`ArbEngineDriver`] background task that drives reth's engine tree, producing exactly
/// one block per sequencer feed message.
pub struct ArbLauncher {
    /// Base launch context: task executor + data directory.
    pub ctx: LaunchContext,
    /// Arbitrum chain id (42161 = mainnet, 421614 = Sepolia).
    pub chain_id: u64,
    /// L2 genesis block number (`GenesisBlockNum`): message index 0 is the init/genesis block, so a
    /// feed message's sequence number maps to L2 block `seq + genesis_block`. Seeds the driver's
    /// sequence-dedup cursor so feed and L1-derivation messages reconcile without double-applying.
    pub genesis_block: u64,
    /// Engine-tree persistence tuning (batch/buffer/backpressure knobs).
    pub tuning: ArbEngineTuning,
    /// Feed channel of sequencer messages. The driver infers the ArbOS version from the chain
    /// tip, so no per-message version is carried.
    pub messages: tokio::sync::mpsc::Receiver<BroadcastFeedMessage>,
    /// Correlates live WebSocket feed messages with canonical in-memory state for Prometheus.
    /// `None` keeps replay and L1-only operation free of feed-latency instrumentation.
    pub feed_latency: Option<FeedLatencyTracker>,
    /// Optional HTTP bind address for the `eth_*` RPC server (`None` disables RPC).
    pub rpc_addr: Option<SocketAddr>,
}

impl<N, DB, T, CB> LaunchNode<NodeBuilderWithComponents<T, CB, ()>> for ArbLauncher
where
    N: Node<RethFullAdapter<DB, N>>
        + NodeTypesForProvider
        + NodeTypes<
            Primitives = ArbPrimitives,
            Payload = arb_reth_engine::ArbPayloadTypes,
            ChainSpec: reth_chainspec::EthChainSpec
                           + reth_chainspec::EthereumHardforks
                           + reth_chainspec::Hardforks,
        >,
    DB: Database + DatabaseMetrics + Clone + Unpin + 'static,
    T: FullNodeTypes<
            Types = N,
            Provider = BlockchainProvider<NodeTypesWithDBAdapter<N, DB>>,
            DB = DB,
        >,
    CB: NodeComponentsBuilder<T> + 'static,
    <CB::Components as NodeComponents<T>>::Evm:
        ConfigureEvm<Primitives = ArbPrimitives> + Into<arb_reth_evm::ArbEvmConfig> + Clone,
    CB::Components: NodeComponents<T, Evm = arb_reth_evm::ArbEvmConfig>,
    NodeTypesWithDBAdapter<N, DB>: ProviderNodeTypes<Primitives = ArbPrimitives>,
    // Explicit equality bounds to help the compiler resolve the associated type projections
    // from NodeTypesWithDBAdapter<N, DB>.
    NodeTypesWithDBAdapter<N, DB>:
        NodeTypes<ChainSpec = <N as NodeTypes>::ChainSpec, Primitives = ArbPrimitives>,
    NodeTypesWithDBAdapter<N, DB>: reth_node_api::NodeTypesWithDB<DB = DB>,
    // Engine-tree (Tier-1) bounds: mirror `EngineApiTreeHandler::spawn_new`'s P-bounds with
    // P = BlockchainProvider<NodeTypesWithDBAdapter<N, DB>> (see engine.rs).
    BlockchainProvider<NodeTypesWithDBAdapter<N, DB>>: DatabaseProviderFactory<DB = DB>
        + BlockReader<Block = ArbBlock, Header = Header>
        + reth_storage_api::TransactionsProvider<
            Transaction = arbitrum_alloy_consensus::ArbTxEnvelope,
        > + reth_storage_api::ReceiptProvider<Receipt = ArbReceiptEnvelope>
        + StateProviderFactory
        + StateReader<Receipt = ArbReceiptEnvelope>
        + HashedPostStateProvider
        + BalProvider
        + ChangeSetReader
        + BlockNumReader
        + Clone
        + 'static,
    <BlockchainProvider<NodeTypesWithDBAdapter<N, DB>> as DatabaseProviderFactory>::Provider:
        BlockReader<Block = ArbBlock, Header = Header>
            + StageCheckpointReader
            + PruneCheckpointReader
            + ChangeSetReader
            + StorageChangeSetReader
            + BlockNumReader
            + StorageSettingsCache,
{
    type Node = ArbNodeHandle<BlockchainProvider<NodeTypesWithDBAdapter<N, DB>>>;
    type Future = Pin<Box<dyn Future<Output = eyre::Result<Self::Node>> + Send>>;

    fn launch_node(self, target: NodeBuilderWithComponents<T, CB, ()>) -> Self::Future {
        Box::pin(self.launch_impl(target))
    }
}

impl ArbLauncher {
    /// Core async launch body. Separated from `launch_node` so it can be `async fn`
    /// (the trait requires a boxed future; `launch_node` boxes it).
    async fn launch_impl<N, DB, T, CB>(
        self,
        target: NodeBuilderWithComponents<T, CB, ()>,
    ) -> eyre::Result<ArbNodeHandle<BlockchainProvider<NodeTypesWithDBAdapter<N, DB>>>>
    where
        N: Node<RethFullAdapter<DB, N>>
            + NodeTypesForProvider
            + NodeTypes<
                Primitives = ArbPrimitives,
                Payload = arb_reth_engine::ArbPayloadTypes,
                ChainSpec: reth_chainspec::EthChainSpec
                               + reth_chainspec::EthereumHardforks
                               + reth_chainspec::Hardforks,
            >,
        DB: Database + DatabaseMetrics + Clone + Unpin + 'static,
        T: FullNodeTypes<
                Types = N,
                Provider = BlockchainProvider<NodeTypesWithDBAdapter<N, DB>>,
                DB = DB,
            >,
        CB: NodeComponentsBuilder<T> + 'static,
        <CB::Components as NodeComponents<T>>::Evm:
            ConfigureEvm<Primitives = ArbPrimitives> + Into<arb_reth_evm::ArbEvmConfig> + Clone,
        CB::Components: NodeComponents<T, Evm = arb_reth_evm::ArbEvmConfig>,
        NodeTypesWithDBAdapter<N, DB>: ProviderNodeTypes<Primitives = ArbPrimitives>,
        NodeTypesWithDBAdapter<N, DB>:
            NodeTypes<ChainSpec = <N as NodeTypes>::ChainSpec, Primitives = ArbPrimitives>,
        NodeTypesWithDBAdapter<N, DB>: reth_node_api::NodeTypesWithDB<DB = DB>,
    {
        let Self {
            ctx,
            chain_id,
            genesis_block,
            tuning,
            messages,
            feed_latency,
            rpc_addr,
        } = self;

        let NodeBuilderWithComponents {
            adapter: NodeTypesAdapter { database },
            rocksdb_provider,
            components_builder,
            add_ons:
                AddOns {
                    hooks,
                    exexs: _,
                    add_ons: _,
                },
            config,
        } = target;
        let NodeHooks {
            on_component_initialized,
            ..
        } = hooks;

        // Drive RPC config from the explicit `rpc_addr`: canonical `RpcAddOns` reads addresses from
        // `NodeConfig.rpc`, not an arg. Enable http+ws with the full module fleet; this node is
        // self-driven from L1 derivation, so the auth/engine server is disabled.
        let mut config = config;
        if let Some(addr) = rpc_addr {
            config.rpc.http = true;
            config.rpc.http_addr = addr.ip();
            config.rpc.http_port = addr.port();
            config.rpc.http_api = Some(reth_rpc_server_types::RpcModuleSelection::All);
            config.rpc.ws = true;
            config.rpc.ws_addr = addr.ip();
            config.rpc.ws_port = addr.port();
            config.rpc.ws_api = Some(reth_rpc_server_types::RpcModuleSelection::All);
            config.rpc.disable_auth_server = true;
        }

        let changeset_cache = ChangesetCache::new();
        let disabled_stages = N::disabled_stages();

        let ctx = ctx
            .with_configured_globals(0)
            .with_loaded_toml_config(config)?
            .attach(database.clone())
            .with_adjusted_configs()
            .with_provider_factory::<NodeTypesWithDBAdapter<N, DB>, <CB::Components as NodeComponents<T>>::Evm>(
                changeset_cache.clone(),
                rocksdb_provider,
                disabled_stages,
            )
            .await?;

        // Install reth's Prometheus recorder before any feed metric handles are initialized, and
        // serve it when `--metrics <addr>` is configured.
        let ctx = ctx.with_prometheus_server().await?;

        // Open the DB in storage v2 (hashed-state canonical, `PackedKeyAdapter`). This has to
        // happen before `with_genesis()` uses the factory. Cache the flag so every provider
        // uses v2, and persist it idempotently: an importer-made DB already has v2 in metadata, so
        // we only write when no settings flag is persisted (fresh DB) or it differs.
        {
            let factory = ctx.provider_factory();
            factory.set_storage_settings_cache(reth_db_api::models::StorageSettings::v2());
            let current = {
                let p = factory.database_provider_ro()?;
                p.storage_settings()?
            };
            if current != Some(reth_db_api::models::StorageSettings::v2()) {
                let provider_rw = factory.provider_rw()?;
                provider_rw.write_storage_settings(reth_db_api::models::StorageSettings::v2())?;
                provider_rw
                    .commit()
                    .map_err(|e| eyre!("persist storage settings v2: {e}"))?;
            }
        }

        let ctx = ctx
            .with_genesis()?
            .with_metrics_task()
            .with_blockchain_db::<T, _>(move |provider_factory| {
                Ok(BlockchainProvider::new(provider_factory)?)
            })?
            .with_components(components_builder, on_component_initialized)
            .await?;

        let provider: BlockchainProvider<NodeTypesWithDBAdapter<N, DB>> =
            ctx.node_adapter().provider.clone();
        let provider_factory: ProviderFactory<NodeTypesWithDBAdapter<N, DB>> =
            ctx.provider_factory().clone();
        let task_executor: TaskExecutor = ctx.task_executor().clone();
        let head = ctx.head();

        // Clone the in-memory state from the provider so the tree updates the same instance that
        // BlockchainProvider serves for RPC queries.
        let canonical: CanonicalInMemoryState<ArbPrimitives> = provider.canonical_in_memory_state();

        let genesis_tip: SealedHeader<Header> =
            HeaderProvider::sealed_header(&provider, head.number)?
                .ok_or_else(|| eyre!("missing head header at block {}", head.number))?;

        // `arb_evm_config` (hoisted from the RPC block below): also drives the engine tree.
        let arb_evm_config: arb_reth_evm::ArbEvmConfig =
            ctx.node_adapter().components.evm_config().clone();

        // Stand up reth's engine tree (Tier-1 `InsertExecutedBlock` seam) and drive the
        // sequencer feed through it. Persistence to MDBX is async (tree background service).
        let mut driver: ArbEngineDriver<NodeTypesWithDBAdapter<N, DB>> = ArbEngineDriver::spawn(
            provider_factory,
            provider.clone(),
            arb_evm_config.clone(),
            chain_id,
            genesis_tip,
            genesis_block,
            canonical,
            task_executor.clone(),
            tuning,
        )?;

        let (exit_tx, exit_rx) = oneshot::channel::<eyre::Result<()>>();
        let mut messages_rx = messages;

        task_executor.spawn_critical_task("arb-engine-driver", async move {
            let res: eyre::Result<()> = async {
                // Bench accounting: separate time spent WAITING for the next derived feed
                // message (L1-fetch-bound) from time spent in advance() (compute/persist-bound).
                // Emitted every 1000 blocks at target "arb-reth::bench"; harmless at info off.
                let mut bench_recv_us: u128 = 0;
                let mut bench_work_us: u128 = 0;
                let mut bench_n: u64 = 0;
                let mut bench_wall = std::time::Instant::now();
                loop {
                    let __r = std::time::Instant::now();
                    let Some(msg) = messages_rx.recv().await else {
                        break;
                    };
                    bench_recv_us += __r.elapsed().as_micros();
                    let driver_dequeued_at = std::time::Instant::now();
                    if let Some(feed_latency) = feed_latency.as_ref() {
                        feed_latency.record_driver_dequeue(msg.sequence_number, driver_dequeued_at);
                    }
                    let __w = std::time::Instant::now();
                    driver
                        .advance_with_applied(&msg, |sequence_number, applied| {
                            if let Some(feed_latency) = feed_latency.as_ref() {
                                feed_latency.record_canonical(sequence_number, applied);
                            }
                        })
                        .await?;
                    bench_work_us += __w.elapsed().as_micros();
                    bench_n += 1;
                    if bench_n.is_multiple_of(1000) {
                        let wall_ms = bench_wall.elapsed().as_millis().max(1);
                        tracing::info!(
                            target: "arb-reth::bench",
                            blocks = bench_n,
                            blk_per_s = (1000u128 * 1000 / wall_ms) as u64,
                            recv_ms = (bench_recv_us / 1000) as u64,
                            work_ms = (bench_work_us / 1000) as u64,
                            recv_pct = (100 * bench_recv_us
                                / (bench_recv_us + bench_work_us).max(1)) as u64,
                            "bench: 1000-block window",
                        );
                        bench_recv_us = 0;
                        bench_work_us = 0;
                        bench_wall = std::time::Instant::now();
                    }
                }
                driver.shutdown().await;
                Ok(())
            }
            .await;
            let _ = exit_tx.send(res); // ignore error if receiver was dropped
        });

        // Serve RPC through reth's canonical `RpcAddOns::launch_add_ons` (full fleet + ws +
        // subscriptions via `NodeConfig.rpc`), not the bespoke server. This node is self-driven
        // from L1 derivation, so the beacon-engine handle is a stub (dangling receiver: engine_*
        // calls would return `EngineUnavailable`), and the auth/engine server is disabled, so
        // nothing ever reaches it.
        let rpc_handle = if rpc_addr.is_some() {
            let (engine_tx, _engine_rx) = tokio::sync::mpsc::unbounded_channel();
            let beacon_engine_handle =
                reth_engine_primitives::ConsensusEngineHandle::new(engine_tx);
            let add_ons_ctx = AddOnsContext {
                node: ctx.node_adapter().clone(),
                config: ctx.node_config(),
                beacon_engine_handle,
                engine_events: reth_tokio_util::EventSender::default(),
                jwt_secret: ctx.auth_jwt_secret()?,
            };
            let handle = crate::addons::arb_add_ons()
                .launch_add_ons(add_ons_ctx)
                .await?;
            Some(handle.rpc_server_handles.rpc)
        } else {
            None
        };

        Ok(ArbNodeHandle {
            provider,
            exit_rx,
            rpc_handle,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{U256, address};
    use arbitrum_alloy_sequencer::sequencer::feed::BroadcastFeedMessage;
    use reth_chainspec::MAINNET;
    use reth_node_builder::{LaunchNode, NodeBuilder, NodeConfig};
    use reth_provider::{BlockNumReader, HeaderProvider, StateProviderFactory};
    use reth_storage_api::AccountReader;
    use reth_tasks::Runtime;

    use crate::ArbNode;

    /// `ArbLauncher` boots over reth's `LaunchContext`, processes two deposit messages,
    /// and persists blocks 1 & 2 with cumulative balance = 2 x 111_000_000_000_000_000.
    #[tokio::test(flavor = "multi_thread")]
    async fn launcher_boots_and_produces_blocks() {
        let fixtures_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        let json = std::fs::read_to_string(fixtures_dir.join("deposit_message_only.json"))
            .expect("read fixture");
        let feed_msg: BroadcastFeedMessage =
            serde_json::from_str(&json).expect("parse BroadcastFeedMessage");

        let task_executor = Runtime::test();

        // The driver dedups by sequence number, so the two messages must be sequential (a fresh
        // genesis DB has genesis_block 0, so the first digested message is index 1).
        let (tx, rx) = tokio::sync::mpsc::channel::<BroadcastFeedMessage>(4);
        let mut m1 = feed_msg.clone();
        m1.sequence_number = 1;
        let mut m2 = feed_msg.clone();
        m2.sequence_number = 2;
        tx.send(m1).await.unwrap();
        tx.send(m2).await.unwrap();
        drop(tx);

        let datadir = reth_db::test_utils::tempdir_path();
        let db = reth_db::test_utils::create_test_rw_db_with_datadir(&datadir);

        // Build the ChainPath (data_dir) that LaunchContext needs.
        let maybe_path =
            reth_node_core::dirs::MaybePlatformPath::<reth_node_core::dirs::DataDirPath>::from(
                datadir.clone(),
            );
        let config = NodeConfig::test()
            .with_chain(MAINNET.clone())
            .with_datadir_args(reth_node_core::args::DatadirArgs {
                datadir: maybe_path.clone(),
                ..Default::default()
            });
        let data_dir = maybe_path.unwrap_or_chain_default(MAINNET.chain(), config.datadir.clone());

        let node_builder_with_components = NodeBuilder::new(config).with_database(db).node(ArbNode);

        let launcher = ArbLauncher {
            ctx: LaunchContext::new(task_executor.clone(), data_dir),
            chain_id: crate::ARB_ONE_CHAIN_ID,
            genesis_block: 0,
            tuning: ArbEngineTuning::reth_defaults(),
            messages: rx,
            feed_latency: None,
            rpc_addr: None,
        };

        let handle = launcher
            .launch_node(node_builder_with_components)
            .await
            .expect("launch must succeed");

        let provider = handle.provider.clone();
        handle
            .wait_for_node_exit()
            .await
            .expect("driver task must succeed");

        // The launcher opens the DB in storage v2; confirm the flag is persisted.
        {
            use reth_provider::DatabaseProviderFactory;
            use reth_storage_api::MetadataProvider;
            let p = provider.database_provider_ro().expect("ro provider");
            assert_eq!(
                p.storage_settings().expect("storage_settings"),
                Some(reth_db_api::models::StorageSettings::v2()),
                "launcher DB must be storage v2"
            );
        }

        assert_eq!(
            provider.best_block_number().unwrap(),
            2,
            "best block must be 2"
        );
        assert!(
            provider.header_by_number(1).unwrap().is_some(),
            "block 1 must exist"
        );
        assert!(
            provider.header_by_number(2).unwrap().is_some(),
            "block 2 must exist"
        );

        let deposit_to = address!("3f1eae7d46d88f08fc2f8ed27fcb2ab183eb2d0e");
        let single_deposit = U256::from(111_000_000_000_000_000u128);
        let state = provider.latest().expect("latest state must open");
        let acct = state
            .basic_account(&deposit_to)
            .expect("account lookup")
            .expect("deposit recipient must exist");
        assert_eq!(
            acct.balance,
            single_deposit * U256::from(2),
            "cumulative balance must be 2× single deposit"
        );
    }
}
