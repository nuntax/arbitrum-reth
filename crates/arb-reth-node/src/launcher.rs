//! D.3b.2: `ArbLauncher`, a custom `LaunchNode` for the no-engine Arbitrum node.
//!
//! Mirrors reth's `EngineNodeLauncher::launch_node` type-state chain but stops after
//! `.with_components(...)` (no pipeline, no engine orchestrator, no RpcAddOns; AddOns = ()).
//! After standing up the provider stack it extracts `ProviderFactory` + `BlockchainProvider`,
//! constructs an `ArbChainDriver` wired to the provider's `CanonicalInMemoryState`, and spawns
//! a background task that calls `driver.advance()` per message and `driver.flush()` on close.
//!
//! Deadlock rule: never hold a read provider across a `provider_rw()`/`save_blocks()` call.

use core::{future::Future, pin::Pin};
use std::net::SocketAddr;

use alloy_consensus::Header;
use arb_alloy_consensus::reth::ArbPrimitives;
use arb_sequencer_network::sequencer::feed::BroadcastFeedMessage;
use eyre::eyre;
use reth_chain_state::CanonicalInMemoryState;
use reth_db::{Database, database_metrics::DatabaseMetrics};
use reth_evm::ConfigureEvm;
use reth_network_api::noop::NoopNetwork;
use reth_node_api::{FullNodeTypes, NodeTypes, NodeTypesWithDBAdapter};
use reth_node_builder::{
    AddOns, LaunchContext, LaunchNode, Node, NodeBuilderWithComponents, NodeComponents,
    NodeComponentsBuilder, NodeTypesAdapter, RethFullAdapter,
};
use reth_node_builder::hooks::NodeHooks;
use reth_primitives_traits::SealedHeader;
use reth_provider::{
    providers::{BlockchainProvider, NodeTypesForProvider, ProviderNodeTypes},
    ProviderFactory,
};
use reth_rpc_builder::RpcServerHandle;
use reth_storage_api::HeaderProvider;
use reth_tasks::TaskExecutor;
use reth_transaction_pool::noop::NoopTransactionPool;
use reth_trie_db::ChangesetCache;
use tokio::sync::oneshot;

use crate::{
    driver::ArbChainDriver,
    pooled::ArbPooledTransaction,
    rpc::serve_rpc,
};

type ArbNoopNetwork = NoopNetwork;
type ArbNoopPool = NoopTransactionPool<ArbPooledTransaction>;

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
/// infrastructure but skips the engine pipeline and consensus-engine task. Spawns
/// an [`ArbChainDriver`] background task that processes sequencer feed messages exactly
/// once per block.
pub struct ArbLauncher {
    /// Base launch context: task executor + data directory.
    pub ctx: LaunchContext,
    /// Arbitrum chain id (42161 = mainnet, 421614 = Sepolia).
    pub chain_id: u64,
    /// Flush every `persistence_threshold` blocks (1 = flush every block).
    pub persistence_threshold: u64,
    /// Feed channel; each item is `(message, arbos_format_version)`.
    pub messages: tokio::sync::mpsc::Receiver<(BroadcastFeedMessage, u8)>,
    /// Optional HTTP bind address for the `eth_*` RPC server (`None` disables RPC).
    pub rpc_addr: Option<SocketAddr>,
}

impl<N, DB, T, CB>
    LaunchNode<NodeBuilderWithComponents<T, CB, ()>>
    for ArbLauncher
where
    N: Node<RethFullAdapter<DB, N>>
        + NodeTypesForProvider
        + NodeTypes<Primitives = ArbPrimitives, ChainSpec: reth_chainspec::EthChainSpec + reth_chainspec::EthereumHardforks + reth_chainspec::Hardforks>,
    DB: Database + DatabaseMetrics + Clone + Unpin + 'static,
    T: FullNodeTypes<
        Types = N,
        Provider = BlockchainProvider<NodeTypesWithDBAdapter<N, DB>>,
        DB = DB,
    >,
    CB: NodeComponentsBuilder<T> + 'static,
    <CB::Components as NodeComponents<T>>::Evm: ConfigureEvm<Primitives = ArbPrimitives>
        + Into<arb_reth_evm::ArbEvmConfig>
        + Clone,
    NodeTypesWithDBAdapter<N, DB>: ProviderNodeTypes<Primitives = ArbPrimitives>,
    // Explicit equality bounds to help the compiler resolve the associated type projections
    // from NodeTypesWithDBAdapter<N, DB>.
    NodeTypesWithDBAdapter<N, DB>: NodeTypes<ChainSpec = <N as NodeTypes>::ChainSpec, Primitives = ArbPrimitives>,
    NodeTypesWithDBAdapter<N, DB>: reth_node_api::NodeTypesWithDB<DB = DB>,
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
            + NodeTypes<Primitives = ArbPrimitives, ChainSpec: reth_chainspec::EthChainSpec + reth_chainspec::EthereumHardforks + reth_chainspec::Hardforks>,
        DB: Database + DatabaseMetrics + Clone + Unpin + 'static,
        T: FullNodeTypes<
            Types = N,
            Provider = BlockchainProvider<NodeTypesWithDBAdapter<N, DB>>,
            DB = DB,
        >,
        CB: NodeComponentsBuilder<T> + 'static,
        <CB::Components as NodeComponents<T>>::Evm: ConfigureEvm<Primitives = ArbPrimitives>
            + Into<arb_reth_evm::ArbEvmConfig>
            + Clone,
        NodeTypesWithDBAdapter<N, DB>: ProviderNodeTypes<Primitives = ArbPrimitives>,
        NodeTypesWithDBAdapter<N, DB>: NodeTypes<ChainSpec = <N as NodeTypes>::ChainSpec, Primitives = ArbPrimitives>,
        NodeTypesWithDBAdapter<N, DB>: reth_node_api::NodeTypesWithDB<DB = DB>,
    {
        let Self { ctx, chain_id, persistence_threshold, messages, rpc_addr } = self;

        let NodeBuilderWithComponents {
            adapter: NodeTypesAdapter { database },
            rocksdb_provider,
            components_builder,
            add_ons: AddOns { hooks, exexs: _, add_ons: _ },
            config,
        } = target;
        let NodeHooks { on_component_initialized, .. } = hooks;

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
            .await?
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

        // Clone the in-memory state from the provider so the driver updates
        // the SAME instance that BlockchainProvider serves for RPC queries.
        let canonical: CanonicalInMemoryState<ArbPrimitives> =
            provider.canonical_in_memory_state();

        let genesis_tip: SealedHeader<Header> =
            HeaderProvider::sealed_header(&provider, head.number)?
                .ok_or_else(|| eyre!("missing head header at block {}", head.number))?;

        let mut driver: ArbChainDriver<NodeTypesWithDBAdapter<N, DB>> =
            ArbChainDriver::with_canonical_state(
                provider_factory,
                chain_id,
                genesis_tip,
                persistence_threshold,
                canonical,
            );

        let (exit_tx, exit_rx) = oneshot::channel::<eyre::Result<()>>();
        let mut messages_rx = messages;

        task_executor.spawn_critical_task(
            "arb-chain-driver",
            async move {
                let res: eyre::Result<()> = async {
                    while let Some((msg, version)) = messages_rx.recv().await {
                        driver.advance(&msg, version)?;
                    }
                    driver.flush()?;
                    Ok(())
                }
                .await;
                let _ = exit_tx.send(res); // ignore error if receiver was dropped
            },
        );

        let rpc_handle = if let Some(addr) = rpc_addr {
            let runtime = ctx.task_executor().clone();
            let arb_evm_config: arb_reth_evm::ArbEvmConfig =
                ctx.node_adapter().components.evm_config().clone().into();
            let rpc_provider = provider.clone();
            let handle = serve_rpc(
                rpc_provider,
                ArbNoopPool::new(),
                ArbNoopNetwork::default().with_chain_id(chain_id),
                arb_evm_config,
                addr,
                runtime,
            )
            .await?;
            Some(handle)
        } else {
            None
        };

        Ok(ArbNodeHandle { provider, exit_rx, rpc_handle })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{address, U256};
    use arb_sequencer_network::sequencer::feed::BroadcastFeedMessage;
    use reth_chainspec::MAINNET;
    use reth_node_builder::{NodeBuilder, NodeConfig, LaunchNode};
    use reth_provider::{BlockNumReader, HeaderProvider, StateProviderFactory};
    use reth_storage_api::AccountReader;
    use reth_tasks::Runtime;

    use crate::ArbNode;

    /// D.3b.2: `ArbLauncher` boots over reth's `LaunchContext`, processes two deposit messages,
    /// and persists blocks 1 & 2 with cumulative balance = 2 × 111_000_000_000_000_000.
    #[tokio::test(flavor = "multi_thread")]
    async fn launcher_boots_and_produces_blocks() {
        let fixtures_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../arb_revm/testdata/fixtures");
        let json = std::fs::read_to_string(fixtures_dir.join("deposit_message_only.json"))
            .expect("read fixture");
        let feed_msg: BroadcastFeedMessage =
            serde_json::from_str(&json).expect("parse BroadcastFeedMessage");

        let task_executor = Runtime::test();

        let (tx, rx) = tokio::sync::mpsc::channel::<(BroadcastFeedMessage, u8)>(4);
        tx.send((feed_msg.clone(), 0)).await.unwrap();
        tx.send((feed_msg.clone(), 0)).await.unwrap();
        drop(tx);

        let datadir = reth_db::test_utils::tempdir_path();
        let db = reth_db::test_utils::create_test_rw_db_with_datadir(&datadir);

        // Build the ChainPath (data_dir) that LaunchContext needs.
        let maybe_path = reth_node_core::dirs::MaybePlatformPath::<
            reth_node_core::dirs::DataDirPath,
        >::from(datadir.clone());
        let config = NodeConfig::test().with_chain(MAINNET.clone()).with_datadir_args(
            reth_node_core::args::DatadirArgs {
                datadir: maybe_path.clone(),
                ..Default::default()
            },
        );
        let data_dir = maybe_path.unwrap_or_chain_default(
            MAINNET.chain(),
            config.datadir.clone(),
        );

        let node_builder_with_components = NodeBuilder::new(config)
            .with_database(db)
            .node(ArbNode);

        let launcher = ArbLauncher {
            ctx: LaunchContext::new(task_executor.clone(), data_dir),
            chain_id: crate::ARB_ONE_CHAIN_ID,
            persistence_threshold: 1,
            messages: rx,
            rpc_addr: None,
        };

        let handle = launcher
            .launch_node(node_builder_with_components)
            .await
            .expect("launch must succeed");

        let provider = handle.provider.clone();
        handle.wait_for_node_exit().await.expect("driver task must succeed");

        assert_eq!(provider.best_block_number().unwrap(), 2, "best block must be 2");
        assert!(provider.header_by_number(1).unwrap().is_some(), "block 1 must exist");
        assert!(provider.header_by_number(2).unwrap().is_some(), "block 2 must exist");

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
