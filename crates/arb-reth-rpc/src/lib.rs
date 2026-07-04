//! Arbitrum `eth_*` RPC layer.
//!
//! Provides:
//! - [`ArbReceiptConverter`]: converts `ArbReceiptEnvelope<Log>` primitives to
//!   [`ArbTransactionReceipt`] RPC responses, surfacing `gas_used_for_l1`.
//! - [`serve_rpc`]: standalone helper that wires an [`EthApi`] with the Arb
//!   converter into a jsonrpsee HTTP server and returns its handle.
//!
//! The helper is intentionally isolated: it does not use `RpcAddOns`, does not touch
//! the engine, and can be swapped out later if we migrate to `RpcAddOns`-based wiring.

use std::{fmt::Debug, net::SocketAddr};

use alloy_consensus::{Receipt, ReceiptWithBloom};
use alloy_primitives::Log;
use alloy_rpc_types_eth::Log as RpcLog;
use arb_alloy_consensus::{
    ArbReceipt, ArbReceiptEnvelope, ArbTxEnvelope,
    reth::ArbPrimitives,
};
use arb_alloy_network::Arbitrum;
use arb_alloy_rpc_types::ArbTransactionReceipt;
use arb_reth_evm::ArbEvmConfig;
use eyre::WrapErr;
use reth_chain_state::CanonStateSubscriptions;
use reth_chainspec::{ChainSpecProvider, EthChainSpec, EthereumHardforks, Hardforks};
use reth_consensus::noop::NoopConsensus;
use reth_engine_primitives::ConsensusEngineEvent;
use reth_node_api::NodePrimitives;
use reth_primitives_traits::SealedBlock;
use reth_rpc::EthApi;
use reth_rpc::eth::EthApiBuilder;
use reth_rpc_builder::{
    RpcModuleBuilder, RpcServerConfig, RpcServerHandle,
    TransportRpcModuleConfig,
};
use reth_rpc_convert::{RpcConverter, transaction::{ConvertReceiptInput, ReceiptConverter}};
use reth_rpc_eth_api::{
    FullEthApiServer,
    node::RpcNodeCoreAdapter,
};
use reth_rpc_eth_types::{EthApiError, receipt::build_receipt};
use reth_rpc_server_types::RethRpcModule;
use reth_storage_api::{
    BalProvider, BlockReaderIdExt, FullRpcProvider, PruneCheckpointReader,
    StageCheckpointReader,
};
use reth_tasks::Runtime;
use reth_tokio_util::EventSender;

/// Converts `ArbReceiptEnvelope<Log>` primitives into [`ArbTransactionReceipt`] RPC responses.
///
/// Analogous to op-reth's `OpReceiptConverter`. `gas_used_for_l1` is stored on the receipt
/// by the block executor; no L1-fee hardfork math is needed.
#[derive(Debug, Clone)]
pub struct ArbReceiptConverter<Provider> {
    provider: Provider,
}

impl<Provider> ArbReceiptConverter<Provider> {
    /// Creates a new [`ArbReceiptConverter`].
    pub const fn new(provider: Provider) -> Self {
        Self { provider }
    }
}

impl<Provider, N> ReceiptConverter<N> for ArbReceiptConverter<Provider>
where
    N: NodePrimitives<
        Receipt = ArbReceiptEnvelope<Log>,
        SignedTx = ArbTxEnvelope,
    >,
    Provider: Debug + Clone + 'static,
{
    type RpcReceipt = ArbTransactionReceipt;
    type Error = EthApiError;

    fn convert_receipts(
        &self,
        inputs: Vec<ConvertReceiptInput<'_, N>>,
    ) -> Result<Vec<Self::RpcReceipt>, Self::Error> {
        inputs.into_iter().map(build_arb_receipt).collect()
    }

    fn convert_receipts_with_block(
        &self,
        inputs: Vec<ConvertReceiptInput<'_, N>>,
        _block: &SealedBlock<N::Block>,
    ) -> Result<Vec<Self::RpcReceipt>, Self::Error> {
        self.convert_receipts(inputs)
    }
}

/// Maps a consensus `ArbReceiptEnvelope<Log>` to an RPC `ArbReceiptEnvelope<RpcLog>`,
/// returning `(gas_used_for_l1, rpc_envelope)`.
fn map_arb_receipt_envelope(
    envelope: ArbReceiptEnvelope<Log>,
    next_log_index: usize,
    meta: reth_primitives_traits::TransactionMeta,
) -> (u64, ArbReceiptEnvelope<RpcLog>) {
    /// Maps `ReceiptWithBloom<ArbReceipt<Log>>` to `ReceiptWithBloom<ArbReceipt<RpcLog>>`
    /// while capturing `gas_used_for_l1`.
    fn map_rwb(
        rwb: ReceiptWithBloom<ArbReceipt<Log>>,
        next_log_index: usize,
        meta: reth_primitives_traits::TransactionMeta,
    ) -> (u64, ReceiptWithBloom<ArbReceipt<RpcLog>>) {
        let logs_bloom = rwb.logs_bloom;
        let ArbReceipt { inner: receipt, gas_used_for_l1 } = rwb.receipt;
        let Receipt { status, cumulative_gas_used, logs } = receipt;

        let mut idx = next_log_index;
        let rpc_logs: Vec<RpcLog> = logs
            .into_iter()
            .map(|log| {
                let log_index = idx;
                idx += 1;
                RpcLog {
                    inner: log,
                    block_hash: Some(meta.block_hash),
                    block_number: Some(meta.block_number),
                    block_timestamp: Some(meta.timestamp),
                    transaction_hash: Some(meta.tx_hash),
                    transaction_index: Some(meta.index),
                    log_index: Some(log_index as u64),
                    removed: false,
                }
            })
            .collect();

        let arb_rpc = ArbReceipt {
            inner: Receipt { status, cumulative_gas_used, logs: rpc_logs },
            gas_used_for_l1,
        };
        (gas_used_for_l1, ReceiptWithBloom { receipt: arb_rpc, logs_bloom })
    }

    match envelope {
        ArbReceiptEnvelope::Legacy(rwb) => {
            let (g, r) = map_rwb(rwb, next_log_index, meta);
            (g, ArbReceiptEnvelope::Legacy(r))
        }
        ArbReceiptEnvelope::Eip2930(rwb) => {
            let (g, r) = map_rwb(rwb, next_log_index, meta);
            (g, ArbReceiptEnvelope::Eip2930(r))
        }
        ArbReceiptEnvelope::Eip1559(rwb) => {
            let (g, r) = map_rwb(rwb, next_log_index, meta);
            (g, ArbReceiptEnvelope::Eip1559(r))
        }
        ArbReceiptEnvelope::Eip4844(rwb) => {
            let (g, r) = map_rwb(rwb, next_log_index, meta);
            (g, ArbReceiptEnvelope::Eip4844(r))
        }
        ArbReceiptEnvelope::Eip7702(rwb) => {
            let (g, r) = map_rwb(rwb, next_log_index, meta);
            (g, ArbReceiptEnvelope::Eip7702(r))
        }
        ArbReceiptEnvelope::Deposit(rwb) => {
            let (g, r) = map_rwb(rwb, next_log_index, meta);
            (g, ArbReceiptEnvelope::Deposit(r))
        }
        ArbReceiptEnvelope::Unsigned(rwb) => {
            let (g, r) = map_rwb(rwb, next_log_index, meta);
            (g, ArbReceiptEnvelope::Unsigned(r))
        }
        ArbReceiptEnvelope::Contract(rwb) => {
            let (g, r) = map_rwb(rwb, next_log_index, meta);
            (g, ArbReceiptEnvelope::Contract(r))
        }
        ArbReceiptEnvelope::Retry(rwb) => {
            let (g, r) = map_rwb(rwb, next_log_index, meta);
            (g, ArbReceiptEnvelope::Retry(r))
        }
        ArbReceiptEnvelope::SubmitRetryable(rwb) => {
            let (g, r) = map_rwb(rwb, next_log_index, meta);
            (g, ArbReceiptEnvelope::SubmitRetryable(r))
        }
        ArbReceiptEnvelope::Internal(rwb) => {
            let (g, r) = map_rwb(rwb, next_log_index, meta);
            (g, ArbReceiptEnvelope::Internal(r))
        }
    }
}

/// Builds a single [`ArbTransactionReceipt`] from a [`ConvertReceiptInput`].
fn build_arb_receipt<N>(
    input: ConvertReceiptInput<'_, N>,
) -> Result<ArbTransactionReceipt, EthApiError>
where
    N: NodePrimitives<Receipt = ArbReceiptEnvelope<Log>>,
{
    // Use a Cell to capture gas_used_for_l1 from within the mapping closure.
    let gas_cell = std::cell::Cell::new(0u64);

    let core = build_receipt::<N, _>(input, None, |envelope, next_log_index, meta| {
        let (g, rpc_envelope) = map_arb_receipt_envelope(envelope, next_log_index, meta);
        gas_cell.set(g);
        rpc_envelope
    });

    Ok(ArbTransactionReceipt {
        inner: core,
        gas_used_for_l1: gas_cell.get(),
        // l1_block_number: not available at receipt-conversion time without reading block
        // extra_data. Will be populated in Stage F from block metadata.
        l1_block_number: None,
        timeboosted: None,
    })
}

/// Convenience type alias for the Arb [`RpcConverter`].
pub type ArbRpcConverter<Provider> = RpcConverter<
    Arbitrum,
    ArbEvmConfig,
    ArbReceiptConverter<Provider>,
>;

/// Starts a jsonrpsee HTTP server exposing `eth_*` methods for Arbitrum.
///
/// Isolated from the engine and `RpcAddOns`. Returns a [`RpcServerHandle`] whose
/// lifetime controls the server. Generic over `Pool` and `Network`; pass a noop pool
/// with `Consensus = ArbTxEnvelope` for sequencer-only nodes.
pub async fn serve_rpc<Provider, Pool, Network>(
    provider: Provider,
    pool: Pool,
    network: Network,
    evm_config: ArbEvmConfig,
    addr: SocketAddr,
    runtime: Runtime,
) -> eyre::Result<RpcServerHandle>
where
    Provider: FullRpcProvider<
            Block = <ArbPrimitives as NodePrimitives>::Block,
            Receipt = <ArbPrimitives as NodePrimitives>::Receipt,
            Header = <ArbPrimitives as NodePrimitives>::BlockHeader,
            Transaction = <ArbPrimitives as NodePrimitives>::SignedTx,
        > + BlockReaderIdExt<
            Block = <ArbPrimitives as NodePrimitives>::Block,
            Receipt = <ArbPrimitives as NodePrimitives>::Receipt,
            Header = <ArbPrimitives as NodePrimitives>::BlockHeader,
            Transaction = <ArbPrimitives as NodePrimitives>::SignedTx,
        > + ChainSpecProvider<
            ChainSpec: EthChainSpec<Header = <ArbPrimitives as NodePrimitives>::BlockHeader>
                + EthereumHardforks
                + Hardforks,
        > + CanonStateSubscriptions<Primitives = ArbPrimitives>
        + reth_chain_state::ForkChoiceSubscriptions<
            Header = <ArbPrimitives as NodePrimitives>::BlockHeader,
        > + reth_chain_state::PersistedBlockSubscriptions
        + reth_storage_api::AccountReader
        + reth_storage_api::ChangeSetReader
        + StageCheckpointReader
        + PruneCheckpointReader
        + BalProvider
        + Debug
        + Clone
        + Unpin
        + 'static,
    Pool: reth_transaction_pool::TransactionPool<
            Transaction: reth_transaction_pool::PoolTransaction<
                Consensus = <ArbPrimitives as NodePrimitives>::SignedTx,
            >,
        > + Debug
        + Clone
        + Unpin
        + Send
        + Sync
        + 'static,
    Network: reth_network_api::NetworkInfo
        + reth_network_api::Peers
        + Debug
        + Clone
        + Unpin
        + 'static,
    // RpcNodeCoreAdapter must satisfy RpcNodeCore so EthApiBuilder::new_with_components works.
    RpcNodeCoreAdapter<Provider, Pool, Network, ArbEvmConfig>: reth_rpc_eth_api::RpcNodeCore<
        Provider = Provider,
        Pool = Pool,
        Network = Network,
        Evm = ArbEvmConfig,
        Primitives = ArbPrimitives,
    >,
    // The built EthApi must satisfy FullEthApiServer.
    EthApi<RpcNodeCoreAdapter<Provider, Pool, Network, ArbEvmConfig>, ArbRpcConverter<Provider>>:
        FullEthApiServer<Provider = Provider, Pool = Pool>,
{
    let rpc_converter = RpcConverter::new(ArbReceiptConverter::new(provider.clone()));

    let components = RpcNodeCoreAdapter::new(
        provider.clone(),
        pool.clone(),
        network.clone(),
        evm_config.clone(),
    );

    let eth_api = EthApiBuilder::new_with_components(components)
        .with_rpc_converter(rpc_converter)
        .task_spawner(runtime.clone())
        .build();

    // Required by RpcModuleBuilder::build; we never emit engine events.
    let engine_events = EventSender::<ConsensusEngineEvent<ArbPrimitives>>::default();

    let module_config =
        TransportRpcModuleConfig::default().with_http([RethRpcModule::Eth]);

    let modules = RpcModuleBuilder::<ArbPrimitives, _, _, _, _, _>::new(
        provider,
        pool,
        network,
        runtime,
        evm_config,
        NoopConsensus::default(),
    )
    .build(module_config, eth_api, engine_events);

    let handle = RpcServerConfig::default()
        .with_http(jsonrpsee::server::ServerConfigBuilder::default())
        .with_http_address(addr)
        .start(&modules)
        .await
        .wrap_err("failed to start Arbitrum RPC HTTP server")?;

    Ok(handle)
}
