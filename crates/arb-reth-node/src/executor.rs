//! `ArbExecutorBuilder` plugs `ArbEvmConfig` into reth's `ExecutorBuilder` trait.

use arb_reth_evm::ArbEvmConfig;
use reth_chainspec::EthChainSpec;
use reth_node_builder::BuilderContext;
use reth_node_builder::components::ExecutorBuilder;

/// Builds [`ArbEvmConfig`] during node assembly.
///
/// The chain id comes from the node's chain spec at build time, so the executor's EVM config
/// always matches the chain the node booted with (`--chain` derives it from the config JSON,
/// `--snapshot-head` from the head header). Reading it from the spec keeps it from diverging
/// from the driver/RPC chain id, which the launcher threads from the same source.
#[derive(Debug, Clone, Default)]
pub struct ArbExecutorBuilder;

impl<N> ExecutorBuilder<N> for ArbExecutorBuilder
where
    N: reth_node_builder::FullNodeTypes,
    N::Types: reth_node_types::NodeTypes<Primitives = arb_alloy_consensus::reth::ArbPrimitives>,
{
    type EVM = ArbEvmConfig;

    async fn build_evm(self, ctx: &BuilderContext<N>) -> eyre::Result<Self::EVM> {
        Ok(ArbEvmConfig::new(ctx.chain_spec().chain().id()))
    }
}
