//! D.3 — `ArbExecutorBuilder`: plugs `ArbEvmConfig` into reth's `ExecutorBuilder` trait.

use arb_reth_evm::ArbEvmConfig;
use reth_node_builder::BuilderContext;
use reth_node_builder::components::ExecutorBuilder;

/// Builds [`ArbEvmConfig`] during node assembly.
///
/// The chain id is carried so the EVM can be configured for the correct network
/// (42161 for Arbitrum One mainnet).
#[derive(Debug, Clone, Default)]
pub struct ArbExecutorBuilder {
    pub chain_id: u64,
}

impl ArbExecutorBuilder {
    pub const fn new(chain_id: u64) -> Self {
        Self { chain_id }
    }
}

impl<N> ExecutorBuilder<N> for ArbExecutorBuilder
where
    N: reth_node_builder::FullNodeTypes,
    N::Types: reth_node_types::NodeTypes<Primitives = arb_alloy_consensus::reth::ArbPrimitives>,
{
    type EVM = ArbEvmConfig;

    async fn build_evm(self, _ctx: &BuilderContext<N>) -> eyre::Result<Self::EVM> {
        Ok(ArbEvmConfig::new(self.chain_id))
    }
}
