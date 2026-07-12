//! `ArbExecutorBuilder` plugs `ArbEvmConfig` into reth's `ExecutorBuilder` trait.

use arb_reth_evm::ArbEvmConfig;
use arb_revm::{constants::ARBOS_STATE_ADDRESS, storage::read_serialized_chain_config};
use reth_chainspec::EthChainSpec;
use reth_node_builder::BuilderContext;
use reth_node_builder::components::ExecutorBuilder;
use reth_provider::{StateProvider, StateProviderFactory};
use serde::Deserialize;

/// Extract `arbitrum.MaxCodeSize` from a Nitro serialized chain-config JSON blob. Returns 0 when
/// absent (the caller treats 0 as "use the default"), mirroring Nitro's `MaxCodeSize()==0` sentinel.
fn max_code_size_from_serialized_config(serialized_chain_config: &[u8]) -> u64 {
    #[derive(Deserialize)]
    struct Wrapper {
        arbitrum: Option<ArbParams>,
    }
    #[derive(Deserialize)]
    struct ArbParams {
        #[serde(rename = "MaxCodeSize", default)]
        max_code_size: u64,
    }
    serde_json::from_slice::<Wrapper>(serialized_chain_config)
        .ok()
        .and_then(|w| w.arbitrum)
        .map(|a| a.max_code_size)
        .unwrap_or(0)
}

/// Builds [`ArbEvmConfig`] during node assembly.
///
/// The chain id comes from the node's chain spec. The chain's `MaxCodeSize` is recovered from ArbOS
/// state (subspace 7 = `chainConfigSubspace`), exactly as Nitro loads the chain config from state at
/// startup (`cmd/replay` / `TryReadStoredChainConfig`). This works for every boot path — orbit
/// (config written at genesis) and snapshot (config imported) — because the launcher runs
/// `with_genesis()` before component build, so the head state always carries the config.
#[derive(Debug, Clone, Default)]
pub struct ArbExecutorBuilder;

impl<N> ExecutorBuilder<N> for ArbExecutorBuilder
where
    N: reth_node_builder::FullNodeTypes,
    N::Types: reth_node_types::NodeTypes<Primitives = arbitrum_alloy_consensus::reth::ArbPrimitives>,
{
    type EVM = ArbEvmConfig;

    async fn build_evm(self, ctx: &BuilderContext<N>) -> eyre::Result<Self::EVM> {
        let chain_id = ctx.chain_spec().chain().id();

        // Read the serialized chain config out of ArbOS state and pull MaxCodeSize from it. Absent/0
        // (a chain that doesn't raise the limit) falls back to the EIP-170 default in `ArbEvmConfig`.
        let state = ctx.provider().latest()?;
        let serialized = read_serialized_chain_config(|slot| {
            state
                .storage(ARBOS_STATE_ADDRESS, slot)
                .ok()
                .flatten()
                .unwrap_or_default()
        });
        let max_code_size = max_code_size_from_serialized_config(&serialized);

        Ok(ArbEvmConfig::new(chain_id).with_max_code_size(max_code_size as usize))
    }
}
