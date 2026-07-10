//! Canonical reth `RpcAddOns` wiring for `ArbNode` (position 3: canonical RPC surface, direct-tree
//! guts). Mirrors reth's `EthereumEthApiBuilder`/`EthereumAddOns` but swaps in the Arbitrum receipt
//! converter and payload validator.
//!
//! This lets `ArbNode` declare `type AddOns = ArbAddOns<..>` and serve the full RPC fleet
//! (eth/trace/debug/net/web3/txpool + ws) through reth's own `launch_add_ons` wiring instead of the
//! bespoke `serve_rpc`. We serve no engine API: this node is self-driven from L1 derivation, so the
//! beacon-engine handle is a stub and the engine API is [`NoopEngineApiBuilder`].

use arb_reth_engine::ArbPayloadValidator;
use arb_reth_rpc::{ArbReceiptConverter, ArbRpcConverter};

use reth_evm::ConfigureEvm;
use reth_node_api::{AddOnsContext, FullNodeComponents, HeaderTy, NodeTypes};
use reth_node_builder::rpc::{
    EthApiBuilder, EthApiCtx, NoopEngineApiBuilder, PayloadValidatorBuilder, RpcAddOns,
};
use reth_rpc::EthApi;
use reth_rpc_eth_api::helpers::pending_block::BuildPendingEnv;
use reth_rpc_eth_api::FullEthApiServer;

/// Builds the Arbitrum `eth` API for reth's `RpcAddOns`. Reuses reth's preconfigured
/// [`EthApiCtx::eth_api_builder`] and only swaps the RPC converter for the Arbitrum one
/// (surfaces `gas_used_for_l1` on receipts).
#[derive(Debug, Default, Clone)]
pub struct ArbEthApiBuilder;

impl<N> EthApiBuilder<N> for ArbEthApiBuilder
where
    N: FullNodeComponents<
        Types: NodeTypes<
            ChainSpec: reth_chainspec::Hardforks + reth_chainspec::EthereumHardforks,
        >,
        Evm: ConfigureEvm<NextBlockEnvCtx: BuildPendingEnv<HeaderTy<N::Types>>>,
    >,
    EthApi<N, ArbRpcConverter<N::Provider>>:
        FullEthApiServer<Provider = N::Provider, Pool = N::Pool>,
{
    type EthApi = EthApi<N, ArbRpcConverter<N::Provider>>;

    async fn build_eth_api(self, ctx: EthApiCtx<'_, N>) -> eyre::Result<Self::EthApi> {
        let provider = ctx.components.provider().clone();
        let converter = ArbRpcConverter::new(ArbReceiptConverter::new(provider));
        Ok(ctx.eth_api_builder().with_rpc_converter(converter).build())
    }
}

/// Supplies the minimal [`ArbPayloadValidator`] for reth's `RpcAddOns` payload-validator slot.
/// Never invoked on our node (engine API is noop), but the type must satisfy the bound.
#[derive(Debug, Default, Clone)]
pub struct ArbPayloadValidatorBuilder;

impl<N> PayloadValidatorBuilder<N> for ArbPayloadValidatorBuilder
where
    N: FullNodeComponents<Types: NodeTypes<Payload = arb_reth_engine::ArbPayloadTypes>>,
{
    type Validator = ArbPayloadValidator;

    async fn build(self, _ctx: &AddOnsContext<'_, N>) -> eyre::Result<Self::Validator> {
        Ok(ArbPayloadValidator)
    }
}

/// Canonical reth `RpcAddOns` for `ArbNode`. Arbitrum eth-api + payload validator; engine API and
/// validator use reth's `Basic*` defaults (backed at launch by a stub beacon-engine handle, since
/// this node is self-driven from L1 derivation and never takes external engine drive).
pub type ArbAddOns<N> =
    RpcAddOns<N, ArbEthApiBuilder, ArbPayloadValidatorBuilder, NoopEngineApiBuilder>;

/// Constructs [`ArbAddOns`] for `ArbNode` (there is no `Default` impl on `RpcAddOns`).
pub fn arb_add_ons<N>() -> ArbAddOns<N>
where
    N: FullNodeComponents,
    ArbEthApiBuilder: EthApiBuilder<N>,
{
    RpcAddOns::new(
        ArbEthApiBuilder,
        ArbPayloadValidatorBuilder,
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
    )
}
