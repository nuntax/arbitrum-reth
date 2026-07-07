//! arb-reth-node: the Arbitrum node skeleton.
//!
//! # Why this is not a reth sync `Stage`
//!
//! reth's staged pipeline is download-then-execute: `HeaderStage`/`BodyStage` download a
//! trustless header+body, `ExecutionStage` executes the stored body, and `MerkleStage` computes the
//! state root and validates it against the stored header's `state_root`. That model exists to
//! check a header you downloaded from a peer.
//!
//! An Arbitrum node has no such header. It is execute-to-derive: a sequencer message is the
//! input, and the block (including its state root) is the output of executing that message.
//! We mint it. So we follow the path reth uses for locally-produced (payload/engine) blocks:
//! produce, execute, compute the state root, seal the header, persist the executed block
//! via the provider's block-writer (`save_blocks` / `ExecutedBlock`).
//!
//! `ArbNode : NodeTypes` lets a reth `ProviderFactory` stand up over an MDBX database. The four
//! associated types are:
//!
//! - `Primitives = ArbPrimitives`: Arbitrum tx/receipt/block types from arb-alloy.
//! - `ChainSpec  = reth_chainspec::ChainSpec`: reth's stock chain spec (satisfies
//!   `EthChainSpec<Header = alloy_consensus::Header>`).
//! - `Storage    = EthStorage<ArbTxEnvelope>`: reth's generic body storage, parameterised
//!   for Arbitrum transactions.
//! - `Payload    = ArbPayloadTypes`: a minimal stub that satisfies the `PayloadTypes` bound;
//!   the execute-once driver never builds engine payloads.

// Some modules (`persist`, `pooled`) reference `alloc::`; keep the crate available.
extern crate alloc;

pub mod genesis;
pub use genesis::{
    arb_chain_spec, arb_chain_spec_with_header, arbos_init_from_chain_config_json,
    arbos_init_from_parsed, read_head_header,
};

pub mod hashed_db;
pub use hashed_db::{account_by_address, code_of, storage_at};

pub mod persist;
pub use persist::persist_executed_block;

// The L1-derivation catch-up runtime and its resume-checkpoint log now live in the
// `arb-reth-sync` crate; re-export for API stability.
pub use arb_reth_sync::l1_sync::{run_l1_sync, L1SyncConfig};
pub use arb_reth_sync::resume::{L1ResumeCheckpoint, L1ResumeLog};

pub mod launcher;
pub use launcher::{ArbLauncher, ArbNodeHandle};

pub mod executor;
pub use executor::ArbExecutorBuilder;

pub mod pooled;
pub use pooled::ArbPooledTransaction;

// The `eth_*` RPC layer now lives in the `arb-reth-rpc` crate; re-export for API stability.
pub use arb_reth_rpc::{ArbReceiptConverter, serve_rpc};

// The engine-tree driver, the payload-type stubs, and the minimal engine validator now live in the
// `arb-reth-engine` crate; re-export for API stability.
pub use arb_reth_engine::{
    ArbBuiltPayload, ArbEngineDriver, ArbEngineTuning, ArbExecutionData, ArbPayloadAttributes,
    ArbPayloadTypes, ArbPayloadValidator,
};

use alloy_consensus::Header;
use reth_node_types::NodeTypes;
use reth_storage_api::EthStorage;

use arb_alloy_consensus::{ArbTxEnvelope, reth::ArbPrimitives};

/// Arbitrum One mainnet chain id.
pub const ARB_ONE_CHAIN_ID: u64 = 42161;

/// Arbitrum node type. Wires Arbitrum primitives into reth's `NodeTypes` surface.
///
/// This struct is stateless; it exists only as a type-level tag so that reth's
/// generic provider infrastructure can be instantiated for Arbitrum.
#[derive(Debug, Clone, Default)]
pub struct ArbNode;

impl NodeTypes for ArbNode {
    type Primitives = ArbPrimitives;
    type ChainSpec = reth_chainspec::ChainSpec;
    type Storage = EthStorage<ArbTxEnvelope, Header>;
    type Payload = ArbPayloadTypes;
}

/// Network primitives for Arbitrum, used by the noop network builder.
///
/// Both `BroadcastedTransaction` and `PooledTransaction` are `ArbTxEnvelope`;
/// the noop network never serves pooled txs (Arbitrum has no p2p tx gossip).
pub type ArbNetworkPrimitives =
    reth_eth_wire_types::BasicNetworkPrimitives<ArbPrimitives, ArbTxEnvelope>;

// impl Node<N> for ArbNode: required so `builder.node(ArbNode)` produces the
// `NodeBuilderWithComponents` our `ArbLauncher` consumes. All components are noop
// except the executor (Arbitrum has no tx gossip, p2p, or fork-choice engine).
// `ArbLauncher` reuses reth's `LaunchContext` for DB/provider/tasks but skips the sync
// pipeline and consensus-engine orchestrator, spawning `ArbEngineDriver` to drive reth's
// engine tree directly; AddOns = () (no engine-coupled RpcAddOns).

use reth_node_builder::components::{
    ComponentsBuilder, NoopConsensusBuilder, NoopNetworkBuilder, NoopPayloadBuilder,
    NoopTransactionPoolBuilder,
};
use reth_node_builder::{FullNodeTypes, Node};

impl<N> Node<N> for ArbNode
where
    N: FullNodeTypes<Types = Self>,
{
    type ComponentsBuilder = ComponentsBuilder<
        N,
        NoopTransactionPoolBuilder<ArbPooledTransaction>,
        NoopPayloadBuilder,
        NoopNetworkBuilder<ArbNetworkPrimitives>,
        ArbExecutorBuilder,
        NoopConsensusBuilder,
    >;

    type AddOns = ();

    fn components_builder(&self) -> Self::ComponentsBuilder {
        ComponentsBuilder::<(), (), (), (), (), ()>::default()
            .node_types::<N>()
            .executor(ArbExecutorBuilder)
            .noop_pool::<ArbPooledTransaction>()
            .noop_payload()
            .noop_network::<ArbNetworkPrimitives>()
            .noop_consensus()
    }

    fn add_ons(&self) -> Self::AddOns {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_chainspec::MAINNET;

    /// Smoke test: verify that a reth `ProviderFactory` can be instantiated for `ArbNode`.
    #[test]
    fn provider_factory_stands_up() {
        let chain_spec = MAINNET.clone();
        let factory = reth_provider::test_utils::create_test_provider_factory_with_node_types::<
            ArbNode,
        >(chain_spec);
        let provider = factory.provider().expect("provider should open");
        use reth_provider::BlockNumReader;
        let best = provider
            .best_block_number()
            .expect("best_block_number should succeed");
        assert_eq!(best, 0, "fresh DB should have block 0 as best");
    }
}
