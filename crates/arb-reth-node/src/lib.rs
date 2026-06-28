//! arb-reth-node: the Arbitrum node skeleton (Stage D.2).
//!
//! # Why this is not a reth sync `Stage`
//!
//! reth's staged pipeline is *download-then-execute*: `HeaderStage`/`BodyStage` download a
//! trustless header+body, `ExecutionStage` executes the stored body, and `MerkleStage` computes the
//! state root and **validates it against the stored header's `state_root`**. That model exists to
//! check a header you *downloaded* from a peer.
//!
//! An Arbitrum node has no such header. It is **execute-to-derive**: a sequencer message is the
//! input, and the block (including its state root) is the *output* of executing that message.
//! We mint it. So we follow the path reth uses for locally-produced (payload/engine) blocks:
//! produce, execute, compute the state root, seal the header, persist the executed block
//! via the provider's block-writer (`save_blocks` / `ExecutedBlock`).
//!
//! # D.2.1: `NodeTypes` + `ProviderFactory` smoke test
//!
//! This increment wires `ArbNode : NodeTypes` so that a reth `ProviderFactory` can stand up
//! over a temp-MDBX database. The four associated types are:
//!
//! - `Primitives = ArbPrimitives`: Arbitrum tx/receipt/block types from arb-alloy.
//! - `ChainSpec  = reth_chainspec::ChainSpec`: reth's stock chain spec (satisfies
//!   `EthChainSpec<Header = alloy_consensus::Header>`).
//! - `Storage    = EthStorage<ArbTxEnvelope>`: reth's generic body storage, parameterised
//!   for Arbitrum transactions.
//! - `Payload    = ArbPayloadTypes`: a minimal stub that satisfies the `PayloadTypes` bound;
//!   the execute-once driver never builds engine payloads.

extern crate alloc;

use alloc::sync::Arc;

pub mod genesis;
pub use genesis::{
    arb_chain_spec, arb_chain_spec_with_header, arbos_init_from_chain_config_json,
    arbos_init_from_parsed, read_head_header,
};

pub mod hashed_db;
pub use hashed_db::{HashedStateDb, account_by_address, code_of, storage_at};

pub mod persist;
pub use persist::persist_executed_block;

pub mod driver;
pub use driver::ArbChainDriver;

pub mod l1_sync;
pub use l1_sync::{run_l1_sync, L1SyncConfig};

pub mod node;
pub use node::run as run_node;

pub mod launcher;
pub use launcher::{ArbLauncher, ArbNodeHandle};

pub mod executor;
pub use executor::ArbExecutorBuilder;

pub mod pooled;
pub use pooled::ArbPooledTransaction;

pub mod rpc;
pub use rpc::{ArbReceiptConverter, serve_rpc};

use alloy_consensus::Header;
use alloy_eips::eip4895::Withdrawal;
use alloy_primitives::{Bytes, U256};
use alloy_rpc_types_engine::{
    ExecutionData, ExecutionPayload as AlloyExecutionPayload, PayloadId,
};
use reth_node_types::NodeTypes;
use reth_payload_primitives::{
    BuiltPayload, ExecutionPayload, PayloadAttributes, PayloadTypes,
};
use reth_primitives_traits::{NodePrimitives, RecoveredBlock, SealedBlock};
use reth_storage_api::EthStorage;

use arb_alloy_consensus::{
    ArbTxEnvelope,
    reth::{ArbBlock, ArbPrimitives},
};

/// Arbitrum One mainnet chain id.
pub const ARB_ONE_CHAIN_ID: u64 = 42161;

/// A minimal built-payload stub for Arbitrum.
///
/// Exists solely to satisfy `NodeTypes::Payload`; the execute-once driver never builds engine payloads.
#[derive(Debug, Clone)]
pub struct ArbBuiltPayload {
    block: Arc<RecoveredBlock<ArbBlock>>,
    fees: U256,
}

impl ArbBuiltPayload {
    /// Creates a new `ArbBuiltPayload`.
    pub fn new(block: Arc<RecoveredBlock<ArbBlock>>, fees: U256) -> Self {
        Self { block, fees }
    }
}

impl BuiltPayload for ArbBuiltPayload {
    type Primitives = ArbPrimitives;

    fn block(&self) -> &SealedBlock<<Self::Primitives as NodePrimitives>::Block> {
        self.block.sealed_block()
    }

    fn fees(&self) -> U256 {
        self.fees
    }

    fn requests(&self) -> Option<alloy_eips::eip7685::Requests> {
        None
    }
}

/// Thin wrapper around [`ExecutionData`] for Arbitrum.
///
/// Wraps alloy's `ExecutionData` so that `From<ArbBuiltPayload>` can be implemented (orphan rule:
/// we own both types). Exists solely to satisfy `NodeTypes::Payload`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ArbExecutionData(pub ExecutionData);

impl From<ArbBuiltPayload> for ArbExecutionData {
    fn from(payload: ArbBuiltPayload) -> Self {
        let block_hash = payload.block.hash();
        let block = Arc::unwrap_or_clone(payload.block).into_block();
        let (execution_payload, sidecar) =
            AlloyExecutionPayload::from_block_unchecked_with_extras(
                block_hash,
                &block,
                None, // no block access list
            );
        ArbExecutionData(ExecutionData { payload: execution_payload, sidecar })
    }
}

impl ExecutionPayload for ArbExecutionData {
    fn parent_hash(&self) -> alloy_primitives::B256 {
        self.0.parent_hash()
    }

    fn block_hash(&self) -> alloy_primitives::B256 {
        self.0.block_hash()
    }

    fn block_number(&self) -> u64 {
        self.0.block_number()
    }

    fn withdrawals(&self) -> Option<&alloc::vec::Vec<Withdrawal>> {
        self.0.withdrawals()
    }

    fn block_access_list(&self) -> Option<&Bytes> {
        self.0.block_access_list()
    }

    fn parent_beacon_block_root(&self) -> Option<alloy_primitives::B256> {
        self.0.parent_beacon_block_root()
    }

    fn timestamp(&self) -> u64 {
        self.0.timestamp()
    }

    fn gas_used(&self) -> u64 {
        self.0.gas_used()
    }

    fn gas_limit(&self) -> u64 {
        self.0.gas_limit()
    }

    fn transaction_count(&self) -> usize {
        self.0.transaction_count()
    }

    fn slot_number(&self) -> Option<u64> {
        self.0.slot_number()
    }
}

/// Minimal payload-attributes stub. Exists solely to satisfy `NodeTypes::Payload`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ArbPayloadAttributes {
    /// Timestamp (required by `PayloadAttributes`).
    pub timestamp: u64,
}

impl PayloadAttributes for ArbPayloadAttributes {
    fn payload_id(&self, parent: &alloy_primitives::B256) -> PayloadId {
        reth_payload_primitives::payload_id(
            parent,
            &alloy_rpc_types_engine::PayloadAttributes {
                timestamp: self.timestamp,
                prev_randao: Default::default(),
                suggested_fee_recipient: Default::default(),
                withdrawals: None,
                parent_beacon_block_root: None,
                slot_number: None,
                target_gas_limit: None,
            },
        )
    }

    fn timestamp(&self) -> u64 {
        self.timestamp
    }

    fn withdrawals(&self) -> Option<&alloc::vec::Vec<Withdrawal>> {
        None
    }

    fn parent_beacon_block_root(&self) -> Option<alloy_primitives::B256> {
        None
    }

    fn slot_number(&self) -> Option<u64> {
        None
    }
}

/// Payload-types stub for [`ArbNode`]. Exists solely to satisfy `NodeTypes::Payload`.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ArbPayloadTypes;

impl PayloadTypes for ArbPayloadTypes {
    type ExecutionData = ArbExecutionData;
    type BuiltPayload = ArbBuiltPayload;
    type PayloadAttributes = ArbPayloadAttributes;

    fn block_to_payload(
        block: SealedBlock<<<ArbBuiltPayload as BuiltPayload>::Primitives as NodePrimitives>::Block>,
        _bal: Option<Bytes>,
    ) -> Self::ExecutionData {
        let block_hash = block.hash();
        let (execution_payload, sidecar) =
            AlloyExecutionPayload::from_block_unchecked_with_extras(
                block_hash,
                &block.into_block(),
                None,
            );
        ArbExecutionData(ExecutionData { payload: execution_payload, sidecar })
    }
}

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
// `ArbLauncher` reuses reth's `LaunchContext` for DB/provider/tasks but skips the
// engine pipeline and spawns `ArbChainDriver` directly; AddOns = () (no engine-coupled
// RpcAddOns). See `launcher.rs` and `docs/stage-d2-handoff.md` §12.

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
            .executor(ArbExecutorBuilder::new(ARB_ONE_CHAIN_ID))
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
        let factory =
            reth_provider::test_utils::create_test_provider_factory_with_node_types::<ArbNode>(
                chain_spec,
            );
        let provider = factory.provider().expect("provider should open");
        use reth_provider::BlockNumReader;
        let best = provider.best_block_number().expect("best_block_number should succeed");
        assert_eq!(best, 0, "fresh DB should have block 0 as best");
    }
}
