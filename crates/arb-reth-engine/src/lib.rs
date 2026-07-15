//! `arb-reth-engine`: the engine-tree driver and the payload-type plumbing it needs.
//!
//! [`ArbEngineDriver`] stands up reth's `EngineApiTreeHandler` and mints execute-to-derive blocks
//! by sending `InsertExecutedBlock` + `ForkchoiceUpdated` to the tree (no re-execution), reading
//! parent state through reth's native provider path. This crate also owns
//! the payload-type stubs ([`ArbPayloadTypes`] and friends) that exist only to satisfy
//! `NodeTypes::Payload`, and the minimal [`ArbPayloadValidator`] / `ConfigureEngineEvm` impls the
//! engine-tree generics require. They live here rather than in the node crate so the driver can
//! depend on them without a dependency cycle.

extern crate alloc;

use alloc::sync::Arc;

use alloy_eips::eip4895::Withdrawal;
use alloy_primitives::{Bytes, U256};
use alloy_rpc_types_engine::{ExecutionData, ExecutionPayload as AlloyExecutionPayload, PayloadId};
use reth_payload_primitives::{BuiltPayload, ExecutionPayload, PayloadAttributes, PayloadTypes};
use reth_primitives_traits::{NodePrimitives, RecoveredBlock, SealedBlock};

use arbitrum_alloy_consensus::reth::{ArbBlock, ArbPrimitives};

pub mod engine;
pub mod engine_spike;

pub use engine::{
    produce, wait_for_head, ArbAppliedMessageTiming, ArbEngineDriver, ArbEngineTuning,
    DirectStateRootTaskMode,
};
pub use engine_spike::ArbPayloadValidator;

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
        let (execution_payload, sidecar) = AlloyExecutionPayload::from_block_unchecked_with_extras(
            block_hash, &block, None, // no block access list
        );
        ArbExecutionData(ExecutionData {
            payload: execution_payload,
            sidecar,
        })
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

/// Payload-types stub for `ArbNode`. Exists solely to satisfy `NodeTypes::Payload`.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ArbPayloadTypes;

impl PayloadTypes for ArbPayloadTypes {
    type ExecutionData = ArbExecutionData;
    type BuiltPayload = ArbBuiltPayload;
    type PayloadAttributes = ArbPayloadAttributes;

    fn block_to_payload(
        block: SealedBlock<
            <<ArbBuiltPayload as BuiltPayload>::Primitives as NodePrimitives>::Block,
        >,
        _bal: Option<Bytes>,
    ) -> Self::ExecutionData {
        let block_hash = block.hash();
        let (execution_payload, sidecar) = AlloyExecutionPayload::from_block_unchecked_with_extras(
            block_hash,
            &block.into_block(),
            None,
        );
        ArbExecutionData(ExecutionData {
            payload: execution_payload,
            sidecar,
        })
    }
}
