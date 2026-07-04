//! Tier-1 engine-tree adoption spike.
//!
//! Proves that reth's [`EngineApiTreeHandler`] can be stood up for [`ArbNode`] and fed
//! already-executed Arbitrum blocks via the `InsertExecutedBlock` seam (NO re-execution),
//! with the tree providing in-memory canonical state, overlay, and async persistence.
//!
//! # What this exercises
//!
//! For each of the testnode-replay feed messages we:
//!   1. produce a block with an execute-once producer that reads state FROM THE TREE OVERLAY
//!      (`provider.state_by_block_hash(parent_hash)` — auto-overlays pending in-memory blocks),
//!   2. wrap the result in a [`BuiltPayloadExecutedBlock`],
//!   3. send `InsertExecutedBlock` + a `ForkchoiceUpdated` (head = new block) to the tree,
//!   4. wait for the tree to canonicalize the block, then assert its `state_root`/hash equal
//!      the testnode's captured values (same fixtures as `driver::replay_feed_matches_testnode_per_block`).
//!
//! Reaching the gate proves the reth generics line up for ArbNode, the seam canonicalizes +
//! persists without re-exec, and production-against-the-overlay yields correct roots.

use reth_evm::{ConfigureEngineEvm, EvmEnvFor};

use arb_reth_evm::ArbEvmConfig;
use crate::ArbExecutionData;

// -----------------------------------------------------------------------------------------------
// ConfigureEngineEvm for ArbEvmConfig.
//
// `BasicEngineValidator<P, ArbEvmConfig, ArbPayloadValidator>` only satisfies `EngineValidator<T>`
// when `ArbEvmConfig: ConfigureEngineEvm<ArbExecutionData>`. These methods are ONLY invoked on the
// newPayload / execute-the-payload path; on the `InsertExecutedBlock` path (which this spike uses)
// they are never called. We therefore provide trivial bodies. `ConfigureEvm::Error` is `Infallible`
// for ArbEvmConfig, so the `evm_env`/`context` methods cannot return an `Err`; they `unreachable!()`.
// `tx_iterator_for_payload` returns an empty (never-yielding) iterator of the right concrete type.
//
// Orphan rule: `ConfigureEngineEvm` is foreign (reth_evm) and `ArbEvmConfig` is foreign
// (arb-reth-evm), but the trait's type parameter `ArbExecutionData` is LOCAL to this crate, which
// makes the impl legal (RFC 2451: a local type covers the impl).
// -----------------------------------------------------------------------------------------------

impl ConfigureEngineEvm<ArbExecutionData> for ArbEvmConfig {
    fn evm_env_for_payload(
        &self,
        _payload: &ArbExecutionData,
    ) -> Result<EvmEnvFor<Self>, Self::Error> {
        unreachable!("ConfigureEngineEvm::evm_env_for_payload is unused on the InsertExecutedBlock path")
    }

    fn context_for_payload<'a>(
        &self,
        _payload: &'a ArbExecutionData,
    ) -> Result<reth_evm::ExecutionCtxFor<'a, Self>, Self::Error> {
        unreachable!("ConfigureEngineEvm::context_for_payload is unused on the InsertExecutedBlock path")
    }

    fn tx_iterator_for_payload(
        &self,
        _payload: &ArbExecutionData,
    ) -> Result<impl reth_evm::ExecutableTxIterator<Self>, Self::Error> {
        // A `(Vec<Recovered<ArbTxEnvelope>>, closure)` tuple is a valid `ExecutableTxIterator`:
        // `Recovered<ArbTxEnvelope>` is `ExecutableTxFor<ArbEvmConfig>` (the driver executes exactly
        // this via `builder.execute_transaction`). Empty vec ⇒ the iterator never yields; the tuple
        // just gives the opaque return type a nameable concrete type. Never actually called.
        use alloy_consensus::transaction::Recovered;
        use arb_alloy_consensus::ArbTxEnvelope;
        let txs: alloc::vec::Vec<Recovered<ArbTxEnvelope>> = alloc::vec::Vec::new();
        let convert = |tx: Recovered<ArbTxEnvelope>| -> Result<Recovered<ArbTxEnvelope>, core::convert::Infallible> {
            Ok(tx)
        };
        Ok((txs, convert))
    }
}

// -----------------------------------------------------------------------------------------------
// ArbPayloadValidator: the minimal PayloadValidator for ArbNode.
//
// Only `type Block` and `convert_payload_to_block` are required (the other members are defaulted;
// Ethereum's validator implements only these two). `convert_payload_to_block` is on the newPayload
// path and is never called on the InsertExecutedBlock path, so it returns an error.
// -----------------------------------------------------------------------------------------------

use arb_alloy_consensus::reth::ArbBlock;
use reth_engine_primitives::PayloadValidator;
use reth_payload_primitives::NewPayloadError;
use reth_primitives_traits::SealedBlock;

/// Minimal [`PayloadValidator`] for `ArbNode`. Used only to satisfy the engine-tree
/// generic bounds; `convert_payload_to_block` is never invoked on the `InsertExecutedBlock` path.
#[derive(Clone, Debug, Default)]
pub struct ArbPayloadValidator;

impl PayloadValidator<crate::ArbPayloadTypes> for ArbPayloadValidator {
    type Block = ArbBlock;

    fn convert_payload_to_block(
        &self,
        _payload: ArbExecutionData,
    ) -> Result<SealedBlock<Self::Block>, NewPayloadError> {
        Err(NewPayloadError::other(std::io::Error::other(
            "ArbPayloadValidator::convert_payload_to_block is unused on the InsertExecutedBlock path",
        )))
    }
}

// Compile-time proof that ArbEvmConfig satisfies ConfigureEngineEvm<ArbExecutionData>.
const _: fn() = || {
    fn assert_engine_evm<C: ConfigureEngineEvm<ArbExecutionData>>() {}
    assert_engine_evm::<ArbEvmConfig>();
};
