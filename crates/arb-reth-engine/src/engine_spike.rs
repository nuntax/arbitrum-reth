//! Engine-tree compatibility glue for native Arbitrum payload construction.
//!
//! The engine tree owns the in-memory canonical state, overlay, sparse-trie work, and asynchronous
//! persistence. Locally derived ArbOS blocks are built by [`crate::ArbPayloadBuilder`] and returned
//! as executed payloads, so they do not enter Reth's external `newPayload` re-execution path.

use reth_evm::{ConfigureEngineEvm, EvmEnvFor};

use crate::ArbExecutionData;
use arb_reth_evm::ArbEvmConfig;

// -----------------------------------------------------------------------------------------------
// ConfigureEngineEvm for ArbEvmConfig.
//
// `BasicEngineValidator<P, ArbEvmConfig, ArbPayloadValidator>` only satisfies `EngineValidator<T>`
// when `ArbEvmConfig: ConfigureEngineEvm<ArbExecutionData>`. These methods are only invoked on the
// external newPayload / execute-the-payload path. Locally derived payloads are already executed,
// so these methods are never called. `ConfigureEvm::Error` is `Infallible`
// for ArbEvmConfig, so the `evm_env`/`context` methods cannot return an `Err`; they `unreachable!()`.
// `tx_iterator_for_payload` returns an empty (never-yielding) iterator of the right concrete type.
//
// Orphan rule: `ConfigureEngineEvm` is foreign (reth_evm) and `ArbEvmConfig` is foreign
// (arb-reth-evm), but the trait's type parameter `ArbExecutionData` is local to this crate, which
// makes the impl legal (RFC 2451: a local type covers the impl).
// -----------------------------------------------------------------------------------------------

impl ConfigureEngineEvm<ArbExecutionData> for ArbEvmConfig {
    fn evm_env_for_payload(
        &self,
        _payload: &ArbExecutionData,
    ) -> Result<EvmEnvFor<Self>, Self::Error> {
        unreachable!("ConfigureEngineEvm::evm_env_for_payload is unsupported for external payloads")
    }

    fn context_for_payload<'a>(
        &self,
        _payload: &'a ArbExecutionData,
    ) -> Result<reth_evm::ExecutionCtxFor<'a, Self>, Self::Error> {
        unreachable!("ConfigureEngineEvm::context_for_payload is unsupported for external payloads")
    }

    fn tx_iterator_for_payload(
        &self,
        _payload: &ArbExecutionData,
    ) -> Result<impl reth_evm::ExecutableTxIterator<Self>, Self::Error> {
        // A `(Vec<Recovered<ArbTxEnvelope>>, closure)` tuple is a valid `ExecutableTxIterator`:
        // `Recovered<ArbTxEnvelope>` is `ExecutableTxFor<ArbEvmConfig>` (the driver executes exactly
        // this via `builder.execute_transaction`). Empty vec means the iterator never yields; the
        // tuple just gives the opaque return type a nameable concrete type. Never actually called.
        use alloy_consensus::transaction::Recovered;
        use arbitrum_alloy_consensus::ArbTxEnvelope;
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
// Ethereum's validator implements only these two). `convert_payload_to_block` is only on the
// external newPayload path and is deliberately unsupported.
// -----------------------------------------------------------------------------------------------

use arbitrum_alloy_consensus::reth::ArbBlock;
use reth_engine_primitives::PayloadValidator;
use reth_payload_primitives::NewPayloadError;
use reth_primitives_traits::{Block, SealedBlock};

/// Minimal [`PayloadValidator`] for `ArbNode`.
#[derive(Clone, Debug, Default)]
pub struct ArbPayloadValidator;

impl PayloadValidator<crate::ArbPayloadTypes> for ArbPayloadValidator {
    type Block = ArbBlock;

    fn convert_payload_to_block(
        &self,
        _payload: ArbExecutionData,
    ) -> Result<SealedBlock<Self::Block>, NewPayloadError> {
        Err(NewPayloadError::other(std::io::Error::other(
            "ArbPayloadValidator::convert_payload_to_block is unsupported for external payloads",
        )))
    }

    fn validate_payload_attributes_against_header(
        &self,
        attributes: &crate::ArbPayloadAttributes,
        header: &<Self::Block as Block>::Header,
    ) -> Result<(), reth_payload_primitives::InvalidPayloadAttributesError> {
        // ArbOS derives the next L2 timestamp as max(message L1 timestamp, parent timestamp),
        // so equal timestamps are valid and common. The stock Engine API rule is strictly greater.
        if attributes.timestamp < header.timestamp {
            return Err(reth_payload_primitives::InvalidPayloadAttributesError::InvalidTimestamp);
        }
        Ok(())
    }
}

// Compile-time check that ArbEvmConfig satisfies ConfigureEngineEvm<ArbExecutionData>.
const _: fn() = || {
    fn assert_engine_evm<C: ConfigureEngineEvm<ArbExecutionData>>() {}
    assert_engine_evm::<ArbEvmConfig>();
};
