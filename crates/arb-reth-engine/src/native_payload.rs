//! Arbitrum's native Reth payload-builder integration.
//!
//! The engine tree owns construction of the optional sparse state-root task. This builder receives
//! that handle together with one ordered Arbitrum message, executes ArbOS exactly once over the
//! requested parent, and returns the resulting executed block to the engine tree.

use alloy_primitives::U256;
use arb_reth_evm::ArbEvmConfig;
use reth_basic_payload_builder::{
    BuildArguments, BuildOutcome, MissingPayloadBehaviour, PayloadBuilder, PayloadConfig,
};
use reth_payload_builder::PayloadBuilderError;
use reth_provider::StateProviderFactory;

use crate::{ArbBuiltPayload, ArbPayloadAttributes, engine::produce_with_timing};

/// A serial payload builder for ordered ArbOS messages.
#[derive(Debug, Clone)]
pub struct ArbPayloadBuilder<P> {
    provider: P,
    evm_config: ArbEvmConfig,
    chain_id: u64,
}

impl<P> ArbPayloadBuilder<P> {
    /// Creates a payload builder over Reth's canonical/in-memory provider.
    pub const fn new(provider: P, evm_config: ArbEvmConfig, chain_id: u64) -> Self {
        Self {
            provider,
            evm_config,
            chain_id,
        }
    }

    fn build(
        &self,
        args: BuildArguments<ArbPayloadAttributes, ArbBuiltPayload>,
    ) -> Result<ArbBuiltPayload, PayloadBuilderError>
    where
        P: StateProviderFactory,
    {
        let parent = args.config.parent_header;
        let parent_hash = parent.hash();
        let execution_state = self
            .provider
            .state_by_block_hash(parent_hash)
            .map_err(PayloadBuilderError::other)?;
        let trie_state = self
            .provider
            .state_by_block_hash(parent_hash)
            .map_err(PayloadBuilderError::other)?;

        let (executed, timing) = produce_with_timing(
            &self.evm_config,
            self.chain_id,
            &parent,
            &args.config.attributes.message,
            execution_state,
            trie_state,
            args.state_root_handle,
        )
        .map_err(|err| PayloadBuilderError::other(std::io::Error::other(err.to_string())))?;

        Ok(ArbBuiltPayload::from_executed(executed, U256::ZERO, timing))
    }
}

impl<P> PayloadBuilder for ArbPayloadBuilder<P>
where
    P: StateProviderFactory + Clone + Send + Sync + 'static,
{
    type Attributes = ArbPayloadAttributes;
    type BuiltPayload = ArbBuiltPayload;

    fn try_build(
        &self,
        args: BuildArguments<Self::Attributes, Self::BuiltPayload>,
    ) -> Result<BuildOutcome<Self::BuiltPayload>, PayloadBuilderError> {
        let payload = self.build(args)?;

        // An Arbitrum message deterministically defines one block. There is no transaction-pool
        // competition or later improvement cycle, so freeze the first completed result.
        Ok(BuildOutcome::Freeze(payload))
    }

    fn on_missing_payload(
        &self,
        _args: BuildArguments<Self::Attributes, Self::BuiltPayload>,
    ) -> MissingPayloadBehaviour<Self::BuiltPayload> {
        // The only valid payload for an Arbitrum message is the one being executed by this job.
        // Resolving immediately after FCU must wait for it instead of racing an empty block.
        MissingPayloadBehaviour::AwaitInProgress
    }

    fn build_empty_payload(
        &self,
        _config: PayloadConfig<Self::Attributes>,
    ) -> Result<Self::BuiltPayload, PayloadBuilderError> {
        Err(PayloadBuilderError::MissingPayload)
    }
}
