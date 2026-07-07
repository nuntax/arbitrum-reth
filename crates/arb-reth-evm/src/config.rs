//! [`ArbEvmConfig`], reth's [`ConfigureEvm`] for Arbitrum.
//!
//! Ties together the EVM factory ([`ArbEvmFactory`]/[`ArbEvm`](crate::ArbEvm)) and the block layer
//! ([`ArbBlockExecutorFactory`]/[`ArbBlockExecutor`](crate::ArbBlockExecutor)/[`ArbBlockAssembler`]).
//!
//! Mirrors `OpEvmConfig`. Unlike OP (which keys spec from a timestamp-keyed chain spec),
//! Arbitrum's ArbOS version is encoded in the header itself via [`ArbHeaderInfo`] (`extra_data` +
//! `mix_hash`). [`ArbEvmConfig`] therefore needs only the chain id to build a full [`EvmEnv`] from
//! a header; the per-block spec and L1 block number are decoded from each header.
//!
//! ## L1 block number threading
//!
//! On Arbitrum the `NUMBER` opcode returns the L1 block number (not the L2 one). `arb_revm`
//! overrides `opNumber` to read `chain().l1_block_number`. [`evm_env`](ArbEvmConfig::evm_env) and
//! [`context_for_block`](ArbEvmConfig::context_for_block) decode the L1 block number from
//! [`ArbHeaderInfo`] into [`ArbBlockExecutionCtx::l1_block_number`], and
//! [`ArbBlockExecutorFactory::create_executor`](crate::ArbBlockExecutorFactory) threads it into the
//! chain context. An executor built through this config sees the correct L1 block number.
//!
//! ## `impl ConfigureEvm`
//!
//! reth's [`ConfigureEvm`](reth_evm::ConfigureEvm) requires `EvmFactory<Precompiles = PrecompilesMap,
//! Tx: TransactionEnvMut>`. Both hold: `ArbTx` impls `TransactionEnvMut` (see `tx.rs`), and
//! `ArbEvmFactory::Precompiles` is `PrecompilesMap` (ArbOS precompiles re-homed onto alloy-evm's
//! `DynPrecompile`/`EvmInternals` model via `crate::precompiles::arb_precompiles_map`). The
//! [`ConfigureEvm`] impl is at the bottom of this file; per-header logic lives in the inherent
//! methods below, which the trait methods delegate to.

use crate::block::{ArbBlockAssembler, ArbBlockExecutionCtx, ArbBlockExecutorFactory};
use crate::ArbEvmFactory;
use alloy_consensus::{BlockHeader, Header};
use alloy_eips::eip4895::Withdrawals;
use alloy_evm::EvmEnv;
use alloy_primitives::{Address, B256, Bytes, U256};
use arb_alloy_consensus::header::ArbHeaderInfo;
use arb_alloy_consensus::reth::{ArbBlock, ArbPrimitives};
use arb_revm::ArbSpecId;
use core::convert::Infallible;
use reth_evm::ConfigureEvm;
use reth_primitives_traits::{SealedBlock, SealedHeader};
use revm::context::{BlockEnv, CfgEnv};

/// Arbitrum One mainnet chain id.
pub const ARB_ONE_CHAIN_ID: u64 = 42_161;

/// The error type a future `impl ConfigureEvm for ArbEvmConfig` would carry. [`ArbEvmConfig::evm_env`]
/// defaults on non-Arbitrum headers rather than erroring, so the would-be `ConfigureEvm::Error` is
/// [`Infallible`].
pub type ArbEvmConfigError = Infallible;

/// Additional attributes needed to configure the next Arbitrum block, beyond what the parent header
/// carries. Mirrors `OpNextBlockEnvAttributes` / reth's `NextBlockEnvAttributes`.
///
/// On Arbitrum these come from the sequencer message being executed (an `L1IncomingMessage`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArbNextBlockEnvAttributes {
    /// Timestamp for the next block.
    pub timestamp: u64,
    /// Suggested fee recipient / batch poster (coinbase) for the next block.
    pub suggested_fee_recipient: Address,
    /// Prev-randao value for the next block (Arbitrum sets this to zero in practice).
    pub prev_randao: B256,
    /// Block gas limit for the next block.
    pub gas_limit: u64,
    /// L1 block number ArbOS observes for this L2 block (the value the `NUMBER` opcode returns).
    pub l1_block_number: u64,
    /// L1 base fee (wei) for this block.
    pub l1_base_fee_wei: U256,
    /// ArbOS format version for the next block (selects the [`ArbSpecId`]).
    pub arbos_format_version: u64,
    /// Cumulative count of delayed-inbox messages read as of this block. Nitro encodes this into
    /// the header `nonce`.
    pub delayed_messages_read: u64,
    /// Header `extra_data` (carries `send_root` on Arbitrum).
    pub extra_data: Bytes,
    /// Consensus-layer withdrawals (always empty on Arbitrum; kept for trait-surface parity).
    pub withdrawals: Option<Withdrawals>,
}

/// Arbitrum EVM configuration: implements reth's [`ConfigureEvm`], wiring the EVM factory and block
/// layer together.
///
/// Holds the chain id plus the [`ArbBlockExecutorFactory`] and [`ArbBlockAssembler`].
/// Mirrors `OpEvmConfig` but parameterised only by chain id (per-block spec and L1 block number
/// are decoded from each header via [`ArbHeaderInfo`], not from a chain spec).
#[derive(Debug, Clone)]
pub struct ArbEvmConfig {
    /// Inner block-executor factory (wraps [`ArbEvmFactory`]).
    executor_factory: ArbBlockExecutorFactory,
    /// Arbitrum block assembler.
    block_assembler: ArbBlockAssembler,
    /// Chain id used when no header is available (and asserted against headers).
    chain_id: u64,
}

impl ArbEvmConfig {
    /// Creates a new [`ArbEvmConfig`] for the given chain id (e.g. [`ARB_ONE_CHAIN_ID`]).
    pub fn new(chain_id: u64) -> Self {
        Self {
            executor_factory: ArbBlockExecutorFactory::new(ArbEvmFactory, chain_id),
            block_assembler: ArbBlockAssembler,
            chain_id,
        }
    }

    /// Creates a new [`ArbEvmConfig`] for Arbitrum One mainnet (chain id `42161`).
    pub fn arbitrum_one() -> Self {
        Self::new(ARB_ONE_CHAIN_ID)
    }

    /// The chain id this config executes for.
    pub const fn chain_id(&self) -> u64 {
        self.chain_id
    }

    /// Builds the [`CfgEnv`] for the given ArbOS-derived spec.
    ///
    /// Priority-fee check is disabled (Arbitrum prices the tip via its own handler); EIP-7623 is
    /// disabled (Arbitrum prices calldata via the poster fee, not the floor); balance check is on.
    fn cfg_env(&self, spec: ArbSpecId) -> CfgEnv<ArbSpecId> {
        let mut cfg = CfgEnv::new_with_spec(spec)
            .with_chain_id(self.chain_id)
            .with_disable_priority_fee_check(true);
        cfg.disable_balance_check = false;
        cfg.disable_eip7623 = true;
        cfg
    }

    /// Builds an [`EvmEnv`] from the explicit block fields + ArbOS version.
    #[allow(clippy::too_many_arguments)]
    fn build_evm_env(
        &self,
        spec: ArbSpecId,
        number: u64,
        beneficiary: Address,
        timestamp: u64,
        gas_limit: u64,
        basefee: u64,
        difficulty: U256,
        prevrandao: Option<B256>,
    ) -> EvmEnv<ArbSpecId, BlockEnv> {
        let mut block = BlockEnv::default();
        block.number = U256::from(number);
        block.beneficiary = beneficiary;
        block.timestamp = U256::from(timestamp);
        block.gas_limit = gas_limit;
        block.basefee = basefee;
        block.difficulty = difficulty;
        block.prevrandao = prevrandao;
        EvmEnv::new(self.cfg_env(spec), block)
    }
}

/// Decodes the ArbOS format version from a header, falling back to the default spec when the header
/// is not an Arbitrum header (e.g. a genesis or probe header). Never errors, keeping
/// [`ConfigureEvm::evm_env`] infallible, matching `OpEvmConfig`.
fn spec_for_header(header: &Header) -> ArbSpecId {
    match ArbHeaderInfo::decode_header(header) {
        Ok(info) if info.is_arbitrum() => ArbSpecId::from_arbos_version(info.arbos_format_version),
        // Not an Arbitrum header or decode failed: fall back to the default ArbOS spec.
        _ => ArbSpecId::default(),
    }
}

/// Decodes the L1 block number from a header, defaulting to 0 when the header is not an Arbitrum
/// header.
fn l1_block_number_for_header(header: &Header) -> u64 {
    ArbHeaderInfo::decode_header(header)
        .ok()
        .filter(ArbHeaderInfo::is_arbitrum)
        .map(|info| info.l1_block_number)
        .unwrap_or(0)
}

/// Inherent methods mirroring the `ConfigureEvm` surface.
///
/// `evm_env` is infallible (defaults on non-Arbitrum headers, matching `OpEvmConfig::evm_env`),
/// so the error type is [`Infallible`].
impl ArbEvmConfig {
    /// Returns a reference to the configured block-executor factory
    /// (`ConfigureEvm::block_executor_factory`).
    pub const fn block_executor_factory(&self) -> &ArbBlockExecutorFactory {
        &self.executor_factory
    }

    /// Returns a reference to the configured block assembler (`ConfigureEvm::block_assembler`).
    pub const fn block_assembler(&self) -> &ArbBlockAssembler {
        &self.block_assembler
    }

    /// Builds the [`EvmEnv`] for a block from its header (`ConfigureEvm::evm_env`).
    ///
    /// The [`ArbSpecId`] is taken from the ArbOS version embedded in the header
    /// (`extra_data` + `mix_hash`, via [`ArbHeaderInfo`]).
    pub fn evm_env(&self, header: &Header) -> EvmEnv<ArbSpecId, BlockEnv> {
        let spec = spec_for_header(header);
        self.build_evm_env(
            spec,
            header.number(),
            header.beneficiary(),
            header.timestamp(),
            header.gas_limit(),
            header.base_fee_per_gas().unwrap_or_default(),
            header.difficulty(),
            header.mix_hash(),
        )
    }

    /// Builds the [`EvmEnv`] for `parent + 1` from the parent header + next-block attributes
    /// (`ConfigureEvm::next_evm_env`).
    pub fn next_evm_env(
        &self,
        parent: &Header,
        attributes: &ArbNextBlockEnvAttributes,
    ) -> EvmEnv<ArbSpecId, BlockEnv> {
        let spec = ArbSpecId::from_arbos_version(attributes.arbos_format_version);
        self.build_evm_env(
            spec,
            parent.number() + 1,
            attributes.suggested_fee_recipient,
            attributes.timestamp,
            attributes.gas_limit,
            parent.base_fee_per_gas().unwrap_or_default(),
            // Nitro always sets L2 block difficulty to 1 (see `ArbBlockAssembler` + the `evm_env`
            // re-exec path, which reads `header.difficulty()`=1). The `DIFFICULTY` opcode returns
            // `block.difficulty` on pre-merge specs (ArbOS<11 → London), so passing `U256::ZERO`
            // here made sync-produced blocks diverge from canonical the first time a tx reads
            // DIFFICULTY. Must be 1.
            U256::from(1u64),
            Some(attributes.prev_randao),
        )
    }

    /// Builds the [`ArbBlockExecutionCtx`] for a block from its header
    /// (`ConfigureEvm::context_for_block`).
    ///
    /// Decodes the L1 block number from [`ArbHeaderInfo`] and carries it into the execution ctx.
    /// `ArbBlockExecutorFactory::create_executor` threads it into the chain context so the `NUMBER`
    /// opcode returns the correct L1 block number.
    pub fn context_for_block(&self, header: &Header) -> ArbBlockExecutionCtx {
        ArbBlockExecutionCtx {
            parent_hash: header.parent_hash(),
            extra_data: header.extra_data().clone(),
            l1_block_number: l1_block_number_for_header(header),
            // Block-scoped ArbOS startBlock inputs not in the consensus header are defaulted here;
            // the sequencer `L1IncomingMessage` supplies them on the production path.
            l1_base_fee_wei: U256::ZERO,
            time_last_block: 0,
            sequence_number: None,
            poster: header.beneficiary(),
            // The header nonce holds the cumulative delayed-messages-read count (Nitro `EncodeNonce`).
            delayed_messages_read: u64::from_be_bytes(header.nonce.0),
            header_info_out: Default::default(),
        }
    }

    /// Builds the [`ArbBlockExecutionCtx`] for `parent + 1` from the parent header (+ its hash) and
    /// next-block attributes (`ConfigureEvm::context_for_next_block`).
    pub fn context_for_next_block(
        &self,
        parent: &Header,
        parent_hash: B256,
        attributes: ArbNextBlockEnvAttributes,
    ) -> ArbBlockExecutionCtx {
        ArbBlockExecutionCtx {
            parent_hash,
            extra_data: attributes.extra_data,
            l1_block_number: attributes.l1_block_number,
            l1_base_fee_wei: attributes.l1_base_fee_wei,
            time_last_block: attributes.timestamp.saturating_sub(parent.timestamp()),
            sequence_number: None,
            poster: attributes.suggested_fee_recipient,
            delayed_messages_read: attributes.delayed_messages_read,
            header_info_out: Default::default(),
        }
    }

    /// Reference to the wrapped [`ArbEvmFactory`] (`ConfigureEvm::evm_factory`).
    pub const fn evm_factory(&self) -> &ArbEvmFactory {
        self.executor_factory.evm_factory_ref()
    }
}

/// reth's [`ConfigureEvm`] for Arbitrum. Each method delegates to the equally-named inherent method
/// above (inherent methods win method resolution, so `self.evm_env(header)` here is delegation, not
/// recursion). The trait adapts the surface (sealed-block wrapping, `Result` return) and pins the
/// associated types. `Error` is [`Infallible`] since header decoding falls back to defaults.
impl ConfigureEvm for ArbEvmConfig {
    type Primitives = ArbPrimitives;
    type Error = ArbEvmConfigError;
    type NextBlockEnvCtx = ArbNextBlockEnvAttributes;
    type BlockExecutorFactory = ArbBlockExecutorFactory;
    type BlockAssembler = ArbBlockAssembler;

    fn block_executor_factory(&self) -> &Self::BlockExecutorFactory {
        &self.executor_factory
    }

    fn block_assembler(&self) -> &Self::BlockAssembler {
        &self.block_assembler
    }

    fn evm_env(&self, header: &Header) -> Result<EvmEnv<ArbSpecId, BlockEnv>, Self::Error> {
        Ok(self.evm_env(header))
    }

    fn next_evm_env(
        &self,
        parent: &Header,
        attributes: &ArbNextBlockEnvAttributes,
    ) -> Result<EvmEnv<ArbSpecId, BlockEnv>, Self::Error> {
        Ok(self.next_evm_env(parent, attributes))
    }

    fn context_for_block(
        &self,
        block: &SealedBlock<ArbBlock>,
    ) -> Result<ArbBlockExecutionCtx, Self::Error> {
        Ok(self.context_for_block(block.header()))
    }

    fn context_for_next_block(
        &self,
        parent: &SealedHeader<Header>,
        attributes: ArbNextBlockEnvAttributes,
    ) -> Result<ArbBlockExecutionCtx, Self::Error> {
        Ok(self.context_for_next_block(parent.header(), parent.hash(), attributes))
    }
}

// Compile-time proof that `ArbEvmConfig` satisfies the full reth `ConfigureEvm` bound, including
// `EvmFactory<Precompiles = PrecompilesMap, Tx: TransactionEnvMut + FromRecoveredTx + ...>`.
// Regression guard: if this stops compiling, the node's EVM configuration surface has broken.
const _: fn() = || {
    fn assert_configure_evm<T: ConfigureEvm>() {}
    assert_configure_evm::<ArbEvmConfig>();
};

#[cfg(test)]
mod tests;
