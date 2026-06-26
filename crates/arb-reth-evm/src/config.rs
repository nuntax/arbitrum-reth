//! `arb-reth-evm` Stage D.1 — [`ArbEvmConfig`]: reth's [`ConfigureEvm`] for Arbitrum.
//!
//! This is the trait that lets reth drive Arbitrum execution from a block header. It ties together
//! Stage B ([`ArbEvmFactory`]/[`ArbEvm`](crate::ArbEvm)) and Stage C
//! ([`ArbBlockExecutorFactory`]/[`ArbBlockExecutor`](crate::ArbBlockExecutor)/[`ArbBlockAssembler`]).
//!
//! Mirrors `OpEvmConfig` (`op-reth/crates/evm/src/lib.rs`) and `EthEvmConfig`. The structural
//! difference from op: op derives its spec from a timestamp-keyed chain spec
//! (`spec_by_timestamp_after_bedrock`), whereas Arbitrum's ArbOS version is carried **inside the
//! header itself** ([`ArbHeaderInfo`] decodes it from `extra_data` + `mix_hash`). So
//! [`ArbEvmConfig`] needs only the chain id to build a full [`EvmEnv`] from a header — the per-block
//! spec and the L1 block number come from the header. The full timestamp/fork-keyed `ArbChainSpec`
//! is Stage D.2 (the node skeleton); for Stage D.1 this self-contained config is the right altitude.
//!
//! ## Threading the L1 block number (the Stage B/C deferral, now fixed)
//!
//! On Arbitrum the EVM `NUMBER` opcode returns the **L1** block number, not the L2 one — `arb_revm`
//! overrides `opNumber` to read `chain().l1_block_number`. Stage B's [`ArbEvmFactory::build_ctx`]
//! defaulted that to 0 because the alloy [`EvmEnv`] has no slot for it. [`ArbEvmConfig`] resolves
//! the deferral: [`evm_env`](ArbEvmConfig::evm_env) / [`context_for_block`](ArbEvmConfig::context_for_block)
//! decode it from [`ArbHeaderInfo`] into [`ArbBlockExecutionCtx::l1_block_number`], and
//! [`ArbBlockExecutorFactory::create_executor`](crate::ArbBlockExecutorFactory) threads it into the
//! chain context (see `block.rs`). So an executor built through this config sees the real L1 block
//! number and `NUMBER` reads it.
//!
//! ## `impl ConfigureEvm` (the precompiles fork is resolved)
//!
//! reth's [`ConfigureEvm`](reth_evm::ConfigureEvm) bounds the inner `EvmFactory` with
//! `Precompiles = PrecompilesMap` and `Tx: TransactionEnvMut`. Both now hold: `ArbTx` impls
//! `TransactionEnvMut` (see `tx.rs`), and `ArbEvmFactory::Precompiles` is now `PrecompilesMap` —
//! the ArbOS precompiles were re-homed onto alloy-evm's `DynPrecompile`/`EvmInternals` model
//! (`arb_revm::arb_journal` + `crate::precompiles::arb_precompiles_map`), so the inner EVM executes
//! through a `PrecompilesMap` while keeping `arb_revm`'s parity-validated precompile logic
//! (`run_dispatch`). The [`ConfigureEvm`] impl is at the bottom of this file; the per-header logic
//! lives in the inherent methods below (kept distinct so they stay directly unit-testable) and the
//! trait methods delegate to them.

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
/// On Arbitrum these come from the sequencer message being executed (Stage E will populate them
/// from an `L1IncomingMessage`): the block timestamp, the batch poster (coinbase), the L1 block
/// number observed for this L2 block, the L1 base fee, and the block gas limit.
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
    /// L1 block number ArbOS observes for this L2 block — the value the `NUMBER` opcode returns.
    pub l1_block_number: u64,
    /// L1 base fee (wei) for this block.
    pub l1_base_fee_wei: U256,
    /// ArbOS format version for the next block (selects the [`ArbSpecId`]).
    pub arbos_format_version: u64,
    /// Header `extra_data` (carries `send_root` on Arbitrum).
    pub extra_data: Bytes,
    /// Consensus-layer withdrawals (always empty on Arbitrum; kept for trait-surface parity).
    pub withdrawals: Option<Withdrawals>,
}

/// Arbitrum EVM configuration — implements reth's [`ConfigureEvm`], wiring Stage B + Stage C.
///
/// Holds the chain id plus the Stage C [`ArbBlockExecutorFactory`] and [`ArbBlockAssembler`].
/// Mirrors `OpEvmConfig` but is parameterised only by the chain id (the per-block spec and L1 block
/// number are decoded from each header via [`ArbHeaderInfo`], not from a chain spec).
#[derive(Debug, Clone)]
pub struct ArbEvmConfig {
    /// Inner Stage C block-executor factory (wraps [`ArbEvmFactory`]).
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
    /// Mirrors the cfg `execute_message` / the Stage C test harness use for a fresh ArbOS state:
    /// priority-fee check off (Arbitrum prices the tip via its own handler), EIP-7623 off (Arbitrum
    /// prices calldata via the poster fee, not the floor), balance check on.
    fn cfg_env(&self, spec: ArbSpecId) -> CfgEnv<ArbSpecId> {
        let mut cfg = CfgEnv::new_with_spec(spec)
            .with_chain_id(self.chain_id)
            .with_disable_priority_fee_check(true);
        cfg.disable_balance_check = false;
        cfg.disable_eip7623 = true;
        cfg
    }

    /// Builds an [`EvmEnv`] from the explicit block fields + ArbOS version.
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
    ) -> EvmEnv<ArbSpecId> {
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

/// Decodes the ArbOS format version from a header, defaulting to the current spec when the header
/// is not an Arbitrum header (e.g. a genesis/default header reth may probe). Never errors — keeping
/// [`ConfigureEvm::evm_env`] infallible, matching `OpEvmConfig`.
fn spec_for_header(header: &Header) -> ArbSpecId {
    match ArbHeaderInfo::decode_header(header) {
        Ok(info) if info.is_arbitrum() => ArbSpecId::from_arbos_version(info.arbos_format_version),
        // Not an Arbitrum header (or decode failed): fall back to the default ArbOS spec.
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

/// Inherent methods mirroring the `ConfigureEvm` surface (signatures preserved exactly; see the
/// module docs for why these are not yet the trait methods — the precompiles fork).
///
/// `evm_env` is infallible (it defaults on non-Arbitrum headers, matching `OpEvmConfig::evm_env`),
/// so the error type that `ConfigureEvm::Error` would take is [`Infallible`].
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
    pub fn evm_env(&self, header: &Header) -> EvmEnv<ArbSpecId> {
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
    ) -> EvmEnv<ArbSpecId> {
        let spec = ArbSpecId::from_arbos_version(attributes.arbos_format_version);
        self.build_evm_env(
            spec,
            parent.number() + 1,
            attributes.suggested_fee_recipient,
            attributes.timestamp,
            attributes.gas_limit,
            parent.base_fee_per_gas().unwrap_or_default(),
            U256::ZERO,
            Some(attributes.prev_randao),
        )
    }

    /// Builds the [`ArbBlockExecutionCtx`] for a block from its header
    /// (`ConfigureEvm::context_for_block`).
    ///
    /// This is where the **L1 block number** is decoded from [`ArbHeaderInfo`] and carried into the
    /// execution ctx — the deferred fix: `ArbBlockExecutorFactory::create_executor` threads it into
    /// the chain context so the `NUMBER` opcode returns it.
    pub fn context_for_block(&self, header: &Header) -> ArbBlockExecutionCtx {
        ArbBlockExecutionCtx {
            parent_hash: header.parent_hash(),
            extra_data: header.extra_data().clone(),
            l1_block_number: l1_block_number_for_header(header),
            // Block-scoped ArbOS startBlock inputs not representable in the consensus header are
            // defaulted here; Stage E populates them from the sequencer `L1IncomingMessage`.
            l1_base_fee_wei: U256::ZERO,
            time_last_block: 0,
            sequence_number: None,
            poster: header.beneficiary(),
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
        }
    }

    /// Reference to the wrapped [`ArbEvmFactory`] (`ConfigureEvm::evm_factory`).
    pub const fn evm_factory(&self) -> &ArbEvmFactory {
        self.executor_factory.evm_factory_ref()
    }
}

/// reth's [`ConfigureEvm`] for Arbitrum — the entry point that lets reth drive ArbOS execution from
/// a block header. Each method delegates to the equally-named inherent method above (inherent
/// methods win method resolution, so `self.evm_env(header)` here is delegation, not recursion); the
/// trait only adapts the surface (header→sealed-block, `Result` wrapping) and pins the associated
/// types. `Error` is [`Infallible`] — header decoding falls back to defaults rather than erroring.
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

    fn evm_env(&self, header: &Header) -> Result<EvmEnv<ArbSpecId>, Self::Error> {
        Ok(self.evm_env(header))
    }

    fn next_evm_env(
        &self,
        parent: &Header,
        attributes: &ArbNextBlockEnvAttributes,
    ) -> Result<EvmEnv<ArbSpecId>, Self::Error> {
        Ok(self.next_evm_env(parent, attributes))
    }

    fn context_for_block<'a>(
        &self,
        block: &'a SealedBlock<ArbBlock>,
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

// Compile-time proof that `ArbEvmConfig` satisfies the full reth `ConfigureEvm` bound — including
// the `EvmFactory<Precompiles = PrecompilesMap, Tx: TransactionEnvMut + FromRecoveredTx + …>`
// constraint that the precompiles re-home (#36) unblocked. If this stops compiling, the node's EVM
// configuration surface has regressed.
const _: fn() = || {
    fn assert_configure_evm<T: ConfigureEvm>() {}
    assert_configure_evm::<ArbEvmConfig>();
};

#[cfg(test)]
mod tests;
