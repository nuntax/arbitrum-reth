//! [`ArbBlockEnv`]: revm's [`BlockEnv`] plus the L1 block number, and the context type built from it.
//!
//! On Arbitrum the `NUMBER` opcode returns the L1 block number, which `arb_revm` reads from
//! [`ArbChainContext::l1_block_number`]. revm's [`BlockEnv`] has no field for it, and
//! [`EvmFactory::create_evm`](alloy_evm::EvmFactory::create_evm) receives nothing but an
//! [`EvmEnv`](alloy_evm::EvmEnv), so any path that builds an EVM from an `EvmEnv` alone had no way
//! to supply it. The block executor worked around that by setting the chain context directly from
//! [`ArbBlockExecutionCtx`](crate::ArbBlockExecutionCtx), but the RPC simulation path
//! (`eth_call`, `eth_estimateGas`, `debug_traceCall`) does not go through the executor, so it ran
//! every call with `NUMBER` = 0.
//!
//! Carrying the value in the block env fixes that for every path: `ConfigureEvm::evm_env` decodes
//! it from the header, and [`ArbEvmFactory::build_ctx`](crate::ArbEvmFactory) seeds the chain
//! context from it. alloy-evm supports wrapper block envs through
//! [`BlockEnvironment`], so reth's generic RPC code (block overrides and friends) still reaches the
//! inner [`BlockEnv`].

use alloy_evm::env::BlockEnvironment;
use alloy_primitives::{Address, B256, U256};
use arb_revm::{ArbChainContext, ArbSpecId, ArbTransaction};
use core::ops::{Deref, DerefMut};
use revm::Journal;
use revm::context::{Block, BlockEnv, CfgEnv, Context, TxEnv};
use revm::context_interface::block::BlobExcessGasAndPrice;

/// Arbitrum block environment: revm's [`BlockEnv`] plus the L1 block number this L2 block observes.
///
/// Derefs to the inner [`BlockEnv`], so the standard fields are reached as usual.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ArbBlockEnv {
    /// The standard block environment. `number` stays the **L2** block number (chain rules,
    /// `BLOCKHASH` ring, EIP-2935).
    pub inner: BlockEnv,
    /// L1 block number ArbOS observes for this L2 block: what the `NUMBER` opcode returns.
    ///
    /// This is the block-scoped input, decoded from `ArbHeaderInfo` (or supplied by the sequencer
    /// message). The value the opcode actually reads lives in [`ArbChainContext::l1_block_number`],
    /// seeded from here when the EVM is built; ArbOS's start-block internal transaction then
    /// refreshes it from ArbOS state, which is authoritative for the rest of the block.
    pub l1_block_number: u64,
}

impl ArbBlockEnv {
    /// Wraps a [`BlockEnv`] together with the block's L1 block number.
    pub const fn new(inner: BlockEnv, l1_block_number: u64) -> Self {
        Self {
            inner,
            l1_block_number,
        }
    }
}

impl From<BlockEnv> for ArbBlockEnv {
    fn from(inner: BlockEnv) -> Self {
        Self::new(inner, 0)
    }
}

impl Deref for ArbBlockEnv {
    type Target = BlockEnv;

    fn deref(&self) -> &BlockEnv {
        &self.inner
    }
}

impl DerefMut for ArbBlockEnv {
    fn deref_mut(&mut self) -> &mut BlockEnv {
        &mut self.inner
    }
}

impl Block for ArbBlockEnv {
    fn number(&self) -> U256 {
        self.inner.number()
    }

    fn beneficiary(&self) -> Address {
        self.inner.beneficiary()
    }

    fn timestamp(&self) -> U256 {
        self.inner.timestamp()
    }

    fn gas_limit(&self) -> u64 {
        self.inner.gas_limit()
    }

    fn basefee(&self) -> u64 {
        self.inner.basefee()
    }

    fn difficulty(&self) -> U256 {
        self.inner.difficulty()
    }

    fn prevrandao(&self) -> Option<B256> {
        self.inner.prevrandao()
    }

    fn blob_excess_gas_and_price(&self) -> Option<BlobExcessGasAndPrice> {
        self.inner.blob_excess_gas_and_price()
    }
}

impl BlockEnvironment for ArbBlockEnv {
    fn inner_mut(&mut self) -> &mut BlockEnv {
        &mut self.inner
    }
}

/// The `arb_revm` execution context this crate runs on: `arb_revm::ArbContext` over
/// [`ArbBlockEnv`] instead of revm's [`BlockEnv`].
pub type ArbEvmContext<DB> = Context<
    ArbBlockEnv,
    ArbTransaction<TxEnv>,
    CfgEnv<ArbSpecId>,
    DB,
    Journal<DB>,
    ArbChainContext,
>;
