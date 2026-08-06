// revm's TxEnv/BlockEnv are #[non_exhaustive]; they are built by field assignment.
#![allow(clippy::field_reassign_with_default)]
//! `arb-reth-evm`: bridges `arb_revm` (Arbitrum/ArbOS execution on revm) into reth's EVM and
//! block-execution extension points.
//!
//! The EVM layer wraps `arb_revm`'s ArbOS EVM as alloy-evm's [`EvmFactory`] + [`Evm`], so a single
//! Arbitrum transaction can be executed through the same `create_evm(...).transact_raw(tx)` surface
//! reth drives, with full `arb_revm` semantics (ArbOS handler, NUMBER=L1 block number, Arb
//! precompiles).
//!
//! Mirrors `alloy-op-evm`'s `OpEvm`/`OpEvmFactory`. `arb_revm` already exposes its EVM as a revm
//! [`ExecuteEvm`]/[`InspectEvm`] impl over a concrete [`ArbEvmContext`], so this crate is a thin
//! adapter that owns the inner EVM and reconciles the result with the alloy-evm [`Evm`] trait.
//!
//! The `BlockExecutor`/`BlockAssembler` (block module) and `ConfigureEvm` (config module) build on
//! top of this.

// arb-alloy's `reth` feature satisfies the reth-primitives-traits surface; pull it into the graph
// so unification stays exercised.
use reth_primitives_traits as _;

extern crate alloc;

pub mod tx;
pub use tx::ArbTx;

pub mod precompiles;
pub use precompiles::ArbPrecompilesMap;

pub mod block;
pub use block::{
    ArbBlockAssembler, ArbBlockExecutionCtx, ArbBlockExecutor, ArbBlockExecutorFactory,
    ArbBlockFinishTiming, ArbTxResult,
};

pub mod config;
pub use config::{ARB_ONE_CHAIN_ID, ArbEvmConfig, ArbEvmConfigError, ArbNextBlockEnvAttributes};

pub mod env;
pub use env::{ArbBlockEnv, ArbEvmContext};

/// RPC compatibility impls (TryIntoTxEnv for ArbTransactionRequest → ArbTx).
#[cfg(feature = "rpc")]
pub mod rpc;

use alloy_evm::precompiles::PrecompilesMap;
use alloy_evm::{Database, Evm, EvmEnv, EvmFactory, IntoTxEnv};
use alloy_primitives::{Address, Bytes};
use arb_revm::{ArbBuilder, ArbChainContext, ArbTransaction};
use core::fmt::Debug;
use revm::context::result::{EVMError, HaltReason, InvalidTransaction, ResultAndState};
use revm::context::{CfgEnv, Context, DBErrorMarker, TxEnv};
use revm::handler::instructions::EthInstructions;
use revm::inspector::NoOpInspector;
use revm::interpreter::interpreter::EthInterpreter;
use revm::{ExecuteEvm, InspectEvm, Inspector, MainContext, SystemCallEvm};

use arb_revm::ArbSpecId;

/// The `arb_revm` EVM type this owns: the ArbOS EVM over [`ArbEvmContext<DB>`] with the Arbitrum
/// precompile set.
type ArbRethEvm<DB, I> = arb_revm::ArbEvm<
    ArbEvmContext<DB>,
    I,
    EthInstructions<EthInterpreter, ArbEvmContext<DB>>,
    ArbPrecompilesMap,
>;

/// EVM error surfaced by the Arbitrum bridge. Matches `arb_revm`'s `ArbError`:
/// `EVMError<DBError, InvalidTransaction>`.
pub type ArbEvmError<DBError> = EVMError<DBError, InvalidTransaction>;

/// Arbitrum EVM: alloy-evm [`Evm`] adapter wrapping `arb_revm`'s ArbOS EVM.
///
/// `inspect` is tracked here (not in the inner EVM) so [`Evm::transact_raw`] dispatches to either
/// the inspecting (`inspect_tx`) or plain (`transact`) path, exactly like `OpEvm`.
#[allow(missing_debug_implementations)]
pub struct ArbEvm<DB: Database, I = NoOpInspector> {
    inner: ArbRethEvm<DB, I>,
    inspect: bool,
}

impl<DB: Database, I> ArbEvm<DB, I> {
    /// Creates a new Arbitrum EVM from an inner `arb_revm` EVM.
    ///
    /// `inspect` determines whether the configured [`Inspector`] runs on [`Evm::transact`].
    pub const fn new(inner: ArbRethEvm<DB, I>, inspect: bool) -> Self {
        Self { inner, inspect }
    }

    /// Consumes self and returns the inner `arb_revm` EVM.
    pub fn into_inner(self) -> ArbRethEvm<DB, I> {
        self.inner
    }

    /// Reference to the inner Arbitrum execution context.
    pub fn ctx(&self) -> &ArbEvmContext<DB> {
        &self.inner.0.ctx
    }

    /// Mutable reference to the inner Arbitrum execution context.
    pub fn ctx_mut(&mut self) -> &mut ArbEvmContext<DB> {
        &mut self.inner.0.ctx
    }
}

impl<DB, I> Evm for ArbEvm<DB, I>
where
    DB: Database,
    I: Inspector<ArbEvmContext<DB>, EthInterpreter>,
{
    type DB = DB;
    type Tx = ArbTx;
    type Error = ArbEvmError<DB::Error>;
    type HaltReason = HaltReason;
    type Spec = ArbSpecId;
    type BlockEnv = ArbBlockEnv;
    type Precompiles = PrecompilesMap;
    type Inspector = I;

    fn block(&self) -> &ArbBlockEnv {
        &self.inner.0.ctx.block
    }

    fn cfg_env(&self) -> &CfgEnv<Self::Spec> {
        &self.inner.0.ctx.cfg
    }

    fn chain_id(&self) -> u64 {
        self.inner.0.ctx.cfg.chain_id
    }

    fn transact_raw(
        &mut self,
        tx: Self::Tx,
    ) -> Result<ResultAndState<Self::HaltReason>, Self::Error> {
        // Both `transact` and `inspect_tx` route through `ArbHandler` (ArbOS gas charging, poster
        // fee, NUMBER override, Arb precompiles).
        let inner_tx: ArbTransaction<TxEnv> = tx.0;
        if self.inspect {
            self.inner.inspect_tx(inner_tx)
        } else {
            self.inner.transact(inner_tx)
        }
    }

    fn transact_system_call(
        &mut self,
        caller: Address,
        contract: Address,
        data: Bytes,
    ) -> Result<ResultAndState<Self::HaltReason>, Self::Error> {
        self.inner.system_call_with_caller(caller, contract, data)
    }

    fn finish(self) -> (Self::DB, EvmEnv<Self::Spec, Self::BlockEnv>) {
        let Context {
            block: block_env,
            cfg: cfg_env,
            journaled_state,
            ..
        } = self.inner.0.ctx;
        (journaled_state.database, EvmEnv { block_env, cfg_env })
    }

    fn set_inspector_enabled(&mut self, enabled: bool) {
        self.inspect = enabled;
    }

    fn components(&self) -> (&Self::DB, &Self::Inspector, &Self::Precompiles) {
        (
            &self.inner.0.ctx.journaled_state.database,
            &self.inner.0.inspector,
            self.inner.0.precompiles.precompiles(),
        )
    }

    fn components_mut(&mut self) -> (&mut Self::DB, &mut Self::Inspector, &mut Self::Precompiles) {
        (
            &mut self.inner.0.ctx.journaled_state.database,
            &mut self.inner.0.inspector,
            self.inner.0.precompiles.precompiles_mut(),
        )
    }
}

/// Factory producing [`ArbEvm`]s. Mirrors `OpEvmFactory`.
///
/// `Spec` is `arb_revm`'s [`ArbSpecId`] (ArbOS-version-keyed, derived from `ArbHeaderInfo` by
/// `ArbEvmConfig`); precompile set is the Arbitrum one (version-gated); tx type is [`ArbTx`].
#[derive(Debug, Clone, Copy, Default)]
pub struct ArbEvmFactory;

impl ArbEvmFactory {
    /// Builds an [`ArbEvmContext`] for the given database and EVM environment.
    ///
    /// Seeds [`ArbChainContext::l1_block_number`] (the value `arb_revm`'s `NUMBER` override reads)
    /// from the block env, so every path that builds an EVM from an [`EvmEnv`] alone gets it: the
    /// RPC simulation path (`eth_call`, `eth_estimateGas`, `debug_traceCall`) never goes through
    /// the block executor, which sets it separately from `ArbBlockExecutionCtx`.
    fn build_ctx<DB: Database>(
        db: DB,
        evm_env: EvmEnv<ArbSpecId, ArbBlockEnv>,
    ) -> ArbEvmContext<DB> {
        let chain =
            ArbChainContext::default().with_l1_block_number(evm_env.block_env.l1_block_number);
        Context::mainnet()
            .with_chain(chain)
            .with_db(db)
            .with_block(evm_env.block_env)
            .with_cfg(evm_env.cfg_env)
            .with_tx(ArbTransaction::<TxEnv>::default())
    }
}

impl EvmFactory for ArbEvmFactory {
    type Evm<DB: Database, I: Inspector<Self::Context<DB>>> = ArbEvm<DB, I>;
    type Context<DB: Database> = ArbEvmContext<DB>;
    type Tx = ArbTx;
    type Error<DBError: core::error::Error + Send + Sync + 'static + DBErrorMarker> =
        ArbEvmError<DBError>;
    type HaltReason = HaltReason;
    type Spec = ArbSpecId;
    type BlockEnv = ArbBlockEnv;
    type Precompiles = PrecompilesMap;

    fn create_evm<DB: Database>(
        &self,
        db: DB,
        evm_env: EvmEnv<ArbSpecId, ArbBlockEnv>,
    ) -> Self::Evm<DB, NoOpInspector> {
        // `create_evm` must return `Evm<DB, NoOpInspector>`, so pass an explicit `NoOpInspector`
        // rather than the `()` default of `build_arb`. Swap in the ArbOS precompile provider
        // (dispatches ArbOS addresses with the full `ArbEvmContext`; exposes a `PrecompilesMap` to
        // satisfy `ConfigureEvm`).
        let spec = evm_env.cfg_env.spec;
        let inner = Self::build_ctx(db, evm_env)
            .build_arb_with_inspector(NoOpInspector {})
            .with_precompiles(ArbPrecompilesMap::new(spec));
        ArbEvm::new(inner, false)
    }

    fn create_evm_with_inspector<DB: Database, I: Inspector<Self::Context<DB>>>(
        &self,
        db: DB,
        evm_env: EvmEnv<ArbSpecId, ArbBlockEnv>,
        inspector: I,
    ) -> Self::Evm<DB, I> {
        let spec = evm_env.cfg_env.spec;
        let inner = Self::build_ctx(db, evm_env)
            .build_arb_with_inspector(inspector)
            .with_precompiles(ArbPrecompilesMap::new(spec));
        ArbEvm::new(inner, true)
    }
}

// Verifies that `ArbTx` satisfies `IntoTxEnv<ArbTx> + Default + Clone + Debug` as required by
// alloy-evm, and that the factory's `Tx` type matches the `Evm`'s.
const _: fn() = || {
    fn assert_into_tx_env<T: IntoTxEnv<T> + Default + Clone + Debug>() {}
    assert_into_tx_env::<ArbTx>();
};

#[cfg(test)]
mod tests;
