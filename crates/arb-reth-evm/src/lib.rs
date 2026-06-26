//! `arb-reth-evm` — bridge `arb_revm` (Arbitrum/ArbOS execution on revm 36) into reth's
//! EVM and block-execution extension points.
//!
//! **Stage B** of the arb-reth roadmap (see `docs/arb-reth-roadmap.md`): wrap `arb_revm`'s ArbOS
//! EVM as alloy-evm's [`EvmFactory`] + [`Evm`], so a single Arbitrum transaction can be executed
//! through the same `create_evm(...).transact_raw(tx)` surface reth drives, with `arb_revm`
//! semantics (ArbOS handler, NUMBER=L1 block number, Arb precompiles).
//!
//! Mirrors `alloy-op-evm`'s `OpEvm` / `OpEvmFactory`. The structural difference from op: `arb_revm`
//! already exposes its EVM as a revm [`ExecuteEvm`]/[`InspectEvm`] impl over a concrete
//! [`ArbContext`], so this crate is a thin adapter that owns the inner [`ArbRethEvm`] and reconciles
//! the revm `ExecuteEvm::transact` result (`ExecResultAndState`, which is exactly
//! [`ResultAndState`]) with the alloy-evm [`Evm`] trait.
//!
//! Stage C (`BlockExecutor`/`BlockAssembler`) and Stage D (`ConfigureEvm`) build on top of this.

// Stage A's reth-primitives-traits surface (NodePrimitives/SignedTransaction/Receipt) is satisfied
// inside arb-alloy's `reth` feature; force it into the graph so unification stays exercised.
use reth_primitives_traits as _;

extern crate alloc;

pub mod tx;
pub use tx::ArbTx;

pub mod precompiles;
pub use precompiles::arb_precompiles_map;

pub mod block;
pub use block::{
    ArbBlockAssembler, ArbBlockExecutionCtx, ArbBlockExecutor, ArbBlockExecutorFactory,
    ArbTxResult,
};

pub mod config;
pub use config::{
    ARB_ONE_CHAIN_ID, ArbEvmConfig, ArbEvmConfigError, ArbNextBlockEnvAttributes,
};

use alloy_evm::precompiles::PrecompilesMap;
use alloy_evm::{Database, Evm, EvmEnv, EvmFactory, IntoTxEnv};
use alloy_primitives::{Address, Bytes};
use arb_revm::api::default_ctx::ArbContext;
use arb_revm::{ArbBuilder, ArbChainContext, ArbTransaction};
use core::fmt::Debug;
use revm::context::result::{EVMError, HaltReason, InvalidTransaction, ResultAndState};
use revm::context::{BlockEnv, Context, TxEnv};
use revm::handler::instructions::EthInstructions;
use revm::inspector::NoOpInspector;
use revm::interpreter::interpreter::EthInterpreter;
use revm::{ExecuteEvm, InspectEvm, Inspector, MainContext, SystemCallEvm};

use arb_revm::ArbSpecId;

/// Concrete `arb_revm` EVM type the bridge owns: the ArbOS EVM (`arb_revm::ArbEvm`) over the
/// default Arbitrum context [`ArbContext<DB>`] with the Arbitrum precompile set.
type ArbRethEvm<DB, I> =
    arb_revm::ArbEvm<ArbContext<DB>, I, EthInstructions<EthInterpreter, ArbContext<DB>>, PrecompilesMap>;

/// EVM error surfaced by the Arbitrum bridge. Matches `arb_revm`'s `ArbError`:
/// `EVMError<DBError, InvalidTransaction>`.
pub type ArbEvmError<DBError> = EVMError<DBError, InvalidTransaction>;

/// Arbitrum EVM — alloy-evm [`Evm`] adapter wrapping `arb_revm`'s ArbOS EVM.
///
/// `inspect` is tracked here (not in the inner EVM) so that [`Evm::transact_raw`] dispatches to the
/// inspecting (`inspect_tx`) or plain (`transact`) execution path, exactly like `OpEvm`.
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
    pub fn ctx(&self) -> &ArbContext<DB> {
        &self.inner.0.ctx
    }

    /// Mutable reference to the inner Arbitrum execution context.
    pub fn ctx_mut(&mut self) -> &mut ArbContext<DB> {
        &mut self.inner.0.ctx
    }
}

impl<DB, I> Evm for ArbEvm<DB, I>
where
    DB: Database,
    I: Inspector<ArbContext<DB>, EthInterpreter>,
{
    type DB = DB;
    type Tx = ArbTx;
    type Error = ArbEvmError<DB::Error>;
    type HaltReason = HaltReason;
    type Spec = ArbSpecId;
    type BlockEnv = BlockEnv;
    type Precompiles = PrecompilesMap;
    type Inspector = I;

    fn block(&self) -> &BlockEnv {
        &self.inner.0.ctx.block
    }

    fn chain_id(&self) -> u64 {
        self.inner.0.ctx.cfg.chain_id
    }

    fn transact_raw(
        &mut self,
        tx: Self::Tx,
    ) -> Result<ResultAndState<Self::HaltReason>, Self::Error> {
        // `ArbTransaction<TxEnv>` is `arb_revm`'s tx env; both `transact` and `inspect_tx` route it
        // through `ArbHandler` (ArbOS gas charging, poster fee, NUMBER override, Arb precompiles).
        // revm 36's `ExecuteEvm::transact` returns `ExecResultAndState<ExecutionResult<HaltReason>,
        // EvmState>`, which is exactly `ResultAndState<HaltReason>`.
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

    fn finish(self) -> (Self::DB, EvmEnv<Self::Spec>) {
        let Context { block: block_env, cfg: cfg_env, journaled_state, .. } = self.inner.0.ctx;
        (journaled_state.database, EvmEnv { block_env, cfg_env })
    }

    fn set_inspector_enabled(&mut self, enabled: bool) {
        self.inspect = enabled;
    }

    fn components(&self) -> (&Self::DB, &Self::Inspector, &Self::Precompiles) {
        (
            &self.inner.0.ctx.journaled_state.database,
            &self.inner.0.inspector,
            &self.inner.0.precompiles,
        )
    }

    fn components_mut(&mut self) -> (&mut Self::DB, &mut Self::Inspector, &mut Self::Precompiles) {
        (
            &mut self.inner.0.ctx.journaled_state.database,
            &mut self.inner.0.inspector,
            &mut self.inner.0.precompiles,
        )
    }
}

/// Factory producing [`ArbEvm`]s. Mirrors `OpEvmFactory`.
///
/// The `Spec` is `arb_revm`'s [`ArbSpecId`] (ArbOS-version-keyed, derived from `ArbHeaderInfo` in
/// Stage D); the precompile set is the Arbitrum one ([`ArbPrecompiles`], version-gated); the tx is
/// [`ArbTx`].
#[derive(Debug, Clone, Copy, Default)]
pub struct ArbEvmFactory;

impl ArbEvmFactory {
    /// Builds an [`ArbContext`] for the given database and EVM environment.
    ///
    /// The ArbOS-specific chain context ([`ArbChainContext`]) is defaulted here. Block-scoped
    /// inputs that are not representable in alloy's [`EvmEnv`] — notably the L1 block number read
    /// by the `NUMBER` opcode — are populated from `ArbHeaderInfo` by `ConfigureEvm` in Stage D.
    /// For Stage B's transact-level proof a default chain context is sufficient (a value transfer
    /// never reads `NUMBER`).
    fn build_ctx<DB: Database>(db: DB, evm_env: EvmEnv<ArbSpecId>) -> ArbContext<DB> {
        Context::mainnet()
            .with_chain(ArbChainContext::default())
            .with_db(db)
            .with_block(evm_env.block_env)
            .with_cfg(evm_env.cfg_env)
            .with_tx(ArbTransaction::<TxEnv>::default())
    }
}

impl EvmFactory for ArbEvmFactory {
    type Evm<DB: Database, I: Inspector<Self::Context<DB>>> = ArbEvm<DB, I>;
    type Context<DB: Database> = ArbContext<DB>;
    type Tx = ArbTx;
    type Error<DBError: core::error::Error + Send + Sync + 'static> = ArbEvmError<DBError>;
    type HaltReason = HaltReason;
    type Spec = ArbSpecId;
    type BlockEnv = BlockEnv;
    type Precompiles = PrecompilesMap;

    fn create_evm<DB: Database>(
        &self,
        db: DB,
        evm_env: EvmEnv<ArbSpecId>,
    ) -> Self::Evm<DB, NoOpInspector> {
        // `create_evm` must return `Evm<DB, NoOpInspector>`, so build with an explicit
        // `NoOpInspector` (not the `()` default of `build_arb`). Swap the default `ArbPrecompiles`
        // provider for the reth-required `PrecompilesMap` (ArbOS precompiles re-homed onto
        // `DynPrecompile`s, eth set selected for the spec) — `ConfigureEvm` requires it.
        let spec = evm_env.cfg_env.spec;
        let inner = Self::build_ctx(db, evm_env)
            .build_arb_with_inspector(NoOpInspector {})
            .with_precompiles(arb_precompiles_map(spec));
        ArbEvm::new(inner, false)
    }

    fn create_evm_with_inspector<DB: Database, I: Inspector<Self::Context<DB>>>(
        &self,
        db: DB,
        evm_env: EvmEnv<ArbSpecId>,
        inspector: I,
    ) -> Self::Evm<DB, I> {
        let spec = evm_env.cfg_env.spec;
        let inner = Self::build_ctx(db, evm_env)
            .build_arb_with_inspector(inspector)
            .with_precompiles(arb_precompiles_map(spec));
        ArbEvm::new(inner, true)
    }
}

// `IntoTxEnv` allows `transact(tx)` to accept the bare `ArbTx`; `Default + Clone + Debug` are
// required by alloy-evm's higher-level bounds. Spell out the bound link so the factory's `Tx`
// (`ArbTx`) matches the `Evm`'s `Tx`.
const _: fn() = || {
    fn assert_into_tx_env<T: IntoTxEnv<T> + Default + Clone + Debug>() {}
    assert_into_tx_env::<ArbTx>();
};

#[cfg(test)]
mod tests;
