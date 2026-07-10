//! [`ArbPrecompilesMap`]: the node-path ArbOS precompile provider.
//!
//! reth v2.0.0's `ConfigureEvm` hard-requires `EvmFactory<…, Precompiles = PrecompilesMap, …>` so
//! it can install its precompile cache (`precompiles_mut().map_cacheable_precompiles(…)`). We keep
//! a [`PrecompilesMap`] to satisfy that bound, but the *actual* revm precompile provider is this
//! wrapper: it dispatches ArbOS precompile addresses through `arb_revm`'s static
//! [`ArbPrecompilesEnum::run_dispatch`] with the full `revm::Context` (so precompiles see the real
//! call depth, tx type, and `ArbChainContext`, matching the in-EVM path exactly), and delegates
//! every other address to the inner [`PrecompilesMap`] (so reth's eth-precompile cache still
//! applies). This replaces the earlier `DynPrecompile`/`EvmInternals` bridge, whose context-blind
//! `PrecompileInput` forced a hardcoded call depth and could not expose `ArbChainContext`.

use alloy_evm::Database;
use alloy_evm::env::BlockEnvironment;
use alloy_evm::precompiles::PrecompilesMap;
use arb_revm::ArbSpecId;
use arb_revm::arb_journal::ArbCall;
use arb_revm::precompiles::{ArbPrecompilesEnum, arb_eth_precompiles};
use revm::context::{Cfg, Context, Journal, Transaction};
use revm::handler::PrecompileProvider;
use revm::interpreter::{CallInputs, InterpreterResult};
use revm::primitives::{Address, AddressSet};

/// Arbitrum precompile provider for the reth node EVM. See the module docs.
#[derive(Debug)]
pub struct ArbPrecompilesMap {
    /// The spec's eth precompile set. Dispatched for non-ArbOS addresses (so reth's precompile
    /// cache, installed via `precompiles_mut()`, applies), and handed to reth to satisfy
    /// `ConfigureEvm`'s `Precompiles = PrecompilesMap` bound.
    inner: PrecompilesMap,
    /// Combined warm-address set: eth precompile addresses ∪ ArbOS precompile addresses.
    warm: AddressSet,
}

impl ArbPrecompilesMap {
    /// Builds the provider for an ArbOS version: eth precompile set (Cancun+P256 below ArbOS 50,
    /// Osaka at/after, the same selection as the in-EVM path) plus the ArbOS addresses in the warm
    /// set. ArbOS precompiles are dispatched by [`ArbPrecompilesEnum`], not stored in `inner`.
    pub fn new(spec: ArbSpecId) -> Self {
        let eth = arb_eth_precompiles(spec);
        let inner = PrecompilesMap::from_static(eth);
        let mut warm = AddressSet::default();
        warm.clone_from(eth.addresses_set());
        for address in ArbPrecompilesEnum::all_addresses() {
            warm.insert(address);
        }
        Self { inner, warm }
    }

    /// The inner eth [`PrecompilesMap`] handed to reth (its `Precompiles` associated type).
    pub const fn precompiles(&self) -> &PrecompilesMap {
        &self.inner
    }

    /// Mutable inner [`PrecompilesMap`], used by reth to install its precompile cache.
    pub const fn precompiles_mut(&mut self) -> &mut PrecompilesMap {
        &mut self.inner
    }
}

// Mirrors alloy-evm's own `PrecompileProvider for PrecompilesMap` impl header (generic over the
// context type params, journal fixed to revm's `Journal<DB>`), so this provider slots into the
// same `ArbContext<DB>` the node EVM runs on.
impl<BlockEnv, TxEnv, CfgEnv, DB, Chain>
    PrecompileProvider<Context<BlockEnv, TxEnv, CfgEnv, DB, Journal<DB>, Chain>>
    for ArbPrecompilesMap
where
    BlockEnv: BlockEnvironment,
    TxEnv: Transaction,
    CfgEnv: Cfg,
    DB: Database,
{
    type Output = InterpreterResult;

    fn set_spec(&mut self, _spec: <CfgEnv as Cfg>::Spec) -> bool {
        // The spec is baked in at construction (a fresh provider per `create_evm`); mirror
        // `PrecompilesMap::set_spec`, which is also a no-op.
        false
    }

    fn run(
        &mut self,
        context: &mut Context<BlockEnv, TxEnv, CfgEnv, DB, Journal<DB>, Chain>,
        inputs: &CallInputs,
    ) -> Result<Option<Self::Output>, String> {
        if let Some(arb) = ArbPrecompilesEnum::from_address(&inputs.bytecode_address) {
            // Resolve the (possibly shared-buffer) calldata, then dispatch through the shared
            // `run_dispatch` (gating + per-call gas), same as the in-EVM `ArbPrecompiles::run`.
            let raw = inputs.input.bytes(context);
            let call = ArbCall {
                input: raw.as_ref(),
                gas_limit: inputs.gas_limit,
                caller: inputs.caller,
                value: inputs.call_value(),
                bytecode_address: inputs.bytecode_address,
                acting_address: inputs.target_address,
                is_static: inputs.is_static,
            };
            return Ok(Some(arb.run_dispatch(context, &call)));
        }
        self.inner.run(context, inputs)
    }

    fn warm_addresses(&self) -> &AddressSet {
        &self.warm
    }

    fn contains(&self, address: &Address) -> bool {
        ArbPrecompilesEnum::from_address(address).is_some()
            || <PrecompilesMap as PrecompileProvider<
                Context<BlockEnv, TxEnv, CfgEnv, DB, Journal<DB>, Chain>,
            >>::contains(&self.inner, address)
    }
}
