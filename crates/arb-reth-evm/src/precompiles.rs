//! Node-path ArbOS precompiles as an alloy-evm [`PrecompilesMap`].
//!
//! reth v2.0.0's `ConfigureEvm` requires `EvmFactory<…, Precompiles = PrecompilesMap, …>`.
//! `PrecompilesMap` dispatches each precompile through a [`DynPrecompile`] closure that receives a
//! [`PrecompileInput`] (carrying an `EvmInternals` state handle), NOT a revm `Context`. We bridge
//! to `arb_revm`'s precompiles (unchanged, parity-validated) via `ArbPrecompilesEnum::run_dispatch`
//! over an [`ArbNodeCtx`] (see `arb_revm::arb_journal`). One `DynPrecompile` per ArbOS precompile
//! address is layered over the spec's eth precompile set.

use alloy_evm::precompiles::{DynPrecompile, PrecompileInput, PrecompilesMap};
use arb_revm::ArbSpecId;
use arb_revm::arb_journal::{ArbCall, ArbNodeCtx};
use arb_revm::precompiles::{ArbPrecompilesEnum, arb_eth_precompiles};
use revm::interpreter::{InstructionResult, InterpreterResult};
use revm::precompile::{PrecompileHalt, PrecompileId, PrecompileOutput, PrecompileResult};

/// Builds the Arbitrum [`PrecompilesMap`] for an ArbOS version: the spec's eth precompile set
/// (Cancun+P256 below ArbOS 50, Osaka at/after; same selection as the in-EVM path) with the 16
/// ArbOS precompile addresses layered on top as stateful [`DynPrecompile`]s.
pub fn arb_precompiles_map(spec: ArbSpecId) -> PrecompilesMap {
    let mut map = PrecompilesMap::from_static(arb_eth_precompiles(spec));
    for address in ArbPrecompilesEnum::all_addresses() {
        let arb = ArbPrecompilesEnum::from_address(&address)
            .expect("all_addresses yields only known ArbOS precompile addresses");
        let precompile = DynPrecompile::new_stateful(
            PrecompileId::Custom(arb_precompile_name(arb).into()),
            move |input: PrecompileInput<'_>| arb_node_call(arb, input),
        );
        // Overwrite/insert at the ArbOS address (converts the map to its dynamic representation).
        map.apply_precompile(&address, move |_| Some(precompile));
    }
    map
}

/// Runs one ArbOS precompile on the node path: rebuild the path-agnostic [`ArbCall`] from the
/// [`PrecompileInput`], wrap the `EvmInternals` handle in an [`ArbNodeCtx`], and dispatch through
/// the shared `run_dispatch` (version/method gating + per-call gas live there).
fn arb_node_call(arb: ArbPrecompilesEnum, input: PrecompileInput<'_>) -> PrecompileResult {
    let PrecompileInput {
        data,
        gas,
        caller,
        value,
        is_static,
        bytecode_address,
        target_address,
        mut internals,
        reservoir,
        ..
    } = input;
    let call = ArbCall {
        input: data,
        gas_limit: gas,
        caller,
        value,
        bytecode_address,
        acting_address: target_address,
        is_static,
    };
    // `PrecompileInput` carries no EVM call depth; default to top-level. Only `ArbSys.isTopLevelCall`
    // observes this (a rare path), so this is best-effort on the node path.
    let mut ctx = ArbNodeCtx::new(&mut internals, 1);
    to_precompile_result(arb.run_dispatch(&mut ctx, &call), reservoir)
}

/// Convert `arb_revm`'s `InterpreterResult` (the precompile call result) into alloy-evm's
/// [`PrecompileResult`]. Gas spent maps to `gas_used`; a revert preserves its output bytes; any
/// halt (out-of-gas etc.) maps to a halted [`PrecompileOutput`] (revm treats it as consuming all
/// gas, matching the in-EVM CALL semantics).
fn to_precompile_result(result: InterpreterResult, reservoir: u64) -> PrecompileResult {
    let gas_used = result.gas.total_gas_spent();
    match result.result {
        InstructionResult::Return => Ok(PrecompileOutput::new(gas_used, result.output, reservoir)),
        InstructionResult::Revert => Ok(PrecompileOutput::revert(gas_used, result.output, reservoir)),
        _ => Ok(PrecompileOutput::halt(PrecompileHalt::OutOfGas, reservoir)),
    }
}

/// Stable display name for a precompile's [`PrecompileId`].
const fn arb_precompile_name(arb: ArbPrecompilesEnum) -> &'static str {
    match arb {
        ArbPrecompilesEnum::ArbSys => "ArbSys",
        ArbPrecompilesEnum::ArbInfo => "ArbInfo",
        ArbPrecompilesEnum::ArbAddressTable => "ArbAddressTable",
        ArbPrecompilesEnum::ArbBls => "ArbBLS",
        ArbPrecompilesEnum::ArbFunctionTable => "ArbFunctionTable",
        ArbPrecompilesEnum::ArbOwnerPublic => "ArbOwnerPublic",
        ArbPrecompilesEnum::ArbGasInfo => "ArbGasInfo",
        ArbPrecompilesEnum::ArbAggregator => "ArbAggregator",
        ArbPrecompilesEnum::ArbRetryableTx => "ArbRetryableTx",
        ArbPrecompilesEnum::ArbStatistics => "ArbStatistics",
        ArbPrecompilesEnum::ArbOwner => "ArbOwner",
        ArbPrecompilesEnum::ArbWasm => "ArbWasm",
        ArbPrecompilesEnum::ArbWasmCache => "ArbWasmCache",
        ArbPrecompilesEnum::ArbNativeTokenManager => "ArbNativeTokenManager",
        ArbPrecompilesEnum::ArbFilteredTransactionsManager => "ArbFilteredTransactionsManager",
        ArbPrecompilesEnum::ArbDebug => "ArbDebug",
    }
}
