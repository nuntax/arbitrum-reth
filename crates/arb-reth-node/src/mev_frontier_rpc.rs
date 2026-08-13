//! Exact simulation against an in-progress ArbOS transaction frontier.

use alloy_evm::{Evm as _, rpc::TryIntoTxEnv as _};
use alloy_primitives::{Address, B256, Bytes, Log};
use arb_reth_engine::{ArbExecutionFrontier, ArbExecutionFrontierStore};
use arb_reth_evm::ArbEvmConfig;
use arbitrum_alloy_rpc_types::ArbTransactionRequest;
use jsonrpsee::{RpcModule, types::ErrorObjectOwned};
use reth_evm::ConfigureEvm as _;
use reth_provider::StateProviderFactory;
use reth_revm::{State, database::StateProviderDatabase};
use revm::DatabaseCommit as _;
use revm::context_interface::ContextTr as _;
use serde::{Deserialize, Serialize};

const FRONTIER_UNAVAILABLE: i32 = -32_001;

/// Request for `arb_simulateAtFrontier`.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArbFrontierSimulationRequest {
    /// Exact frontier identifier from the version-2 MEV transaction-log frame.
    pub frontier_id: B256,
    /// Call or transaction to execute without committing its state changes.
    pub transaction: ArbTransactionRequest,
    /// Enforce nonce and base-fee validation. Defaults to `false`, like `eth_simulateV1`.
    #[serde(default)]
    pub validation: bool,
}

/// Result returned by `arb_simulateAtFrontier`.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ArbFrontierSimulationResult {
    /// Frontier that was used. The server never falls back to canonical state.
    pub frontier_id: B256,
    /// Provisional L2 block containing the observed transaction.
    #[serde(with = "alloy_serde::quantity")]
    pub block_number: u64,
    /// Observed transaction index, including ArbOS's start-block transaction.
    #[serde(with = "alloy_serde::quantity")]
    pub transaction_index: u64,
    /// Transaction that produced the observed frontier.
    pub transaction_hash: B256,
    /// `success`, `revert`, or `halt`.
    pub status: &'static str,
    /// EVM return or revert data. Empty for a halt.
    pub return_data: Bytes,
    /// Compute gas used by the simulation.
    #[serde(with = "alloy_serde::quantity")]
    pub gas_used: u64,
    /// L1 poster gas observed by ArbOS. This is zero for ordinary RPC calls without poster bytes.
    #[serde(with = "alloy_serde::quantity")]
    pub gas_used_for_l1: u64,
    /// EVM logs emitted by a successful simulation.
    pub logs: Vec<Log>,
    /// Created contract address, when the simulation is a successful create.
    pub created_address: Option<Address>,
    /// Halt reason, if execution halted rather than returning or reverting.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Builds the custom RPC module. The method is installed only when frontier tracking is enabled.
pub fn module<P>(
    store: ArbExecutionFrontierStore,
    provider: P,
    evm_config: ArbEvmConfig,
    gas_cap: u64,
) -> eyre::Result<RpcModule<()>>
where
    P: StateProviderFactory + Clone + Send + Sync + 'static,
{
    let mut module = RpcModule::new((store, provider, evm_config, gas_cap));
    module.register_async_method("arb_simulateAtFrontier", |params, context, _| async move {
        let request: ArbFrontierSimulationRequest = params.one().map_err(|error| {
            ErrorObjectOwned::owned(
                -32_602,
                "invalid frontier simulation request",
                Some(error.to_string()),
            )
        })?;
        let (store, provider, evm_config, gas_cap) = context.as_ref();
        let frontier = store.get(request.frontier_id).ok_or_else(|| {
            ErrorObjectOwned::owned(
                FRONTIER_UNAVAILABLE,
                "execution frontier unavailable or expired",
                Some(request.frontier_id),
            )
        })?;
        let provider = provider.clone();
        let evm_config = evm_config.clone();
        let gas_cap = *gas_cap;
        tokio::task::spawn_blocking(move || {
            simulate(provider, evm_config, gas_cap, frontier, request)
        })
        .await
        .map_err(|error| {
            ErrorObjectOwned::owned(
                -32_603,
                "frontier simulation task failed",
                Some(error.to_string()),
            )
        })?
    })?;
    Ok(module.remove_context())
}

fn simulate<P>(
    provider: P,
    evm_config: ArbEvmConfig,
    gas_cap: u64,
    frontier: ArbExecutionFrontier,
    mut request: ArbFrontierSimulationRequest,
) -> Result<ArbFrontierSimulationResult, ErrorObjectOwned>
where
    P: StateProviderFactory,
{
    let state_provider = provider
        .state_by_block_hash(frontier.parent_hash())
        .map_err(|error| {
            ErrorObjectOwned::owned(
                FRONTIER_UNAVAILABLE,
                "frontier parent state unavailable",
                Some(error.to_string()),
            )
        })?;
    let mut state = State::builder()
        .with_database(StateProviderDatabase::new(state_provider))
        .with_cached_prestate(frontier.pre_execution_state().clone())
        .build();
    for update in frontier.state_updates() {
        state.commit((*update).clone());
    }

    let mut evm_env = frontier.evm_env().clone();
    evm_env.cfg_env.disable_eip3607 = true;
    evm_env.cfg_env.disable_block_gas_limit = true;
    evm_env.cfg_env.tx_gas_limit_cap = Some(if gas_cap == 0 { u64::MAX } else { gas_cap });
    if !request.validation {
        evm_env.cfg_env.disable_nonce_check = true;
        evm_env.cfg_env.disable_base_fee = true;
        evm_env.block_env.inner.basefee = 0;
    }
    if gas_cap != 0
        && request
            .transaction
            .inner
            .gas
            .is_none_or(|gas| gas > gas_cap)
    {
        request.transaction.inner.gas = Some(gas_cap);
    }
    let tx: arb_reth_evm::ArbTx = request.transaction.try_into_tx_env(&evm_env).map_err(
        |error: alloy_evm::rpc::EthTxEnvError| {
            ErrorObjectOwned::owned(
                -32_602,
                "invalid simulated transaction",
                Some(error.to_string()),
            )
        },
    )?;
    let mut evm = evm_config.evm_with_env(&mut state, evm_env);
    *evm.ctx_mut().chain_mut() = frontier.chain_context().clone();
    let execution = evm.transact_raw(tx).map_err(|error| {
        ErrorObjectOwned::owned(
            -32_000,
            "frontier simulation failed",
            Some(error.to_string()),
        )
    })?;
    let gas_used_for_l1 = evm.ctx().chain.poster_gas;
    let result = execution.result;
    let (status, error) = match &result {
        revm::context::result::ExecutionResult::Success { .. } => ("success", None),
        revm::context::result::ExecutionResult::Revert { .. } => ("revert", None),
        revm::context::result::ExecutionResult::Halt { reason, .. } => {
            ("halt", Some(reason.to_string()))
        }
    };

    Ok(ArbFrontierSimulationResult {
        frontier_id: frontier.frontier_id,
        block_number: frontier.block_number,
        transaction_index: frontier.transaction_index,
        transaction_hash: frontier.transaction_hash,
        status,
        return_data: result.output().cloned().unwrap_or_default(),
        gas_used: result.tx_gas_used(),
        gas_used_for_l1,
        logs: result.logs().to_vec(),
        created_address: result.created_address(),
        error,
    })
}
