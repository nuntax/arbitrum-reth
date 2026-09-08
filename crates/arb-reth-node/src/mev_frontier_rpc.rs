//! Exact simulation against an in-progress ArbOS transaction frontier.

use alloy_evm::{Evm as _, EvmEnv, rpc::TryIntoTxEnv as _};
use alloy_primitives::{Address, B256, Bytes, Log};
use arb_reth_engine::{
    ArbExecutionFrontier, ArbExecutionFrontierStore, EXECUTION_FRONTIER_ID_FORMULA,
    EXECUTION_FRONTIER_ID_SCHEME, EXECUTION_FRONTIER_VERSION,
};
use arb_reth_evm::{ArbBlockEnv, ArbEvmConfig};
use arb_revm::ArbSpecId;
use arbitrum_alloy_rpc_types::ArbTransactionRequest;
use jsonrpsee::{RpcModule, types::ErrorObjectOwned};
use reth_evm::ConfigureEvm as _;
use reth_provider::StateProviderFactory;
use reth_revm::{State, database::StateProviderDatabase};
use reth_storage_api::BlockHashReader;
use revm::DatabaseCommit as _;
use revm::context_interface::ContextTr as _;
use serde::{Deserialize, Serialize};

const FRONTIER_UNAVAILABLE: i32 = -32_001;

/// Request for `arb_simulateAtFrontier`.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArbFrontierSimulationRequest {
    /// Exact frontier identifier from the version-3 MEV transaction-log frame.
    pub frontier_id: B256,
    /// Call or transaction to execute without committing its state changes.
    pub transaction: ArbTransactionRequest,
    /// Enforce ordinary transaction validation. Defaults to call-style relaxed validation.
    #[serde(default)]
    pub validation: bool,
}

/// Result returned by `arb_simulateAtFrontier`.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ArbFrontierSimulationResult {
    /// Frontier that was used. The server never falls back to canonical state.
    pub frontier_id: B256,
    pub frontier_version: u8,
    pub parent_hash: B256,
    pub attempt_id: B256,
    pub validation_checks: ValidationChecks,
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

/// Checks actually enabled for this result, not a promise of eventual inclusion/finality.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ValidationChecks {
    pub nonce: bool,
    pub base_fee: bool,
    pub sender_code: bool,
    pub block_gas_limit: bool,
    pub canonical_parent: bool,
}

fn capabilities() -> serde_json::Value {
    serde_json::json!({
        "frontierVersion": EXECUTION_FRONTIER_VERSION,
        "frameVersion": EXECUTION_FRONTIER_VERSION,
        "frameFixedBodyBytes": crate::mev_tx_logs::FIXED_BODY_LEN,
        "frontierIdScheme": EXECUTION_FRONTIER_ID_SCHEME,
        "frontierIdFormula": EXECUTION_FRONTIER_ID_FORMULA,
        "strictValidation": true,
        "canonicalParentValidation": true
    })
}

/// Builds the custom RPC module. The method is installed only when frontier tracking is enabled.
pub fn module<P>(
    store: ArbExecutionFrontierStore,
    provider: P,
    evm_config: ArbEvmConfig,
    gas_cap: u64,
) -> eyre::Result<RpcModule<()>>
where
    P: StateProviderFactory + BlockHashReader + Clone + Send + Sync + 'static,
{
    let mut module = RpcModule::new((store, provider, evm_config, gas_cap));
    module.register_method("arb_frontierCapabilities", |_, _, _| capabilities())?;
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
        let store = store.clone();
        let evm_config = evm_config.clone();
        let gas_cap = *gas_cap;
        tokio::task::spawn_blocking(move || {
            simulate(provider, store, evm_config, gas_cap, frontier, request)
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
    store: ArbExecutionFrontierStore,
    evm_config: ArbEvmConfig,
    gas_cap: u64,
    frontier: ArbExecutionFrontier,
    mut request: ArbFrontierSimulationRequest,
) -> Result<ArbFrontierSimulationResult, ErrorObjectOwned>
where
    P: StateProviderFactory + BlockHashReader,
{
    validate_anchor(&provider, &store, &frontier)?;
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
    let validation_checks = prepare_validation(&mut evm_env, &mut request, gas_cap)?;
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
    validate_anchor(&provider, &store, &frontier)?;
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
        frontier_version: EXECUTION_FRONTIER_VERSION,
        parent_hash: frontier.parent_hash(),
        attempt_id: frontier.attempt_id(),
        validation_checks,
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

fn validate_anchor<P: BlockHashReader>(
    provider: &P,
    store: &ArbExecutionFrontierStore,
    frontier: &ArbExecutionFrontier,
) -> Result<(), ErrorObjectOwned> {
    let parent_number = frontier.block_number.checked_sub(1).ok_or_else(|| {
        ErrorObjectOwned::owned(FRONTIER_UNAVAILABLE, "frontier has no parent", None::<()>)
    })?;
    let canonical_hash = provider.block_hash(parent_number).map_err(|error| {
        ErrorObjectOwned::owned(
            FRONTIER_UNAVAILABLE,
            "frontier canonical parent unavailable",
            Some(error.to_string()),
        )
    })?;
    if canonical_hash != Some(frontier.parent_hash()) || !store.is_current(frontier) {
        return Err(ErrorObjectOwned::owned(
            FRONTIER_UNAVAILABLE,
            "frontier parent is noncanonical or execution attempt expired",
            None::<()>,
        ));
    }
    Ok(())
}

fn prepare_validation(
    env: &mut EvmEnv<ArbSpecId, ArbBlockEnv>,
    request: &mut ArbFrontierSimulationRequest,
    gas_cap: u64,
) -> Result<ValidationChecks, ErrorObjectOwned> {
    let strict = request.validation;
    if strict {
        // Protocol-delivered ArbOS types bypass normal nonce/code/env checks in ArbHandler.
        // Never advertise strict validation for those privileged transaction types.
        if request
            .transaction
            .inner
            .transaction_type
            .is_some_and(|kind| !matches!(kind, 0..=2 | 4))
        {
            return Err(ErrorObjectOwned::owned(
                -32_602,
                "strict simulation requires a supported user transaction type",
                None::<()>,
            ));
        }
        if request
            .transaction
            .inner
            .gas
            .is_none_or(|gas| gas == 0 || (gas_cap != 0 && gas > gas_cap))
        {
            return Err(ErrorObjectOwned::owned(
                -32_602,
                "strict simulation requires explicit nonzero gas within RPC gas cap",
                None::<()>,
            ));
        }
        // Explicitly undo inherited call-style flags; do not rely on the producer's defaults.
        env.cfg_env.disable_balance_check = false;
    } else {
        env.cfg_env.tx_gas_limit_cap = Some(if gas_cap == 0 { u64::MAX } else { gas_cap });
        env.block_env.inner.basefee = 0;
        if gas_cap != 0
            && request
                .transaction
                .inner
                .gas
                .is_none_or(|gas| gas > gas_cap)
        {
            request.transaction.inner.gas = Some(gas_cap);
        }
    }
    env.cfg_env.disable_nonce_check = !strict;
    env.cfg_env.disable_base_fee = !strict;
    env.cfg_env.disable_eip3607 = !strict;
    env.cfg_env.disable_block_gas_limit = !strict;
    Ok(ValidationChecks {
        nonce: strict,
        base_fee: strict,
        sender_code: strict,
        block_gas_limit: strict,
        canonical_parent: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::U256;
    use arb_reth_engine::ArbTxLogBroadcaster;
    use arb_revm::{ArbChainContext, ArbTransaction};
    use revm::{
        bytecode::Bytecode,
        context::{CfgEnv, TxEnv},
        database::{CacheDB, EmptyDB},
        state::{AccountInfo, EvmState},
    };
    use revm_database::CacheState;
    use std::cell::Cell;

    fn env() -> EvmEnv<ArbSpecId, ArbBlockEnv> {
        let mut env = EvmEnv::new(
            CfgEnv::new_with_spec(ArbSpecId::ARBOS_51).with_chain_id(4663),
            ArbBlockEnv::default(),
        );
        env.block_env.inner.number = U256::from(42);
        env.block_env.inner.basefee = 10;
        env.block_env.inner.gas_limit = 500_000;
        env
    }

    fn request() -> ArbFrontierSimulationRequest {
        serde_json::from_value(serde_json::json!({
            "frontierId": B256::repeat_byte(0x11), "validation": true,
            "transaction": {"from": Address::repeat_byte(0x11), "to": Address::repeat_byte(0x22),
                "chainId":"0x1237", "type":"0x2", "nonce":"0x1", "gas":"0x186a0",
                "maxFeePerGas":"0x14", "maxPriorityFeePerGas":"0x1"}
        }))
        .unwrap()
    }

    #[test]
    fn strict_validation_resets_inherited_flags_and_preserves_gas_intent() {
        let mut env = env();
        env.cfg_env.disable_nonce_check = true;
        env.cfg_env.disable_base_fee = true;
        env.cfg_env.disable_eip3607 = true;
        env.cfg_env.disable_block_gas_limit = true;
        env.cfg_env.disable_balance_check = true;
        env.cfg_env.tx_gas_limit_cap = Some(321_000);
        let mut request = request();
        let original = serde_json::to_value(&request.transaction).unwrap();
        let checks = prepare_validation(&mut env, &mut request, 200_000).unwrap();
        assert_eq!(
            serde_json::to_value(checks).unwrap(),
            serde_json::json!({
                "nonce":true,"baseFee":true,"senderCode":true,"blockGasLimit":true,"canonicalParent":true
            })
        );
        assert!(
            !env.cfg_env.disable_nonce_check
                && !env.cfg_env.disable_base_fee
                && !env.cfg_env.disable_eip3607
                && !env.cfg_env.disable_block_gas_limit
                && !env.cfg_env.disable_balance_check
        );
        assert_eq!(env.block_env.inner.basefee, 10);
        assert_eq!(env.cfg_env.tx_gas_limit_cap, Some(321_000));
        assert_eq!(
            serde_json::to_value(&request.transaction).unwrap(),
            original
        );
        for gas in [None, Some(0), Some(200_001)] {
            request.transaction.inner.gas = gas;
            assert_eq!(
                prepare_validation(&mut env, &mut request, 200_000)
                    .unwrap_err()
                    .code(),
                -32602
            );
            assert_eq!(
                request.transaction.inner.gas, gas,
                "invalid intent must not be clamped"
            );
        }
        request.transaction.inner.gas = Some(100_000);
        for kind in [3, 0x64, 0x66, 0x68, 0x69, 0xff] {
            request.transaction.inner.transaction_type = Some(kind);
            assert!(prepare_validation(&mut env, &mut request, 200_000).is_err());
        }
    }

    #[test]
    fn loose_validation_is_reported_truthfully_and_keeps_call_compatibility() {
        let mut env = env();
        let mut request = request();
        request.validation = false;
        request.transaction.inner.gas = None;
        let checks = prepare_validation(&mut env, &mut request, 200_000).unwrap();
        assert_eq!(request.transaction.inner.gas, Some(200_000));
        assert_eq!(env.block_env.inner.basefee, 0);
        assert_eq!(
            serde_json::to_value(checks).unwrap(),
            serde_json::json!({
                "nonce":false,"baseFee":false,"senderCode":false,"blockGasLimit":false,"canonicalParent":true
            })
        );
    }

    fn execute_request(
        mut request: ArbFrontierSimulationRequest,
        sender_code: bool,
    ) -> Result<(), String> {
        let mut env = env();
        prepare_validation(&mut env, &mut request, 1_000_000).unwrap();
        // Isolate real handler checks from RPC conversion, which itself rejects low fee caps
        // before the EVM is reached. The production path keeps that conversion check as well.
        let inner = request.transaction.inner;
        let tx = arb_reth_evm::ArbTx(ArbTransaction::new(TxEnv {
            tx_type: inner.transaction_type.unwrap(),
            caller: inner.from.unwrap(),
            gas_limit: inner.gas.unwrap(),
            gas_price: inner.max_fee_per_gas.unwrap(),
            gas_priority_fee: inner.max_priority_fee_per_gas,
            kind: inner.to.unwrap(),
            nonce: inner.nonce.unwrap(),
            chain_id: inner.chain_id,
            ..Default::default()
        }));
        let mut db = CacheDB::new(EmptyDB::default());
        let mut account = AccountInfo {
            nonce: 1,
            balance: U256::from(1_000_000_000_000_000_000u64),
            ..Default::default()
        };
        if sender_code {
            let code = Bytecode::new_raw(Bytes::from_static(&[0x00]));
            account.code_hash = code.hash_slow();
            account.code = Some(code);
        }
        db.insert_account_info(Address::repeat_byte(0x11), account);
        let mut evm = ArbEvmConfig::new(4663).evm_with_env(db, env);
        evm.transact_raw(tx)
            .map(|_| ())
            .map_err(|error| format!("{error:?}"))
    }

    #[test]
    fn actual_arb_evm_enforces_each_strict_check_and_relaxes_only_when_requested() {
        assert!(execute_request(request(), false).is_ok());
        for (case, expected) in [
            (0, "Nonce"),
            (1, "GasPriceLessThanBasefee"),
            (2, "RejectCallerWithCode"),
            (3, "CallerGasLimitMoreThanBlock"),
        ] {
            let mut request = request();
            match case {
                0 => request.transaction.inner.nonce = Some(0),
                1 => {
                    request.transaction.inner.max_fee_per_gas = Some(1);
                    request.transaction.inner.max_priority_fee_per_gas = Some(0);
                }
                2 => {}
                3 => request.transaction.inner.gas = Some(600_000),
                _ => unreachable!(),
            }
            let error = execute_request(request.clone(), case == 2).unwrap_err();
            assert!(error.contains(expected), "case {case}: {error}");
            request.validation = false;
            assert!(
                execute_request(request, case == 2).is_ok(),
                "relaxed case {case}"
            );
        }
    }

    struct CanonicalParent(Cell<Option<B256>>);
    impl BlockHashReader for CanonicalParent {
        fn block_hash(&self, number: u64) -> reth_provider::ProviderResult<Option<B256>> {
            assert_eq!(number, 41);
            Ok(self.0.get())
        }
        fn canonical_hashes_range(
            &self,
            _: u64,
            _: u64,
        ) -> reth_provider::ProviderResult<Vec<B256>> {
            unreachable!("single parent lookup only")
        }
    }

    #[test]
    fn anchor_recheck_rejects_parent_reorg_missing_parent_and_same_parent_new_attempt() {
        let parent = B256::repeat_byte(0x11);
        let provider = CanonicalParent(Cell::new(Some(parent)));
        let stream = ArbTxLogBroadcaster::new();
        let store = stream.frontier_store();
        let mut block = stream
            .begin_frontier_block(parent, env(), CacheState::default())
            .unwrap();
        let id = block
            .advance(
                42,
                0,
                B256::repeat_byte(0x22),
                EvmState::default(),
                ArbChainContext::default(),
            )
            .unwrap();
        let held = store.get(id).unwrap();
        validate_anchor(&provider, &store, &held).unwrap();
        provider.0.set(Some(B256::repeat_byte(0x33)));
        assert_eq!(
            validate_anchor(&provider, &store, &held)
                .unwrap_err()
                .code(),
            FRONTIER_UNAVAILABLE
        );
        provider.0.set(None);
        assert!(validate_anchor(&provider, &store, &held).is_err());
        provider.0.set(Some(parent));
        validate_anchor(&provider, &store, &held).unwrap();
        let _next = stream
            .begin_frontier_block(parent, env(), CacheState::default())
            .unwrap();
        assert_eq!(
            validate_anchor(&provider, &store, &held)
                .unwrap_err()
                .code(),
            FRONTIER_UNAVAILABLE
        );
    }

    #[test]
    fn capabilities_pin_wire_and_prefix_contract() {
        assert_eq!(
            capabilities(),
            serde_json::json!({
                "frontierVersion":3,"frameVersion":3,"frameFixedBodyBytes":160,
                "frontierIdScheme":"keccak256-rhf3-prefix-v1",
                "frontierIdFormula":"keccak256(RHF3||parentHash||blockNumberBE64||attemptId||previousFrontierId||transactionIndexBE64||transactionHash)",
                "strictValidation":true,"canonicalParentValidation":true
            })
        );
    }
}
