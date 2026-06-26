//! Stage B exit proof: one Arbitrum transaction executes through
//! `ArbEvmFactory::create_evm(...).transact_raw(tx)` and produces a `ResultAndState` consistent
//! with `arb_revm` running the same tx directly (gas_used matches, balances update).

use super::{ArbEvm, ArbEvmFactory, ArbTx};
use alloy_evm::{Evm, EvmEnv, EvmFactory};
use arb_revm::api::default_ctx::ArbContext;
use arb_revm::{ArbBuilder, ArbChainContext, ArbSpecId, ArbTransaction};
use revm::context::result::ExecutionResult;
use revm::context::{BlockEnv, CfgEnv, TxEnv};
use revm::database::{CacheDB, EmptyDB};
use revm::primitives::{Address, TxKind, U256};
use revm::state::AccountInfo;
use revm::{Context, ExecuteEvm, MainContext};

const CHAIN_ID: u64 = 42_161;
const SENDER: Address = Address::with_last_byte(0x11);
const RECIPIENT: Address = Address::with_last_byte(0x22);
const TRANSFER_VALUE: u128 = 1_000_000_000_000_000_000; // 1 ETH
const START_BALANCE: u128 = 10_000_000_000_000_000_000; // 10 ETH

/// A funded-sender cache db with priority-fee/base-fee checks disabled so a zero-gas-price value
/// transfer executes deterministically (Stage B is a transact-level proof, not a fee-accuracy one).
fn funded_db() -> CacheDB<EmptyDB> {
    let mut db = CacheDB::new(EmptyDB::default());
    db.insert_account_info(
        SENDER,
        AccountInfo {
            balance: U256::from(START_BALANCE),
            nonce: 0,
            ..AccountInfo::default()
        },
    );
    db
}

fn cfg() -> CfgEnv<ArbSpecId> {
    let mut cfg = CfgEnv::new_with_spec(ArbSpecId::ARBOS_51)
        .with_chain_id(CHAIN_ID)
        .with_disable_priority_fee_check(true);
    cfg.disable_balance_check = false;
    cfg
}

/// The value-transfer tx env both paths execute.
fn transfer_tx() -> ArbTransaction<TxEnv> {
    ArbTransaction::new(TxEnv {
        tx_type: 2,
        caller: SENDER,
        gas_limit: 100_000,
        gas_price: 0,
        kind: TxKind::Call(RECIPIENT),
        value: U256::from(TRANSFER_VALUE),
        nonce: 0,
        chain_id: Some(CHAIN_ID),
        ..Default::default()
    })
}

/// Oracle: run the same tx through `arb_revm` directly (the path `replay_block` uses), via the
/// `ArbBuilder` + `ExecuteEvm` surface this bridge wraps.
fn oracle_result() -> (u64, U256, U256) {
    let mut db = funded_db();
    let ctx: ArbContext<&mut _> = Context::mainnet()
        .with_chain(ArbChainContext::default())
        .with_db(&mut db)
        .with_block(BlockEnv::default())
        .with_cfg(cfg())
        .with_tx(ArbTransaction::<TxEnv>::default());
    let mut evm = ctx.build_arb();
    let out = evm.transact(transfer_tx()).expect("oracle execution");
    let gas_used = out.result.gas_used();
    let sender = out.state.get(&SENDER).expect("sender in state").info.balance;
    let recipient = out
        .state
        .get(&RECIPIENT)
        .map(|a| a.info.balance)
        .unwrap_or_default();
    (gas_used, sender, recipient)
}

#[test]
fn arb_evm_factory_transact_matches_arb_revm() {
    let (oracle_gas, oracle_sender_bal, oracle_recipient_bal) = oracle_result();

    // Bridge path: build the EVM exactly as reth would, then transact_raw an ArbTx.
    let db = funded_db();
    let evm_env = EvmEnv::new(cfg(), BlockEnv::default());
    let mut evm: ArbEvm<_, _> = ArbEvmFactory.create_evm(db, evm_env);

    let out = evm
        .transact_raw(ArbTx(transfer_tx()))
        .expect("bridge execution");

    assert!(out.result.is_success(), "transfer must succeed: {:?}", out.result);

    // gas_used must be identical to arb_revm running the same tx.
    assert_eq!(
        out.result.gas_used(),
        oracle_gas,
        "bridge gas_used must match arb_revm oracle"
    );

    // Balances must update consistently with the oracle.
    let sender_bal = out.state.get(&SENDER).expect("sender in state").info.balance;
    let recipient_bal = out
        .state
        .get(&RECIPIENT)
        .map(|a| a.info.balance)
        .unwrap_or_default();

    assert_eq!(sender_bal, oracle_sender_bal, "sender balance must match oracle");
    assert_eq!(recipient_bal, oracle_recipient_bal, "recipient balance must match oracle");

    // And the value actually moved.
    assert_eq!(
        recipient_bal,
        U256::from(TRANSFER_VALUE),
        "recipient must receive the transferred value"
    );
    assert!(
        sender_bal <= U256::from(START_BALANCE - TRANSFER_VALUE),
        "sender debited value (plus any fee)"
    );

    // chain_id / block accessors are wired.
    assert_eq!(evm.chain_id(), CHAIN_ID);
}

/// End-to-end proof of the node-path precompile bridge (#36): a tx that CALLs an ArbOS precompile
/// (`ArbSys.arbOSVersion()`, which reads `arbos_version` from ArbOS storage) executes through the
/// `PrecompilesMap` → `DynPrecompile` → `run_dispatch`-over-`EvmInternals` path and must match the
/// in-EVM `arb_revm` oracle (`ArbPrecompiles`) bit-for-bit in both output and gas. Because the
/// `arbos_version` slot is **seeded** to a known non-default value, a correct result proves the
/// node-path `ArbInternals` adapter actually `sload`s the right slot through `EvmInternals` (not a
/// coincidental zero), and that the `InterpreterResult`→`PrecompileResult` conversion preserves gas.
#[test]
fn arbos_precompile_through_precompiles_map_matches_oracle() {
    use arb_revm::ArbosState;
    use revm::primitives::{Bytes, keccak256};

    // ArbSys lives at 0x64; arbOSVersion() returns 55 + the stored ArbOS version.
    const ARB_SYS: Address = Address::with_last_byte(0x64);
    const SEEDED_ARBOS_VERSION: u64 = 51;

    // A funded-sender db with the ArbOS `arbos_version` storage slot seeded to a known value.
    let seed_db = || {
        let mut db = funded_db();
        let (arbos_acct, arbos_slot) = ArbosState::open().arbos_version.account_and_key();
        db.insert_account_info(arbos_acct, AccountInfo::default());
        db.insert_account_storage(
            arbos_acct,
            U256::from_be_bytes(arbos_slot.0),
            U256::from(SEEDED_ARBOS_VERSION),
        )
        .expect("seed arbos_version slot");
        db
    };

    // calldata = selector of arbOSVersion() (no args).
    let selector = keccak256("arbOSVersion()");
    let call_tx = || {
        ArbTransaction::new(TxEnv {
            tx_type: 2,
            caller: SENDER,
            gas_limit: 1_000_000,
            gas_price: 0,
            kind: TxKind::Call(ARB_SYS),
            value: U256::ZERO,
            nonce: 0,
            chain_id: Some(CHAIN_ID),
            data: Bytes::copy_from_slice(&selector[..4]),
            ..Default::default()
        })
    };

    // Oracle: arb_revm direct (in-EVM `ArbPrecompiles` path — the parity-validated one).
    let mut odb = seed_db();
    let octx: ArbContext<&mut _> = Context::mainnet()
        .with_chain(ArbChainContext::default())
        .with_db(&mut odb)
        .with_block(BlockEnv::default())
        .with_cfg(cfg())
        .with_tx(ArbTransaction::<TxEnv>::default());
    let mut oracle_evm = octx.build_arb();
    let oracle = oracle_evm.transact(call_tx()).expect("oracle precompile call");

    // Bridge: ArbEvmFactory (node `PrecompilesMap` path).
    let evm_env = EvmEnv::new(cfg(), BlockEnv::default());
    let mut bridge_evm = ArbEvmFactory.create_evm(seed_db(), evm_env);
    let bridge = bridge_evm
        .transact_raw(ArbTx(call_tx()))
        .expect("bridge precompile call");

    assert!(oracle.result.is_success(), "oracle call failed: {:?}", oracle.result);
    assert!(bridge.result.is_success(), "bridge call failed: {:?}", bridge.result);

    let oracle_out = oracle.result.output().cloned().unwrap_or_default();
    let bridge_out = bridge.result.output().cloned().unwrap_or_default();

    // The node path must match the validated in-EVM oracle exactly.
    assert_eq!(
        bridge_out, oracle_out,
        "node-path precompile output must match the in-EVM oracle"
    );
    // ...and must reflect the seeded ArbOS state (proves the sload hit the right slot via EvmInternals).
    assert_eq!(
        U256::from_be_slice(&bridge_out),
        U256::from(55 + SEEDED_ARBOS_VERSION),
        "arbOSVersion() must return 55 + the seeded ArbOS version"
    );
    // Gas must survive the InterpreterResult -> PrecompileResult conversion identically.
    assert_eq!(
        bridge.result.gas_used(),
        oracle.result.gas_used(),
        "node-path gas_used must match the in-EVM oracle"
    );
}

#[test]
fn create_evm_with_inspector_runs_inspecting_path() {
    use revm::inspector::NoOpInspector;

    let db = funded_db();
    let evm_env = EvmEnv::new(cfg(), BlockEnv::default());
    let mut evm = ArbEvmFactory.create_evm_with_inspector(db, evm_env, NoOpInspector {});

    let out = evm
        .transact_raw(ArbTx(transfer_tx()))
        .expect("inspecting execution");
    assert!(matches!(out.result, ExecutionResult::Success { .. }));
    assert_eq!(
        out.state.get(&RECIPIENT).map(|a| a.info.balance).unwrap_or_default(),
        U256::from(TRANSFER_VALUE)
    );
}
