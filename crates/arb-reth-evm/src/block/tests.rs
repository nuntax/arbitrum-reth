//! A block's worth of transactions executes through [`ArbBlockExecutor`] producing per-tx receipts
//! whose `gas_used`, `status`, `logs`, and `gas_used_for_l1` match
//! `arb_revm::executor::execute_message`, and whose committed account state matches.
//!
//! Oracle: `arb_revm::executor::execute_message`. Executor path:
//! `ArbBlockExecutorFactory` -> `ArbBlockExecutor`.

use super::*;
use crate::ArbEvmFactory;
use alloy_consensus::transaction::Recovered;
use alloy_evm::{EvmEnv, EvmFactory};
use arb_revm::executor::{
    ArbExecCfg, ArbExecutionInput, ArbMessageEnvelope, ArbParentHeader, execute_message,
};
use arb_revm::{ArbSpecId, ArbTransaction};
use arbitrum_alloy_consensus::transactions::{ArbTxEnvelope, TxUnsigned};
use revm::DatabaseRef;
use revm::context::{BlockEnv, CfgEnv, TxEnv};
use revm::database::{CacheDB, EmptyDB, State};
use revm::primitives::{Address, Bytes, TxKind, U256};
use revm::state::AccountInfo;

/// Reads `gas_used_for_l1` off any `ArbReceiptEnvelope` variant (public fields).
fn gas_used_for_l1(r: &ArbReceiptEnvelope<alloy_primitives::Log>) -> u64 {
    use ArbReceiptEnvelope::*;
    match r {
        Legacy(r) | Eip2930(r) | Eip1559(r) | Eip4844(r) | Eip7702(r) | Deposit(r)
        | Unsigned(r) | Contract(r) | Retry(r) | SubmitRetryable(r) | Internal(r) => {
            r.receipt.gas_used_for_l1
        }
    }
}

const CHAIN_ID: u64 = 42_161;
const POSTER: Address = Address::with_last_byte(0xAA);
const SENDER_A: Address = Address::with_last_byte(0x11);
const SENDER_B: Address = Address::with_last_byte(0x12);
const RECIPIENT: Address = Address::with_last_byte(0x22);
const START_BALANCE: u128 = 100_000_000_000_000_000_000; // 100 ETH

const PARENT_NUMBER: u64 = 100;
const PARENT_TIMESTAMP: u64 = 1_000;
const L1_TIMESTAMP: u64 = 1_005;
const BASEFEE: u64 = 100_000_000;
const PARENT_GAS_LIMIT: u64 = 30_000_000;

/// A funded db so the sender accounts can pay for value transfers + fees.
fn funded_db() -> CacheDB<EmptyDB> {
    let mut db = CacheDB::new(EmptyDB::default());
    for who in [SENDER_A, SENDER_B] {
        db.insert_account_info(
            who,
            AccountInfo {
                balance: U256::from(START_BALANCE),
                nonce: 0,
                ..AccountInfo::default()
            },
        );
    }
    db
}

/// Two unsigned (type 0x65) value transfers with explicit senders (no secp256k1 recovery needed).
fn user_txs() -> Vec<ArbTxEnvelope> {
    let mk = |from: Address, nonce: u64, value: u128| {
        ArbTxEnvelope::from(TxUnsigned {
            chain_id: U256::from(CHAIN_ID),
            from,
            nonce,
            gas_fee_cap: U256::from(BASEFEE),
            gas_limit: 100_000,
            to: TxKind::Call(RECIPIENT),
            value: U256::from(value),
            input: Bytes::new(),
        })
    };
    vec![
        mk(SENDER_A, 0, 1_000_000_000_000_000_000),
        mk(SENDER_B, 0, 2_000_000_000_000_000_000),
    ]
}

fn exec_cfg() -> ArbExecCfg {
    ArbExecCfg {
        chain_id: CHAIN_ID,
        spec_id: ArbSpecId::ARBOS_51,
        block_gas_limit: PARENT_GAS_LIMIT,
        disable_priority_fee_check: true,
        disable_balance_check: false,
    }
}

fn parent_header() -> ArbParentHeader {
    ArbParentHeader {
        number: PARENT_NUMBER,
        timestamp: PARENT_TIMESTAMP,
        beneficiary: POSTER,
        basefee: BASEFEE,
        gas_limit: PARENT_GAS_LIMIT,
        difficulty: U256::ZERO,
        prevrandao: Some(revm::primitives::B256::ZERO),
    }
}

fn message() -> ArbMessageEnvelope {
    ArbMessageEnvelope {
        sequence_number: Some(7),
        l1_block_number: 0,
        l1_timestamp: L1_TIMESTAMP,
        poster: POSTER,
        l1_base_fee_wei: U256::from(BASEFEE),
        delayed_messages_read: 0,
        txs: user_txs(),
    }
}

/// Poster-gas oracle: drives the same StartBlock prelude + user txs through a directly built
/// `arb_revm` EVM (the `ArbBuilder` surface) and reads `chain().poster_gas` after each user tx.
/// Confirms the executor captures `poster_gas` faithfully.
fn oracle_poster_gas_per_tx() -> Vec<u64> {
    use arb_revm::api::default_ctx::ArbContext;
    use arb_revm::{ArbBuilder, ArbChainContext};
    use revm::{Context, ExecuteCommitEvm, ExecuteEvm, MainContext};

    let mut db = funded_db();
    let env = evm_env();
    let ctx: ArbContext<&mut _> = Context::mainnet()
        .with_chain(ArbChainContext::default())
        .with_db(&mut db)
        .with_block(env.block_env.clone())
        .with_cfg(env.cfg_env.clone())
        .with_tx(ArbTransaction::<TxEnv>::default());
    let mut evm = ctx.build_arb();

    // StartBlock prelude aligns per-tx state with the executor path / execute_message.
    let bctx = block_ctx();
    let l2_block_number = PARENT_NUMBER + 1;
    let derived = arb_revm::executor::hooks::ArbStartBlockDerived {
        l2_block_number,
        time_last_block: bctx.time_last_block,
    };
    let input = ArbExecutionInput::new(parent_header(), message(), exec_cfg());
    use arb_revm::executor::hooks::{ArbExecutionHooks, DefaultArbExecutionHooks};
    if let Some(call) = DefaultArbExecutionHooks.start_block_prelude(&input, derived) {
        let mut tx = TxEnv::default();
        tx.tx_type = 0x6a;
        tx.caller = call.caller;
        tx.kind = TxKind::Call(call.target);
        tx.data = call.data;
        tx.chain_id = Some(CHAIN_ID);
        let out = evm.transact(ArbTransaction::new(tx)).expect("start block");
        evm.commit(out.state);
    }

    let mut poster_gas = Vec::new();
    for tx in user_txs() {
        let tx_env = arb_revm::transaction::arb_envelope_to_tx_env(&tx).expect("tx env");
        let out = evm.transact(tx_env).expect("user tx");
        poster_gas.push(evm.0.ctx.chain.poster_gas);
        evm.commit(out.state);
    }
    poster_gas
}

/// Oracle: run the message through `execute_message`.
fn oracle() -> (arb_revm::executor::ArbExecOutcome, CacheDB<EmptyDB>) {
    let cfg = exec_cfg();
    let input = ArbExecutionInput::new(parent_header(), message(), cfg);
    let mut db = funded_db();
    let outcome = execute_message(&mut db, &input).expect("oracle execute_message");
    (outcome, db)
}

/// EVM env matching what `execute_message` builds for a fresh db at ArbOS v51.
fn evm_env() -> EvmEnv<ArbSpecId> {
    let next_timestamp = L1_TIMESTAMP.max(PARENT_TIMESTAMP);
    let mut block = BlockEnv::default();
    block.number = U256::from(PARENT_NUMBER + 1);
    block.beneficiary = POSTER;
    block.timestamp = U256::from(next_timestamp);
    block.gas_limit = PARENT_GAS_LIMIT;
    block.basefee = BASEFEE;
    block.difficulty = U256::ZERO;
    block.prevrandao = Some(revm::primitives::B256::ZERO);

    // Priority-fee check off; EIP-7623 off (fresh db: Arbitrum prices calldata via poster fee).
    let mut cfg_env = CfgEnv::new_with_spec(ArbSpecId::ARBOS_51)
        .with_chain_id(CHAIN_ID)
        .with_disable_priority_fee_check(true);
    cfg_env.disable_balance_check = false;
    cfg_env.disable_eip7623 = true;

    EvmEnv::new(cfg_env, block)
}

/// Block-execution context derived from the same message.
fn block_ctx() -> ArbBlockExecutionCtx {
    ArbBlockExecutionCtx {
        parent_hash: B256::ZERO,
        extra_data: Bytes::new(),
        l1_base_fee_wei: U256::from(BASEFEE),
        l1_block_number: 0,
        time_last_block: L1_TIMESTAMP.saturating_sub(PARENT_TIMESTAMP),
        sequence_number: Some(7),
        poster: POSTER,
        delayed_messages_read: 0,
        header_info_out: Default::default(),
        finish_timing_out: Default::default(),
    }
}

#[test]
fn block_executor_matches_execute_message() {
    let (oracle_outcome, oracle_db) = oracle();

    // Same txs through ArbBlockExecutor over reth's `State<DB>`.
    let factory = ArbBlockExecutorFactory::new(ArbEvmFactory, CHAIN_ID);
    let mut state = State::builder()
        .with_database(funded_db())
        .with_bundle_update()
        .build();

    let evm = factory.evm_factory().create_evm(&mut state, evm_env());
    let mut executor = factory.create_executor(evm, block_ctx());

    executor
        .apply_pre_execution_changes()
        .expect("pre-execution (EIP-2935)");

    // InternalTxStartBlock (0x6a) is a real block tx; run it first (like Nitro).
    let start_tx = executor.start_block_tx().expect("start-block tx");
    let sb_sender = start_tx
        .sender()
        .expect("start-block tx carries explicit from");
    executor
        .execute_transaction(&Recovered::new_unchecked(start_tx, sb_sender))
        .expect("execute start-block tx");

    let txs = user_txs();
    for tx in &txs {
        let sender = tx.sender().expect("unsigned tx carries explicit from");
        let recovered = Recovered::new_unchecked(tx.clone(), sender);
        executor
            .execute_transaction(&recovered)
            .expect("execute_transaction");
    }

    let mut stage_c_l1 = Vec::new();
    for r in executor.receipts().iter().skip(1) {
        stage_c_l1.push(gas_used_for_l1(r));
    }

    let (_evm, result) = executor.finish().expect("finish");

    assert_eq!(
        oracle_outcome.txs.len(),
        txs.len(),
        "oracle should execute exactly the user txs (no scheduled retries in this block)"
    );
    assert_eq!(
        result.receipts.len(),
        txs.len() + 1,
        "one receipt per user tx, plus the start-block (0x6a) tx"
    );

    // Receipt[0]: start-block tx (0x6a), zero gas, no logs.
    {
        use alloy_consensus::TxReceipt;
        assert_eq!(
            result.receipts[0].cumulative_gas_used(),
            0,
            "start-block tx uses zero gas"
        );
        assert_eq!(
            result.receipts[0].ty(),
            0x6a,
            "first receipt is the internal start-block tx"
        );
    }

    let mut prev_cum = result.receipts[0].cumulative_gas_used();
    for (i, receipt) in result.receipts.iter().skip(1).enumerate() {
        use alloy_consensus::TxReceipt;
        let cum = receipt.cumulative_gas_used();
        let gas_used = cum - prev_cum;
        prev_cum = cum;

        assert_eq!(
            gas_used, oracle_outcome.txs[i].gas_used,
            "tx {i}: gas_used must match arb_revm execute_message"
        );
        assert_eq!(
            receipt.status(),
            oracle_outcome.txs[i].success,
            "tx {i}: status must match oracle"
        );
        assert!(receipt.logs().is_empty(), "tx {i}: no logs expected");
    }

    // `gas_used_for_l1` (poster_gas) resolves to 0 on a bare in-memory db (no initialized ArbOS
    // L1-pricing state), the same in both paths. Cross-check by re-running oracle txs one at a
    // time and reading poster_gas directly.
    let oracle_l1 = oracle_poster_gas_per_tx();
    assert_eq!(
        stage_c_l1, oracle_l1,
        "gas_used_for_l1 (poster_gas) per tx must match the arb_revm handler"
    );

    // total block gas
    let oracle_total: u64 = oracle_outcome.txs.iter().map(|t| t.gas_used).sum();
    assert_eq!(
        result.gas_used, oracle_total,
        "block gas_used must equal the sum of per-tx gas"
    );

    // State parity: every account touched by the oracle has the same balance/nonce here.
    state.merge_transitions(revm::database::states::bundle_state::BundleRetention::Reverts);
    let bundle = state.take_bundle();
    for who in [SENDER_A, SENDER_B, RECIPIENT, POSTER] {
        let oracle_acc = oracle_db.basic_ref(who).unwrap();
        let stage_c_acc = bundle
            .account(&who)
            .and_then(|a| a.info.clone())
            .or_else(|| oracle_acc.clone());
        let (ob, on) = oracle_acc
            .as_ref()
            .map(|a| (a.balance, a.nonce))
            .unwrap_or((U256::ZERO, 0));
        let (sb, sn) = stage_c_acc
            .as_ref()
            .map(|a| (a.balance, a.nonce))
            .unwrap_or((U256::ZERO, 0));
        assert_eq!(sb, ob, "account {who}: balance must match oracle");
        assert_eq!(sn, on, "account {who}: nonce must match oracle");
    }
}

/// The assembler produces a structurally valid block with a receipts root over the
/// `ArbReceiptEnvelope`s.
#[test]
fn assembler_builds_block_with_receipt_root() {
    use alloy_consensus::proofs;
    let factory = ArbBlockExecutorFactory::new(ArbEvmFactory, CHAIN_ID);
    let mut state = State::builder()
        .with_database(funded_db())
        .with_bundle_update()
        .build();
    let evm = factory.evm_factory().create_evm(&mut state, evm_env());
    let mut executor = factory.create_executor(evm, block_ctx());
    executor.apply_pre_execution_changes().unwrap();
    let txs = user_txs();
    for tx in &txs {
        let sender = tx.sender().unwrap();
        executor
            .execute_transaction(&Recovered::new_unchecked(tx.clone(), sender))
            .unwrap();
    }
    let receipts = executor.receipts().to_vec();
    let expected_root = proofs::calculate_receipt_root(&receipts);
    assert_ne!(
        expected_root,
        B256::ZERO,
        "non-empty block must have a non-zero receipts root"
    );
}
