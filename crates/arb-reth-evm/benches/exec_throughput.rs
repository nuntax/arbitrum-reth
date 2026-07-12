//! Internal execution-throughput calibration bench (gas/sec).
//!
//! Measures ONLY block execution: the ArbOS start-block internal tx + a "transfer train" of N
//! value transfers driven straight through `ArbBlockExecutor` over an in-memory `CacheDB`. No L1
//! derivation, no persistence, no state-root/trie — this isolates the raw execution engine, the same
//! thing a competitor's in-process microbench (`InProcessRunner` -> `ArbBlockExecutor`) measures, so
//! the numbers are comparable in kind. NOT for publishing; a private calibration point.
//!
//! Run: `cargo bench -p arb-reth-evm --bench exec_throughput`
//! Reports throughput as gas/sec (criterion "thrpt": Elements/s == gas/s here) at 64/256/1024 txs.

use alloy_consensus::transaction::Recovered;
use alloy_evm::EvmFactory;
use alloy_evm::block::{BlockExecutor, BlockExecutorFactory};
use arb_reth_evm::ArbEvmFactory;
use arb_reth_evm::block::{ArbBlockExecutionCtx, ArbBlockExecutorFactory};
use arbitrum_alloy_consensus::transactions::{ArbTxEnvelope, TxUnsigned};
use arb_revm::ArbSpecId;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use revm::context::{BlockEnv, CfgEnv};
use revm::database::{CacheDB, EmptyDB, State};
use revm::primitives::{Address, B256, Bytes, TxKind, U256};
use revm::state::AccountInfo;

const CHAIN_ID: u64 = 42_161;
const POSTER: Address = Address::with_last_byte(0xAA);
const SENDER: Address = Address::with_last_byte(0x11);
const RECIPIENT: Address = Address::with_last_byte(0x22);
const BASEFEE: u64 = 100_000_000;
// High block gas limit so the whole transfer train fits in one block regardless of N.
const BLOCK_GAS_LIMIT: u64 = 100_000_000_000;
const PARENT_NUMBER: u64 = 100;
const PARENT_TIMESTAMP: u64 = 1_000;
const L1_TIMESTAMP: u64 = 1_005;

/// One well-funded sender so the whole train can pay fees + value.
fn funded_db() -> CacheDB<EmptyDB> {
    let mut db = CacheDB::new(EmptyDB::default());
    db.insert_account_info(
        SENDER,
        AccountInfo {
            balance: U256::from(u128::MAX),
            nonce: 0,
            ..AccountInfo::default()
        },
    );
    db
}

/// N unsigned (0x65) value transfers from one sender, nonce 0..N (explicit `from`, no ecrecover).
fn transfer_train(n: u64) -> Vec<ArbTxEnvelope> {
    (0..n)
        .map(|nonce| {
            ArbTxEnvelope::from(TxUnsigned {
                chain_id: U256::from(CHAIN_ID),
                from: SENDER,
                nonce,
                gas_fee_cap: U256::from(BASEFEE),
                gas_limit: 100_000,
                to: TxKind::Call(RECIPIENT),
                value: U256::from(1u64),
                input: Bytes::new(),
            })
        })
        .collect()
}

fn evm_env() -> alloy_evm::EvmEnv<ArbSpecId> {
    let next_timestamp = L1_TIMESTAMP.max(PARENT_TIMESTAMP);
    let block = BlockEnv {
        number: U256::from(PARENT_NUMBER + 1),
        beneficiary: POSTER,
        timestamp: U256::from(next_timestamp),
        gas_limit: BLOCK_GAS_LIMIT,
        basefee: BASEFEE,
        difficulty: U256::ZERO,
        prevrandao: Some(B256::ZERO),
        ..Default::default()
    };

    let mut cfg_env = CfgEnv::new_with_spec(ArbSpecId::ARBOS_51)
        .with_chain_id(CHAIN_ID)
        .with_disable_priority_fee_check(true);
    cfg_env.disable_balance_check = false;
    cfg_env.disable_eip7623 = true;
    alloy_evm::EvmEnv::new(cfg_env, block)
}

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
    }
}

/// Execute one block (start-block tx + the transfer train) and return total gas used. This is the
/// timed unit: build executor -> pre-execution -> start-block tx -> N transfers -> finish.
fn run_block(txs: &[ArbTxEnvelope]) -> u64 {
    let factory = ArbBlockExecutorFactory::new(ArbEvmFactory, CHAIN_ID);
    let mut state = State::builder()
        .with_database(funded_db())
        .with_bundle_update()
        .build();
    let evm = factory.evm_factory_ref().create_evm(&mut state, evm_env());
    let mut executor = factory.create_executor(evm, block_ctx());

    executor
        .apply_pre_execution_changes()
        .expect("pre-execution");
    let start_tx = executor.start_block_tx().expect("start-block tx");
    let sb_sender = start_tx.sender().expect("start-block sender");
    executor
        .execute_transaction(&Recovered::new_unchecked(start_tx, sb_sender))
        .expect("start-block execute");
    for tx in txs {
        let sender = tx.sender().expect("unsigned tx carries from");
        executor
            .execute_transaction(&Recovered::new_unchecked(tx.clone(), sender))
            .expect("execute_transaction");
    }
    let (_evm, result) = executor.finish().expect("finish");
    result.gas_used
}

fn bench_exec(c: &mut Criterion) {
    let mut group = c.benchmark_group("exec_throughput");
    for n in [64u64, 256, 1024] {
        let txs = transfer_train(n);
        // Warm run to learn total block gas, so criterion's throughput reads out as gas/sec.
        let total_gas = run_block(&txs);
        group.throughput(Throughput::Elements(total_gas));
        group.bench_function(BenchmarkId::from_parameter(n), |b| {
            b.iter(|| criterion::black_box(run_block(&txs)));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_exec);
criterion_main!(benches);
