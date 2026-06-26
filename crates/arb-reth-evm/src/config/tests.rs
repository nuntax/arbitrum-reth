//! Stage D.1 exit proof for [`ArbEvmConfig`].
//!
//! 1. `evm_env(header)` derives the [`ArbSpecId`] from the ArbOS version embedded in the header
//!    (via [`ArbHeaderInfo`]), and `context_for_block(header)` carries the header's L1 block number.
//! 2. An executor built through the config path (`evm_env` + `context_for_block` →
//!    `create_executor`) threads that L1 block number into the Arbitrum chain context, so a tx that
//!    reads the `NUMBER` opcode gets the **L1** block number, not 0 — the Stage B/C deferral fixed.

use super::*;
use crate::{ArbBlockExecutorFactory, ArbEvmFactory};
use alloy_consensus::transaction::Recovered;
use alloy_consensus::Header;
use alloy_evm::block::{BlockExecutor, BlockExecutorFactory, TxResult};
use alloy_evm::{Evm, EvmFactory};
use arb_alloy_consensus::header::ArbHeaderInfo;
use arb_alloy_consensus::transactions::{ArbTxEnvelope, TxUnsigned};
use arb_revm::ArbSpecId;
use revm::database::{CacheDB, EmptyDB, State};
use revm::primitives::{Address, Bytes, TxKind, U256};
use revm::state::{AccountInfo, Bytecode};

const CHAIN_ID: u64 = ARB_ONE_CHAIN_ID;
const L1_BLOCK_NUMBER: u64 = 21_000_123;
const ARBOS_VERSION: u64 = 51;
const L2_BLOCK_NUMBER: u64 = 300_000_000;
const SENDER: Address = Address::with_last_byte(0x11);
const NUMBER_READER: Address = Address::with_last_byte(0x42);
const START_BALANCE: u128 = 100_000_000_000_000_000_000;

/// Builds an Arbitrum header carrying `l1_block_number` + `arbos_format_version` via
/// [`ArbHeaderInfo`] (writing `extra_data` + `mix_hash` exactly as Nitro does).
fn arb_header() -> Header {
    let info = ArbHeaderInfo {
        send_root: B256::repeat_byte(0xAB),
        send_count: 7,
        l1_block_number: L1_BLOCK_NUMBER,
        arbos_format_version: ARBOS_VERSION,
    };
    let mut header = Header {
        number: L2_BLOCK_NUMBER,
        gas_limit: 30_000_000,
        ..Header::default()
    };
    info.update_header(&mut header);
    header
}

#[test]
fn evm_env_derives_spec_from_arbos_version() {
    let config = ArbEvmConfig::arbitrum_one();
    let header = arb_header();

    let evm_env = config.evm_env(&header);

    // Spec must be the one mapped from the embedded ArbOS version.
    assert_eq!(
        evm_env.cfg_env.spec,
        ArbSpecId::from_arbos_version(ARBOS_VERSION)
    );
    assert_eq!(evm_env.cfg_env.spec, ArbSpecId::ARBOS_51);
    assert_eq!(evm_env.cfg_env.chain_id, CHAIN_ID);
    // Block env carries the L2 block number (chain-rule number), not the L1 one.
    assert_eq!(evm_env.block_env.number, U256::from(L2_BLOCK_NUMBER));
}

#[test]
fn evm_env_defaults_on_non_arbitrum_header() {
    // A bare/default header is not an Arbitrum header (extra_data len 0): must not panic, and must
    // fall back to the default ArbOS spec rather than erroring.
    let config = ArbEvmConfig::arbitrum_one();
    let evm_env = config.evm_env(&Header::default());
    assert_eq!(evm_env.cfg_env.spec, ArbSpecId::default());
    assert_eq!(evm_env.cfg_env.chain_id, CHAIN_ID);
}

#[test]
fn context_for_block_carries_l1_block_number() {
    let config = ArbEvmConfig::arbitrum_one();
    let header = arb_header();
    let ctx = config.context_for_block(&header);
    assert_eq!(ctx.l1_block_number, L1_BLOCK_NUMBER);
    assert_eq!(ctx.parent_hash, header.parent_hash);
}

/// Bytecode that reads `NUMBER`, stores it at memory[0], and returns 32 bytes:
/// `NUMBER PUSH0 MSTORE PUSH1 0x20 PUSH0 RETURN`.
fn number_reader_code() -> Bytes {
    Bytes::from_static(&[
        0x43, // NUMBER
        0x5f, // PUSH0
        0x52, // MSTORE
        0x60, 0x20, // PUSH1 0x20
        0x5f, // PUSH0
        0xf3, // RETURN
    ])
}

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
    let code = Bytecode::new_raw(number_reader_code());
    db.insert_account_info(
        NUMBER_READER,
        AccountInfo {
            balance: U256::ZERO,
            nonce: 1,
            code_hash: code.hash_slow(),
            code: Some(code),
            ..AccountInfo::default()
        },
    );
    db
}

/// Through the full config path (`evm_env` + `context_for_block` → factory `create_executor`),
/// execute a tx that reads `NUMBER`; assert it returns the L1 block number threaded from the header,
/// not the L2 number and not 0.
#[test]
fn executor_through_config_reads_l1_block_number_for_number_opcode() {
    let config = ArbEvmConfig::arbitrum_one();
    let header = arb_header();

    let evm_env = config.evm_env(&header);
    let block_ctx = config.context_for_block(&header);
    assert_eq!(block_ctx.l1_block_number, L1_BLOCK_NUMBER);

    // Build the EVM + executor exactly as reth's `executor_for_block` default would:
    // evm_factory().create_evm(db, evm_env) then block_executor_factory().create_executor(evm, ctx).
    let factory = ArbBlockExecutorFactory::new(ArbEvmFactory, CHAIN_ID);
    let mut state = State::builder()
        .with_database(funded_db())
        .with_bundle_update()
        .build();
    let evm = factory.evm_factory().create_evm(&mut state, evm_env);

    // Sanity: the EVM block env carries the L2 number.
    assert_eq!(evm.block().number, U256::from(L2_BLOCK_NUMBER));

    let mut executor = factory.create_executor(evm, block_ctx);
    // NOTE: deliberately do NOT run apply_pre_execution_changes (StartBlock) here — that mutates
    // ArbOS state and is exercised in block/tests.rs. We only want to prove the NUMBER override
    // reads the threaded l1_block_number, which `create_executor` set on the chain context.

    // A type-0x65 unsigned tx that calls the NUMBER_READER contract.
    let tx = ArbTxEnvelope::from(TxUnsigned {
        chain_id: U256::from(CHAIN_ID),
        from: SENDER,
        nonce: 0,
        gas_fee_cap: U256::ZERO,
        gas_limit: 200_000,
        to: TxKind::Call(NUMBER_READER),
        value: U256::ZERO,
        input: Bytes::new(),
    });
    let recovered = Recovered::new_unchecked(tx, SENDER);

    let result = executor
        .execute_transaction_without_commit(&recovered)
        .expect("NUMBER-reader tx executes")
        .into_result()
        .result;

    assert!(result.is_success(), "tx must succeed: {result:?}");
    let output = result.output().expect("RETURN output").clone();
    assert_eq!(output.len(), 32, "expected a 32-byte word");
    let returned = U256::from_be_slice(&output);

    assert_eq!(
        returned,
        U256::from(L1_BLOCK_NUMBER),
        "NUMBER opcode must return the L1 block number threaded from the header, got {returned}"
    );
    assert_ne!(returned, U256::from(L2_BLOCK_NUMBER), "must not be the L2 number");
    assert_ne!(returned, U256::ZERO, "must not be the defaulted 0");
}

/// Stage D validation: a whole block executes through reth's GENERIC high-level executor
/// (`ConfigureEvm::executor(db).execute(&RecoveredBlock)`), proving our `impl ConfigureEvm` plugs
/// into reth's block-execution machinery end-to-end — pre-execution changes (StartBlock InternalTx
/// + EIP-2935) → per-tx execution → `finish` → `BlockExecutionOutput` — driven entirely from the
/// header (spec + L1 block number decoded via `ArbHeaderInfo`), not the manual per-tx
/// `execute_transaction` surface that Stage C's `block/tests.rs` covers.
#[test]
fn block_executes_through_reth_generic_executor() {
    use alloy_consensus::{BlockBody, TxReceipt};
    use arb_alloy_consensus::reth::ArbBlock;
    use reth_evm::execute::Executor;
    use reth_evm::ConfigureEvm;
    use reth_primitives_traits::RecoveredBlock;

    const ALICE: Address = Address::with_last_byte(0x11);
    const BOB: Address = Address::with_last_byte(0x22);
    const CAROL: Address = Address::with_last_byte(0x33);
    const FUND: u128 = 100_000_000_000_000_000_000;
    const XFER0: u128 = 1_000_000;
    const XFER1: u128 = 500_000;

    let config = ArbEvmConfig::arbitrum_one();
    let header = arb_header(); // ArbOS v51 + embedded L1 block number

    // Fund ALICE + BOB.
    let mut db = CacheDB::new(EmptyDB::default());
    for a in [ALICE, BOB] {
        db.insert_account_info(
            a,
            AccountInfo { balance: U256::from(FUND), nonce: 0, ..AccountInfo::default() },
        );
    }

    // Two free (gas_fee_cap 0, basefee 0) value transfers: ALICE->BOB, BOB->CAROL.
    let mk = |from, nonce, to, value: u128| {
        ArbTxEnvelope::from(TxUnsigned {
            chain_id: U256::from(CHAIN_ID),
            from,
            nonce,
            gas_fee_cap: U256::ZERO,
            gas_limit: 200_000,
            to: TxKind::Call(to),
            value: U256::from(value),
            input: Bytes::new(),
        })
    };
    let block = ArbBlock::new(
        header,
        BlockBody {
            transactions: vec![mk(ALICE, 0, BOB, XFER0), mk(BOB, 0, CAROL, XFER1)],
            ommers: vec![],
            withdrawals: None,
        },
    );
    let recovered = RecoveredBlock::new_unhashed(block, vec![ALICE, BOB]);

    let output = config
        .executor(db)
        .execute(&recovered)
        .expect("block executes through reth's generic Executor");

    // One receipt per user tx (the StartBlock InternalTx is a pre-execution change, not a tx).
    assert_eq!(output.result.receipts.len(), 2, "one receipt per user tx");
    assert!(output.result.receipts.iter().all(TxReceipt::status), "both txs must succeed");
    assert!(output.result.gas_used > 0, "block must consume gas");

    // Value actually moved through the full high-level path: CAROL (fresh) is credited XFER1, and
    // BOB nets +XFER0-XFER1 (transfers are free at basefee 0 / gas_fee_cap 0).
    let balance_of = |addr: &Address| {
        output.state.account(addr).and_then(|a| a.info.as_ref()).map(|i| i.balance).unwrap_or_default()
    };
    assert_eq!(balance_of(&CAROL), U256::from(XFER1), "CAROL must receive XFER1");
    assert_eq!(
        balance_of(&BOB),
        U256::from(FUND + XFER0 - XFER1),
        "BOB net credited XFER0-XFER1 (no fees)"
    );
}

/// `next_evm_env` derives spec from the next-block attributes' ArbOS version, and `context_for_next_block`
/// carries the attributes' L1 block number.
#[test]
fn next_block_env_and_ctx_use_attributes() {
    let config = ArbEvmConfig::arbitrum_one();
    let parent = arb_header();
    let attrs = ArbNextBlockEnvAttributes {
        timestamp: parent.timestamp + 1,
        suggested_fee_recipient: Address::with_last_byte(0xAA),
        prev_randao: B256::ZERO,
        gas_limit: 32_000_000,
        l1_block_number: L1_BLOCK_NUMBER + 1,
        l1_base_fee_wei: U256::from(7u64),
        arbos_format_version: ARBOS_VERSION,
        extra_data: Bytes::new(),
        withdrawals: None,
    };

    let env = config.next_evm_env(&parent, &attrs);
    assert_eq!(env.cfg_env.spec, ArbSpecId::ARBOS_51);
    assert_eq!(env.block_env.number, U256::from(L2_BLOCK_NUMBER + 1));

    let ctx = config.context_for_next_block(&parent, parent.hash_slow(), attrs);
    assert_eq!(ctx.l1_block_number, L1_BLOCK_NUMBER + 1);
    assert_eq!(ctx.l1_base_fee_wei, U256::from(7u64));
}
