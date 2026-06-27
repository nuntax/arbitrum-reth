//! `arb-reth-evm` Stage C — block executor + assembler for Arbitrum.
//!
//! Adapts `arb_revm`'s existing block-execution machinery (`executor::run::execute_message`,
//! `executor::hooks::ArbStartBlockDerived`) to reth v2.0.0's alloy-evm
//! [`BlockExecutor`]/[`BlockExecutorFactory`]/[`BlockAssembler`] trait surface, built on the
//! Stage B [`ArbEvm`](crate::ArbEvm)/[`ArbEvmFactory`](crate::ArbEvmFactory).
//!
//! Mirrors `alloy-op-evm`'s `OpBlockExecutor`/`OpBlockExecutorFactory` (the per-tx OP fee
//! accounting lives in `op_revm`'s handler; ours lives in `arb_revm::handler`). So this layer
//! does **not** re-implement the ArbOS gas/poster/tip math — it drives [`ArbEvm`] per tx (which
//! routes through `ArbHandler` inside `transact`), reads the resulting `poster_gas` off the chain
//! context for the receipt's `gas_used_for_l1`, and builds an [`ArbReceiptEnvelope`].
//!
//! Pre-execution mirrors `execute_message`'s prelude: the EIP-2935 history-storage parent-hash
//! system call (ArbOS v40+; a no-op state transition before that) followed by Nitro's
//! `InternalTxStartBlock` (typed internal tx `0x6a`), built via `arb_revm`'s
//! [`DefaultArbExecutionHooks`] so the `ArbosActs.startBlock(...)` calldata is identical.

use crate::tx::ArbTx;
use crate::{ArbEvm, ArbEvmFactory};
use alloc::vec::Vec;
use alloy_consensus::{
    Block, BlockBody, EMPTY_OMMER_ROOT_HASH, Eip658Value, Header, Receipt, ReceiptWithBloom,
    TxReceipt, proofs,
};
use alloy_eips::{Encodable2718, Typed2718};
use std::sync::{Arc, Mutex};
use alloy_evm::{
    Database, Evm, EvmFactory, FromRecoveredTx, FromTxWithEncoded, RecoveredTx,
    block::{
        BlockExecutionError, BlockExecutionResult, BlockExecutor, BlockExecutorFactory,
        ExecutableTx, GasOutput, StateDB, TxResult,
    },
};
use alloy_primitives::{Address, B64, B256, Bytes, Log, U256, logs_bloom};
use arb_alloy_consensus::header::ArbHeaderInfo;
use arb_alloy_consensus::receipt::{ArbReceipt, ArbReceiptEnvelope};
use arb_alloy_consensus::transactions::ArbTxEnvelope;
use arb_alloy_consensus::transactions::internal::ArbInternalTx;
use arb_revm::api::default_ctx::ArbContext;
use arb_revm::constants::{BATCH_POSTER_ADDRESS, HISTORY_STORAGE_ADDRESS};
use arb_revm::executor::hooks::{
    ArbExecutionHooks, ArbStartBlockDerived, DefaultArbExecutionHooks,
};
use arb_revm::executor::{ArbExecutionInput, ArbMessageEnvelope, ArbParentHeader};
use arb_revm::{ArbBlockHeaderInfo, ArbExecCfg, ArbosState};
use core::fmt::Debug;
use reth_evm::execute::{BlockAssembler, BlockAssemblerInput};
use revm::context::{Block as _, ContextTr, result::ResultAndState};
use revm::handler::SYSTEM_ADDRESS;
use revm::{DatabaseCommit, Inspector, context::result::ExecutionResult};

/// Block-execution context for an Arbitrum block, beyond what the EVM env carries.
///
/// This is the analogue of `OpBlockExecutionCtx`. It provides the inputs the StartBlock prelude
/// (`InternalTxStartBlock`) and the EIP-2935 parent-hash system call need — values that are not
/// representable in alloy's [`EvmEnv`](alloy_evm::EvmEnv): the L1 base fee / L1 block number / poster
/// for this L2 block, and the parent block hash for the history-storage write.
///
/// Must be [`Clone`] (a [`BlockExecutorFactory`] requirement).
#[derive(Debug, Default, Clone)]
pub struct ArbBlockExecutionCtx {
    /// Parent (L2) block hash, written to the EIP-2935 history-storage contract pre-execution.
    pub parent_hash: B256,
    /// Parent block extra_data (carried through to the assembled header).
    pub extra_data: Bytes,
    /// L1 base fee (wei) for this block — the `l1BaseFee` arg of `ArbosActs.startBlock`.
    pub l1_base_fee_wei: U256,
    /// L1 block number for this L2 block — the `l1BlockNumber` arg of `ArbosActs.startBlock`,
    /// and the value the EVM `NUMBER` opcode returns during execution.
    pub l1_block_number: u64,
    /// Seconds elapsed since the parent block (`timeLastBlock` arg of `ArbosActs.startBlock`).
    pub time_last_block: u64,
    /// Sequencer feed sequence number for this message.
    pub sequence_number: Option<u64>,
    /// Batch poster / coinbase for the block (`message.poster`); receives the L1 poster fee.
    pub poster: Address,
    /// Cumulative count of delayed-inbox messages read as of this block. Nitro encodes this into
    /// the header `nonce` (`EncodeNonce(delayedMessagesRead)`).
    pub delayed_messages_read: u64,
    /// Shared cell the executor's [`finish`](BlockExecutor::finish) writes the post-execution
    /// [`ArbBlockHeaderInfo`] into, for the [`ArbBlockAssembler`] to read when sealing the header.
    ///
    /// This is the only channel from the executor to the assembler in reth's block-builder flow:
    /// [`BasicBlockBuilder`](reth_evm::execute) hands the assembler the *builder's* `ctx`, not the
    /// executor's EVM. Because `create_executor` is given `ctx.clone()` and `Arc` clones share the
    /// inner cell, a value the executor stores here is visible to the assembler via its `ctx`.
    pub header_info_out: Arc<Mutex<Option<ArbBlockHeaderInfo>>>,
}

/// Result of executing one Arbitrum transaction through the block executor.
///
/// Carries the `gas_used_for_l1` (the tx's ArbOS `poster_gas`, read off the chain context after
/// `transact`), the tx type byte (which selects the [`ArbReceiptEnvelope`] variant), and the
/// inner revm [`ResultAndState`].
#[derive(Debug)]
pub struct ArbTxResult<H> {
    /// Inner revm execution result + state delta.
    pub result: ResultAndState<H>,
    /// ArbOS L1 poster gas for this tx — the receipt's `gas_used_for_l1`.
    pub gas_used_for_l1: u64,
    /// Consensus tx type byte (selects the receipt envelope variant).
    pub tx_type: u8,
}

impl<H: Send + 'static> TxResult for ArbTxResult<H> {
    type HaltReason = H;

    fn result(&self) -> &ResultAndState<Self::HaltReason> {
        &self.result
    }

    fn into_result(self) -> ResultAndState<Self::HaltReason> {
        self.result
    }
}

/// Block executor for Arbitrum. Mirrors `OpBlockExecutor`.
#[allow(missing_debug_implementations)]
pub struct ArbBlockExecutor<E, H = DefaultArbExecutionHooks> {
    /// The EVM (Stage B [`ArbEvm`]) the executor drives, one tx at a time.
    evm: E,
    /// Block-execution context (StartBlock prelude inputs, parent hash).
    ctx: ArbBlockExecutionCtx,
    /// `arb_revm` start-block hook set (produces the identical `ArbosActs.startBlock` calldata).
    hooks: H,
    /// Chain id (for the internal-tx env).
    chain_id: u64,
    /// Receipts of executed transactions, in order.
    receipts: Vec<ArbReceiptEnvelope<Log>>,
    /// Cumulative gas used across all executed transactions.
    gas_used: u64,
}

impl<E, H> ArbBlockExecutor<E, H> {
    /// Creates a new [`ArbBlockExecutor`].
    pub fn new(evm: E, ctx: ArbBlockExecutionCtx, hooks: H, chain_id: u64) -> Self {
        Self {
            evm,
            ctx,
            hooks,
            chain_id,
            receipts: Vec::new(),
            gas_used: 0,
        }
    }
}

/// Builds an `ArbReceiptEnvelope<Log>` for a transaction of type `tx_type`.
///
/// The bloom is computed from the logs (so receipt encoding / receipts-root are correct), and the
/// Arbitrum-specific `gas_used_for_l1` (= the tx's `poster_gas`) is recorded on the receipt body.
fn build_arb_receipt<H>(
    tx_type: u8,
    result: ExecutionResult<H>,
    cumulative_gas_used: u64,
    gas_used_for_l1: u64,
) -> ArbReceiptEnvelope<Log> {
    let success = result.is_success();
    let logs = result.into_logs();
    let logs_bloom = logs_bloom(logs.iter());
    let receipt = ArbReceipt {
        inner: Receipt {
            status: Eip658Value::Eip658(success),
            cumulative_gas_used,
            logs,
        },
        gas_used_for_l1,
    };
    let rwb = ReceiptWithBloom {
        receipt,
        logs_bloom,
    };
    receipt_envelope_for_type(tx_type, rwb)
}

#[inline]
fn receipt_envelope_for_type(
    tx_type: u8,
    rwb: ReceiptWithBloom<ArbReceipt<Log>>,
) -> ArbReceiptEnvelope<Log> {
    match tx_type {
        0x00 => ArbReceiptEnvelope::Legacy(rwb),
        0x01 => ArbReceiptEnvelope::Eip2930(rwb),
        0x02 => ArbReceiptEnvelope::Eip1559(rwb),
        0x03 => ArbReceiptEnvelope::Eip4844(rwb),
        0x04 => ArbReceiptEnvelope::Eip7702(rwb),
        0x64 => ArbReceiptEnvelope::Deposit(rwb),
        0x65 => ArbReceiptEnvelope::Unsigned(rwb),
        0x66 => ArbReceiptEnvelope::Contract(rwb),
        0x68 => ArbReceiptEnvelope::Retry(rwb),
        0x69 => ArbReceiptEnvelope::SubmitRetryable(rwb),
        0x6a => ArbReceiptEnvelope::Internal(rwb),
        // Unknown / future type bytes fall back to Legacy (matches alloy's bare-RLP convention).
        _ => ArbReceiptEnvelope::Legacy(rwb),
    }
}

impl<DB, I, H> BlockExecutor for ArbBlockExecutor<ArbEvm<DB, I>, H>
where
    DB: Database + DatabaseCommit + StateDB,
    I: Inspector<ArbContext<DB>>,
    H: ArbExecutionHooks,
{
    type Transaction = ArbTxEnvelope;
    type Receipt = ArbReceiptEnvelope<Log>;
    type Evm = ArbEvm<DB, I>;
    type Result = ArbTxResult<<ArbEvm<DB, I> as Evm>::HaltReason>;

    fn apply_pre_execution_changes(&mut self) -> Result<(), BlockExecutionError> {
        // EIP-2935 / Nitro ProcessParentBlockHash: write the parent block hash into the
        // history-storage contract under SYSTEM_ADDRESS. On pre-v40 chains where the contract is not
        // installed this is a no-op state transition (matching `execute_message`). We commit it just
        // like a system call. This is a pure system call — it has no block transaction or receipt.
        //
        // Nitro's `InternalTxStartBlock` (0x6a) is NOT run here: it is a real *block transaction*
        // (the first tx of every L2 block, with its own receipt), so the caller drives it through
        // `execute_transaction` like any other tx (see `ArbChainDriver::advance` /
        // `block::build_start_block_tx`). Running it here instead would keep it out of the block's
        // transactions/receipts roots and diverge the block hash from Nitro.
        let result = self
            .evm
            .transact_system_call(
                SYSTEM_ADDRESS,
                HISTORY_STORAGE_ADDRESS,
                Bytes::copy_from_slice(self.ctx.parent_hash.as_slice()),
            )
            .map_err(|err| BlockExecutionError::evm(err, self.ctx.parent_hash))?;
        self.evm.db_mut().commit(result.state);

        Ok(())
    }

    fn execute_transaction_without_commit(
        &mut self,
        tx: impl ExecutableTx<Self>,
    ) -> Result<Self::Result, BlockExecutionError> {
        let (tx_env, tx) = tx.into_parts();
        let tx_type = tx.tx().ty();

        let result = self
            .evm
            .transact(tx_env)
            .map_err(|err| BlockExecutionError::evm(err, tx.tx().trie_hash()))?;

        // The ArbOS handler set `chain().poster_gas` during pre_execution of this tx; it is the
        // L2-gas equivalent of the L1 poster cost and is exactly the receipt's `gas_used_for_l1`.
        let gas_used_for_l1 = self.evm.ctx().chain.poster_gas;

        Ok(ArbTxResult {
            result,
            gas_used_for_l1,
            tx_type,
        })
    }

    fn commit_transaction(&mut self, output: Self::Result) -> GasOutput {
        let ArbTxResult {
            result: ResultAndState { result, state },
            gas_used_for_l1,
            tx_type,
        } = output;

        let gas_used = result.tx_gas_used();
        self.gas_used += gas_used;

        self.receipts.push(build_arb_receipt(
            tx_type,
            result,
            self.gas_used,
            gas_used_for_l1,
        ));

        self.evm.db_mut().commit(state);

        GasOutput::new(gas_used)
    }

    fn finish(
        mut self,
    ) -> Result<(Self::Evm, BlockExecutionResult<Self::Receipt>), BlockExecutionError> {
        // Read the post-execution ArbOS header info (send-Merkle root/count, the possibly-upgraded
        // ArbOS version, the L2 base fee) and hand it to the assembler via the shared ctx cell.
        // All txs have been committed by now, so the journal loads the committed values straight
        // from state (it holds the same `&mut State` the commits wrote to). This mirrors what
        // Nitro's `FinalizeBlock`/`createNewHeader` pull from `arbosState` for the block header.
        let header_info = ArbosState::read_block_header_info(self.evm.ctx_mut().journal_mut())
            .map_err(|e| BlockExecutionError::msg(format!("read ArbOS block header info: {e}")))?;
        if let Ok(mut slot) = self.ctx.header_info_out.lock() {
            *slot = Some(header_info);
        }

        let gas_used = self
            .receipts
            .last()
            .map(|r| r.cumulative_gas_used())
            .unwrap_or_default();
        Ok((
            self.evm,
            BlockExecutionResult {
                receipts: self.receipts,
                requests: Default::default(),
                gas_used,
                blob_gas_used: 0,
            },
        ))
    }

    fn evm_mut(&mut self) -> &mut Self::Evm {
        &mut self.evm
    }

    fn evm(&self) -> &Self::Evm {
        &self.evm
    }

    fn receipts(&self) -> &[Self::Receipt] {
        &self.receipts
    }
}

impl<E, H> ArbBlockExecutor<E, H> {
    /// Reconstructs the `ArbExecutionInput` the `arb_revm` start-block hook expects, from the EVM
    /// env + the block-execution ctx. Only the `message` fields the hook reads are meaningful.
    fn start_block_input(&self, l2_block_number: u64) -> ArbExecutionInput {
        ArbExecutionInput::new(
            ArbParentHeader {
                number: l2_block_number.saturating_sub(1),
                ..ArbParentHeader::default()
            },
            ArbMessageEnvelope {
                sequence_number: self.ctx.sequence_number,
                l1_block_number: self.ctx.l1_block_number,
                l1_timestamp: 0,
                poster: self.ctx.poster,
                l1_base_fee_wei: self.ctx.l1_base_fee_wei,
                delayed_messages_read: 0,
                txs: Vec::new(),
            },
            ArbExecCfg {
                chain_id: self.chain_id,
                ..ArbExecCfg::default()
            },
        )
    }
}

impl<DB, I, H> ArbBlockExecutor<ArbEvm<DB, I>, H>
where
    DB: Database + DatabaseCommit + StateDB,
    I: Inspector<ArbContext<DB>>,
    H: ArbExecutionHooks,
{
    /// Builds Nitro's `InternalTxStartBlock` (0x6a) for this block — the first transaction of every
    /// L2 block, carrying the `ArbosActs.startBlock(l1BaseFee, l1BlockNumber, l2BlockNumber,
    /// timeLastBlock)` calldata. Returns `None` if the hook yields no prelude.
    ///
    /// The caller runs this through the normal [`execute_transaction`](reth_evm::execute::BlockBuilder)
    /// path (NOT in `apply_pre_execution_changes`) so it appears in the block's transactions and
    /// receipts — matching Nitro, where the start-block tx is a real block transaction with its own
    /// receipt and contributes to the transactions/receipts roots.
    pub fn start_block_tx(&self) -> Option<ArbTxEnvelope> {
        let l2_block_number = self.evm.block().number().saturating_to::<u64>();
        let derived = ArbStartBlockDerived {
            l2_block_number,
            time_last_block: self.ctx.time_last_block,
        };
        let input = self.start_block_input(l2_block_number);
        let call = self.hooks.start_block_prelude(&input, derived)?;
        Some(ArbTxEnvelope::from(ArbInternalTx::new(self.chain_id, call.data)))
    }
}

/// Factory producing [`ArbBlockExecutor`]s. Mirrors `OpBlockExecutorFactory`.
///
/// `EvmFactory = ArbEvmFactory`, `Transaction = ArbTxEnvelope`,
/// `Receipt = ArbReceiptEnvelope<Log>`.
#[derive(Debug, Clone, Default)]
pub struct ArbBlockExecutorFactory<H = DefaultArbExecutionHooks> {
    evm_factory: ArbEvmFactory,
    hooks: H,
    chain_id: u64,
}

impl ArbBlockExecutorFactory<DefaultArbExecutionHooks> {
    /// Creates a new factory with the default Arbitrum start-block hook set.
    pub fn new(evm_factory: ArbEvmFactory, chain_id: u64) -> Self {
        Self {
            evm_factory,
            hooks: DefaultArbExecutionHooks,
            chain_id,
        }
    }
}

impl<H> ArbBlockExecutorFactory<H> {
    /// Creates a new factory with an explicit start-block hook set.
    pub const fn with_hooks(evm_factory: ArbEvmFactory, hooks: H, chain_id: u64) -> Self {
        Self {
            evm_factory,
            hooks,
            chain_id,
        }
    }

    /// The wrapped [`ArbEvmFactory`].
    pub const fn evm_factory_ref(&self) -> &ArbEvmFactory {
        &self.evm_factory
    }
}

impl<H> BlockExecutorFactory for ArbBlockExecutorFactory<H>
where
    H: ArbExecutionHooks + Clone + Debug + Send + 'static,
{
    type EvmFactory = ArbEvmFactory;
    type TxExecutionResult = ArbTxResult<<ArbEvmFactory as EvmFactory>::HaltReason>;
    type ExecutionCtx<'a> = ArbBlockExecutionCtx;
    type Transaction = ArbTxEnvelope;
    type Receipt = ArbReceiptEnvelope<Log>;
    type Executor<'a, DB: StateDB, I: Inspector<<ArbEvmFactory as EvmFactory>::Context<DB>>> =
        ArbBlockExecutor<ArbEvm<DB, I>, H>;

    fn evm_factory(&self) -> &Self::EvmFactory {
        &self.evm_factory
    }

    fn create_executor<'a, DB, I>(
        &'a self,
        mut evm: ArbEvm<DB, I>,
        ctx: Self::ExecutionCtx<'a>,
    ) -> Self::Executor<'a, DB, I>
    where
        DB: StateDB,
        I: Inspector<ArbContext<DB>>,
    {
        // Thread the block's L1 block number into the Arbitrum chain context, so the `NUMBER`
        // opcode (which `arb_revm` overrides to read `chain().l1_block_number`) returns the L1
        // block number, not 0. This is the Stage B/C deferral (`ArbEvmFactory::build_ctx` defaults
        // it) now resolved at the executor seam: `ConfigureEvm::context_for_block` populates
        // `ArbBlockExecutionCtx::l1_block_number` from `ArbHeaderInfo`, and it flows through here.
        evm.ctx_mut().chain.l1_block_number = ctx.l1_block_number;
        ArbBlockExecutor::new(evm, ctx, self.hooks.clone(), self.chain_id)
    }
}

// `ArbTx` must be constructible from a recovered `ArbTxEnvelope` (with and without encoded bytes)
// for the `BlockExecutor::Transaction = ArbTxEnvelope` wiring — proven by Stage B's `tx.rs`.
const _: fn() = || {
    fn assert_from<T: FromRecoveredTx<ArbTxEnvelope> + FromTxWithEncoded<ArbTxEnvelope>>() {}
    assert_from::<ArbTx>();
};

/// Block assembler for Arbitrum. Mirrors `OpBlockAssembler`.
///
/// Builds an [`ArbBlock`](arb_alloy_consensus::ArbBlock)-shaped `Block<ArbTxEnvelope>` from the
/// execution output: receipts root from the `ArbReceiptEnvelope`s, logs bloom from their logs,
/// gas used, and the post-execution state root.
///
/// Note: Arbitrum header `extra_data` / `mix_hash` carry `send_root` / `l1_block_number` /
/// `arbos_version` (decoded by `ArbHeaderInfo`); wiring those for byte-identical Nitro header
/// hashes is Stage D/E. This assembler produces a structurally-correct block with a correct
/// receipts root.
#[derive(Debug, Clone, Default)]
pub struct ArbBlockAssembler;

impl<F> BlockAssembler<F> for ArbBlockAssembler
where
    F: for<'a> BlockExecutorFactory<
            ExecutionCtx<'a> = ArbBlockExecutionCtx,
            Transaction = ArbTxEnvelope,
            Receipt = ArbReceiptEnvelope<Log>,
        >,
{
    type Block = Block<ArbTxEnvelope>;

    fn assemble_block(
        &self,
        input: BlockAssemblerInput<'_, '_, F, Header>,
    ) -> Result<Self::Block, BlockExecutionError> {
        let BlockAssemblerInput {
            evm_env,
            execution_ctx: ctx,
            transactions,
            output: BlockExecutionResult {
                receipts, gas_used, ..
            },
            state_root,
            ..
        } = input;

        let timestamp = evm_env.block_env.timestamp().saturating_to();

        let transactions_root = proofs::calculate_transaction_root(&transactions);
        let receipts_root = proofs::calculate_receipt_root(receipts);
        let logs_bloom = logs_bloom(receipts.iter().flat_map(|r| r.logs()));

        // Arbitrum header metadata (Nitro `HeaderInfo`): the executor stored the post-execution
        // send-Merkle root/count + ArbOS version into the shared ctx cell during `finish`. Combine
        // with the block's L1 block number to encode `extra_data` (= send_root) and `mix_hash`
        // (= send_count | l1_block_number | arbos_version). Falls back to zeros if unset (e.g. a
        // non-Arbitrum/genesis probe), which decodes back as a non-Arbitrum header.
        let computed = ctx
            .header_info_out
            .lock()
            .ok()
            .and_then(|guard| *guard)
            .unwrap_or_default();
        // Delayed-message blocks (coinbase != batch poster) never collect tips, regardless of the
        // chain-wide setting (Nitro `block_processor.go`); all txs in a block share the coinbase.
        let collect_tips =
            computed.collect_tips && evm_env.block_env.beneficiary() == BATCH_POSTER_ADDRESS;
        let arb_info = ArbHeaderInfo {
            send_root: computed.send_root,
            send_count: computed.send_count,
            // The L1 block number ArbOS *recorded* for this block (post-state Blockhashes), which
            // is what Nitro packs into `mix_hash` — not the raw message `l1BlockNumber` in `ctx`.
            l1_block_number: computed.l1_block_number,
            arbos_format_version: computed.arbos_version,
            collect_tips,
        };

        let header = Header {
            parent_hash: ctx.parent_hash,
            ommers_hash: EMPTY_OMMER_ROOT_HASH,
            beneficiary: evm_env.block_env.beneficiary(),
            state_root,
            transactions_root,
            receipts_root,
            withdrawals_root: None,
            logs_bloom,
            // Nitro sets every L2 block's difficulty to 1 (`createNewHeader`).
            difficulty: U256::from(1u64),
            number: evm_env.block_env.number().saturating_to(),
            gas_limit: evm_env.block_env.gas_limit(),
            gas_used: *gas_used,
            timestamp,
            // `mix_hash` carries send_count/l1_block_number/arbos_version; `extra_data` is send_root.
            mix_hash: arb_info.encode_mix_hash(),
            // Nitro encodes `delayedMessagesRead` into the header nonce (`EncodeNonce`).
            nonce: B64::new(ctx.delayed_messages_read.to_be_bytes()),
            base_fee_per_gas: Some(evm_env.block_env.basefee()),
            extra_data: arb_info.encode_extra_data(),
            parent_beacon_block_root: None,
            blob_gas_used: None,
            excess_blob_gas: None,
            requests_hash: None,
            // EIP-7928 (Amsterdam): not used on Arbitrum — set to None.
            block_access_list_hash: None,
            // EIP-7928 slot number: not used on Arbitrum — set to None.
            slot_number: None,
        };

        Ok(Block::new(
            header,
            BlockBody {
                transactions,
                ommers: Default::default(),
                withdrawals: None,
            },
        ))
    }
}

#[cfg(test)]
mod tests;
