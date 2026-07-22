//! Block executor + assembler for Arbitrum.
//!
//! Adapts `arb_revm`'s block-execution machinery (`executor::run::execute_message`,
//! `executor::hooks::ArbStartBlockDerived`) to reth's alloy-evm
//! [`BlockExecutor`]/[`BlockExecutorFactory`]/[`BlockAssembler`] trait surface, built on the
//! [`ArbEvm`](crate::ArbEvm)/[`ArbEvmFactory`](crate::ArbEvmFactory).
//!
//! Mirrors `alloy-op-evm`'s `OpBlockExecutor`/`OpBlockExecutorFactory`. Per-tx ArbOS gas/poster/tip
//! math lives in `arb_revm::handler`; this layer drives [`ArbEvm`] per tx (routing through
//! `ArbHandler` inside `transact`), reads `poster_gas` off the chain context for the receipt's
//! `gas_used_for_l1`, and builds an [`ArbReceiptEnvelope`].
//!
//! Pre-execution mirrors `execute_message`'s prelude: the EIP-2935 history-storage parent-hash
//! system call (ArbOS v40+; a no-op before that) followed by Nitro's `InternalTxStartBlock`
//! (typed internal tx `0x6a`), built via `arb_revm`'s [`DefaultArbExecutionHooks`].

use crate::tx::ArbTx;
use crate::{ArbEvm, ArbEvmFactory};
use alloc::vec::Vec;
use alloy_consensus::{
    Block, BlockBody, EMPTY_OMMER_ROOT_HASH, Eip658Value, Header, Receipt, ReceiptWithBloom,
    TxReceipt, proofs,
};
use alloy_eips::{Encodable2718, Typed2718};
use alloy_evm::{
    Database, Evm, EvmFactory, FromRecoveredTx, FromTxWithEncoded, RecoveredTx,
    block::{
        BlockExecutionError, BlockExecutionResult, BlockExecutor, BlockExecutorFactory,
        ExecutableTx, GasOutput, StateDB, TxResult,
    },
};
use alloy_primitives::{Address, B64, B256, Bytes, Log, U256, logs_bloom};
use arb_revm::api::default_ctx::ArbContext;
use arb_revm::constants::{BATCH_POSTER_ADDRESS, HISTORY_STORAGE_ADDRESS};
use arb_revm::executor::hooks::{
    ArbExecutionHooks, ArbStartBlockDerived, DefaultArbExecutionHooks,
};
use arb_revm::executor::{ArbExecutionInput, ArbMessageEnvelope, ArbParentHeader};
use arb_revm::{ArbBlockHeaderInfo, ArbExecCfg, ArbosState};
use arbitrum_alloy_consensus::header::ArbHeaderInfo;
use arbitrum_alloy_consensus::receipt::{ArbReceipt, ArbReceiptEnvelope};
use arbitrum_alloy_consensus::transactions::ArbTxEnvelope;
use arbitrum_alloy_consensus::transactions::internal::ArbInternalTx;
use core::fmt::Debug;
use metrics::Histogram;
use reth_evm::execute::{BlockAssembler, BlockAssemblerInput};
use revm::context::{Block as _, ContextTr, result::ResultAndState};
use revm::handler::SYSTEM_ADDRESS;
use revm::{DatabaseCommit, Inspector, context::result::ExecutionResult};
use std::{
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

/// Block-execution context for an Arbitrum block, beyond what the EVM env carries.
///
/// Analogue of `OpBlockExecutionCtx`. Provides the inputs that the StartBlock prelude
/// (`InternalTxStartBlock`) and the EIP-2935 parent-hash system call need: values not representable
/// in alloy's [`EvmEnv`](alloy_evm::EvmEnv) such as L1 base fee, L1 block number, poster, and the
/// parent block hash for the history-storage write.
///
/// Must be [`Clone`] (a [`BlockExecutorFactory`] requirement).
#[derive(Debug, Default, Clone)]
pub struct ArbBlockExecutionCtx {
    /// Parent (L2) block hash, written to the EIP-2935 history-storage contract pre-execution.
    pub parent_hash: B256,
    /// Parent block extra_data (carried through to the assembled header).
    pub extra_data: Bytes,
    /// L1 base fee (wei) for this block (the `l1BaseFee` arg of `ArbosActs.startBlock`).
    pub l1_base_fee_wei: U256,
    /// L1 block number for this L2 block: the `l1BlockNumber` arg of `ArbosActs.startBlock`,
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
    /// This is the only channel from executor to assembler in reth's block-builder flow:
    /// `BasicBlockBuilder` hands the assembler the builder's `ctx`, not the executor's EVM.
    /// Because `create_executor` receives `ctx.clone()` and `Arc` clones share the inner cell,
    /// a value stored here by the executor is visible to the assembler.
    pub header_info_out: Arc<Mutex<Option<ArbBlockHeaderInfo>>>,
    /// Per-block timings from the two Arbitrum-owned portions of generic block finalization.
    /// The engine adds state hashing and state-root timing around reth's generic builder.
    pub finish_timing_out: Arc<Mutex<ArbBlockFinishTiming>>,
}

/// Timings emitted by Arbitrum's executor and assembler while finalizing one block.
#[derive(Clone, Copy, Debug, Default)]
pub struct ArbBlockFinishTiming {
    /// Reads the post-execution ArbOS header information and packages executor output.
    pub executor_finish: Duration,
    /// Builds transaction/receipt roots, logs bloom, and seals the Arbitrum block header.
    pub block_assembly: Duration,
}

/// Result of executing one Arbitrum transaction through the block executor.
#[derive(Debug)]
pub struct ArbTxResult<H> {
    /// Inner revm execution result + state delta.
    pub result: ResultAndState<H>,
    /// ArbOS L1 poster gas for this tx (the receipt's `gas_used_for_l1`).
    pub gas_used_for_l1: u64,
    /// Consensus tx type byte (selects the receipt envelope variant).
    pub tx_type: u8,
    /// Whether detailed transaction metrics were selected for this transaction.
    pub metrics_sampled: bool,
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
    evm: E,
    ctx: ArbBlockExecutionCtx,
    hooks: H,
    chain_id: u64,
    receipts: Vec<ArbReceiptEnvelope<Log>>,
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
/// Bloom is computed from the logs, and `gas_used_for_l1` (= the tx's `poster_gas`) is recorded
/// on the receipt body.
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

/// Stable, low-cardinality labels for the consensus transaction families ArbOS executes.
const TX_TYPE_LABELS: [&str; 12] = [
    "legacy",
    "eip2930",
    "eip1559",
    "eip4844",
    "eip7702",
    "deposit",
    "unsigned",
    "contract",
    "retry",
    "submit_retryable",
    "internal",
    "unknown",
];

#[inline]
const fn tx_type_metric_index(tx_type: u8) -> usize {
    match tx_type {
        0x00 => 0,
        0x01 => 1,
        0x02 => 2,
        0x03 => 3,
        0x04 => 4,
        0x64 => 5,
        0x65 => 6,
        0x66 => 7,
        0x68 => 8,
        0x69 => 9,
        0x6a => 10,
        _ => 11,
    }
}

struct TransactionMetrics {
    execution: Histogram,
    receipt_build: Histogram,
    state_commit: Histogram,
    commit: Histogram,
    gas_used: Histogram,
    l1_gas_used: Histogram,
}

impl TransactionMetrics {
    fn for_type(tx_type: u8) -> &'static Self {
        static METRICS: [OnceLock<TransactionMetrics>; TX_TYPE_LABELS.len()] =
            [const { OnceLock::new() }; TX_TYPE_LABELS.len()];
        let index = tx_type_metric_index(tx_type);
        METRICS[index].get_or_init(|| {
            let tx_type = TX_TYPE_LABELS[index];
            Self {
                execution: metrics::histogram!(
                    "arb_reth.arbos.transaction_execution_seconds",
                    "tx_type" => tx_type,
                ),
                receipt_build: metrics::histogram!(
                    "arb_reth.arbos.transaction_receipt_build_seconds",
                    "tx_type" => tx_type,
                ),
                state_commit: metrics::histogram!(
                    "arb_reth.arbos.transaction_state_commit_seconds",
                    "tx_type" => tx_type,
                ),
                commit: metrics::histogram!(
                    "arb_reth.arbos.transaction_commit_seconds",
                    "tx_type" => tx_type,
                ),
                gas_used: metrics::histogram!(
                    "arb_reth.arbos.transaction_gas_used",
                    "tx_type" => tx_type,
                ),
                l1_gas_used: metrics::histogram!(
                    "arb_reth.arbos.transaction_l1_gas_used",
                    "tx_type" => tx_type,
                ),
            }
        })
    }
}

/// Selects one transaction per type and sample-rate window. A rate of zero disables the detailed
/// transaction histograms; block-level execution and end-to-end latency metrics remain complete.
fn sample_transaction_metrics(tx_type: u8) -> bool {
    static SAMPLE_RATE: OnceLock<u64> = OnceLock::new();
    static COUNTERS: [AtomicU64; TX_TYPE_LABELS.len()] =
        [const { AtomicU64::new(0) }; TX_TYPE_LABELS.len()];

    let sample_rate = *SAMPLE_RATE.get_or_init(|| {
        std::env::var("ARB_EXECUTION_METRICS_SAMPLE_RATE")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(1)
    });
    match sample_rate {
        0 => false,
        1 => true,
        rate => COUNTERS[tx_type_metric_index(tx_type)]
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(rate),
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
        // history-storage contract under SYSTEM_ADDRESS. On pre-v40 chains this is a no-op state
        // transition (matching `execute_message`).
        //
        // `InternalTxStartBlock` (0x6a) is NOT run here. It is a real block transaction with its
        // own receipt; the caller drives it through `execute_transaction` like any other tx. Running
        // it here would exclude it from the transactions/receipts roots and diverge from Nitro.
        let execution_histogram =
            metrics::histogram!("arb_reth.arbos.pre_execution_system_call_seconds");
        let started_at = Instant::now();
        let result = self.evm.transact_system_call(
            SYSTEM_ADDRESS,
            HISTORY_STORAGE_ADDRESS,
            Bytes::copy_from_slice(self.ctx.parent_hash.as_slice()),
        );
        let execution_seconds = started_at.elapsed().as_secs_f64();
        execution_histogram.record(execution_seconds);
        let result = result.map_err(|err| BlockExecutionError::evm(err, self.ctx.parent_hash))?;
        self.evm.db_mut().commit(result.state);

        Ok(())
    }

    fn execute_transaction_without_commit(
        &mut self,
        tx: impl ExecutableTx<Self>,
    ) -> Result<Self::Result, BlockExecutionError> {
        let (tx_env, tx) = tx.into_parts();
        let tx_type = tx.tx().ty();
        let metrics_sampled = sample_transaction_metrics(tx_type);
        let started_at = metrics_sampled.then(Instant::now);
        let result = self.evm.transact(tx_env);
        if let Some(started_at) = started_at {
            TransactionMetrics::for_type(tx_type)
                .execution
                .record(started_at.elapsed().as_secs_f64());
        }
        let result = result.map_err(|err| BlockExecutionError::evm(err, tx.tx().trie_hash()))?;

        // `chain().poster_gas` is set by the ArbOS handler during pre_execution; it is the
        // L2-gas equivalent of the L1 poster cost, recorded as the receipt's `gas_used_for_l1`.
        let gas_used_for_l1 = self.evm.ctx().chain.poster_gas;

        Ok(ArbTxResult {
            result,
            gas_used_for_l1,
            tx_type,
            metrics_sampled,
        })
    }

    fn commit_transaction(&mut self, output: Self::Result) -> GasOutput {
        let ArbTxResult {
            result: ResultAndState { result, state },
            gas_used_for_l1,
            tx_type,
            metrics_sampled,
        } = output;

        let gas_used = result.tx_gas_used();

        let started_at = metrics_sampled.then(Instant::now);
        let receipt_started_at = metrics_sampled.then(Instant::now);
        self.gas_used += gas_used;
        self.receipts.push(build_arb_receipt(
            tx_type,
            result,
            self.gas_used,
            gas_used_for_l1,
        ));
        let receipt_seconds =
            receipt_started_at.map(|started_at| started_at.elapsed().as_secs_f64());

        let state_commit_started_at = metrics_sampled.then(Instant::now);
        self.evm.db_mut().commit(state);
        let state_commit_seconds =
            state_commit_started_at.map(|started_at| started_at.elapsed().as_secs_f64());
        let commit_seconds = started_at.map(|started_at| started_at.elapsed().as_secs_f64());

        if let (Some(receipt_seconds), Some(state_commit_seconds), Some(commit_seconds)) =
            (receipt_seconds, state_commit_seconds, commit_seconds)
        {
            let tx_metrics = TransactionMetrics::for_type(tx_type);
            tx_metrics.receipt_build.record(receipt_seconds);
            tx_metrics.state_commit.record(state_commit_seconds);
            tx_metrics.commit.record(commit_seconds);
            tx_metrics.gas_used.record(gas_used as f64);
            tx_metrics.l1_gas_used.record(gas_used_for_l1 as f64);
        }

        GasOutput::new(gas_used)
    }

    fn finish(
        mut self,
    ) -> Result<(Self::Evm, BlockExecutionResult<Self::Receipt>), BlockExecutionError> {
        let finish_started_at = Instant::now();
        // Read the post-execution ArbOS header info (send-Merkle root/count, ArbOS version, L2
        // base fee) and write it to the shared ctx cell for the assembler. All txs are committed at
        // this point, so the journal reads committed values. Mirrors what Nitro's `FinalizeBlock`/
        // `createNewHeader` pull from `arbosState`.
        let header_info_histogram =
            metrics::histogram!("arb_reth.arbos.post_execution_header_info_seconds");
        let started_at = Instant::now();
        let header_info = ArbosState::read_block_header_info(self.evm.ctx_mut().journal_mut())
            .map_err(|e| BlockExecutionError::msg(format!("read ArbOS block header info: {e}")))?;
        let header_info_seconds = started_at.elapsed().as_secs_f64();
        header_info_histogram.record(header_info_seconds);
        if let Ok(mut slot) = self.ctx.header_info_out.lock() {
            *slot = Some(header_info);
        }

        let gas_used = self
            .receipts
            .last()
            .map(|r| r.cumulative_gas_used())
            .unwrap_or_default();
        if let Ok(mut timing) = self.ctx.finish_timing_out.lock() {
            timing.executor_finish = finish_started_at.elapsed();
        }
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
    /// Reconstructs the `ArbExecutionInput` the `arb_revm` start-block hook expects from the EVM
    /// env and block-execution ctx.
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
    /// Builds Nitro's `InternalTxStartBlock` (0x6a) for this block: the first transaction of every
    /// L2 block, carrying the `ArbosActs.startBlock(l1BaseFee, l1BlockNumber, l2BlockNumber,
    /// timeLastBlock)` calldata. Returns `None` if the hook yields no prelude.
    ///
    /// The caller runs this through `execute_transaction` (not `apply_pre_execution_changes`) so it
    /// appears in the block's transactions and receipts, matching Nitro.
    pub fn start_block_tx(&self) -> Option<ArbTxEnvelope> {
        let l2_block_number = self.evm.block().number().saturating_to::<u64>();
        let derived = ArbStartBlockDerived {
            l2_block_number,
            time_last_block: self.ctx.time_last_block,
        };
        let input = self.start_block_input(l2_block_number);
        let call = self.hooks.start_block_prelude(&input, derived)?;
        Some(ArbTxEnvelope::from(ArbInternalTx::new(
            self.chain_id,
            call.data,
        )))
    }
}

/// Factory producing [`ArbBlockExecutor`]s. Mirrors `OpBlockExecutorFactory`.
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
        // Thread the L1 block number into the chain context so `NUMBER` (overridden by `arb_revm`
        // to read `chain().l1_block_number`) returns the correct value. Populated from `ArbHeaderInfo`
        // by `ConfigureEvm::context_for_block`.
        evm.ctx_mut().chain.l1_block_number = ctx.l1_block_number;
        ArbBlockExecutor::new(evm, ctx, self.hooks.clone(), self.chain_id)
    }
}

// `ArbTx` must be constructible from a recovered `ArbTxEnvelope` (with and without encoded bytes)
// for the `BlockExecutor::Transaction = ArbTxEnvelope` wiring.
const _: fn() = || {
    fn assert_from<T: FromRecoveredTx<ArbTxEnvelope> + FromTxWithEncoded<ArbTxEnvelope>>() {}
    assert_from::<ArbTx>();
};

/// Block assembler for Arbitrum. Mirrors `OpBlockAssembler`.
///
/// Builds an [`ArbBlock`](arbitrum_alloy_consensus::ArbBlock)-shaped `Block<ArbTxEnvelope>` from
/// execution output: receipts root, logs bloom, gas used, and the post-execution state root.
/// Encodes Arbitrum header metadata (`extra_data` / `mix_hash`) from the post-execution
/// [`ArbBlockHeaderInfo`] stored in the shared ctx cell.
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
        let assembly_started_at = Instant::now();
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

        // Arbitrum header metadata (Nitro `HeaderInfo`): executor stored post-execution
        // send-Merkle root/count + ArbOS version into the shared ctx cell during `finish`.
        // Falls back to zeros if unset (e.g. a non-Arbitrum/genesis probe).
        let computed = ctx
            .header_info_out
            .lock()
            .ok()
            .and_then(|guard| *guard)
            .unwrap_or_default();
        // Delayed-message blocks (coinbase != batch poster) never collect tips (Nitro
        // `block_processor.go`); all txs in a block share the coinbase.
        let collect_tips =
            computed.collect_tips && evm_env.block_env.beneficiary() == BATCH_POSTER_ADDRESS;
        let arb_info = ArbHeaderInfo {
            send_root: computed.send_root,
            send_count: computed.send_count,
            // The L1 block number ArbOS recorded post-execution (what Nitro packs into `mix_hash`),
            // not the raw message `l1BlockNumber` from `ctx`.
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
            difficulty: U256::from(1u64), // Nitro always sets L2 block difficulty to 1.
            number: evm_env.block_env.number().saturating_to(),
            gas_limit: evm_env.block_env.gas_limit(),
            gas_used: *gas_used,
            timestamp,
            mix_hash: arb_info.encode_mix_hash(), // carries send_count/l1_block_number/arbos_version
            nonce: B64::new(ctx.delayed_messages_read.to_be_bytes()), // Nitro EncodeNonce
            base_fee_per_gas: Some(evm_env.block_env.basefee()),
            extra_data: arb_info.encode_extra_data(),
            parent_beacon_block_root: None,
            blob_gas_used: None,
            excess_blob_gas: None,
            requests_hash: None,
            block_access_list_hash: None, // EIP-7928: not used on Arbitrum
            slot_number: None,
        };

        let block = Block::new(
            header,
            BlockBody {
                transactions,
                ommers: Default::default(),
                withdrawals: None,
            },
        );
        if let Ok(mut timing) = ctx.finish_timing_out.lock() {
            timing.block_assembly = assembly_started_at.elapsed();
        }
        Ok(block)
    }
}

#[cfg(test)]
mod tests;
