//! Engine-tree driver: the production block-production code, shared with the `engine_spike` gate.
//!
//! [`ArbEngineDriver`] stands up reth's [`EngineApiTreeHandler`] for `ArbNode` and drives
//! `feed message -> payload attributes -> native payload builder -> InsertExecutedBlock + ForkchoiceUpdated -> canonicalize` with
//! async persistence: production waits only for fast in-memory canonicalization while the tree's
//! persistence service flushes to MDBX in the background.
//!
//! [`wait_for_head`] is shared by the engine integration tests.

use alloc::collections::BTreeMap;
use alloc::collections::VecDeque;
use alloc::sync::Arc;
use alloc::vec;
use alloc::vec::Vec;

use alloy_consensus::Header;
use alloy_consensus::transaction::Recovered;
use alloy_eips::eip2718::Typed2718;
use alloy_primitives::{Address, B256, BlockNumber, Bytes, Log, StorageKey, StorageValue};
use arb_reth_evm::ArbEvmConfig;
use arb_reth_evm::config::ArbNextBlockEnvAttributes;
use arb_revm::ArbosState;
use arb_revm::executor::{
    ArbExecCfg, ArbParentHeader, digest_message, is_redeem_scheduled_log,
    scheduled_retries_from_redeem_logs,
};
use arbitrum_alloy_consensus::reth::{ArbBlock, ArbPrimitives};
use arbitrum_alloy_consensus::{ArbReceiptEnvelope, ArbTxEnvelope};
use arbitrum_alloy_sequencer::sequencer::feed::BroadcastFeedMessage;
use eyre::{WrapErr as _, eyre};
use metrics::{Counter, Histogram};
use std::{
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use reth_chain_state::{CanonStateSubscriptions, CanonicalInMemoryState, StateTrieOverlayManager};
use reth_engine_primitives::{
    BeaconEngineMessage, ConsensusEngineEvent, NoopInvalidBlockHook, TreeConfig,
};
use reth_engine_tree::engine::{EngineApiEvent, EngineApiKind, EngineApiRequest, FromEngine};
use reth_engine_tree::persistence::PersistenceHandle;
use reth_engine_tree::tree::state_root_strategy::PayloadStateRootHandle;
use reth_engine_tree::tree::{BasicEngineValidator, EngineApiTreeHandler};
use reth_evm::execute::BlockBuilder as _;
use reth_evm::{ConfigureEvm as _, Evm as _};
use reth_execution_cache::CacheStats;
use reth_execution_types::{BlockExecutionOutput, BlockExecutionResult};
use reth_payload_builder::{PayloadBuilderHandle, PayloadBuilderService};
use reth_payload_primitives::{BuiltPayload as _, BuiltPayloadExecutedBlock, PayloadKind};
use reth_primitives_traits::{Account, Bytecode, RecoveredBlock, SealedHeader};
use reth_provider::providers::{BlockchainProvider, ProviderNodeTypes};
use reth_provider::{
    AccountReader, BalProvider, BlockHashReader, BlockNumReader, BlockReader, BytecodeReader,
    ChangeSetReader, DatabaseProviderFactory, HashedPostStateProvider, ProviderFactory,
    ProviderResult, StateProofProvider, StateProviderFactory, StateReader, StateRootProvider,
    StorageChangeSetReader, StorageRootProvider,
};
use reth_prune::{Pruner, PrunerBuilder};
use reth_revm::State;
use reth_revm::database::StateProviderDatabase;
use reth_storage_api::{
    DBProvider, PruneCheckpointReader, StageCheckpointReader, StateProvider, StorageSettingsCache,
};
use reth_tasks::Runtime;
use reth_trie::{
    AccountProof, ExecutionWitnessMode, HashedPostState, HashedStorage, MultiProof,
    MultiProofTargets, StorageMultiProof, StorageProof, TrieInput, updates::TrieUpdates,
};
use reth_trie_db::ChangesetCache;
use revm::context_interface::ContextTr as _;

use crate::native_payload::ArbPayloadJobGenerator;
use crate::{ArbPayloadAttributes, ArbPayloadBuilder, ArbPayloadTypes, ArbPayloadValidator};

/// The concrete sender type returned by [`EngineApiTreeHandler::spawn_new`] for `ArbNode`.
type ToTree = crossbeam_channel::Sender<
    FromEngine<EngineApiRequest<ArbPayloadTypes, ArbPrimitives>, ArbBlock>,
>;

/// Produce one block and retain a breakdown of the local block-production work.
pub(crate) fn produce_with_timing<'a>(
    evm_config: &ArbEvmConfig,
    chain_id: u64,
    parent: &SealedHeader<Header>,
    feed_msg: &BroadcastFeedMessage,
    exec_state_provider: Box<dyn StateProvider + 'a>,
    trie_state_provider: Box<dyn StateProvider + 'a>,
    mut state_root_task: Option<PayloadStateRootHandle>,
) -> eyre::Result<(
    BuiltPayloadExecutedBlock<ArbPrimitives>,
    ArbBlockProductionTiming,
)> {
    let started_at = Instant::now();
    let parent_header = parent.header();
    let arbos_version =
        arbitrum_alloy_consensus::header::ArbHeaderInfo::decode_header(parent_header)
            .ok()
            .map(|i| i.arbos_format_version as u8);
    let version = arbos_version.unwrap_or(0);

    let arb_parent = ArbParentHeader {
        number: parent_header.number,
        timestamp: parent_header.timestamp,
        beneficiary: parent_header.beneficiary,
        basefee: parent_header.base_fee_per_gas.unwrap_or(0),
        gas_limit: parent_header.gas_limit,
        difficulty: parent_header.difficulty,
        prevrandao: Some(parent_header.mix_hash),
    };
    let cfg = ArbExecCfg {
        chain_id,
        ..ArbExecCfg::default()
    };
    let input =
        digest_message(feed_msg, arb_parent, cfg, version).wrap_err("digest_message failed")?;

    let next_timestamp = input.message.l1_timestamp.max(arb_parent.timestamp);
    let finish_timing_out = Arc::new(std::sync::Mutex::new(Default::default()));
    let attrs = ArbNextBlockEnvAttributes {
        timestamp: next_timestamp,
        suggested_fee_recipient: input.message.poster,
        prev_randao: B256::ZERO,
        gas_limit: input.cfg.block_gas_limit,
        l1_block_number: input.message.l1_block_number,
        l1_base_fee_wei: input.message.l1_base_fee_wei,
        arbos_format_version: version as u64,
        delayed_messages_read: input.message.delayed_messages_read,
        extra_data: Bytes::default(),
        withdrawals: None,
        finish_timing_out: Arc::clone(&finish_timing_out),
    };

    let phase_started_at = Instant::now();

    // `exec_state_provider` / `trie_state_provider` are independent instances. Sharing one would
    // corrupt execution reads versus the trie build.
    let mut state = State::builder()
        .with_database(StateProviderDatabase::new(exec_state_provider))
        .with_bundle_update()
        .build();

    let state_setup = phase_started_at.elapsed();
    let message_preparation = started_at.elapsed().saturating_sub(state_setup);
    let phase_started_at = Instant::now();

    let mut builder = evm_config
        .builder_for_next_block(&mut state, parent, attrs)
        .map_err(|e| eyre!("builder_for_next_block: {e:?}"))?;
    if let Some(task) = state_root_task.as_mut() {
        builder
            .evm_mut()
            .db_mut()
            .set_state_hook(Some(Box::new(task.take_state_hook())));
    }
    builder
        .apply_pre_execution_changes()
        .wrap_err("apply_pre_execution_changes failed")?;

    // Tx-sequencing priority mirrors Nitro (arbos/block_processor.go:366-374): the start-block
    // internal tx first, then, each iteration, any scheduled redeem (FIFO) before the next sequenced
    // user tx. A user tx that calls `redeem()` schedules an `ArbitrumRetryTx` that Nitro runs
    // immediately after it. Appending scheduled retries to the back of a single user-tx queue runs
    // them after the remaining user txs, which does not change execution/state/gas (the txs are
    // independent) but reorders the block, diverging `transactionsRoot`/`receiptsRoot` and thus the
    // block hash from Nitro. That wrong hash is invisible to a state-root parity check until a later
    // L1-advancing block bakes the (wrong) parent hash into ArbOS state via `record_new_l1_block`.
    // Sender recovery is pure per-tx work; recover the sequenced user txs in parallel up front
    // instead of one ecrecover at a time inside the execution loop. Results (including failures)
    // are carried per-tx so the loop reports the same first-in-order error it would have hit
    // serially. Retries scheduled mid-block and the internal start-block tx carry their sender in
    // the envelope and stay on the cheap inline path.
    let mut user_txs: VecDeque<(
        ArbTxEnvelope,
        Result<Address, alloy_primitives::SignatureError>,
    )> = {
        let txs: Vec<ArbTxEnvelope> = input.message.txs.into_iter().collect();
        if txs.len() > 1 {
            use rayon::iter::{IntoParallelIterator as _, ParallelIterator as _};
            txs.into_par_iter()
                .map(|tx| {
                    let sender = tx.sender();
                    (tx, sender)
                })
                .collect::<Vec<_>>()
                .into()
        } else {
            txs.into_iter()
                .map(|tx| {
                    let sender = tx.sender();
                    (tx, sender)
                })
                .collect()
        }
    };
    let mut redeems: VecDeque<ArbTxEnvelope> = VecDeque::new();
    // Set the block's L2 base fee. ArbOS stores `L2PricingState.BaseFeeWei` = the fee for the next
    // block (each block's start-block `update_pricing_model` computes and stores the successor's
    // fee). So this block's basefee is the value already in state at block start (what the parent's
    // update produced), read here before the start-block tx overwrites it with the next block's fee.
    // Our block env was seeded with the parent header's basefee (`config.rs` `next_evm_env`), which
    // is the fee from two blocks back and is only correct while the fee sits at the `minBaseFee`
    // floor. Fixing it makes user txs pay the right L2 fee + L1 poster gas (posterCost / basefee),
    // and the sealed header (assembler reads `block_env.basefee()`) carry it. Only matters once the
    // gas backlog pushes the fee off the floor.
    let block_base_fee = ArbosState::open()
        .l2_pricing
        .base_fee_wei
        .get(builder.evm_mut().ctx_mut().journal_mut())
        .map_err(|e| eyre!("read L2 base fee for block env: {e}"))?;
    let block_base_fee = u64::try_from(block_base_fee).unwrap_or(u64::MAX);
    builder
        .evm_mut()
        .ctx_mut()
        .modify_block(|b| b.basefee = block_base_fee);

    let execution_setup = phase_started_at.elapsed();
    let phase_started_at = Instant::now();
    let mut first = builder.executor().start_block_tx();
    let start_block_transaction_construction = phase_started_at.elapsed();
    let phase_started_at = Instant::now();
    let mut start_block_transaction = Duration::ZERO;
    let mut derived_transactions = Duration::ZERO;
    let mut derived_transaction_execution = Duration::ZERO;
    let mut derived_retry_scheduling = Duration::ZERO;
    loop {
        let (tx, sender_result, is_internal) = if let Some(t) = first.take() {
            let sender = t.sender();
            (t, sender, true)
        } else if let Some(t) = redeems.pop_front() {
            let sender = t.sender();
            (t, sender, false)
        } else if let Some((t, sender)) = user_txs.pop_front() {
            (t, sender, false)
        } else {
            break;
        };
        let tx_ty = tx.ty();
        let sender: Address =
            sender_result.map_err(|e| eyre!("failed to determine sender for tx {tx_ty}: {e}"))?;
        let recovered = Recovered::new_unchecked(tx, sender);
        let mut tx_logs: Vec<Log> = Vec::new();
        let mut tx_success = false;
        let tx_started_at = Instant::now();
        if let Err(e) = builder.execute_transaction_with_result_closure(recovered, |res| {
            tx_success = res.result.result.is_success();
            tx_logs.extend(
                res.result
                    .result
                    .logs()
                    .iter()
                    .filter(|log| is_redeem_scheduled_log(log))
                    .cloned(),
            );
        }) {
            let tx_execution = tx_started_at.elapsed();
            if is_internal {
                start_block_transaction += tx_execution;
            } else {
                derived_transactions += tx_execution;
                derived_transaction_execution += tx_execution;
            }
            // Nitro `arbos/block_processor.go` (~l.503-549): a derived tx that is INVALID under the
            // state transition, a validation failure like lack-of-funds / NonceTooHigh, NOT a
            // revert, is reverted and dropped, and block production continues without it. This is
            // real on mainnet: an unsigned/contract tx from the delayed inbox whose sender can't
            // pay yields an internal-only block. revm rejects such
            // a tx before applying it, so nothing is added to the block; just skip and move on. Only
            // the internal start-block tx must always succeed. A wrong-but-not-invalid execution
            // divergence is still caught by the state-root parity check downstream.
            if is_internal {
                return Err(e).wrap_err("internal start-block tx failed");
            }
            tracing::warn!(
                target: "arb-reth::engine",
                block = parent_header.number + 1,
                tx_type = tx_ty,
                %sender,
                error = %e,
                "dropping invalid derived transaction (Nitro drop-and-continue)",
            );
            continue;
        }
        let mut retry_scheduling = Duration::ZERO;
        if tx_success && !tx_logs.is_empty() {
            // FIFO, drained before the next user tx, matching Nitro's cascading-redeem order.
            let retry_scheduling_started_at = Instant::now();
            let retries =
                scheduled_retries_from_redeem_logs(builder.evm_mut().ctx_mut(), &tx_logs, chain_id);
            retry_scheduling = retry_scheduling_started_at.elapsed();
            redeems.extend(retries);
        }

        let tx_execution = tx_started_at.elapsed();
        if is_internal {
            start_block_transaction += tx_execution;
        } else {
            derived_transactions += tx_execution;
            derived_transaction_execution += tx_execution.saturating_sub(retry_scheduling);
            derived_retry_scheduling += retry_scheduling;
        }
    }

    let derived_transactions_unattributed = derived_transactions
        .saturating_sub(derived_transaction_execution + derived_retry_scheduling);

    let execution =
        phase_started_at.elapsed() + execution_setup + start_block_transaction_construction;
    let execution_unattributed = execution.saturating_sub(
        execution_setup
            + start_block_transaction_construction
            + start_block_transaction
            + derived_transactions,
    );
    let phase_started_at = Instant::now();

    let finish_state_timings = Arc::new(FinishStateTimings::default());
    let (state_root_precomputed, changed_paths, state_root_task_wait, state_root_task_succeeded) =
        if let Some(mut task) = state_root_task {
            // Dropping the hook signals that the task has received every ArbOS state transition,
            // including the EIP-2935 prelude and start-block transaction.
            builder.evm_mut().db_mut().set_state_hook(None);
            let wait_started_at = Instant::now();
            match task.state_root() {
                Ok(outcome) => (
                    Some((
                        outcome.state_root,
                        Arc::unwrap_or_clone(outcome.trie_updates),
                    )),
                    outcome.changed_paths,
                    Some(wait_started_at.elapsed()),
                    true,
                ),
                Err(err) => {
                    tracing::warn!(
                        target: "arb-reth::engine",
                        block = parent_header.number + 1,
                        job = task.name(),
                        %err,
                        "state-root task failed; falling back to synchronous state root",
                    );
                    (None, None, Some(wait_started_at.elapsed()), false)
                }
            }
        } else {
            (None, None, None, false)
        };
    let outcome = builder
        .finish(
            FinishTimingStateProvider::new(trie_state_provider, Arc::clone(&finish_state_timings)),
            state_root_precomputed,
        )
        .wrap_err("BlockBuilder::finish failed")?;

    let finish = phase_started_at.elapsed();
    let finish_state_root = finish_state_timings.state_root();
    let finish_hashed_state = finish_state_timings.hashed_post_state();
    let finish_timing = finish_timing_out
        .lock()
        .map(|timing| *timing)
        .unwrap_or_default();
    let finish_unattributed = finish.saturating_sub(
        finish_timing.executor_finish
            + finish_hashed_state
            + finish_state_root
            + state_root_task_wait.unwrap_or_default()
            + finish_timing.block_assembly,
    );

    let bundle = state.take_bundle();
    drop(state);

    let recovered_block: RecoveredBlock<ArbBlock> = outcome.block;
    let execution_output = Arc::new(BlockExecutionOutput {
        result: BlockExecutionResult {
            receipts: outcome.execution_result.receipts,
            requests: outcome.execution_result.requests,
            gas_used: outcome.execution_result.gas_used,
            blob_gas_used: outcome.execution_result.blob_gas_used,
        },
        state: bundle,
    });

    // BuiltPayloadExecutedBlock wants unsorted hashed_state / trie_updates.
    Ok((
        BuiltPayloadExecutedBlock {
            recovered_block: Arc::new(recovered_block),
            execution_output,
            hashed_state: Arc::new(outcome.hashed_state),
            trie_updates: Arc::new(outcome.trie_updates),
            changed_paths,
        },
        ArbBlockProductionTiming {
            total: started_at.elapsed(),
            parent_state: Duration::ZERO,
            message_preparation,
            state_setup,
            execution,
            execution_setup,
            start_block_transaction_construction,
            start_block_transaction,
            derived_transactions,
            derived_transaction_execution,
            derived_retry_scheduling,
            derived_transactions_unattributed,
            execution_unattributed,
            finish,
            finish_executor: finish_timing.executor_finish,
            finish_hashed_state,
            finish_state_root,
            finish_state_root_task_wait: state_root_task_wait,
            state_root_task_succeeded,
            finish_assembly: finish_timing.block_assembly,
            finish_unattributed,
        },
    ))
}

struct EngineBlockMetricHandles {
    payload_attributes: Histogram,
    payload_job: Histogram,
    payload_job_launch: Histogram,
    payload_job_resolve: Histogram,
    payload_job_overhead: Histogram,
    production: Histogram,
    production_unattributed: Histogram,
    parent_state: Histogram,
    message_preparation: Histogram,
    state_setup: Histogram,
    execution: Histogram,
    execution_setup: Histogram,
    start_block_transaction_construction: Histogram,
    start_block_transaction: Histogram,
    derived_transactions: Histogram,
    derived_transaction_execution: Histogram,
    derived_retry_scheduling: Histogram,
    derived_transactions_unattributed: Histogram,
    execution_unattributed: Histogram,
    finish: Histogram,
    finish_executor: Histogram,
    finish_hashed_state: Histogram,
    finish_state_root: Histogram,
    finish_state_root_task_wait: Histogram,
    state_root_task_native_success: Counter,
    state_root_task_fallback: Counter,
    finish_assembly: Histogram,
    finish_unattributed: Histogram,
    engine_handoff: Histogram,
    engine_insert: Histogram,
    engine_forkchoice: Histogram,
    canonicalization_wait: Histogram,
    apply_overhead: Histogram,
    total: Histogram,
    mgas_per_second: Histogram,
}

fn engine_block_metric_handles() -> &'static EngineBlockMetricHandles {
    static HANDLES: OnceLock<EngineBlockMetricHandles> = OnceLock::new();
    HANDLES.get_or_init(|| EngineBlockMetricHandles {
        payload_attributes: metrics::histogram!("arb_reth.engine_block_payload_attributes_seconds"),
        payload_job: metrics::histogram!("arb_reth.engine_block_payload_job_seconds"),
        payload_job_launch: metrics::histogram!("arb_reth.engine_block_payload_job_launch_seconds"),
        payload_job_resolve: metrics::histogram!(
            "arb_reth.engine_block_payload_job_resolve_seconds"
        ),
        payload_job_overhead: metrics::histogram!(
            "arb_reth.engine_block_payload_job_overhead_seconds"
        ),
        production: metrics::histogram!("arb_reth.engine_block_production_seconds"),
        production_unattributed: metrics::histogram!(
            "arb_reth.engine_block_production_unattributed_seconds"
        ),
        parent_state: metrics::histogram!("arb_reth.engine_block_parent_state_seconds"),
        message_preparation: metrics::histogram!(
            "arb_reth.engine_block_message_preparation_seconds"
        ),
        state_setup: metrics::histogram!("arb_reth.engine_block_state_setup_seconds"),
        execution: metrics::histogram!("arb_reth.engine_block_execution_seconds"),
        execution_setup: metrics::histogram!("arb_reth.engine_block_execution_setup_seconds"),
        start_block_transaction_construction: metrics::histogram!(
            "arb_reth.engine_block_start_block_transaction_construction_seconds"
        ),
        start_block_transaction: metrics::histogram!(
            "arb_reth.engine_block_start_block_transaction_seconds"
        ),
        derived_transactions: metrics::histogram!(
            "arb_reth.engine_block_derived_transactions_seconds"
        ),
        derived_transaction_execution: metrics::histogram!(
            "arb_reth.engine_block_derived_transaction_execution_seconds"
        ),
        derived_retry_scheduling: metrics::histogram!(
            "arb_reth.engine_block_derived_retry_scheduling_seconds"
        ),
        derived_transactions_unattributed: metrics::histogram!(
            "arb_reth.engine_block_derived_transactions_unattributed_seconds"
        ),
        execution_unattributed: metrics::histogram!(
            "arb_reth.engine_block_execution_unattributed_seconds"
        ),
        finish: metrics::histogram!("arb_reth.engine_block_finish_seconds"),
        finish_executor: metrics::histogram!("arb_reth.engine_block_finish_executor_seconds"),
        finish_hashed_state: metrics::histogram!(
            "arb_reth.engine_block_finish_hashed_state_seconds"
        ),
        finish_state_root: metrics::histogram!("arb_reth.engine_block_finish_state_root_seconds"),
        finish_state_root_task_wait: metrics::histogram!(
            "arb_reth.engine_block_finish_state_root_task_wait_seconds"
        ),
        state_root_task_native_success: metrics::counter!(
            "arb_reth.engine_block_state_root_task_total",
            "mode" => "native_payload_builder",
            "result" => "success",
        ),
        state_root_task_fallback: metrics::counter!(
            "arb_reth.engine_block_state_root_task_total",
            "mode" => "native_payload_builder",
            "result" => "fallback",
        ),
        finish_assembly: metrics::histogram!("arb_reth.engine_block_finish_assembly_seconds"),
        finish_unattributed: metrics::histogram!(
            "arb_reth.engine_block_finish_unattributed_seconds"
        ),
        engine_handoff: metrics::histogram!("arb_reth.engine_block_engine_handoff_seconds"),
        engine_insert: metrics::histogram!("arb_reth.engine_block_engine_insert_seconds"),
        engine_forkchoice: metrics::histogram!("arb_reth.engine_block_engine_forkchoice_seconds"),
        canonicalization_wait: metrics::histogram!(
            "arb_reth.engine_block_canonicalization_wait_seconds"
        ),
        apply_overhead: metrics::histogram!("arb_reth.engine_block_apply_overhead_seconds"),
        total: metrics::histogram!("arb_reth.engine_block_total_seconds"),
        mgas_per_second: metrics::histogram!("arb_reth.engine_block_mgas_per_second"),
    })
}

/// Poll the tree's view (events, best block number, and in-memory head) until block `bn` with
/// hash `expected_hash` is canonical, or a bounded timeout elapses.
pub async fn wait_for_head<P>(
    provider: &P,
    canonical: &CanonicalInMemoryState<ArbPrimitives>,
    obs_rx: &mut tokio::sync::mpsc::UnboundedReceiver<(u64, B256)>,
    bn: u64,
    expected_hash: B256,
) -> bool
where
    P: BlockHashReader + BlockNumReader,
{
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if matches!(provider.block_hash(bn), Ok(Some(hash)) if hash == expected_hash) {
            return true;
        }
        let head = canonical.get_canonical_head();
        if head.header().number == bn && head.hash() == expected_hash {
            return true;
        }
        while let Ok((number, hash)) = obs_rx.try_recv() {
            if number == bn && hash == expected_hash {
                return true;
            }
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return false;
        }
        match tokio::time::timeout(remaining, obs_rx.recv()).await {
            Ok(Some((number, hash))) if number == bn && hash == expected_hash => return true,
            Ok(Some(_)) => {}
            Ok(None) | Err(_) => return false,
        }
    }
}

/// Engine-tree persistence tuning (reth [`TreeConfig`]).
///
/// reth's defaults (`persistence_threshold=2`, `memory_block_buffer_target=0`,
/// `persistence_backpressure_threshold=16`) suit a live validator: persist promptly, hold almost
/// nothing in memory.
///
#[derive(Debug, Clone, Copy)]
pub struct ArbEngineTuning {
    /// Persist once the canonical tip is this many blocks ahead of the last persisted block.
    pub persistence_threshold: u64,
    /// Keep this many of the most-recent blocks in memory (target size of the unpersisted buffer).
    pub memory_block_buffer_target: u64,
    /// Hard backpressure: stall block production once this many blocks are unpersisted.
    pub persistence_backpressure_threshold: u64,
    /// Size in bytes of reth's cross-block execution cache.
    ///
    /// Reth's bare [`TreeConfig`] default is intentionally large for a general-purpose node. A
    /// 256 MiB cache keeps the serial Arbitrum producer's fixed-cache tables dense and matches
    /// the previously measured direct-driver configuration.
    pub execution_cache_size: usize,
    /// Share Reth's cross-block execution cache with the native payload builder.
    ///
    /// The Arbitrum driver builds one payload at a time, so the shared cache cannot race a
    /// concurrent payload job and can safely serve repeated account, storage, and bytecode reads.
    pub share_execution_cache_with_payload_builder: bool,
    /// Share reth's sparse trie task with the native payload builder.
    ///
    /// This overlaps state-root computation with ArbOS execution. It is disabled by default to
    /// retain reth's conservative payload-builder behavior on hosts that may build payloads in
    /// parallel.
    pub share_sparse_trie_with_payload_builder: bool,
}

impl Default for ArbEngineTuning {
    fn default() -> Self {
        // reth's stock shallow defaults: prompt persistence and a minimal buffer.
        Self::reth_defaults()
    }
}

impl ArbEngineTuning {
    /// reth's stock defaults: prompt persistence, minimal in-memory buffer. Right for low-latency
    /// / small runs (and tests that assert a produced block is durably persisted immediately);
    /// use [`Default`] for bulk historical sync throughput.
    pub fn reth_defaults() -> Self {
        Self {
            persistence_threshold: 2,
            memory_block_buffer_target: 0,
            persistence_backpressure_threshold: 16,
            execution_cache_size: 256 * 1024 * 1024,
            share_execution_cache_with_payload_builder: true,
            share_sparse_trie_with_payload_builder: false,
        }
    }

    /// Build a reth [`TreeConfig`] from these knobs (all other fields keep reth defaults).
    pub fn to_tree_config(self) -> TreeConfig {
        TreeConfig::default()
            // TreeConfig validates its invariants after every builder call. Set the upper bound
            // first so deep configurations are valid in debug builds too.
            .with_persistence_backpressure_threshold(self.persistence_backpressure_threshold)
            .with_persistence_threshold(self.persistence_threshold)
            .with_memory_block_buffer_target(self.memory_block_buffer_target)
            .with_cross_block_cache_size(self.execution_cache_size)
            .with_share_execution_cache_with_payload_builder(
                self.share_execution_cache_with_payload_builder,
            )
            .with_share_sparse_trie_with_payload_builder(
                self.share_sparse_trie_with_payload_builder,
            )
    }
}

/// Timings for one message that produced a canonical block.
///
/// `block_production` includes ArbOS execution, state-root computation, and header sealing.
/// The other fields cover the engine-tree handoff through in-memory canonicalization. Persistence
/// to MDBX remains asynchronous and is intentionally outside this critical-path measurement.
#[derive(Debug, Clone, Copy)]
pub struct ArbAppliedMessageTiming {
    /// Instant immediately before native payload attributes are constructed.
    pub started_at: Instant,
    /// Instant at which the produced block became canonical, before metric emission.
    pub completed_at: Instant,
    /// Constructing Arbitrum payload attributes from the ordered message and current parent.
    pub payload_attributes: Duration,
    /// Full Reth payload-job lifecycle, from attributes FCU send until the built payload resolves.
    pub payload_job: Duration,
    /// Payload-job launch through the attributes FCU response.
    pub payload_job_launch: Duration,
    /// Waiting for the launched payload job to resolve after the attributes FCU response.
    pub payload_job_resolve: Duration,
    /// Payload-job lifecycle time outside the builder's measured block production.
    pub payload_job_overhead: Duration,
    /// Execution, state-root computation, and block/header construction.
    pub block_production: Duration,
    /// Block-production work outside parent state, message preparation, state setup, execution,
    /// and finalisation.
    pub block_production_unattributed: Duration,
    /// Parent-state provider setup before `produce` begins.
    pub block_parent_state: Duration,
    /// Feed-message digesting and next-block environment construction.
    pub block_message_preparation: Duration,
    /// Creation of revm's journaled state over the parent provider.
    pub block_state_setup: Duration,
    /// ArbOS pre-execution and transaction execution.
    pub block_execution: Duration,
    /// Block-builder creation, pre-execution changes, and base-fee setup before the first tx.
    pub block_execution_setup: Duration,
    /// Construction of ArbOS's mandatory internal start-block transaction.
    pub block_start_block_transaction_construction: Duration,
    /// Execution of ArbOS's mandatory internal start-block transaction.
    pub block_start_block_transaction: Duration,
    /// Execution of derived user and retry transactions, including retry scheduling.
    pub block_derived_transactions: Duration,
    /// Derived transaction execution and commit work, excluding retry scheduling.
    pub block_derived_transaction_execution: Duration,
    /// Extraction and enqueueing of retries emitted by successful derived transactions.
    pub block_derived_retry_scheduling: Duration,
    /// Small remainder after named derived-transaction phases, kept for exact accounting.
    pub block_derived_transactions_unattributed: Duration,
    /// Small remainder after the named block-execution phases, kept to make the breakdown exact.
    pub block_execution_unattributed: Duration,
    /// Total generic block finalization after ArbOS transactions complete.
    pub block_finish: Duration,
    /// ArbOS executor finalization, principally reading post-execution header metadata.
    pub block_finish_executor: Duration,
    /// Hashing the executed bundle into the post-state representation used by the trie.
    pub block_finish_hashed_state: Duration,
    /// Computing the post-state root and trie updates.
    pub block_finish_state_root: Duration,
    /// Waiting for the sparse state-root task after execution, if one was started.
    pub block_finish_state_root_task_wait: Option<Duration>,
    /// Whether the sparse task supplied the result. `None` means no task was started.
    pub block_finish_state_root_task_succeeded: Option<bool>,
    /// Transaction/receipt roots, logs bloom, and Arbitrum header/block assembly.
    pub block_finish_assembly: Duration,
    /// Generic finalization work not assigned to one of the named phases.
    pub block_finish_unattributed: Duration,
    /// Full engine-tree handoff, from executed-block insertion until canonical state is observable.
    pub engine_handoff: Duration,
    /// Sending the executed block to the engine tree.
    pub engine_insert: Duration,
    /// Forkchoice request and response from the engine tree, nested inside `engine_handoff`.
    pub engine_forkchoice: Duration,
    /// Waiting for canonical state, concurrently with `engine_forkchoice` and nested inside
    /// `engine_handoff`.
    pub canonicalization_wait: Duration,
    /// Total time in the in-order apply path.
    pub total: Duration,
}

/// Breakdown of local work performed while producing an Arbitrum block.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ArbBlockProductionTiming {
    pub(crate) total: Duration,
    pub(crate) parent_state: Duration,
    pub(crate) message_preparation: Duration,
    pub(crate) state_setup: Duration,
    pub(crate) execution: Duration,
    pub(crate) execution_setup: Duration,
    pub(crate) start_block_transaction_construction: Duration,
    pub(crate) start_block_transaction: Duration,
    pub(crate) derived_transactions: Duration,
    pub(crate) derived_transaction_execution: Duration,
    pub(crate) derived_retry_scheduling: Duration,
    pub(crate) derived_transactions_unattributed: Duration,
    pub(crate) execution_unattributed: Duration,
    pub(crate) finish: Duration,
    pub(crate) finish_executor: Duration,
    pub(crate) finish_hashed_state: Duration,
    pub(crate) finish_state_root: Duration,
    pub(crate) finish_state_root_task_wait: Option<Duration>,
    pub(crate) state_root_task_succeeded: bool,
    pub(crate) finish_assembly: Duration,
    pub(crate) finish_unattributed: Duration,
}

/// Driver-side timing around Reth's native payload-job lifecycle.
#[derive(Debug, Clone, Copy)]
struct ArbPayloadJobTiming {
    attributes: Duration,
    job: Duration,
    launch: Duration,
    resolve: Duration,
}

/// Completion of a final forkchoice request queued behind an inserted local block.
struct ArbForkchoiceCompletion {
    completed_at: Instant,
    elapsed: Duration,
}

/// A produced block whose final forkchoice request is still in flight.
///
/// Keeping this in the driver lets the next payload-attributes request enter the engine queue
/// before we await this response. The engine processes those requests in order, so the next
/// payload job can only start after this block is canonical, without requiring the producer to
/// round-trip through the final FCU first.
struct PendingAppliedBlock {
    sequence_number: u64,
    new_hash: B256,
    new_header: Header,
    production_timing: ArbBlockProductionTiming,
    execution_cache_stats: Option<Arc<CacheStats>>,
    payload_timing: ArbPayloadJobTiming,
    started_at: Instant,
    engine_handoff_started_at: Instant,
    canonicalization_started_at: Instant,
    engine_insert: Duration,
    forkchoice: Option<tokio::task::JoinHandle<eyre::Result<ArbForkchoiceCompletion>>>,
}

/// The generic reth block builder owns post-state hashing and state-root calculation. Wrap its
/// provider so ArbOS production can expose those two phases without changing consensus behavior.
#[derive(Debug, Default)]
struct FinishStateTimings {
    hashed_post_state_nanos: AtomicU64,
    state_root_nanos: AtomicU64,
}

impl FinishStateTimings {
    fn record_hashed_post_state(&self, elapsed: Duration) {
        self.hashed_post_state_nanos.store(
            elapsed.as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }

    fn record_state_root(&self, elapsed: Duration) {
        self.state_root_nanos.store(
            elapsed.as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }

    fn hashed_post_state(&self) -> Duration {
        Duration::from_nanos(self.hashed_post_state_nanos.load(Ordering::Relaxed))
    }

    fn state_root(&self) -> Duration {
        Duration::from_nanos(self.state_root_nanos.load(Ordering::Relaxed))
    }
}

#[derive(Debug)]
struct FinishTimingStateProvider<P> {
    inner: P,
    timings: Arc<FinishStateTimings>,
}

impl<P> FinishTimingStateProvider<P> {
    const fn new(inner: P, timings: Arc<FinishStateTimings>) -> Self {
        Self { inner, timings }
    }
}

impl<P: AccountReader> AccountReader for FinishTimingStateProvider<P> {
    fn basic_account(&self, address: &Address) -> ProviderResult<Option<Account>> {
        self.inner.basic_account(address)
    }
}

impl<P: BytecodeReader> BytecodeReader for FinishTimingStateProvider<P> {
    fn bytecode_by_hash(&self, code_hash: &B256) -> ProviderResult<Option<Bytecode>> {
        self.inner.bytecode_by_hash(code_hash)
    }
}

impl<P: StateProvider> StateProvider for FinishTimingStateProvider<P> {
    fn storage(
        &self,
        account: Address,
        storage_key: StorageKey,
    ) -> ProviderResult<Option<StorageValue>> {
        self.inner.storage(account, storage_key)
    }
}

impl<P: StateRootProvider> StateRootProvider for FinishTimingStateProvider<P> {
    fn state_root(&self, hashed_state: HashedPostState) -> ProviderResult<B256> {
        self.inner.state_root(hashed_state)
    }

    fn state_root_from_nodes(&self, input: TrieInput) -> ProviderResult<B256> {
        self.inner.state_root_from_nodes(input)
    }

    fn state_root_with_updates(
        &self,
        hashed_state: HashedPostState,
    ) -> ProviderResult<(B256, TrieUpdates)> {
        let started_at = Instant::now();
        let result = self.inner.state_root_with_updates(hashed_state);
        self.timings.record_state_root(started_at.elapsed());
        result
    }

    fn state_root_from_nodes_with_updates(
        &self,
        input: TrieInput,
    ) -> ProviderResult<(B256, TrieUpdates)> {
        self.inner.state_root_from_nodes_with_updates(input)
    }
}

impl<P: StateProofProvider> StateProofProvider for FinishTimingStateProvider<P> {
    fn proof(
        &self,
        input: TrieInput,
        address: Address,
        slots: &[B256],
    ) -> ProviderResult<AccountProof> {
        self.inner.proof(input, address, slots)
    }

    fn multiproof(
        &self,
        input: TrieInput,
        targets: MultiProofTargets,
    ) -> ProviderResult<MultiProof> {
        self.inner.multiproof(input, targets)
    }

    fn witness(
        &self,
        input: TrieInput,
        target: HashedPostState,
        mode: ExecutionWitnessMode,
    ) -> ProviderResult<Vec<Bytes>> {
        self.inner.witness(input, target, mode)
    }
}

impl<P: StorageRootProvider> StorageRootProvider for FinishTimingStateProvider<P> {
    fn storage_root(
        &self,
        address: Address,
        hashed_storage: HashedStorage,
    ) -> ProviderResult<B256> {
        self.inner.storage_root(address, hashed_storage)
    }

    fn storage_proof(
        &self,
        address: Address,
        slot: B256,
        hashed_storage: HashedStorage,
    ) -> ProviderResult<StorageProof> {
        self.inner.storage_proof(address, slot, hashed_storage)
    }

    fn storage_multiproof(
        &self,
        address: Address,
        slots: &[B256],
        hashed_storage: HashedStorage,
    ) -> ProviderResult<StorageMultiProof> {
        self.inner
            .storage_multiproof(address, slots, hashed_storage)
    }
}

impl<P: BlockHashReader> BlockHashReader for FinishTimingStateProvider<P> {
    fn block_hash(&self, number: BlockNumber) -> ProviderResult<Option<B256>> {
        self.inner.block_hash(number)
    }

    fn canonical_hashes_range(
        &self,
        start: BlockNumber,
        end: BlockNumber,
    ) -> ProviderResult<Vec<B256>> {
        self.inner.canonical_hashes_range(start, end)
    }
}

impl<P: HashedPostStateProvider> HashedPostStateProvider for FinishTimingStateProvider<P> {
    fn hashed_post_state(&self, bundle_state: &reth_revm::db::BundleState) -> HashedPostState {
        let started_at = Instant::now();
        let result = self.inner.hashed_post_state(bundle_state);
        self.timings.record_hashed_post_state(started_at.elapsed());
        result
    }
}

/// Engine-tree driver for `ArbNode`.
///
/// Owns the tree's request sender, the current tip, and a receiver of canonicalization
/// observations. Each [`advance`](ArbEngineDriver::advance) produces a block against the tree
/// overlay, feeds it via `InsertExecutedBlock` + `ForkchoiceUpdated`, and returns once the block
/// is canonical in memory. Persistence to MDBX happens asynchronously in the tree's background
/// persistence service.
pub struct ArbEngineDriver<N>
where
    N: ProviderNodeTypes<Primitives = ArbPrimitives>,
    BlockchainProvider<N>: DatabaseProviderFactory
        + BlockReader<Block = ArbBlock, Header = Header>
        + StateProviderFactory
        + StateReader<Receipt = ArbReceiptEnvelope>
        + HashedPostStateProvider
        + BalProvider
        + ChangeSetReader
        + BlockNumReader
        + Clone
        + 'static,
    <BlockchainProvider<N> as DatabaseProviderFactory>::Provider: BlockReader<Block = ArbBlock, Header = Header>
        + StageCheckpointReader
        + PruneCheckpointReader
        + ChangeSetReader
        + StorageChangeSetReader
        + BlockNumReader
        + StorageSettingsCache
        + DBProvider,
{
    provider: BlockchainProvider<N>,
    tip: SealedHeader<Header>,
    to_tree: ToTree,
    /// Reth's local payload-builder service for deterministic ArbOS message payloads.
    payload_builder: PayloadBuilderHandle<ArbPayloadTypes>,
    canonical: CanonicalInMemoryState<ArbPrimitives>,
    obs_rx: tokio::sync::mpsc::UnboundedReceiver<(u64, B256)>,
    /// The final FCU for the most recently produced block. Historical catch-up may leave this in
    /// flight until the next payload-attributes request has been queued behind it.
    pending_applied: Option<PendingAppliedBlock>,
    /// Sequence-reconciliation cursor (arb-reth's `TransactionStreamer` analogue). `next_seq` is the
    /// next message index to apply; a feed/derived message with `sequence_number` maps to L2 block
    /// `sequence_number + genesis_block`. Messages below `next_seq` are already-applied duplicates
    /// (dropped); the one equal to it is applied; ones above it are feed-ahead and buffered in
    /// `pending` until derivation closes the gap. This lets `--l1-rpc` derivation and `--feed-url`
    /// both feed the single driver channel without double-applying (Nitro's model, sans byte-compare
    /// reorg: an honest sequencer's feed and L1 agree, so index dedup suffices).
    next_seq: u64,
    /// Feed-ahead reorder buffer: messages with `sequence_number > next_seq`, keyed by sequence.
    /// Bounded by `MAX_PENDING` to cap memory if the feed runs far ahead of derivation.
    pending: BTreeMap<u64, BroadcastFeedMessage>,
}

impl<N> ArbEngineDriver<N>
where
    N: ProviderNodeTypes<Primitives = ArbPrimitives>,
    BlockchainProvider<N>: DatabaseProviderFactory
        + BlockReader<Block = ArbBlock, Header = Header>
        + StateProviderFactory
        + StateReader<Receipt = ArbReceiptEnvelope>
        + HashedPostStateProvider
        + BalProvider
        + ChangeSetReader
        + BlockNumReader
        + Clone
        + 'static,
    <BlockchainProvider<N> as DatabaseProviderFactory>::Provider: BlockReader<Block = ArbBlock, Header = Header>
        + StageCheckpointReader
        + PruneCheckpointReader
        + ChangeSetReader
        + StorageChangeSetReader
        + BlockNumReader
        + StorageSettingsCache
        + DBProvider,
{
    /// Stand up the engine tree over `factory`/`provider` and wire the event-drain task.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        factory: ProviderFactory<N>,
        provider: BlockchainProvider<N>,
        evm_config: ArbEvmConfig,
        chain_id: u64,
        genesis_tip: SealedHeader<Header>,
        genesis_block: u64,
        canonical: CanonicalInMemoryState<ArbPrimitives>,
        runtime: Runtime,
        tuning: ArbEngineTuning,
        prune_builder: Option<PrunerBuilder>,
    ) -> eyre::Result<Self> {
        // ---- persistence service (real MDBX writer; pruner from --prune.* flags) ----
        let (_finished_exex_height_tx, finished_exex_height_rx) =
            tokio::sync::watch::channel(reth_exex_types::FinishedExExHeight::NoExExs);
        // With no `--prune.*` flags this stays an archive node: a noop pruner (empty segment set)
        // that keeps all history. When pruning is configured, reth's `PrunerBuilder` turns the
        // requested `PruneModes` into the real segment set; the engine-tree persistence service
        // below runs it after each commit batch (at the configured block interval).
        let pruner = match prune_builder {
            Some(builder) => builder.build_with_provider_factory(factory.clone()),
            None => Pruner::new_with_factory(
                factory.clone(),
                vec![],
                5,
                0,
                None,
                finished_exex_height_rx,
            ),
        };
        let (sync_metrics_tx, _sync_metrics_rx) =
            tokio::sync::mpsc::unbounded_channel::<reth_stages_api::MetricEvent>();
        let persistence = PersistenceHandle::<ArbPrimitives>::spawn_service::<N>(
            factory,
            pruner,
            sync_metrics_tx,
        );

        // ---- engine-tree wiring (all reth components) ----
        let consensus: Arc<dyn reth_consensus::FullConsensus<ArbPrimitives>> =
            Arc::new(reth_consensus::noop::NoopConsensus::default());
        let changeset_cache = ChangesetCache::new();
        let state_trie_overlays =
            StateTrieOverlayManager::new(runtime.state_trie_overlay_worker_pool());
        let tree_config = tuning.to_tree_config();
        tracing::info!(
            target: "arb-reth::engine",
            persistence_threshold = tuning.persistence_threshold,
            memory_block_buffer_target = tuning.memory_block_buffer_target,
            persistence_backpressure_threshold = tuning.persistence_backpressure_threshold,
            share_execution_cache = tuning.share_execution_cache_with_payload_builder,
            share_sparse_trie = tuning.share_sparse_trie_with_payload_builder,
            "engine-tree payload and persistence configuration",
        );

        let payload_validator = BasicEngineValidator::new(
            provider.clone(),
            consensus.clone(),
            evm_config.clone(),
            ArbPayloadValidator,
            tree_config.clone(),
            Box::new(NoopInvalidBlockHook::default()),
            changeset_cache.clone(),
            state_trie_overlays.clone(),
            runtime.clone(),
        );

        let builder = ArbPayloadBuilder::new(provider.clone(), evm_config.clone(), chain_id);
        let generator = ArbPayloadJobGenerator::new(provider.clone(), runtime.clone(), builder);
        let (service, payload_builder) = PayloadBuilderService::<_, _, ArbPayloadTypes>::new(
            generator,
            provider.canonical_state_stream(),
        );
        runtime.spawn_critical_os_thread(
            "arb-payload-service",
            "arb native payload builder service",
            service,
        );

        let (to_tree, mut from_tree) = EngineApiTreeHandler::spawn_new(
            provider.clone(),
            consensus,
            payload_validator,
            persistence,
            payload_builder.clone(),
            canonical.clone(),
            state_trie_overlays.clone(),
            tree_config.clone(),
            EngineApiKind::Ethereum,
            evm_config.clone(),
            changeset_cache.clone(),
            runtime.clone(),
        );

        // Drain events on a background task so the tree channel never blocks. Only forward
        // committed-chain events: `CanonicalBlockAdded` is emitted when an executed block is
        // inserted as pending and is not proof that RPC-visible canonical state has advanced.
        let (obs_tx, obs_rx) = tokio::sync::mpsc::unbounded_channel::<(u64, B256)>();
        tokio::spawn(async move {
            while let Some(ev) = from_tree.recv().await {
                if let EngineApiEvent::BeaconConsensus(
                    ConsensusEngineEvent::CanonicalChainCommitted(header, _),
                ) = ev
                {
                    let _ = obs_tx.send((header.number, header.hash()));
                }
            }
        });

        // Seed the dedup cursor from the resumed tip: the next message to apply is the one that
        // produces block tip+1, i.e. message index tip.number - genesis_block + 1.
        let next_seq = genesis_tip.number.saturating_sub(genesis_block) + 1;
        Ok(Self {
            provider,
            tip: genesis_tip,
            to_tree,
            payload_builder,
            canonical,
            obs_rx,
            pending_applied: None,
            next_seq,
            pending: BTreeMap::new(),
        })
    }

    /// Produce, insert, and canonicalize one block from a feed message.
    ///
    /// Waits only for fast in-memory canonicalization (the tree persists to MDBX asynchronously).
    /// Returns the hash of the newly-produced block, which becomes the new tip.
    /// Reconcile and apply one incoming message (from either the feed or L1 derivation), Nitro
    /// `TransactionStreamer` style. Drops it if already applied (`sequence_number < next_seq`),
    /// applies it if it is the next expected message, or buffers it as feed-ahead otherwise; after
    /// applying, drains any now-contiguous buffered messages. Returns the resulting head hash.
    pub async fn advance(&mut self, msg: &BroadcastFeedMessage) -> eyre::Result<B256> {
        self.advance_with_applied(msg, |_, _| {}).await
    }

    /// Like [`Self::advance`], while notifying the caller after each message has become the
    /// canonical in-memory head. This includes buffered feed-ahead messages drained after a gap
    /// closes, and deliberately excludes duplicate messages that were not applied.
    pub async fn advance_with_applied<F>(
        &mut self,
        msg: &BroadcastFeedMessage,
        mut on_applied: F,
    ) -> eyre::Result<B256>
    where
        F: FnMut(u64, ArbAppliedMessageTiming),
    {
        self.advance_with_applied_inner(msg, false, &mut on_applied)
            .await
    }

    /// Like [`Self::advance_with_applied`], but allows the final block's forkchoice response to
    /// remain in flight when the caller already has another message ready. The next call queues
    /// its payload-attributes FCU first, then settles this block, preserving engine request order
    /// while overlapping the two driver round trips.
    pub async fn advance_with_applied_overlap<F>(
        &mut self,
        msg: &BroadcastFeedMessage,
        defer_tail: bool,
        mut on_applied: F,
    ) -> eyre::Result<B256>
    where
        F: FnMut(u64, ArbAppliedMessageTiming),
    {
        self.advance_with_applied_inner(msg, defer_tail, &mut on_applied)
            .await
    }

    async fn advance_with_applied_inner<F>(
        &mut self,
        msg: &BroadcastFeedMessage,
        defer_tail: bool,
        on_applied: &mut F,
    ) -> eyre::Result<B256>
    where
        F: FnMut(u64, ArbAppliedMessageTiming),
    {
        const MAX_PENDING: usize = 50_000;
        let seq = msg.sequence_number;
        if seq < self.next_seq {
            // Already applied by the other producer (feed/L1 overlap). Idempotent drop.
            if let Some((sequence_number, timing)) = self.settle_pending_applied().await? {
                on_applied(sequence_number, timing);
            }
            return Ok(self.tip.hash());
        }
        if seq > self.next_seq {
            // Feed-ahead: hold until derivation fills the gap up to this sequence, then it drains.
            if self.pending.len() < MAX_PENDING {
                self.pending.insert(seq, msg.clone());
            }
            if let Some((sequence_number, timing)) = self.settle_pending_applied().await? {
                on_applied(sequence_number, timing);
            }
            return Ok(self.tip.hash());
        }

        // seq == next_seq: queue this block, then drain the contiguous feed-ahead buffer. Each
        // subsequent payload-attributes request is queued before the previous final FCU is
        // awaited, which restores the native engine overlap without reading pending state.
        let (mut hash, completed) = self.apply_one_native(seq, msg, Instant::now()).await?;
        if let Some((sequence_number, timing)) = completed {
            on_applied(sequence_number, timing);
        }
        self.next_seq += 1;
        while let Some(buffered) = self.pending.remove(&self.next_seq) {
            let sequence_number = self.next_seq;
            let (new_hash, completed) = self
                .apply_one_native(sequence_number, &buffered, Instant::now())
                .await?;
            hash = new_hash;
            if let Some((completed_sequence, timing)) = completed {
                on_applied(completed_sequence, timing);
            }
            self.next_seq += 1;
        }

        if !defer_tail
            && let Some((sequence_number, timing)) = self.settle_pending_applied().await?
        {
            on_applied(sequence_number, timing);
        }

        // Discard any stragglers now below the cursor (a feed dup that lost the race to L1).
        self.pending.retain(|k, _| *k >= self.next_seq);
        Ok(hash)
    }

    /// Drive Reth's local payload lifecycle for one already-ordered Arbitrum message.
    async fn apply_one_native(
        &mut self,
        sequence_number: u64,
        msg: &BroadcastFeedMessage,
        started_at: Instant,
    ) -> eyre::Result<(B256, Option<(u64, ArbAppliedMessageTiming)>)> {
        let payload_builder = self.payload_builder.clone();
        let parent = self.tip.hash();
        let phase_started_at = Instant::now();
        let attributes = self.native_payload_attributes(msg);
        let payload_attributes = phase_started_at.elapsed();

        // This is Reth's standard local-builder entry point. The engine tree validates the
        // attributes, creates the sparse state-root task, and passes its handle to the builder.
        let payload_job_started_at = Instant::now();
        let (fcu_tx, fcu_rx) = tokio::sync::oneshot::channel();
        self.to_tree
            .send(FromEngine::Request(EngineApiRequest::Beacon(
                BeaconEngineMessage::ForkchoiceUpdated {
                    state: alloy_rpc_types_engine::ForkchoiceState {
                        head_block_hash: parent,
                        safe_block_hash: parent,
                        finalized_block_hash: B256::ZERO,
                    },
                    payload_attrs: Some(attributes),
                    tx: fcu_tx,
                },
            )))
            .map_err(|e| eyre!("send native payload FCU: {e}"))?;

        // The preceding block's final FCU was enqueued before this attributes FCU. Settle it now:
        // Reth can process this request and launch the next payload job as soon as canonicalization
        // completes, instead of waiting for another producer-to-engine round trip.
        let completed_previous = self.settle_pending_applied().await?;

        let build_fcu = fcu_rx
            .await
            .wrap_err("native payload FCU response channel")?;
        let build_fcu = build_fcu.wrap_err("native payload FCU RethResult")?;
        let build_fcu = build_fcu
            .await
            .map_err(|e| eyre!("native payload FCU error: {e:?}"))?;
        let payload_id = build_fcu
            .payload_id
            .ok_or_else(|| eyre!("native payload FCU returned no payload id"))?;
        let payload_job_launch = payload_job_started_at.elapsed();

        // Arbitrum has no competitive transaction-pool selection: resolve the deterministic
        // one-message build immediately, then hand its executed result back to the tree exactly
        // as Reth's regular engine launcher does for a locally built payload.
        let payload_resolve_started_at = Instant::now();
        let payload = payload_builder
            .resolve_kind(payload_id, PayloadKind::Earliest)
            .await
            .ok_or_else(|| eyre!("native payload job {payload_id:?} disappeared"))?
            .map_err(|e| eyre!("native payload job {payload_id:?} failed: {e}"))?;
        let payload_job_resolve = payload_resolve_started_at.elapsed();
        let payload_job = payload_job_started_at.elapsed();
        let production_timing = payload.production_timing();
        let execution_cache_stats = payload.execution_cache_stats();
        let built = payload
            .executed_block()
            .ok_or_else(|| eyre!("native payload {payload_id:?} omitted execution output"))?;

        let new_hash = self.queue_applied_block(
            sequence_number,
            built,
            production_timing,
            execution_cache_stats,
            ArbPayloadJobTiming {
                attributes: payload_attributes,
                job: payload_job,
                launch: payload_job_launch,
                resolve: payload_job_resolve,
            },
            started_at,
        )?;

        Ok((new_hash, completed_previous))
    }

    fn native_payload_attributes(&self, msg: &BroadcastFeedMessage) -> ArbPayloadAttributes {
        let parent = self.tip.header();
        let l1_timestamp = msg
            .message_with_meta_data
            .l1_incoming_message
            .header
            .timestamp;

        ArbPayloadAttributes {
            timestamp: l1_timestamp.max(parent.timestamp),
            message: msg.clone(),
        }
    }

    /// Insert one locally executed block and queue the FCU that makes it canonical.
    fn queue_applied_block(
        &mut self,
        sequence_number: u64,
        built: BuiltPayloadExecutedBlock<ArbPrimitives>,
        production_timing: ArbBlockProductionTiming,
        execution_cache_stats: Option<Arc<CacheStats>>,
        payload_timing: ArbPayloadJobTiming,
        started_at: Instant,
    ) -> eyre::Result<B256> {
        debug_assert!(self.pending_applied.is_none());
        let new_hash = built.recovered_block.hash();
        let new_header = built.recovered_block.header().clone();
        let new_number = new_header.number;

        // Feed the executed block to the tree (no re-execution).
        let engine_handoff_started_at = Instant::now();
        let phase_started_at = Instant::now();
        self.to_tree
            .send(FromEngine::Request(EngineApiRequest::InsertExecutedBlock(
                built,
            )))
            .map_err(|e| eyre!("send InsertExecutedBlock: {e}"))?;
        let engine_insert = phase_started_at.elapsed();

        // Drive canonicalization via ForkchoiceUpdated (head = new block).
        let phase_started_at = Instant::now();
        let (fcu_tx, fcu_rx) = tokio::sync::oneshot::channel();
        let fcu_state = alloy_rpc_types_engine::ForkchoiceState {
            head_block_hash: new_hash,
            safe_block_hash: new_hash,
            finalized_block_hash: B256::ZERO,
        };
        self.to_tree
            .send(FromEngine::Request(EngineApiRequest::Beacon(
                BeaconEngineMessage::ForkchoiceUpdated {
                    state: fcu_state,
                    payload_attrs: None,
                    tx: fcu_tx,
                },
            )))
            .map_err(|e| eyre!("send ForkchoiceUpdated: {e}"))?;

        // Poll the response independently of the producer. If another message is already queued,
        // its attributes FCU can be sent behind this request before the driver joins this task.
        let canonicalization_started_at = Instant::now();
        let forkchoice = tokio::spawn(async move {
            let fcu_res = fcu_rx.await.wrap_err("FCU response channel")?;
            let fcu_res = fcu_res.wrap_err("FCU RethResult")?;
            fcu_res
                .await
                .map_err(|e| eyre!("block {new_number} FCU error: {e:?}"))?;
            Ok(ArbForkchoiceCompletion {
                completed_at: Instant::now(),
                elapsed: phase_started_at.elapsed(),
            })
        });

        self.tip = SealedHeader::new(new_header.clone(), new_hash);
        self.pending_applied = Some(PendingAppliedBlock {
            sequence_number,
            new_hash,
            new_header,
            production_timing,
            execution_cache_stats,
            payload_timing,
            started_at,
            engine_handoff_started_at,
            canonicalization_started_at,
            engine_insert,
            forkchoice: Some(forkchoice),
        });
        Ok(new_hash)
    }

    /// Settle and account for the queued final FCU, if any.
    async fn settle_pending_applied(
        &mut self,
    ) -> eyre::Result<Option<(u64, ArbAppliedMessageTiming)>> {
        let Some(mut pending) = self.pending_applied.take() else {
            return Ok(None);
        };
        let forkchoice = pending
            .forkchoice
            .take()
            .expect("queued block must retain its final FCU task")
            .await
            .wrap_err("final FCU task panicked")??;

        // A successful FCU response should already imply this, but retain an exact hash check so
        // a future engine-tree behavior change cannot turn the overlap into an early callback.
        let was_observable_at_response = matches!(self.provider.block_hash(pending.new_header.number), Ok(Some(hash)) if hash == pending.new_hash)
            || {
                let head = self
                    .provider
                    .canonical_in_memory_state()
                    .get_canonical_head();
                head.header().number == pending.new_header.number && head.hash() == pending.new_hash
            };
        if was_observable_at_response {
            // The exact provider check is authoritative. Drain the redundant committed events so
            // the observation channel cannot grow throughout a long historical sync.
            while self.obs_rx.try_recv().is_ok() {}
        } else {
            let canonicalized = wait_for_head(
                &self.provider,
                &self.provider.canonical_in_memory_state(),
                &mut self.obs_rx,
                pending.new_header.number,
                pending.new_hash,
            )
            .await;
            if !canonicalized {
                return Err(eyre!(
                    "block {} was NOT canonicalized within timeout (head hash {:#x})",
                    pending.new_header.number,
                    pending.new_hash,
                ));
            }
        }

        let completed_at = if was_observable_at_response {
            forkchoice.completed_at
        } else {
            Instant::now()
        };
        let engine_handoff =
            completed_at.saturating_duration_since(pending.engine_handoff_started_at);
        let canonicalization_wait =
            completed_at.saturating_duration_since(pending.canonicalization_started_at);
        let sequence_number = pending.sequence_number;
        let timing = Self::record_completed_block(
            pending,
            forkchoice.elapsed,
            canonicalization_wait,
            engine_handoff,
            completed_at,
        );
        Ok(Some((sequence_number, timing)))
    }

    fn record_completed_block(
        pending: PendingAppliedBlock,
        engine_forkchoice: Duration,
        canonicalization_wait: Duration,
        engine_handoff: Duration,
        completed_at: Instant,
    ) -> ArbAppliedMessageTiming {
        let PendingAppliedBlock {
            new_hash,
            new_header,
            production_timing,
            execution_cache_stats,
            payload_timing,
            started_at,
            engine_insert,
            ..
        } = pending;
        let new_number = new_header.number;
        let block_production = production_timing.total;
        let named_production = production_timing.parent_state
            + production_timing.message_preparation
            + production_timing.state_setup
            + production_timing.execution
            + production_timing.finish;
        let block_production_unattributed = block_production.saturating_sub(named_production);
        let payload_job_overhead = payload_timing.job.saturating_sub(block_production);
        let total = completed_at.saturating_duration_since(started_at);

        // Source-independent production timings. The feed-latency recorder only observes messages
        // seen on the websocket and therefore intentionally omits L1-derived catch-up blocks.
        // These histograms cover every canonical block and are the stable benchmark surface for
        // execution/cache/state-root work.
        if let Some(stats) = execution_cache_stats {
            crate::native_payload::record_execution_cache_stats(&stats);
        }
        let block_metrics = engine_block_metric_handles();
        block_metrics
            .payload_attributes
            .record(payload_timing.attributes.as_secs_f64());
        block_metrics
            .payload_job
            .record(payload_timing.job.as_secs_f64());
        block_metrics
            .payload_job_launch
            .record(payload_timing.launch.as_secs_f64());
        block_metrics
            .payload_job_resolve
            .record(payload_timing.resolve.as_secs_f64());
        block_metrics
            .payload_job_overhead
            .record(payload_job_overhead.as_secs_f64());
        block_metrics
            .production
            .record(block_production.as_secs_f64());
        block_metrics
            .production_unattributed
            .record(block_production_unattributed.as_secs_f64());
        block_metrics
            .parent_state
            .record(production_timing.parent_state.as_secs_f64());
        block_metrics
            .message_preparation
            .record(production_timing.message_preparation.as_secs_f64());
        block_metrics
            .state_setup
            .record(production_timing.state_setup.as_secs_f64());
        block_metrics
            .execution
            .record(production_timing.execution.as_secs_f64());
        block_metrics
            .execution_setup
            .record(production_timing.execution_setup.as_secs_f64());
        block_metrics.start_block_transaction_construction.record(
            production_timing
                .start_block_transaction_construction
                .as_secs_f64(),
        );
        block_metrics
            .start_block_transaction
            .record(production_timing.start_block_transaction.as_secs_f64());
        block_metrics
            .derived_transactions
            .record(production_timing.derived_transactions.as_secs_f64());
        block_metrics.derived_transaction_execution.record(
            production_timing
                .derived_transaction_execution
                .as_secs_f64(),
        );
        block_metrics
            .derived_retry_scheduling
            .record(production_timing.derived_retry_scheduling.as_secs_f64());
        block_metrics.derived_transactions_unattributed.record(
            production_timing
                .derived_transactions_unattributed
                .as_secs_f64(),
        );
        block_metrics
            .execution_unattributed
            .record(production_timing.execution_unattributed.as_secs_f64());
        block_metrics
            .finish
            .record(production_timing.finish.as_secs_f64());
        block_metrics
            .finish_executor
            .record(production_timing.finish_executor.as_secs_f64());
        block_metrics
            .finish_hashed_state
            .record(production_timing.finish_hashed_state.as_secs_f64());
        block_metrics
            .finish_state_root
            .record(production_timing.finish_state_root.as_secs_f64());
        if let Some(wait) = production_timing.finish_state_root_task_wait {
            block_metrics
                .finish_state_root_task_wait
                .record(wait.as_secs_f64());
            if production_timing.state_root_task_succeeded {
                block_metrics.state_root_task_native_success.increment(1);
            } else {
                block_metrics.state_root_task_fallback.increment(1);
            }
        }
        block_metrics
            .finish_assembly
            .record(production_timing.finish_assembly.as_secs_f64());
        block_metrics
            .finish_unattributed
            .record(production_timing.finish_unattributed.as_secs_f64());
        block_metrics
            .engine_handoff
            .record(engine_handoff.as_secs_f64());
        block_metrics
            .engine_insert
            .record(engine_insert.as_secs_f64());
        block_metrics
            .engine_forkchoice
            .record(engine_forkchoice.as_secs_f64());
        block_metrics
            .canonicalization_wait
            .record(canonicalization_wait.as_secs_f64());
        let named_apply = payload_timing.attributes + payload_timing.job + engine_handoff;
        block_metrics
            .apply_overhead
            .record(total.saturating_sub(named_apply).as_secs_f64());
        block_metrics.total.record(total.as_secs_f64());
        let production_seconds = block_production.as_secs_f64();
        let mgas_per_second = if production_seconds > 0.0 {
            new_header.gas_used as f64 / 1_000_000.0 / production_seconds
        } else {
            0.0
        };
        block_metrics.mgas_per_second.record(mgas_per_second);

        // Per-block production trace (observability) + per-phase timing breakdown.
        tracing::info!(
            target: "arb-reth::engine",
            number = new_number,
            %new_hash,
            state_root = %new_header.state_root,
            gas_used = new_header.gas_used,
            "produced block",
        );
        tracing::debug!(
            target: "arb-reth::engine::timing",
            number = new_number,
            us_attributes = payload_timing.attributes.as_micros(),
            us_payload_job = payload_timing.job.as_micros(),
            us_payload_overhead = payload_job_overhead.as_micros(),
            us_produce = block_production.as_micros(),
            us_handoff = engine_handoff.as_micros(),
            us_insert = engine_insert.as_micros(),
            us_fcu = engine_forkchoice.as_micros(),
            us_wait = canonicalization_wait.as_micros(),
            us_total = total.as_micros(),
            "advance timing",
        );

        ArbAppliedMessageTiming {
            started_at,
            completed_at,
            payload_attributes: payload_timing.attributes,
            payload_job: payload_timing.job,
            payload_job_launch: payload_timing.launch,
            payload_job_resolve: payload_timing.resolve,
            payload_job_overhead,
            block_production,
            block_production_unattributed,
            block_parent_state: production_timing.parent_state,
            block_message_preparation: production_timing.message_preparation,
            block_state_setup: production_timing.state_setup,
            block_execution: production_timing.execution,
            block_execution_setup: production_timing.execution_setup,
            block_start_block_transaction_construction: production_timing
                .start_block_transaction_construction,
            block_start_block_transaction: production_timing.start_block_transaction,
            block_derived_transactions: production_timing.derived_transactions,
            block_derived_transaction_execution: production_timing.derived_transaction_execution,
            block_derived_retry_scheduling: production_timing.derived_retry_scheduling,
            block_derived_transactions_unattributed: production_timing
                .derived_transactions_unattributed,
            block_execution_unattributed: production_timing.execution_unattributed,
            block_finish: production_timing.finish,
            block_finish_executor: production_timing.finish_executor,
            block_finish_hashed_state: production_timing.finish_hashed_state,
            block_finish_state_root: production_timing.finish_state_root,
            block_finish_state_root_task_wait: production_timing.finish_state_root_task_wait,
            block_finish_state_root_task_succeeded: production_timing
                .finish_state_root_task_wait
                .map(|_| production_timing.state_root_task_succeeded),
            block_finish_assembly: production_timing.finish_assembly,
            block_finish_unattributed: production_timing.finish_unattributed,
            engine_handoff,
            engine_insert,
            engine_forkchoice,
            canonicalization_wait,
            total,
        }
    }

    /// Returns the current chain tip (the parent for the next block).
    pub fn tip(&self) -> &SealedHeader<Header> {
        &self.tip
    }

    /// Returns a clone of the in-memory canonical state (shared with the `BlockchainProvider`).
    pub fn canonical_in_memory(&self) -> CanonicalInMemoryState<ArbPrimitives> {
        self.canonical.clone()
    }

    /// Best-effort wait until all produced blocks are durably persisted before exit.
    ///
    /// The tree persists asynchronously; on shutdown we give the persistence service a bounded
    /// window (~10s) to flush pending blocks up to the current tip so nothing is lost.
    pub async fn shutdown(&self) {
        let target = self.tip.number;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if let Ok(best) = self.provider.best_block_number()
                && best >= target
            {
                return;
            }
            if std::time::Instant::now() >= deadline {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }
}
