//! Engine-tree driver: the production block-production code, shared with the `engine_spike` gate.
//!
//! [`ArbEngineDriver`] stands up reth's [`EngineApiTreeHandler`] for `ArbNode` and drives
//! `feed message -> executed block -> InsertExecutedBlock + ForkchoiceUpdated -> canonicalize` with
//! async persistence: production waits only for fast in-memory canonicalization while the tree's
//! persistence service flushes to MDBX in the background.
//!
//! [`produce`] and [`wait_for_head`] are the single source of truth for the block-production and
//! head-observation logic; the `engine_spike` test imports them from here.

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

use reth_chain_state::{CanonicalInMemoryState, StateTrieOverlayManager};
use reth_engine_primitives::{
    BeaconEngineMessage, ConsensusEngineEvent, NoopInvalidBlockHook, TreeConfig,
};
use reth_engine_tree::engine::{EngineApiEvent, EngineApiKind, EngineApiRequest, FromEngine};
use reth_engine_tree::persistence::PersistenceHandle;
use reth_engine_tree::tree::{BasicEngineValidator, EngineApiTreeHandler};
use reth_evm::ConfigureEvm as _;
use reth_evm::execute::BlockBuilder as _;
use reth_execution_cache::{
    CacheFillMode, CacheStats, CachedStateCacheMetrics, CachedStateProvider, ExecutionCache,
};
use reth_execution_types::{BlockExecutionOutput, BlockExecutionResult};
use reth_payload_builder::PayloadBuilderHandle;
use reth_payload_primitives::BuiltPayloadExecutedBlock;
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

use crate::{ArbPayloadTypes, ArbPayloadValidator};

/// The concrete sender type returned by [`EngineApiTreeHandler::spawn_new`] for `ArbNode`.
type ToTree = crossbeam_channel::Sender<
    FromEngine<EngineApiRequest<ArbPayloadTypes, ArbPrimitives>, ArbBlock>,
>;

/// Produce one executed Arbitrum block from a feed message.
///
/// The caller supplies the two parent-state providers (`exec_state_provider` for execution reads,
/// `trie_state_provider` for the state-root `finish`); they must be independent instances. The
/// driver ([`ArbEngineDriver::build_block`]) constructs them for the current parent. Returns a
/// [`BuiltPayloadExecutedBlock`] (unsorted hashed/trie) ready to feed the tree via
/// `InsertExecutedBlock`; it does not persist.
pub fn produce<'a>(
    evm_config: &ArbEvmConfig,
    chain_id: u64,
    parent: &SealedHeader<Header>,
    feed_msg: &BroadcastFeedMessage,
    exec_state_provider: Box<dyn StateProvider + 'a>,
    trie_state_provider: Box<dyn StateProvider + 'a>,
) -> eyre::Result<BuiltPayloadExecutedBlock<ArbPrimitives>> {
    Ok(produce_with_timing(
        evm_config,
        chain_id,
        parent,
        feed_msg,
        exec_state_provider,
        trie_state_provider,
    )?
    .0)
}

/// Produce one block and retain a breakdown of the local block-production work.
///
/// Kept private so the public [`produce`] helper remains the small, stable test-facing API.
fn produce_with_timing<'a>(
    evm_config: &ArbEvmConfig,
    chain_id: u64,
    parent: &SealedHeader<Header>,
    feed_msg: &BroadcastFeedMessage,
    exec_state_provider: Box<dyn StateProvider + 'a>,
    trie_state_provider: Box<dyn StateProvider + 'a>,
) -> eyre::Result<(
    BuiltPayloadExecutedBlock<ArbPrimitives>,
    ArbBlockProductionTiming,
)> {
    let started_at = Instant::now();
    let parent_header = parent.header();
    let version = arbitrum_alloy_consensus::header::ArbHeaderInfo::decode_header(parent_header)
        .map(|i| i.arbos_format_version as u8)
        .unwrap_or(0);

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
    let mut user_txs: VecDeque<ArbTxEnvelope> = input.message.txs.into_iter().collect();
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
        let (tx, is_internal) = if let Some(t) = first.take() {
            (t, true)
        } else if let Some(t) = redeems.pop_front() {
            (t, false)
        } else if let Some(t) = user_txs.pop_front() {
            (t, false)
        } else {
            break;
        };
        let tx_ty = tx.ty();
        let sender: Address = tx
            .sender()
            .map_err(|e| eyre!("failed to determine sender for tx {tx_ty}: {e}"))?;
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
    let outcome = builder
        .finish(
            FinishTimingStateProvider::new(trie_state_provider, Arc::clone(&finish_state_timings)),
            None,
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
            changed_paths: None,
        },
        ArbBlockProductionTiming {
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
            finish_assembly: finish_timing.block_assembly,
            finish_unattributed,
        },
    ))
}

struct CacheAccessMetricHandles {
    hit_counter: Counter,
    miss_counter: Counter,
    hit_histogram: Histogram,
    miss_histogram: Histogram,
}

impl CacheAccessMetricHandles {
    fn new(kind: &'static str) -> Self {
        Self {
            hit_counter: metrics::counter!(
                "arb_reth.execution_cache_access_total",
                "kind" => kind,
                "result" => "hit",
            ),
            miss_counter: metrics::counter!(
                "arb_reth.execution_cache_access_total",
                "kind" => kind,
                "result" => "miss",
            ),
            hit_histogram: metrics::histogram!(
                "arb_reth.execution_cache_accesses_per_block",
                "kind" => kind,
                "result" => "hit",
            ),
            miss_histogram: metrics::histogram!(
                "arb_reth.execution_cache_accesses_per_block",
                "kind" => kind,
                "result" => "miss",
            ),
        }
    }
}

struct ExecutionCacheMetricHandles {
    account: CacheAccessMetricHandles,
    storage: CacheAccessMetricHandles,
    bytecode: CacheAccessMetricHandles,
}

fn execution_cache_metric_handles() -> &'static ExecutionCacheMetricHandles {
    static HANDLES: OnceLock<ExecutionCacheMetricHandles> = OnceLock::new();
    HANDLES.get_or_init(|| ExecutionCacheMetricHandles {
        account: CacheAccessMetricHandles::new("account"),
        storage: CacheAccessMetricHandles::new("storage"),
        bytecode: CacheAccessMetricHandles::new("bytecode"),
    })
}

/// Flush one block's cross-block execution-cache statistics after the provider has dropped.
/// Keeping these counters block-local avoids a metrics operation on every cache access, and the
/// recorder handles are registered once instead of once per block.
fn record_execution_cache_stats(stats: &CacheStats) {
    let handles = execution_cache_metric_handles();
    for (handles, hits, misses) in [
        (
            &handles.account,
            stats.account_hits(),
            stats.account_misses(),
        ),
        (
            &handles.storage,
            stats.storage_hits(),
            stats.storage_misses(),
        ),
        (&handles.bytecode, stats.code_hits(), stats.code_misses()),
    ] {
        handles.hit_counter.increment(hits as u64);
        handles.miss_counter.increment(misses as u64);
        handles.hit_histogram.record(hits as f64);
        handles.miss_histogram.record(misses as f64);
    }
}

struct EngineBlockMetricHandles {
    production: Histogram,
    parent_state: Histogram,
    execution: Histogram,
    finish: Histogram,
    finish_executor: Histogram,
    finish_hashed_state: Histogram,
    finish_state_root: Histogram,
    finish_assembly: Histogram,
    finish_unattributed: Histogram,
    total: Histogram,
    mgas_per_second: Histogram,
    execution_cache_update: Histogram,
}

fn engine_block_metric_handles() -> &'static EngineBlockMetricHandles {
    static HANDLES: OnceLock<EngineBlockMetricHandles> = OnceLock::new();
    HANDLES.get_or_init(|| EngineBlockMetricHandles {
        production: metrics::histogram!("arb_reth.engine_block_production_seconds"),
        parent_state: metrics::histogram!("arb_reth.engine_block_parent_state_seconds"),
        execution: metrics::histogram!("arb_reth.engine_block_execution_seconds"),
        finish: metrics::histogram!("arb_reth.engine_block_finish_seconds"),
        finish_executor: metrics::histogram!("arb_reth.engine_block_finish_executor_seconds"),
        finish_hashed_state: metrics::histogram!(
            "arb_reth.engine_block_finish_hashed_state_seconds"
        ),
        finish_state_root: metrics::histogram!("arb_reth.engine_block_finish_state_root_seconds"),
        finish_assembly: metrics::histogram!("arb_reth.engine_block_finish_assembly_seconds"),
        finish_unattributed: metrics::histogram!(
            "arb_reth.engine_block_finish_unattributed_seconds"
        ),
        total: metrics::histogram!("arb_reth.engine_block_total_seconds"),
        mgas_per_second: metrics::histogram!("arb_reth.engine_block_mgas_per_second"),
        execution_cache_update: metrics::histogram!("arb_reth.execution_cache_update_seconds"),
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
    P: BlockNumReader,
{
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        if let Ok(best) = provider.best_block_number()
            && best >= bn
        {
            return true;
        }
        let head = canonical.get_canonical_head();
        if head.header().number >= bn && head.hash() == expected_hash {
            return true;
        }
        while let Ok((number, hash)) = obs_rx.try_recv() {
            if number == bn && hash == expected_hash {
                return true;
            }
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
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
    }
}

/// Timings for one message that produced a canonical block.
///
/// `block_production` includes ArbOS execution, state-root computation, and header sealing.
/// The other fields cover the engine-tree handoff through in-memory canonicalization. Persistence
/// to MDBX remains asynchronous and is intentionally outside this critical-path measurement.
#[derive(Debug, Clone, Copy)]
pub struct ArbAppliedMessageTiming {
    /// Instant immediately before block production begins.
    pub started_at: Instant,
    /// Execution, state-root computation, and block/header construction.
    pub block_production: Duration,
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
    /// Transaction/receipt roots, logs bloom, and Arbitrum header/block assembly.
    pub block_finish_assembly: Duration,
    /// Generic finalization work not assigned to one of the named phases.
    pub block_finish_unattributed: Duration,
    /// Sending the executed block to the engine tree.
    pub engine_insert: Duration,
    /// Forkchoice request and response from the engine tree.
    pub engine_forkchoice: Duration,
    /// Waiting for the shared canonical in-memory state to observe the new head.
    pub canonicalization_wait: Duration,
    /// Total time in the in-order apply path.
    pub total: Duration,
}

/// Breakdown of local work performed while producing an Arbitrum block.
#[derive(Debug, Clone, Copy)]
struct ArbBlockProductionTiming {
    parent_state: Duration,
    message_preparation: Duration,
    state_setup: Duration,
    execution: Duration,
    execution_setup: Duration,
    start_block_transaction_construction: Duration,
    start_block_transaction: Duration,
    derived_transactions: Duration,
    derived_transaction_execution: Duration,
    derived_retry_scheduling: Duration,
    derived_transactions_unattributed: Duration,
    execution_unattributed: Duration,
    finish: Duration,
    finish_executor: Duration,
    finish_hashed_state: Duration,
    finish_state_root: Duration,
    finish_assembly: Duration,
    finish_unattributed: Duration,
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
    evm_config: ArbEvmConfig,
    chain_id: u64,
    tip: SealedHeader<Header>,
    to_tree: ToTree,
    canonical: CanonicalInMemoryState<ArbPrimitives>,
    obs_rx: tokio::sync::mpsc::UnboundedReceiver<(u64, B256)>,
    /// Cross-block account/storage/bytecode cache used by the execution provider. This is updated
    /// from each canonical block's final `BundleState`, so it always represents `execution_cache_tip`.
    execution_cache: ExecutionCache,
    /// Reth-native cache occupancy and collision gauges. Updated after every canonical block.
    execution_cache_metrics: Option<CachedStateCacheMetrics>,
    execution_cache_tip: B256,
    execution_cache_enabled: bool,
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
        // The engine tree owns a separate cache for its validation/payload paths. The direct
        // feed driver does not execute through that path, so keep a dedicated sequential cache.
        // 256 MiB holds roughly 1.8 million storage entries and avoids duplicating reth's 4 GiB
        // default allocation. Operators can tune it without changing consensus behavior.
        let execution_cache_enabled = std::env::var("ARB_EXECUTION_CACHE")
            .map(|v| !matches!(v.as_str(), "0" | "false" | "off" | "no"))
            .unwrap_or(true);
        let execution_cache_mb = std::env::var("ARB_EXECUTION_CACHE_MB")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&mb| mb > 0)
            .unwrap_or(256);
        let execution_cache_bytes = execution_cache_mb.saturating_mul(1024 * 1024);
        let execution_cache = ExecutionCache::new(execution_cache_bytes);
        let execution_cache_metrics =
            execution_cache_enabled.then(CachedStateCacheMetrics::default);
        let execution_cache_tip = genesis_tip.hash();

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
            execution_cache_enabled,
            execution_cache_mb,
            "engine-tree persistence tuning",
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

        let (to_payload_service, _payload_cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        let payload_builder: PayloadBuilderHandle<ArbPayloadTypes> =
            PayloadBuilderHandle::new(to_payload_service);

        let (to_tree, mut from_tree) = EngineApiTreeHandler::spawn_new(
            provider.clone(),
            consensus,
            payload_validator,
            persistence,
            payload_builder,
            canonical.clone(),
            state_trie_overlays,
            tree_config,
            EngineApiKind::Ethereum,
            evm_config.clone(),
            changeset_cache,
            runtime,
        );

        // Drain events on a background task so the tree channel never blocks; forward every
        // canonicalized block (number -> hash) to an mpsc the driver polls in `wait_for_head`.
        let (obs_tx, obs_rx) = tokio::sync::mpsc::unbounded_channel::<(u64, B256)>();
        tokio::spawn(async move {
            while let Some(ev) = from_tree.recv().await {
                if let EngineApiEvent::BeaconConsensus(ce) = ev {
                    match ce {
                        ConsensusEngineEvent::CanonicalChainCommitted(header, _) => {
                            let _ = obs_tx.send((header.number, header.hash()));
                        }
                        ConsensusEngineEvent::CanonicalBlockAdded(block, _) => {
                            let rb = block.recovered_block();
                            let _ = obs_tx.send((rb.header().number, rb.hash()));
                        }
                        _ => {}
                    }
                }
            }
        });

        // Seed the dedup cursor from the resumed tip: the next message to apply is the one that
        // produces block tip+1, i.e. message index tip.number - genesis_block + 1.
        let next_seq = genesis_tip.number.saturating_sub(genesis_block) + 1;

        Ok(Self {
            provider,
            evm_config,
            chain_id,
            tip: genesis_tip,
            to_tree,
            canonical,
            obs_rx,
            execution_cache,
            execution_cache_metrics,
            execution_cache_tip,
            execution_cache_enabled,
            next_seq,
            pending: BTreeMap::new(),
        })
    }

    /// Build independent execution and trie providers for the current parent, then produce one block.
    fn build_block(
        &mut self,
        msg: &BroadcastFeedMessage,
    ) -> eyre::Result<(
        BuiltPayloadExecutedBlock<ArbPrimitives>,
        ArbBlockProductionTiming,
    )> {
        // A discontinuity can only happen on restart/reorg today, but the hash guard makes stale
        // cached values impossible if another caller changes the driver's tip in the future.
        if self.execution_cache_enabled && self.execution_cache_tip != self.tip.hash() {
            tracing::warn!(
                target: "arb-reth::engine",
                cache_tip = %self.execution_cache_tip,
                parent = %self.tip.hash(),
                "execution cache tip mismatch; clearing cache",
            );
            self.execution_cache.clear();
            self.execution_cache_tip = self.tip.hash();
        }

        let started_at = Instant::now();
        let exec_sp = self
            .provider
            .state_by_block_hash(self.tip.hash())
            .wrap_err("state_by_block_hash (exec) failed")?;
        let trie_sp = self
            .provider
            .state_by_block_hash(self.tip.hash())
            .wrap_err("state_by_block_hash (trie) failed")?;
        let cache_stats = self
            .execution_cache_enabled
            .then(|| Arc::new(CacheStats::default()));
        let exec_sp: Box<dyn StateProvider + '_> = match &cache_stats {
            Some(stats) => Box::new(CachedStateProvider::new_with_mode(
                exec_sp,
                self.execution_cache.clone(),
                CacheFillMode::FillOnMiss,
                None,
                Some(Arc::clone(stats)),
            )),
            None => exec_sp,
        };
        let (built, mut timing) = produce_with_timing(
            &self.evm_config,
            self.chain_id,
            &self.tip,
            msg,
            exec_sp,
            trie_sp,
        )?;
        if let Some(stats) = cache_stats {
            record_execution_cache_stats(&stats);
        }
        timing.parent_state = started_at.elapsed().saturating_sub(
            timing.message_preparation + timing.state_setup + timing.execution + timing.finish,
        );
        Ok((built, timing))
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
        const MAX_PENDING: usize = 50_000;
        let seq = msg.sequence_number;
        if seq < self.next_seq {
            // Already applied by the other producer (feed/L1 overlap). Idempotent drop.
            return Ok(self.tip.hash());
        }
        if seq > self.next_seq {
            // Feed-ahead: hold until derivation fills the gap up to this sequence, then it drains.
            if self.pending.len() < MAX_PENDING {
                self.pending.insert(seq, msg.clone());
            }
            return Ok(self.tip.hash());
        }
        // seq == next_seq: this is the next block. Apply it, then drain the buffer forward.
        let (mut hash, timing) = self.apply_one(msg).await?;
        on_applied(seq, timing);
        self.next_seq += 1;
        while let Some(buffered) = self.pending.remove(&self.next_seq) {
            let (new_hash, timing) = self.apply_one(&buffered).await?;
            hash = new_hash;
            on_applied(self.next_seq, timing);
            self.next_seq += 1;
        }
        // Discard any stragglers now below the cursor (a feed dup that lost the race to L1).
        self.pending.retain(|k, _| *k >= self.next_seq);
        Ok(hash)
    }

    /// Apply exactly one in-order message: build the next block from `self.tip`, insert it into the
    /// engine tree, canonicalize, and advance `self.tip`. [`Self::advance`]'s sequence guard ensures
    /// this is only called for the message that produces `tip + 1`.
    async fn apply_one(
        &mut self,
        msg: &BroadcastFeedMessage,
    ) -> eyre::Result<(B256, ArbAppliedMessageTiming)> {
        let started_at = Instant::now();

        let (built, production_timing) = self.build_block(msg)?;
        let new_hash = built.recovered_block.hash();
        let new_header = built.recovered_block.header().clone();
        let new_number = new_header.number;
        let execution_output = Arc::clone(&built.execution_output);
        let block_production = started_at.elapsed();

        // Feed the executed block to the tree (no re-execution).
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
        let fcu_res = fcu_rx.await.wrap_err("FCU response channel")?;
        let fcu_res = fcu_res.wrap_err("FCU RethResult")?;
        fcu_res
            .await
            .map_err(|e| eyre!("block {new_number} FCU error: {e:?}"))?;
        let engine_forkchoice = phase_started_at.elapsed();

        // Wait for the tree to actually canonicalize the block (bounded).
        let phase_started_at = Instant::now();
        let canonicalized = wait_for_head(
            &self.provider,
            &self.provider.canonical_in_memory_state(),
            &mut self.obs_rx,
            new_number,
            new_hash,
        )
        .await;
        if !canonicalized {
            return Err(eyre!(
                "block {new_number} was NOT canonicalized within timeout (head hash {new_hash:#x})"
            ));
        }
        let canonicalization_wait = phase_started_at.elapsed();
        let total = started_at.elapsed();

        // Source-independent production timings. The feed-latency recorder only observes messages
        // seen on the websocket and therefore intentionally omits L1-derived catch-up blocks.
        // These histograms cover every canonical block and are the stable benchmark surface for
        // execution/cache/state-root work.
        let block_metrics = engine_block_metric_handles();
        block_metrics
            .production
            .record(block_production.as_secs_f64());
        block_metrics
            .parent_state
            .record(production_timing.parent_state.as_secs_f64());
        block_metrics
            .execution
            .record(production_timing.execution.as_secs_f64());
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
        block_metrics
            .finish_assembly
            .record(production_timing.finish_assembly.as_secs_f64());
        block_metrics
            .finish_unattributed
            .record(production_timing.finish_unattributed.as_secs_f64());
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
            us_produce = block_production.as_micros(),
            us_insert = engine_insert.as_micros(),
            us_fcu = engine_forkchoice.as_micros(),
            us_wait = canonicalization_wait.as_micros(),
            us_total = total.as_micros(),
            "advance timing",
        );

        self.tip = SealedHeader::new(new_header, new_hash);
        if self.execution_cache_enabled {
            let cache_update_started_at = Instant::now();
            if self
                .execution_cache
                .insert_state(&execution_output.state)
                .is_err()
            {
                tracing::warn!(
                    target: "arb-reth::engine",
                    number = new_number,
                    "inconsistent execution-cache update; clearing cache",
                );
                self.execution_cache.clear();
            }
            if let Some(metrics) = &self.execution_cache_metrics {
                self.execution_cache.update_metrics(metrics);
            }
            self.execution_cache_tip = new_hash;
            let cache_update_seconds = cache_update_started_at.elapsed().as_secs_f64();
            block_metrics
                .execution_cache_update
                .record(cache_update_seconds);
        }
        Ok((
            new_hash,
            ArbAppliedMessageTiming {
                started_at,
                block_production,
                block_parent_state: production_timing.parent_state,
                block_message_preparation: production_timing.message_preparation,
                block_state_setup: production_timing.state_setup,
                block_execution: production_timing.execution,
                block_execution_setup: production_timing.execution_setup,
                block_start_block_transaction_construction: production_timing
                    .start_block_transaction_construction,
                block_start_block_transaction: production_timing.start_block_transaction,
                block_derived_transactions: production_timing.derived_transactions,
                block_derived_transaction_execution: production_timing
                    .derived_transaction_execution,
                block_derived_retry_scheduling: production_timing.derived_retry_scheduling,
                block_derived_transactions_unattributed: production_timing
                    .derived_transactions_unattributed,
                block_execution_unattributed: production_timing.execution_unattributed,
                block_finish: production_timing.finish,
                block_finish_executor: production_timing.finish_executor,
                block_finish_hashed_state: production_timing.finish_hashed_state,
                block_finish_state_root: production_timing.finish_state_root,
                block_finish_assembly: production_timing.finish_assembly,
                block_finish_unattributed: production_timing.finish_unattributed,
                engine_insert,
                engine_forkchoice,
                canonicalization_wait,
                total,
            },
        ))
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
