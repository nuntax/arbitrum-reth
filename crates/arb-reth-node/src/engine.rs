//! Tier-1 engine-tree driver: reusable production code lifted from the `engine_spike` gate.
//!
//! [`ArbEngineDriver`] stands up reth's [`EngineApiTreeHandler`] for [`crate::ArbNode`] and drives
//! `feed message → executed block → InsertExecutedBlock + ForkchoiceUpdated → canonicalize` with
//! ASYNC persistence: production waits only for fast in-memory canonicalization while the tree's
//! persistence service flushes to MDBX in the background.
//!
//! [`produce`] and [`wait_for_head`] are the single source of truth for the block-production and
//! head-observation logic; the `engine_spike` test imports them from here.

use alloc::collections::VecDeque;
use alloc::sync::Arc;
use alloc::vec;
use alloc::vec::Vec;

use alloy_consensus::Header;
use alloy_consensus::transaction::Recovered;
use alloy_eips::eip2718::Typed2718;
use alloy_primitives::{Address, B256, Bytes, Log};
use arb_alloy_consensus::reth::{ArbBlock, ArbPrimitives};
use arb_alloy_consensus::{ArbReceiptEnvelope, ArbTxEnvelope};
use arb_reth_evm::ArbEvmConfig;
use arb_reth_evm::config::ArbNextBlockEnvAttributes;
use arb_revm::executor::{
    ArbExecCfg, ArbParentHeader, digest_message, scheduled_retries_from_redeem_logs,
};
use arb_sequencer_network::sequencer::feed::BroadcastFeedMessage;
use eyre::{WrapErr as _, eyre};

use reth_chain_state::{
    CanonicalInMemoryState, ComputedTrieData, ExecutedBlock, MemoryOverlayStateProviderRef,
};
use reth_engine_primitives::{
    BeaconEngineMessage, ConsensusEngineEvent, NoopInvalidBlockHook, TreeConfig,
};
use reth_engine_tree::engine::{EngineApiEvent, EngineApiKind, EngineApiRequest, FromEngine};
use reth_engine_tree::persistence::PersistenceHandle;
use reth_engine_tree::tree::{BasicEngineValidator, EngineApiTreeHandler};
use reth_evm::ConfigureEvm as _;
use reth_evm::execute::BlockBuilder as _;
use reth_execution_types::{BlockExecutionOutput, BlockExecutionResult};
use reth_payload_builder::PayloadBuilderHandle;
use reth_payload_primitives::BuiltPayloadExecutedBlock;
use reth_primitives_traits::{RecoveredBlock, SealedHeader};
use reth_provider::providers::{BlockchainProvider, ProviderNodeTypes};
use reth_provider::{
    BalProvider, BlockNumReader, BlockReader, ChangeSetReader, DatabaseProviderFactory,
    HashedPostStateProvider, LatestStateProviderRef, ProviderFactory, StateProviderFactory,
    StateReader, StorageChangeSetReader,
};
use reth_prune::Pruner;
use reth_revm::State;
use reth_revm::database::StateProviderDatabase;
use reth_storage_api::{
    DBProvider, PruneCheckpointReader, StageCheckpointReader, StateProvider, StorageSettingsCache,
};
use reth_tasks::Runtime;
use reth_trie_db::ChangesetCache;

use crate::{ArbPayloadTypes, ArbPayloadValidator};

/// The concrete sender type returned by [`EngineApiTreeHandler::spawn_new`] for [`crate::ArbNode`].
type ToTree = crossbeam_channel::Sender<
    FromEngine<EngineApiRequest<ArbPayloadTypes, ArbPrimitives>, ArbBlock>,
>;

/// Produce ONE executed Arbitrum block from a feed message.
///
/// The caller supplies the two parent-state providers (`exec_state_provider` for execution reads,
/// `trie_state_provider` for the state-root `finish`) — they MUST be independent instances. The
/// driver ([`ArbEngineDriver::build_block`]) selects between the legacy
/// `provider.state_by_block_hash(parent)` path and the immune ring overlay. Returns a
/// [`BuiltPayloadExecutedBlock`] (unsorted hashed/trie) ready to feed the tree via
/// `InsertExecutedBlock` — it does NOT persist.
pub(crate) fn produce<'a>(
    evm_config: &ArbEvmConfig,
    chain_id: u64,
    parent: &SealedHeader<Header>,
    feed_msg: &BroadcastFeedMessage,
    exec_state_provider: Box<dyn StateProvider + 'a>,
    trie_state_provider: Box<dyn StateProvider + 'a>,
) -> eyre::Result<BuiltPayloadExecutedBlock<ArbPrimitives>> {
    let parent_header = parent.header();
    let version = arb_alloy_consensus::header::ArbHeaderInfo::decode_header(parent_header)
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
    };

    // Bench sub-timing: overlay build vs execution vs state-root `finish`.
    let __ov0 = std::time::Instant::now();

    // `exec_state_provider` / `trie_state_provider` are supplied by the caller and MUST be
    // independent instances (sharing one corrupts execution reads vs the trie build). The caller
    // built them either via `state_by_block_hash(parent)` (legacy) or the immune ring overlay.
    let mut state = State::builder()
        .with_database(StateProviderDatabase::new(exec_state_provider))
        .with_bundle_update()
        .build();

    let __us_overlay = __ov0.elapsed().as_micros();
    let __ex0 = std::time::Instant::now();

    let mut builder = evm_config
        .builder_for_next_block(&mut state, parent, attrs)
        .map_err(|e| eyre!("builder_for_next_block: {e:?}"))?;
    builder
        .apply_pre_execution_changes()
        .wrap_err("apply_pre_execution_changes failed")?;

    // Tx-sequencing priority mirrors Nitro (arbos/block_processor.go:366-374): the start-block
    // internal tx first, then—each iteration—any SCHEDULED REDEEM (FIFO) BEFORE the next sequenced
    // user tx. A user tx that calls `redeem()` schedules an `ArbitrumRetryTx` that Nitro runs
    // immediately after it. Appending scheduled retries to the back of a single user-tx queue runs
    // them AFTER the remaining user txs, which does not change execution/state/gas (the txs are
    // independent) but reorders the block, diverging `transactionsRoot`/`receiptsRoot` and thus the
    // block HASH from Nitro. That wrong hash is invisible to a state-root parity check until a later
    // L1-advancing block bakes the (wrong) parent hash into ArbOS state via `record_new_l1_block`
    // (first observed as a state-root divergence at Arb One block 22476703; real cause at 22476646).
    let mut user_txs: VecDeque<ArbTxEnvelope> = input.message.txs.into_iter().collect();
    let mut redeems: VecDeque<ArbTxEnvelope> = VecDeque::new();
    let mut first = builder.executor().start_block_tx();
    loop {
        let tx = if let Some(t) = first.take() {
            t
        } else if let Some(t) = redeems.pop_front() {
            t
        } else if let Some(t) = user_txs.pop_front() {
            t
        } else {
            break;
        };
        let sender: Address = tx
            .sender()
            .map_err(|e| eyre!("failed to determine sender for tx {}: {e}", tx.ty()))?;
        let recovered = Recovered::new_unchecked(tx, sender);
        let mut tx_logs: Vec<Log> = Vec::new();
        let mut tx_success = false;
        builder
            .execute_transaction_with_result_closure(recovered, |res| {
                tx_success = res.result.result.is_success();
                tx_logs = res.result.result.logs().to_vec();
            })
            // In trusted replay every tx is known-valid, so an EVM *validation* failure here
            // (NonceTooHigh / lack-of-funds) is definitionally a torn parent-state read — the
            // overlay `state_by_block_hash(parent)` snapshot raced the engine tree's async
            // persistence (see `advance`'s retry loop, which rebuilds a fresh snapshot).
            .wrap_err("execute_transaction failed")?;
        if tx_success {
            // FIFO, drained before the next user tx — matches Nitro's cascading-redeem order.
            let retries =
                scheduled_retries_from_redeem_logs(builder.evm_mut().ctx_mut(), &tx_logs, chain_id);
            redeems.extend(retries);
        }
    }

    let __us_exec = __ex0.elapsed().as_micros();
    let __fi0 = std::time::Instant::now();

    let outcome = builder
        .finish(trie_state_provider, None)
        .wrap_err("BlockBuilder::finish failed")?;

    tracing::debug!(
        target: "arb-reth::engine::timing",
        block = parent_header.number + 1,
        us_overlay = __us_overlay as u64,
        us_exec = __us_exec as u64,
        us_finish = __fi0.elapsed().as_micros() as u64,
        "produce timing",
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

    // BuiltPayloadExecutedBlock wants UNSORTED hashed_state / trie_updates.
    Ok(BuiltPayloadExecutedBlock {
        recovered_block: Arc::new(recovered_block),
        execution_output,
        hashed_state: Arc::new(outcome.hashed_state),
        trie_updates: Arc::new(outcome.trie_updates),
    })
}

/// Poll the tree's view (events + `best_block_number` + in-memory head) until block `bn` with
/// hash `expected_hash` is canonical, or a bounded timeout elapses.
pub(crate) async fn wait_for_head<P>(
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
        // committed to DB?
        if let Ok(best) = provider.best_block_number() {
            if best >= bn {
                return true;
            }
        }
        // canonical in memory (not yet persisted)?
        let head = canonical.get_canonical_head();
        if head.header().number >= bn && head.hash() == expected_hash {
            return true;
        }
        // absorb observed events (non-blocking)
        while let Ok((n, h)) = obs_rx.try_recv() {
            if n == bn && h == expected_hash {
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
/// **A deep in-memory buffer SILENTLY CORRUPTS THE CHAIN here — do not widen these.** Running
/// production far ahead of persistence (e.g. 128/128/1024) ~doubled timing-run throughput, but a
/// mainnet re-validation (2026-07-02) proved it wrong: `produce()` reads parent state via
/// `state_by_block_hash(parent)`, which snapshots the tree's in-memory overlay + the DB
/// *non-atomically*. Once the persisted DB tip runs ahead of the in-memory anchor (only possible
/// with a deep buffer), that read races the tree's async persistence thread (commit +
/// `remove_persisted_blocks` + `remove_before`) and intermittently returns a *stale/empty* account.
/// Two symptoms: (a) the stale read trips validation → `NonceTooHigh`/"lack of funds" crash; (b)
/// WORSE, the stale read mis-executes WITHOUT erroring → a wrong-but-internally-consistent state
/// root is baked in and the chain diverges from canonical (observed: roots matched to block
/// 22212000 then MISMATCHED by 22214000, ~170 blocks before any crash). A retry cannot fix (b): the
/// corruption is permanent once produced. At the shallow default below the anchor tracks the DB tip
/// (`LatestStateProvider`, no gap), the racy path is never taken, and the sync produced 32,624
/// blocks with 10/10 roots + hashes == canonical. Recovering throughput requires a state-read path
/// that is NOT raced by persistence (e.g. thread the just-executed parent post-state forward in the
/// driver instead of reading it back through the provider) — NOT a bigger buffer.
#[derive(Debug, Clone, Copy)]
pub struct ArbEngineTuning {
    /// Persist once the canonical tip is this many blocks ahead of the last persisted block.
    pub persistence_threshold: u64,
    /// Keep this many of the most-recent blocks in memory (target size of the unpersisted buffer).
    pub memory_block_buffer_target: u64,
    /// Hard backpressure: stall block production once this many blocks are unpersisted.
    pub persistence_backpressure_threshold: u64,
    /// EXPERIMENTAL toggle (`--ring-overlay`): read parent state for `produce()` from a driver-held
    /// ring of just-executed blocks overlaid on the IMMUNE `LatestStateProvider` (MDBX-only, single
    /// pinned tx), instead of `provider.state_by_block_hash(parent)` (which races async persistence
    /// at depth). When true, the torn-read hazard is eliminated by construction, so deep buffers
    /// become safe. Default false = legacy path. See `arb-reth/docs/blockprod-decouple-spec.md` (#3).
    pub ring_overlay: bool,
}

impl Default for ArbEngineTuning {
    fn default() -> Self {
        // Shallow buffer = the ONLY config proven parity-correct on mainnet (see the type doc):
        // deep buffers make produce()'s state_by_block_hash(parent) overlay read go stale and
        // crash with "lack of funds". Matches reth's stock defaults. ~80 blk/s until the overlay
        // read-path staleness is fixed; do NOT widen these without re-running the mainnet parity
        // check (arb-check-progress.sh must show 10/10 == canonical).
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
            ring_overlay: false,
        }
    }

    /// Build a reth [`TreeConfig`] from these knobs (all other fields keep reth defaults).
    pub fn to_tree_config(self) -> TreeConfig {
        TreeConfig::default()
            .with_persistence_threshold(self.persistence_threshold)
            .with_memory_block_buffer_target(self.memory_block_buffer_target)
            .with_persistence_backpressure_threshold(self.persistence_backpressure_threshold)
    }
}

/// Tier-1 engine-tree driver for [`crate::ArbNode`].
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
    /// `--ring-overlay` mode: read parent state from `ring` overlaid on the immune latest provider.
    ring_overlay: bool,
    /// Just-executed blocks not yet known to be persisted, oldest→newest, contiguous down to the
    /// persisted tip. Only maintained when `ring_overlay` is on; drained as persistence advances.
    ring: VecDeque<ExecutedBlock<ArbPrimitives>>,
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
    pub fn spawn(
        factory: ProviderFactory<N>,
        provider: BlockchainProvider<N>,
        evm_config: ArbEvmConfig,
        chain_id: u64,
        genesis_tip: SealedHeader<Header>,
        canonical: CanonicalInMemoryState<ArbPrimitives>,
        runtime: Runtime,
        tuning: ArbEngineTuning,
    ) -> eyre::Result<Self> {
        // ---- persistence service (real MDBX writer, noop pruner) ----
        let (_finished_exex_height_tx, finished_exex_height_rx) =
            tokio::sync::watch::channel(reth_exex_types::FinishedExExHeight::NoExExs);
        let pruner =
            Pruner::new_with_factory(factory.clone(), vec![], 5, 0, None, finished_exex_height_rx);
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
        let tree_config = tuning.to_tree_config();
        tracing::info!(
            target: "arb-reth::engine",
            persistence_threshold = tuning.persistence_threshold,
            memory_block_buffer_target = tuning.memory_block_buffer_target,
            persistence_backpressure_threshold = tuning.persistence_backpressure_threshold,
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

        Ok(Self {
            provider,
            evm_config,
            chain_id,
            tip: genesis_tip,
            to_tree,
            canonical,
            obs_rx,
            ring_overlay: tuning.ring_overlay,
            ring: VecDeque::new(),
        })
    }

    /// Build the two parent-state providers and produce one block. Selects the immune ring overlay
    /// (`--ring-overlay`) or the legacy `state_by_block_hash(parent)` path.
    fn build_block(
        &self,
        msg: &BroadcastFeedMessage,
    ) -> eyre::Result<BuiltPayloadExecutedBlock<ArbPrimitives>> {
        if self.ring_overlay {
            // Pin ONE MDBX RO tx: read the persisted tip and the immune latest state from the SAME
            // tx so the state-root path's historical anchor sits exactly at `persisted_tip` (the
            // ring adds `(persisted_tip, parent]` on top with no double-apply). `latest()` reads
            // MDBX HashedAccounts only (no RocksDB) → not raced by async persistence.
            let db_ro = self
                .provider
                .database_provider_ro()
                .wrap_err("database_provider_ro failed")?;
            let persisted_tip = db_ro.best_block_number().wrap_err("best_block_number failed")?;
            let parent_num = self.tip.number;

            // Ring blocks strictly above the persisted tip, newest→oldest (MemoryOverlay order).
            let ring_vec: Vec<ExecutedBlock<ArbPrimitives>> = self
                .ring
                .iter()
                .rev()
                .take_while(|b| b.recovered_block.header().number > persisted_tip)
                .cloned()
                .collect();

            // Tripwire: the overlay MUST cover the whole gap (persisted_tip, parent]. A short ring
            // means a read would fall through to `latest` for a block the ring should carry → stale.
            let expected = parent_num.saturating_sub(persisted_tip);
            if ring_vec.len() as u64 != expected {
                return Err(eyre!(
                    "ring overlay gap: parent={parent_num} persisted_tip={persisted_tip} expected \
                     {expected} in-ring blocks, have {}",
                    ring_vec.len()
                ));
            }

            // `LatestStateProviderRef` over the pinned RO tx = the immune anchor (equivalent to the
            // inherent `db_ro.latest()`, but callable under the generic Provider bounds).
            let exec_hist: Box<dyn StateProvider + '_> =
                Box::new(LatestStateProviderRef::new(&db_ro));
            let trie_hist: Box<dyn StateProvider + '_> =
                Box::new(LatestStateProviderRef::new(&db_ro));
            let exec_sp = MemoryOverlayStateProviderRef::new(exec_hist, ring_vec.clone()).boxed();
            let trie_sp = MemoryOverlayStateProviderRef::new(trie_hist, ring_vec).boxed();
            produce(&self.evm_config, self.chain_id, &self.tip, msg, exec_sp, trie_sp)
        } else {
            let exec_sp = self
                .provider
                .state_by_block_hash(self.tip.hash())
                .wrap_err("state_by_block_hash (exec) failed")?;
            let trie_sp = self
                .provider
                .state_by_block_hash(self.tip.hash())
                .wrap_err("state_by_block_hash (trie) failed")?;
            produce(&self.evm_config, self.chain_id, &self.tip, msg, exec_sp, trie_sp)
        }
    }

    /// Push the just-produced block onto the ring, then drop entries the DB has now persisted.
    /// No-op unless `ring_overlay` is on.
    fn maintain_ring(&mut self, built: &BuiltPayloadExecutedBlock<ArbPrimitives>) {
        if !self.ring_overlay {
            return;
        }
        // ComputedTrieData wants SORTED hashed-state + trie-updates (MemoryOverlay's `trie_input`
        // extends from sorted). `built` carries the unsorted forms; sort clones here.
        let hashed = Arc::new((*built.hashed_state).clone().into_sorted());
        let trie = Arc::new((*built.trie_updates).clone().into_sorted());
        self.ring.push_back(ExecutedBlock::new(
            built.recovered_block.clone(),
            built.execution_output.clone(),
            ComputedTrieData::new(hashed, trie),
        ));
        // Prune everything at/below the persisted (on-disk) tip. Use the RO provider's tip, NOT
        // `BlockchainProvider::best_block_number` (that's the in-memory canonical tip).
        if let Ok(db_ro) = self.provider.database_provider_ro() {
            if let Ok(persisted) = db_ro.best_block_number() {
                while self
                    .ring
                    .front()
                    .is_some_and(|b| b.recovered_block.header().number <= persisted)
                {
                    self.ring.pop_front();
                }
            }
        }
    }

    /// Produce, insert, and canonicalize one block from a feed message.
    ///
    /// Waits only for fast in-memory canonicalization (the tree persists to MDBX asynchronously).
    /// Returns the hash of the newly-produced block, which becomes the new tip.
    pub async fn advance(&mut self, msg: &BroadcastFeedMessage) -> eyre::Result<B256> {
        use std::time::Instant;
        let __t0 = Instant::now();

        // Legacy path (`ring_overlay=false`): `produce` reads parent state via
        // `provider.state_by_block_hash(parent)`, which snapshots the tree overlay + the DB
        // non-atomically — SAFE only at a shallow buffer (see `ArbEngineTuning`). Do NOT paper over
        // deep-buffer failures with a retry: some torn reads mis-execute WITHOUT erroring, baking a
        // wrong root into the chain. The `--ring-overlay` path reads from the driver-held ring over
        // the immune latest provider and is not raced by persistence.
        let built = self.build_block(msg)?;
        let new_hash = built.recovered_block.hash();
        let new_header = built.recovered_block.header().clone();
        let new_number = new_header.number;
        let __us_produce = __t0.elapsed().as_micros();

        // Ring bookkeeping (no-op unless `--ring-overlay`): record this block as an unpersisted
        // parent for the next `build_block`, and prune what the DB has since persisted. Must run
        // before `built` is moved into the InsertExecutedBlock message below.
        self.maintain_ring(&built);

        // Feed the executed block to the tree (no re-execution).
        let __t = Instant::now();
        self.to_tree
            .send(FromEngine::Request(EngineApiRequest::InsertExecutedBlock(
                built,
            )))
            .map_err(|e| eyre!("send InsertExecutedBlock: {e}"))?;
        let __us_insert = __t.elapsed().as_micros();

        // Drive canonicalization via ForkchoiceUpdated (head = new block).
        let __t = Instant::now();
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
        let __us_fcu = __t.elapsed().as_micros();

        // Wait for the tree to actually canonicalize the block (bounded).
        let __t = Instant::now();
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
        let __us_wait = __t.elapsed().as_micros();

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
            us_produce = __us_produce,
            us_insert = __us_insert,
            us_fcu = __us_fcu,
            us_wait = __us_wait,
            us_total = __t0.elapsed().as_micros(),
            "advance timing",
        );

        self.tip = SealedHeader::new(new_header, new_hash);
        Ok(new_hash)
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
            if let Ok(best) = self.provider.best_block_number() {
                if best >= target {
                    return;
                }
            }
            if std::time::Instant::now() >= deadline {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }
}
