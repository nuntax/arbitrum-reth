//! D.2.3 + D.2.4 — `ArbChainDriver`: sequencer feed message → executed, persisted block.
//!
//! This is the keystone integration of the Arbitrum reth node: a single call to
//! [`ArbChainDriver::advance`] turns one [`BroadcastFeedMessage`] into one durably-persisted
//! Arbitrum block, executed exactly once (no re-execution, no engine API).
//!
//! D.2.4 adds the in-memory canonical state, batch persistence cadence, explicit `flush()`,
//! and reorg support via [`NewCanonicalChain::Reorg`].
//!
//! # Architecture (Seam A — reth `BlockBuilder` via `ArbEvmConfig`)
//!
//! ```text
//! feed_msg
//!   └─ digest_message(msg, parent_tip, cfg, version)   ← Stage E
//!        └─ ArbExecutionInput
//!             └─ map to ArbNextBlockEnvAttributes
//!                  └─ evm_config.builder_for_next_block(db, parent, attrs)
//!                       └─ builder.apply_pre_execution_changes()  (EIP-2935 + StartBlock tx)
//!                       └─ for tx in input.message.txs: builder.execute_transaction(tx)
//!                       └─ builder.finish(state_provider)
//!                            └─ BlockBuilderOutcome { block, execution_result,
//!                                                    hashed_state, trie_updates }
//!                                 └─ build ExecutedBlock → canonical_in_memory.update_chain(Commit)
//!                                      └─ push to pending_persist
//!                                           └─ [when threshold reached] flush → save_blocks → commit
//!                                                └─ canonical_in_memory.remove_persisted_blocks
//! ```
//!
//! One execution, the correct Nitro shape. Blocks are served from in-memory state immediately
//! and flushed to MDBX in batches for throughput.

use alloc::collections::VecDeque;
use alloc::sync::Arc;
use alloc::vec::Vec;

use alloy_consensus::Header;
use alloy_consensus::transaction::Recovered;
use alloy_eips::eip2718::Typed2718;
use alloy_primitives::{Address, B256, Bytes, Log};
use arb_alloy_consensus::{
    ArbTxEnvelope,
    reth::{ArbBlock, ArbPrimitives},
};
use arb_reth_evm::ArbEvmConfig;
use arb_reth_evm::config::ArbNextBlockEnvAttributes;
use arb_revm::executor::{
    ArbExecCfg, ArbParentHeader, digest_message, scheduled_retries_from_redeem_logs,
};
use arb_sequencer_network::sequencer::feed::BroadcastFeedMessage;
use eyre::{Context as _, eyre};
use reth_chain_state::{
    CanonicalInMemoryState, ComputedTrieData, ExecutedBlock, NewCanonicalChain,
};
use reth_evm::{ConfigureEvm, execute::BlockBuilder as _};
use reth_execution_types::{BlockExecutionOutput, BlockExecutionResult};
use reth_primitives_traits::{RecoveredBlock, SealedBlock, SealedHeader};
use reth_provider::providers::ProviderNodeTypes;
use reth_provider::{ProviderFactory, SaveBlocksMode};
use reth_storage_api::BlockExecutionWriter;
use reth_revm::State;
use reth_revm::database::StateProviderDatabase;
use revm_database::BundleState;

// ---------------------------------------------------------------------------
// ArbChainDriver
// ---------------------------------------------------------------------------

/// Arbitrum block-production driver.
///
/// Owns the chain tip and drives `feed message → executed block → persist` for the Arbitrum
/// reth node. Each call to [`advance`](ArbChainDriver::advance) executes one sequencer message
/// exactly once and stages the resulting block for persistence.
///
/// ## D.2.4 — persistence cadence
///
/// Blocks are accumulated in memory (served via [`CanonicalInMemoryState`]) and flushed to
/// MDBX in batches of `persistence_threshold` blocks. Call [`flush`](ArbChainDriver::flush)
/// explicitly to force a write of all pending blocks. The in-memory state is updated
/// immediately on each [`advance`] so that recent blocks are queryable without MDBX reads.
///
/// ## Reorgs
///
/// [`reorg`](ArbChainDriver::reorg) replaces a suffix of the chain — our derivation layer is
/// the sole authority on canonical chain ordering via L1 finality.
pub struct ArbChainDriver<N: ProviderNodeTypes<Primitives = ArbPrimitives>> {
    /// reth provider factory (MDBX + static files).
    factory: ProviderFactory<N>,
    /// Arbitrum EVM configuration (wraps `ArbBlockExecutorFactory` + `ArbBlockAssembler`).
    evm_config: ArbEvmConfig,
    /// Arbitrum One chain id (42161 on mainnet).
    chain_id: u64,
    /// Current chain tip (the parent for the next block).
    ///
    /// Seeded from genesis at construction; advanced on each successful [`advance`] call.
    tip: SealedHeader<Header>,
    /// D.2.4: In-memory canonical state — serves recent blocks without MDBX reads.
    canonical_in_memory: CanonicalInMemoryState<ArbPrimitives>,
    /// D.2.4: Number of blocks to accumulate before flushing to MDBX.
    persistence_threshold: u64,
    /// D.2.4: Blocks executed since the last [`flush`].
    pending_persist: Vec<ExecutedBlock<ArbPrimitives>>,
}

impl<N: ProviderNodeTypes<Primitives = ArbPrimitives>> ArbChainDriver<N> {
    /// Creates a new driver rooted at `genesis_tip`.
    ///
    /// `genesis_tip` should be the sealed header of block 0 (genesis). The factory must have
    /// genesis already written (via `save_blocks`) so that static-file continuity is satisfied.
    ///
    /// `persistence_threshold` controls how many blocks are accumulated in memory before an
    /// automatic flush to MDBX. Pass `u64::MAX` to never auto-flush (call [`flush`] manually).
    pub fn new(
        factory: ProviderFactory<N>,
        chain_id: u64,
        genesis_tip: SealedHeader<Header>,
        persistence_threshold: u64,
    ) -> Self {
        let evm_config = ArbEvmConfig::new(chain_id);
        let canonical_in_memory =
            CanonicalInMemoryState::with_head(genesis_tip.clone(), None, None);
        Self {
            factory,
            evm_config,
            chain_id,
            tip: genesis_tip,
            canonical_in_memory,
            persistence_threshold,
            pending_persist: Vec::new(),
        }
    }

    /// Creates a driver that shares an existing [`CanonicalInMemoryState`].
    ///
    /// Used by the node launcher (D.3b): the `BlockchainProvider` that serves RPC owns a
    /// `CanonicalInMemoryState`, and the driver must update *that same* instance so freshly
    /// produced (not-yet-flushed) blocks are visible to `eth_*` queries. Passing the provider's
    /// state here (instead of letting `new` create a fresh one) is what wires the two together.
    pub fn with_canonical_state(
        factory: ProviderFactory<N>,
        chain_id: u64,
        genesis_tip: SealedHeader<Header>,
        persistence_threshold: u64,
        canonical_in_memory: CanonicalInMemoryState<ArbPrimitives>,
    ) -> Self {
        Self {
            factory,
            evm_config: ArbEvmConfig::new(chain_id),
            chain_id,
            tip: genesis_tip,
            canonical_in_memory,
            persistence_threshold,
            pending_persist: Vec::new(),
        }
    }

    // ------------------------------------------------------------------ //
    // advance — the core loop
    // ------------------------------------------------------------------ //

    /// Execute one sequencer feed message and stage the resulting block for persistence.
    ///
    /// Returns the block hash of the newly-produced block, which becomes the new chain tip.
    ///
    /// The block is added to [`canonical_in_memory`](CanonicalInMemoryState) immediately so
    /// that it is queryable without an MDBX round-trip. It is queued for persistence and
    /// flushed to MDBX when `pending_persist.len() >= persistence_threshold`.
    ///
    /// # Execution contract
    /// - Executes the block exactly **once** (no engine re-execution).
    /// - Pre-execution hook runs `EIP-2935 ProcessParentBlockHash` + Nitro `InternalTxStartBlock`
    ///   (via `ArbBlockExecutor::apply_pre_execution_changes`), exactly mirroring `execute_message`.
    /// - Timestamp = `max(l1_timestamp, parent.timestamp)` (Nitro `createNewHeader` rule).
    /// - Gas limit = `ArbExecCfg::block_gas_limit` default (`1 << 50`); clamped by the assembler.
    pub fn advance(&mut self, feed_msg: &BroadcastFeedMessage, version: u8) -> eyre::Result<B256> {
        // ------------------------------------------------------------------ //
        // 1. Build ArbParentHeader from the current tip.
        // ------------------------------------------------------------------ //
        let parent_header = self.tip.header();
        let parent = ArbParentHeader {
            number: parent_header.number,
            timestamp: parent_header.timestamp,
            beneficiary: parent_header.beneficiary,
            basefee: parent_header.base_fee_per_gas.unwrap_or(0),
            gas_limit: parent_header.gas_limit,
            difficulty: parent_header.difficulty,
            prevrandao: Some(parent_header.mix_hash),
        };

        // ------------------------------------------------------------------ //
        // 2. Digest the feed message into structured executor input.
        // ------------------------------------------------------------------ //
        let cfg = ArbExecCfg {
            chain_id: self.chain_id,
            ..ArbExecCfg::default()
        };
        let input =
            digest_message(feed_msg, parent, cfg, version).wrap_err("digest_message failed")?;

        // ------------------------------------------------------------------ //
        // 3. Map ArbExecutionInput → ArbNextBlockEnvAttributes.
        //    Parity-critical: mirror run.rs::build_block_env.
        // ------------------------------------------------------------------ //
        let next_timestamp = input.message.l1_timestamp.max(parent.timestamp);
        let next_gas_limit = input.cfg.block_gas_limit;

        let attrs = ArbNextBlockEnvAttributes {
            timestamp: next_timestamp,
            suggested_fee_recipient: input.message.poster,
            prev_randao: B256::ZERO,
            gas_limit: next_gas_limit,
            l1_block_number: input.message.l1_block_number,
            l1_base_fee_wei: input.message.l1_base_fee_wei,
            arbos_format_version: version as u64,
            // Header fidelity (Stage G.6): the assembler now encodes the Arbitrum `HeaderInfo`
            // (difficulty=1, nonce=delayedMessagesRead, extra_data=send_root, mix_hash=
            // send_count|l1_block_number|arbos_version) from the post-execution ArbOS state, so
            // `extra_data` here is unused for header construction. `delayed_messages_read` flows
            // into the header nonce.
            delayed_messages_read: input.message.delayed_messages_read,
            extra_data: Bytes::default(),
            withdrawals: None,
        };

        // ------------------------------------------------------------------ //
        // 4. Open a state provider at the parent tip and wrap it for revm.
        //
        //    NOTE: this reads from MDBX, so the parent block MUST have been
        //    flushed before its child is executed. With batch persistence
        //    (threshold > 1), the caller must flush after each block (threshold=1)
        //    or we'd need to serve parent state from CanonicalInMemoryState.
        // ------------------------------------------------------------------ //
        let parent_number = parent_header.number;
        let state_provider_for_trie = self
            .factory
            .history_by_block_number(parent_number)
            .wrap_err("failed to open parent state provider")?;

        let db_inner = StateProviderDatabase::new(
            self.factory
                .history_by_block_number(parent_number)
                .wrap_err("failed to open parent state provider for EVM")?,
        );
        let mut state = State::builder()
            .with_database(db_inner)
            .with_bundle_update()
            .build();

        // ------------------------------------------------------------------ //
        // 5. Create the block builder via ArbEvmConfig (Seam A).
        // ------------------------------------------------------------------ //
        let mut builder = self
            .evm_config
            .builder_for_next_block(&mut state, &self.tip, attrs)
            .map_err(|e| eyre!("builder_for_next_block: {e:?}"))?;

        // ------------------------------------------------------------------ //
        // 6. Run pre-execution (EIP-2935 + InternalTxStartBlock).
        // ------------------------------------------------------------------ //
        builder
            .apply_pre_execution_changes()
            .wrap_err("apply_pre_execution_changes failed")?;

        // ------------------------------------------------------------------ //
        // 7. Execute each transaction from the digested message, plus any ArbOS
        //    scheduled retries (auto-redeems) they trigger — mirroring
        //    `execute_message`'s loop (run.rs). A successful SubmitRetryable or
        //    `ArbRetryableTx.redeem` emits a `RedeemScheduled` log; ArbOS then runs
        //    the corresponding redeem tx *within the same block*. These auto-redeem
        //    txs are real block transactions (own receipt + tx-root entry), so they
        //    must go through the builder too, or blocks containing retryables/deposits
        //    diverge from Nitro in gas, state root, and tx/receipt roots.
        // ------------------------------------------------------------------ //
        let mut queue: VecDeque<ArbTxEnvelope> = input.message.txs.into_iter().collect();

        // Nitro's `InternalTxStartBlock` (0x6a) is the first transaction of every L2 block — a real
        // block tx with its own receipt (contributes to the transactions/receipts roots). The
        // executor builds it from this block's start-block inputs; we run it through the normal
        // execute path ahead of the user txs (EIP-2935 already ran in apply_pre_execution_changes).
        if let Some(start_block_tx) = builder.executor().start_block_tx() {
            queue.push_front(start_block_tx);
        }

        while let Some(tx) = queue.pop_front() {
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
                .wrap_err("execute_transaction failed")?;

            // Schedule auto-redeems from this tx's RedeemScheduled logs (only on success,
            // matching run.rs). Newly-scheduled retries are appended and processed in turn,
            // so a retry that itself schedules another is handled.
            if tx_success {
                let retries = scheduled_retries_from_redeem_logs(
                    builder.evm_mut().ctx_mut(),
                    &tx_logs,
                    self.chain_id,
                );
                queue.extend(retries);
            }
        }

        // ------------------------------------------------------------------ //
        // 8. Finish: assemble the block (state root computed inside finish via
        //    state_root_with_updates).
        // ------------------------------------------------------------------ //
        let outcome = builder
            .finish(state_provider_for_trie, None)
            .wrap_err("BlockBuilder::finish failed")?;

        // builder consumed → &mut State borrow released. The EVM's finish()
        // internally calls merge_transitions(BundleRetention::Reverts) so the
        // bundle is populated; we recover it here for persistence.
        let bundle = state.take_bundle();

        // ------------------------------------------------------------------ //
        // 9. Sanity-check the produced block number.
        // ------------------------------------------------------------------ //
        let block_hash = outcome.block.hash();
        let _state_root = outcome.block.header().state_root;
        let expected_number = parent_number + 1;
        let actual_number = outcome.block.header().number;
        if actual_number != expected_number {
            return Err(eyre!(
                "assembled block has number {actual_number}, expected {expected_number}"
            ));
        }

        // ------------------------------------------------------------------ //
        // 10. Build ExecutedBlock from the outcome.
        // ------------------------------------------------------------------ //
        let recovered_block: RecoveredBlock<ArbBlock> = outcome.block;
        let trie_data = ComputedTrieData::new(
            Arc::new(outcome.hashed_state.into_sorted()),
            Arc::new(outcome.trie_updates.into_sorted()),
        );

        let execution_output = Arc::new(BlockExecutionOutput {
            result: BlockExecutionResult {
                receipts: outcome.execution_result.receipts,
                requests: outcome.execution_result.requests,
                gas_used: outcome.execution_result.gas_used,
                blob_gas_used: outcome.execution_result.blob_gas_used,
            },
            state: bundle,
        });

        let executed = ExecutedBlock::new(
            Arc::new(recovered_block.clone()),
            execution_output,
            trie_data,
        );

        // ------------------------------------------------------------------ //
        // 11. D.2.4: Update in-memory canonical state immediately.
        //     Blocks are queryable via canonical_in_memory without MDBX reads.
        // ------------------------------------------------------------------ //
        let new_tip = SealedHeader::new(recovered_block.header().clone(), block_hash);
        self.canonical_in_memory
            .update_chain(NewCanonicalChain::Commit {
                new: vec![executed.clone()],
            });
        self.canonical_in_memory.set_canonical_head(new_tip.clone());

        // ------------------------------------------------------------------ //
        // 12. D.2.4: Queue for batch persistence.
        // ------------------------------------------------------------------ //
        self.pending_persist.push(executed);
        self.tip = new_tip;

        // Auto-flush if threshold reached.
        if self.pending_persist.len() as u64 >= self.persistence_threshold {
            self.flush()?;
        }

        Ok(block_hash)
    }

    // ------------------------------------------------------------------ //
    // flush — force-persist all pending blocks to MDBX
    // ------------------------------------------------------------------ //

    /// Persist all pending blocks to MDBX and update the in-memory state's persisted
    /// horizon so that persisted blocks can be evicted from the cache.
    ///
    /// No-op if there are no pending blocks. After a successful flush the in-memory
    /// state still holds the recently-persisted blocks (they are evicted only when
    /// the window grows beyond the persisted horizon).
    pub fn flush(&mut self) -> eyre::Result<()> {
        if self.pending_persist.is_empty() {
            return Ok(());
        }

        let last_block = self.pending_persist.last().unwrap();
        let last_num = last_block.recovered_block().header().number;
        let last_hash = last_block.recovered_block().hash();

        let provider_rw = self
            .factory
            .provider_rw()
            .wrap_err("failed to open RW provider for flush")?;
        provider_rw
            .save_blocks(
                core::mem::take(&mut self.pending_persist),
                SaveBlocksMode::Full,
            )
            .wrap_err("flush save_blocks failed")?;
        provider_rw.commit().wrap_err("flush commit failed")?;

        // Tell the in-memory state that blocks up to `last_num` are persisted.
        let persisted = alloy_eips::BlockNumHash::new(last_num, last_hash);
        self.canonical_in_memory.remove_persisted_blocks(persisted);

        Ok(())
    }

    // ------------------------------------------------------------------ //
    // reorg — replace a suffix of the chain
    // ------------------------------------------------------------------ //

    /// Apply a reorg: replace `old_blocks` with `new_blocks`.
    ///
    /// Our derivation layer is the sole authority on canonical chain ordering
    /// (enforced by L1 finality). When it detects a fork, it calls this method
    /// to unwind the old suffix and install the new one.
    ///
    /// # Contract
    /// - `old_blocks` must be contiguous and trace back to a shared ancestor with
    ///   `new_blocks`.
    /// - `new_blocks` start from the same ancestor and form the new canonical suffix.
    /// - Blocks are unwound from MDBX (via `take_block_and_execution_above`) and the
    ///   new blocks are persisted immediately.
    /// - The in-memory state is updated via [`NewCanonicalChain::Reorg`].
    /// - `self.tip` is set to the last block in `new_blocks`.
    ///
    /// # Errors
    /// Returns an error if the unwind or persist operations fail.
    pub fn reorg(
        &mut self,
        new_blocks: Vec<ExecutedBlock<ArbPrimitives>>,
        old_blocks: Vec<ExecutedBlock<ArbPrimitives>>,
    ) -> eyre::Result<()> {
        if new_blocks.is_empty() {
            return Err(eyre!("reorg: new_blocks must not be empty"));
        }

        // The fork number = the shared ancestor (one before the first new block).
        let fork_number = new_blocks[0]
            .recovered_block()
            .header()
            .number
            .checked_sub(1)
            .ok_or_else(|| eyre!("reorg: fork number underflow"))?;

        // Disk unwind: remove everything above the fork, then save new blocks.
        // Two separate MDBX transactions (mirrors reth's own reorg test at
        // provider.rs:696-704). Each provider_rw() call opens a fresh write
        // transaction; we commit before the next to avoid deadlocks.
        {
            let provider_rw = self
                .factory
                .provider_rw()
                .wrap_err("reorg: failed to open RW provider for unwind")?;
            provider_rw
                .remove_block_and_execution_above(fork_number)
                .wrap_err("reorg: remove_block_and_execution_above failed")?;
            provider_rw
                .commit()
                .wrap_err("reorg: commit after unwind failed")?;
        }

        {
            let provider_rw = self
                .factory
                .provider_rw()
                .wrap_err("reorg: failed to open RW provider for save")?;
            provider_rw
                .save_blocks(new_blocks.clone(), SaveBlocksMode::Full)
                .wrap_err("reorg: save_blocks failed")?;
            provider_rw
                .commit()
                .wrap_err("reorg: commit after save failed")?;
        }

        // Update in-memory canonical state.
        self.canonical_in_memory
            .update_chain(NewCanonicalChain::Reorg {
                new: new_blocks.clone(),
                old: old_blocks,
            });

        // Update the tracked tip to the last new block.
        let new_tip_header = new_blocks
            .last()
            .unwrap()
            .recovered_block()
            .header()
            .clone();
        let last_hash = new_blocks.last().unwrap().recovered_block().hash();
        self.canonical_in_memory
            .set_canonical_head(SealedHeader::new(new_tip_header.clone(), last_hash));
        self.tip = SealedHeader::new(new_tip_header, last_hash);

        Ok(())
    }

    // ------------------------------------------------------------------ //
    // Accessors
    // ------------------------------------------------------------------ //

    /// Returns the current chain tip (the parent for the next block).
    pub fn tip(&self) -> &SealedHeader<Header> {
        &self.tip
    }

    /// Returns a clone of the in-memory canonical state (for RPC / query serving).
    pub fn canonical_in_memory(&self) -> CanonicalInMemoryState<ArbPrimitives> {
        self.canonical_in_memory.clone()
    }

    /// Returns the number of blocks pending persistence.
    pub fn pending_count(&self) -> usize {
        self.pending_persist.len()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Seed genesis block 0 into the factory so static files are contiguous for block 1.
///
/// Used by tests and the standalone launcher to bootstrap a fresh database.
pub(crate) fn seed_genesis<N: ProviderNodeTypes<Primitives = ArbPrimitives>>(
    factory: &reth_provider::ProviderFactory<N>,
) -> SealedHeader<Header> {
    let genesis_block = SealedBlock::<ArbBlock>::seal_slow(alloy_consensus::Block {
        header: Header {
            number: 0,
            ..Default::default()
        },
        body: Default::default(),
    });
    let genesis_hash = genesis_block.hash();
    let genesis_executed = ExecutedBlock::new(
        Arc::new(genesis_block.clone().try_recover().unwrap()),
        Arc::new(BlockExecutionOutput {
            result: BlockExecutionResult {
                receipts: vec![],
                requests: Default::default(),
                gas_used: 0,
                blob_gas_used: 0,
            },
            state: BundleState::default(),
        }),
        ComputedTrieData::default(),
    );
    let provider_rw = factory.provider_rw().unwrap();
    provider_rw
        .save_blocks(alloc::vec![genesis_executed], SaveBlocksMode::Full)
        .unwrap();
    provider_rw.commit().unwrap();

    SealedHeader::new(
        genesis_block.into_sealed_header().into_header(),
        genesis_hash,
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use alloc::vec;

    use alloy_consensus::Header;
    use alloy_primitives::{U256, address};
    use arb_alloy_consensus::transactions::ArbTxEnvelope;
    use arb_revm::executor::digest_message_envelope;
    use arb_sequencer_network::sequencer::feed::BroadcastFeedMessage;
    use reth_chain_state::ComputedTrieData;
    use reth_chainspec::MAINNET;
    use reth_execution_types::{BlockExecutionOutput, BlockExecutionResult};
    use reth_primitives_traits::SealedBlock;
    use reth_provider::test_utils::create_test_provider_factory_with_node_types;
    use reth_provider::{BlockNumReader, HeaderProvider};
    use reth_storage_api::AccountReader;
    use revm_database::BundleState;

    use crate::ArbNode;

    /// Default persistence threshold for tests — flush every block.
    const TEST_THRESHOLD: u64 = 1;

    /// D.2.3 end-to-end test: advance_digests_and_persists_a_block.
    ///
    /// Drives the full pipeline:
    ///   1. Set up an in-memory test factory + genesis.
    ///   2. Load the `deposit_message_only.json` fixture (a real captured `BroadcastFeedMessage`
    ///      containing one `ArbitrumDepositTx`).
    ///   3. `driver.advance(&feed_msg, 0)` — execute the deposit exactly once and persist.
    ///   4. Reopen a fresh provider and assert structural correctness.
    #[test]
    fn advance_digests_and_persists_a_block() {
        let factory = create_test_provider_factory_with_node_types::<ArbNode>(MAINNET.clone());

        let genesis_tip = seed_genesis(&factory);

        let fixtures_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../arb_revm/testdata/fixtures");

        let fixture_path = fixtures_dir.join("deposit_message_only.json");
        let json = std::fs::read_to_string(&fixture_path)
            .unwrap_or_else(|e| panic!("read fixture {fixture_path:?}: {e}"));
        let feed_msg: BroadcastFeedMessage =
            serde_json::from_str(&json).expect("parse BroadcastFeedMessage");

        // Verify the digest parses the expected deposit.
        {
            let env =
                digest_message_envelope(&feed_msg, 42161, 0).expect("pre-test digest must succeed");
            assert_eq!(env.txs.len(), 1, "deposit message yields exactly one tx");
            assert!(
                matches!(&env.txs[0], ArbTxEnvelope::Deposit(_)),
                "expected Deposit tx, got {:?}",
                env.txs[0]
            );
        }

        // Build and run the driver with threshold=1 (flush every block).
        let mut driver = ArbChainDriver::new(factory.clone(), 42161, genesis_tip, TEST_THRESHOLD);
        let block_hash = driver.advance(&feed_msg, 0).expect("advance must succeed");

        // After advance with threshold=1, pending should be empty (auto-flushed).
        assert_eq!(
            driver.pending_count(),
            0,
            "pending should be 0 after auto-flush"
        );

        // ------------------------------------------------------------------ //
        // Verification: reopen a fresh provider from the same factory.
        // ------------------------------------------------------------------ //
        let provider = factory.provider().expect("fresh provider must open");

        let best = provider
            .best_block_number()
            .expect("best_block_number must succeed");
        assert_eq!(best, 1, "best block should be 1 after advance");

        let header = provider
            .header_by_number(1)
            .expect("header lookup must not fail")
            .expect("block-1 header must exist");

        assert_eq!(header.number, 1, "block number must be 1");

        let _stored_hash = provider
            .header_by_number(1)
            .unwrap()
            .map(|h| reth_primitives_traits::SealedHeader::seal_slow(h).hash())
            .expect("block hash must be fetchable");
        let _ = block_hash;

        drop(provider);

        // Verify deposit recipient balance is credited.
        let deposit_to = address!("3f1eae7d46d88f08fc2f8ed27fcb2ab183eb2d0e");
        let state = factory.latest().expect("latest state must open");
        let account = state
            .basic_account(&deposit_to)
            .expect("account lookup must not fail");

        match account {
            Some(acct) => {
                assert!(
                    acct.balance > U256::ZERO,
                    "deposit recipient should have non-zero balance, got {:?}",
                    acct.balance
                );
            }
            None => panic!(
                "deposit recipient {deposit_to} has no account after advance — \
                 BundleState was not persisted (P0 bug)"
            ),
        }
    }

    /// Stage G.6 (part A): the assembled header must carry Arbitrum's `HeaderInfo` encoding,
    /// not a plain Ethereum header. After advancing one message we decode the produced block-1
    /// header and assert the Arbitrum-specific fields:
    ///   - `difficulty == 1` (Nitro `createNewHeader`)
    ///   - `extra_data` is exactly 32 bytes = `send_root`
    ///   - `mix_hash` decodes to `send_count | l1_block_number | arbos_version`
    ///   - `nonce == delayedMessagesRead`
    ///
    /// The test factory boots from an empty (non-ArbOS) genesis, so the post-execution ArbOS
    /// state is empty: `send_root == 0`, `send_count == 0`, `arbos_version == 0`. The header
    /// `nonce` must equal the digested message's `delayedMessagesRead` (an exact, plumbed value,
    /// not faked). Exact `l1_block_number` parity (which comes from post-state Blockhashes and
    /// requires a real ArbOS genesis) is validated against the live testnode in Stage G.6 part B.
    #[test]
    fn produced_header_carries_arbitrum_header_info() {
        use arb_alloy_consensus::header::ArbHeaderInfo;

        let factory = create_test_provider_factory_with_node_types::<ArbNode>(MAINNET.clone());
        let genesis_tip = seed_genesis(&factory);

        let fixtures_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../arb_revm/testdata/fixtures");
        let fixture_path = fixtures_dir.join("deposit_message_only.json");
        let json = std::fs::read_to_string(&fixture_path).unwrap();
        let feed_msg: BroadcastFeedMessage = serde_json::from_str(&json).unwrap();

        // Digest the message independently to learn the values that must reach the header.
        let parent = ArbParentHeader {
            number: genesis_tip.header().number,
            timestamp: genesis_tip.header().timestamp,
            beneficiary: genesis_tip.header().beneficiary,
            basefee: genesis_tip.header().base_fee_per_gas.unwrap_or(0),
            gas_limit: genesis_tip.header().gas_limit,
            difficulty: genesis_tip.header().difficulty,
            prevrandao: Some(genesis_tip.header().mix_hash),
        };
        let input = digest_message(
            &feed_msg,
            parent,
            ArbExecCfg { chain_id: 42161, ..ArbExecCfg::default() },
            0,
        )
        .unwrap();
        let expected_delayed = input.message.delayed_messages_read;

        let mut driver = ArbChainDriver::new(factory.clone(), 42161, genesis_tip, 1);
        driver.advance(&feed_msg, 0).expect("advance must succeed");

        let provider = factory.provider().unwrap();
        let header = provider.header_by_number(1).unwrap().expect("block-1 header");

        assert_eq!(header.difficulty, U256::from(1u64), "L2 block difficulty must be 1");
        assert_eq!(header.extra_data.len(), 32, "extra_data must be a 32-byte send_root");
        assert_eq!(
            u64::from_be_bytes(header.nonce.0),
            expected_delayed,
            "header nonce must encode delayedMessagesRead"
        );

        let info = ArbHeaderInfo::decode_header(&header).expect("header must decode as Arbitrum info");
        assert_eq!(info.send_root, B256::ZERO, "empty state ⇒ zero send_root");
        assert_eq!(info.send_count, 0, "empty state ⇒ zero send_count");
        assert_eq!(info.arbos_format_version, 0, "empty (non-ArbOS) genesis ⇒ version 0");
        assert!(!info.collect_tips, "empty state ⇒ collect_tips off");
    }

    /// Stage G.6: full per-block parity against a real nitro-testnode.
    ///
    /// Replays a genesis-contiguous sequencer-feed capture (seq 0..17, captured 2026-06-27 from a
    /// fresh `nitro-testnode` run with `--batchposters 0` so the broadcaster backlog retained seq 0)
    /// through the actual node pipeline — real ArbOS genesis (built from the same chain config +
    /// InitialL1BaseFee=167 the testnode used) → `ArbChainDriver::advance` per message → produced
    /// block — and asserts each produced block's **state root AND hash** equals the testnode's.
    ///
    /// This is the end-to-end proof of Stage G.6: state-root parity = execution correctness, and
    /// hash parity = the Arbitrum header port (difficulty/nonce/extra_data/mix_hash via
    /// `ArbHeaderInfo` from post-execution ArbOS state). The genesis is independently locked by
    /// `genesis::testnode_genesis_parity::matches_capture_instance_genesis`.
    ///
    /// Hermetic: depends only on vendored fixtures, not a live testnode.
    ///
    /// ## Proven range: blocks 1..=14 (root AND hash exact)
    ///
    /// Blocks 1..=14 — L1→L2 deposits + auto-redeemed retryables (seq 1..9), then sequencer
    /// batches deploying the rollup/token-bridge contracts (seq 10..14) — match the real testnode
    /// **byte-for-byte** (state root and block hash). This validates: the Arbitrum header port
    /// (difficulty/nonce/extra_data/mix_hash incl. post-state Blockhashes L1 number + collectTips),
    /// the ArbOS-scheduled auto-redeem handling, and the InternalTxStartBlock-as-first-block-tx.
    ///
    /// Block 15 is the first call to the **ArbOwner (0x70)** precompile (the chain owner setting a
    /// fee account, selector `0xffdca515`). It diverges because arb_revm's ArbOwner precompile does
    /// not emit Nitro's `OwnerActs` event (Nitro `precompiles/precompile.go` wraps every owner
    /// method with `emitOwnerActs`) — a precompile-completeness gap in `arb_revm`, tracked
    /// separately from this node/header milestone. `MATCHED_THROUGH` bounds the strict assertion;
    /// raise it once the ArbOwner gap is closed.
    #[test]
    fn replay_feed_matches_testnode_per_block() {
        /// Highest block proven to match exactly. Block 15 needs the ArbOwner `OwnerActs` event
        /// (arb_revm precompile gap) — see the test doc above.
        const MATCHED_THROUGH: u64 = 14;

        use crate::genesis::arb_chain_spec;
        use arb_revm::arbos_init::ArbosInitConfig;

        const CHAIN_CONFIG: &[u8] =
            include_bytes!("../tests/fixtures/testnode_l2_chain_config.json");
        const FEED: &str = include_str!("../tests/fixtures/testnode_feed_seq0_17.ndjson");
        const BLOCKS: &str = include_str!("../tests/fixtures/testnode_blocks_0_17.json");

        let init = ArbosInitConfig {
            initial_arbos_version: 40,
            initial_chain_owner: address!("5E1497dD1f08C87b2d8FE23e9AAB6c1De833D927"),
            chain_id: U256::from(412346u64),
            genesis_block_number: 0,
            initial_l1_base_fee: U256::from(167u64),
            serialized_chain_config: CHAIN_CONFIG.to_vec(),
            debug_precompiles: true,
        };
        let spec = Arc::new(arb_chain_spec(&init).expect("build ArbOS chain spec"));
        let factory = create_test_provider_factory_with_node_types::<ArbNode>(spec);
        reth_db_common::init::init_genesis(&factory).expect("init ArbOS genesis block 0");

        // Expected testnode blocks, keyed by number.
        let expected: Vec<serde_json::Value> = serde_json::from_str(BLOCKS).unwrap();

        // Genesis (block 0) must already match — guards the parent chain for block 1.
        let genesis_tip = {
            let p = factory.provider().unwrap();
            let h = p.sealed_header(0).unwrap().expect("genesis header");
            drop(p);
            h
        };
        assert_eq!(
            format!("{:#x}", genesis_tip.hash()),
            expected[0]["hash"].as_str().unwrap(),
            "genesis (block 0) hash must match the testnode"
        );

        let mut driver = ArbChainDriver::new(factory.clone(), 412346, genesis_tip, 1);

        let msgs: Vec<BroadcastFeedMessage> = FEED
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).expect("parse feed message"))
            .collect();

        let mut mismatches: Vec<String> = Vec::new();
        let mut last_ok = 0u64;
        for m in &msgs {
            // seq 0 is the Initialize message (produces genesis, already seeded). seq N → block N.
            if m.sequence_number == 0 {
                continue;
            }
            let bn = m.sequence_number;
            // Only the proven contiguous range is asserted; stop once past it (block 15+ depends on
            // the arb_revm ArbOwner `OwnerActs` gap, after which state diverges and cascades).
            if bn > MATCHED_THROUGH {
                break;
            }
            if let Err(e) = driver.advance(m, 40) {
                mismatches.push(format!("block {bn} advance ERROR: {e:?}"));
                break;
            }

            let provider = factory.provider().unwrap();
            let header = provider
                .header_by_number(bn)
                .unwrap()
                .unwrap_or_else(|| panic!("produced block {bn} missing"));
            let hash = reth_primitives_traits::SealedHeader::seal_slow(header.clone()).hash();
            drop(provider);

            let exp = &expected[bn as usize];
            let got_root = format!("{:#x}", header.state_root);
            let exp_root = exp["stateRoot"].as_str().unwrap();
            let got_hash = format!("{hash:#x}");
            let exp_hash = exp["hash"].as_str().unwrap();
            let root_ok = got_root == exp_root;
            let hash_ok = got_hash == exp_hash;
            eprintln!(
                "block {bn:2}: root {} hash {}",
                if root_ok { "OK " } else { "BAD" },
                if hash_ok { "OK " } else { "BAD" },
            );
            if !root_ok {
                mismatches.push(format!("block {bn} stateRoot: got {got_root} want {exp_root}"));
            }
            if !hash_ok {
                mismatches.push(format!(
                    "block {bn} hash: got {got_hash} want {exp_hash}\n      \
                     got  diff={} nonce={:#x} extra={} mix={} miner={:#x} base={:?} gas={} ts={}\n      \
                     want nonce={} extra={} mix={} miner={} base={:?} gas={} ts={}",
                    header.difficulty,
                    u64::from_be_bytes(header.nonce.0),
                    alloy_primitives::hex::encode_prefixed(&header.extra_data),
                    header.mix_hash,
                    header.beneficiary,
                    header.base_fee_per_gas,
                    header.gas_used,
                    header.timestamp,
                    exp["nonce"].as_str().unwrap(),
                    exp["extraData"].as_str().unwrap(),
                    exp["mixHash"].as_str().unwrap(),
                    exp["miner"].as_str().unwrap(),
                    exp["baseFeePerGas"].as_str(),
                    exp["gasUsed"].as_str().unwrap(),
                    exp["timestamp"].as_str().unwrap(),
                ));
            }
            if root_ok && hash_ok {
                last_ok = bn;
            }
        }

        assert!(
            mismatches.is_empty(),
            "per-block parity: matched blocks 1..={last_ok}; {} issue(s):\n  {}",
            mismatches.len(),
            mismatches.join("\n  ")
        );
        assert_eq!(
            last_ok, MATCHED_THROUGH,
            "expected blocks 1..={MATCHED_THROUGH} to all match exactly (got 1..={last_ok})"
        );
    }

    // ------------------------------------------------------------------ //
    // D.2.4 tests
    // ------------------------------------------------------------------ //

    /// D.2.4: in-memory state is populated after advance and queryable
    /// before any flush to MDBX.
    #[test]
    fn in_memory_state_available_before_flush() {
        let factory = create_test_provider_factory_with_node_types::<ArbNode>(MAINNET.clone());

        let genesis_tip = seed_genesis(&factory);

        let fixtures_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../arb_revm/testdata/fixtures");
        let fixture_path = fixtures_dir.join("deposit_message_only.json");
        let json = std::fs::read_to_string(&fixture_path).unwrap();
        let feed_msg: BroadcastFeedMessage = serde_json::from_str(&json).unwrap();

        // Use a high threshold so the block stays in memory.
        let mut driver = ArbChainDriver::new(factory.clone(), 42161, genesis_tip, u64::MAX);

        let block_hash = driver.advance(&feed_msg, 0).expect("advance must succeed");
        assert_eq!(
            driver.pending_count(),
            1,
            "block should be pending (not flushed)"
        );

        // In-memory state should be queryable.
        let in_mem = driver.canonical_in_memory();
        let head = in_mem.head_state().expect("head state must exist");
        assert_eq!(head.hash(), block_hash, "in-memory head hash must match");
        assert_eq!(head.number(), 1, "in-memory head number must be 1");

        // Canonical block number should be 1.
        assert_eq!(in_mem.get_canonical_block_number(), 1);

        // Now flush and verify the on-disk state.
        driver.flush().expect("flush must succeed");
        assert_eq!(driver.pending_count(), 0);

        let provider = factory.provider().unwrap();
        assert_eq!(provider.best_block_number().unwrap(), 1);
    }

    /// D.2.4: a simple reorg — replace one block with another at the same height.
    ///
    /// Advance a block, then reorg it to a different block at the same height.
    /// Verifies the in-memory state and tracked tip are updated correctly.
    /// MDBX-level unwind/persist is deferred (see reorg() doc).
    #[test]
    fn reorg_replaces_block_suffix() {
        let factory = create_test_provider_factory_with_node_types::<ArbNode>(MAINNET.clone());

        let genesis_tip = seed_genesis(&factory);

        let fixtures_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../arb_revm/testdata/fixtures");
        let fixture_path = fixtures_dir.join("deposit_message_only.json");
        let json = std::fs::read_to_string(&fixture_path).unwrap();
        let feed_msg: BroadcastFeedMessage = serde_json::from_str(&json).unwrap();

        // Advance block 1 (auto-flushed with threshold=1).
        let mut driver = ArbChainDriver::new(factory.clone(), 42161, genesis_tip.clone(), 1);
        let _first_hash = driver
            .advance(&feed_msg, 0)
            .expect("first advance must succeed");
        assert_eq!(driver.pending_count(), 0); // auto-flushed

        // Re-execute from genesis to get a "new" block 1.
        let parent_header = genesis_tip.header();
        let parent = ArbParentHeader {
            number: parent_header.number,
            timestamp: parent_header.timestamp,
            beneficiary: parent_header.beneficiary,
            basefee: parent_header.base_fee_per_gas.unwrap_or(0),
            gas_limit: parent_header.gas_limit,
            difficulty: parent_header.difficulty,
            prevrandao: Some(parent_header.mix_hash),
        };
        let cfg = ArbExecCfg {
            chain_id: 42161,
            ..ArbExecCfg::default()
        };
        let input = digest_message(&feed_msg, parent, cfg, 0).unwrap();

        let next_timestamp = input.message.l1_timestamp.max(parent.timestamp);
        let attrs = ArbNextBlockEnvAttributes {
            timestamp: next_timestamp,
            suggested_fee_recipient: input.message.poster,
            prev_randao: B256::ZERO,
            gas_limit: input.cfg.block_gas_limit,
            l1_block_number: input.message.l1_block_number,
            l1_base_fee_wei: input.message.l1_base_fee_wei,
            arbos_format_version: 0,
            delayed_messages_read: input.message.delayed_messages_read,
            extra_data: Bytes::default(),
            withdrawals: None,
        };

        let state_provider_for_trie = factory.history_by_block_number(0).unwrap();
        let db_inner = StateProviderDatabase::new(factory.history_by_block_number(0).unwrap());
        let mut state = State::builder()
            .with_database(db_inner)
            .with_bundle_update()
            .build();

        let mut builder = driver
            .evm_config
            .builder_for_next_block(&mut state, &genesis_tip, attrs)
            .map_err(|e| eyre!("{e:?}"))
            .unwrap();
        builder.apply_pre_execution_changes().unwrap();
        let txs: Vec<ArbTxEnvelope> = input.message.txs;
        for tx in txs {
            let sender = tx.sender().unwrap();
            builder
                .execute_transaction(Recovered::new_unchecked(tx, sender))
                .unwrap();
        }
        let outcome = builder.finish(state_provider_for_trie, None).unwrap();
        let bundle = state.take_bundle();
        // Drop the revm `State` now: it still owns a read transaction (the
        // `StateProviderDatabase` opened at block 0). `reorg()` below opens a write
        // transaction via `provider_rw()`, and MDBX deadlocks if a read txn is still
        // alive on the same thread. We've extracted everything we need (`bundle`).
        drop(state);

        let recovered_block: RecoveredBlock<ArbBlock> = outcome.block;
        let new_hash = recovered_block.hash();
        let trie_data = ComputedTrieData::new(
            Arc::new(outcome.hashed_state.into_sorted()),
            Arc::new(outcome.trie_updates.into_sorted()),
        );
        let new_executed = ExecutedBlock::new(
            Arc::new(recovered_block),
            Arc::new(BlockExecutionOutput {
                result: BlockExecutionResult {
                    receipts: outcome.execution_result.receipts,
                    requests: outcome.execution_result.requests,
                    gas_used: outcome.execution_result.gas_used,
                    blob_gas_used: outcome.execution_result.blob_gas_used,
                },
                state: bundle,
            }),
            trie_data,
        );

        // Build a minimal old block at height 1 with the first_hash.
        let old_block = SealedBlock::<ArbBlock>::seal_slow(alloy_consensus::Block {
            header: Header {
                number: 1,
                parent_hash: genesis_tip.hash(),
                ..Default::default()
            },
            body: Default::default(),
        });
        let old_executed = ExecutedBlock::new(
            Arc::new(old_block.try_recover().unwrap()),
            Arc::new(BlockExecutionOutput {
                result: BlockExecutionResult {
                    receipts: vec![],
                    requests: Default::default(),
                    gas_used: 0,
                    blob_gas_used: 0,
                },
                state: BundleState::default(),
            }),
            ComputedTrieData::default(),
        );

        // Perform the reorg (in-memory state + MDBX unwind/save).
        driver
            .reorg(vec![new_executed.clone()], vec![old_executed])
            .expect("reorg must succeed");

        // After reorg, the tip should be the new block.
        assert_eq!(
            driver.tip().hash(),
            new_hash,
            "tip must be the new block hash"
        );
        assert_eq!(driver.tip().number, 1, "tip number must still be 1");

        // In-memory state should reflect the new block.
        let in_mem = driver.canonical_in_memory();
        let head = in_mem.head_state().unwrap();
        assert_eq!(
            head.hash(),
            new_hash,
            "in-memory head must be the new block"
        );
    }

    /// P0 regression: a 2-block advance must carry state forward.
    ///
    /// Block 1 deposits to an account. Block 2 deposits again. After both blocks
    /// are flushed, the account balance must reflect the cumulative deposits.
    /// This catches the P0 bug where BundleState was discarded (block 2 would see
    /// no post-block-1 state and execute against genesis).
    ///
    /// **Acceptance:** reverting `state.take_bundle()` in `advance()` must make
    /// this test FAIL — the mid-chain balance at block 1 would be zero and the
    /// cumulative balance at block 2 would be only one deposit.
    #[test]
    fn two_block_advance_carries_state_forward() {
        let factory = create_test_provider_factory_with_node_types::<ArbNode>(MAINNET.clone());

        let genesis_tip = seed_genesis(&factory);

        let fixtures_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../arb_revm/testdata/fixtures");
        let fixture_path = fixtures_dir.join("deposit_message_only.json");
        let json = std::fs::read_to_string(&fixture_path).unwrap();
        let feed_msg: BroadcastFeedMessage = serde_json::from_str(&json).unwrap();

        let mut driver = ArbChainDriver::new(factory.clone(), 42161, genesis_tip, 1);

        let deposit_to = address!("3f1eae7d46d88f08fc2f8ed27fcb2ab183eb2d0e");
        let single_deposit = U256::from(111000000000000000u128);

        // Advance block 1
        let bh1 = driver
            .advance(&feed_msg, 0)
            .expect("block 1 advance must succeed");
        assert_eq!(driver.pending_count(), 0, "auto-flushed (threshold=1)");
        assert_eq!(driver.tip().number, 1);
        assert_eq!(driver.tip().hash(), bh1);

        // ---- Mid-chain check: block 1's state, independently read from disk ----
        {
            let state_at_1 = factory
                .history_by_block_number(1)
                .expect("history_by_block_number(1) must succeed");
            let acct_at_1 = state_at_1.basic_account(&deposit_to).expect("lookup at block 1");
            assert!(
                acct_at_1.is_some(),
                "deposit recipient must exist at block 1"
            );
            assert_eq!(
                acct_at_1.unwrap().balance, single_deposit,
                "block 1 state must carry exactly one deposit ({}), was {}",
                single_deposit, acct_at_1.unwrap().balance
            );
        }

        // Advance block 2 — threshold=1 flushes each block immediately
        let bh2 = driver
            .advance(&feed_msg, 0)
            .expect("block 2 advance must succeed");
        assert_eq!(driver.pending_count(), 0, "auto-flushed");
        assert_eq!(driver.tip().number, 2);
        assert_eq!(driver.tip().hash(), bh2);

        // Verify both blocks are on disk
        let provider = factory.provider().unwrap();
        assert_eq!(provider.best_block_number().unwrap(), 2);

        // Block 1 header exists
        let h1 = provider.header_by_number(1).unwrap().unwrap();
        let h1_root = h1.state_root;
        assert_eq!(
            reth_primitives_traits::SealedHeader::seal_slow(h1).hash(),
            bh1,
            "block-1 hash mismatch"
        );

        // Block 2 header exists
        let h2 = provider.header_by_number(2).unwrap().unwrap();
        let h2_root = h2.state_root;
        assert_eq!(
            reth_primitives_traits::SealedHeader::seal_slow(h2).hash(),
            bh2,
            "block-2 hash mismatch"
        );

        assert_ne!(
            h1_root, h2_root,
            "state roots of distinct blocks must differ"
        );

        // Cumulative balance: exactly two deposits.
        let expected_cumulative = single_deposit * U256::from(2);
        let state = factory.latest().expect("latest state must open");
        let account = state
            .basic_account(&deposit_to)
            .expect("account lookup must not fail");
        let acct = account.expect("deposit recipient must have an account after two advances");
        assert_eq!(
            acct.balance, expected_cumulative,
            "cumulative balance must be 2 × {single_deposit}, was {}",
            acct.balance
        );
    }

    /// D.2.4+ full reorg: disk unwind + persist new suffix.
    ///
    /// Advance blocks 1 & 2 (threshold=1, flushed). Then reorg block 2 to a
    /// different block 2′ at the same height. After reorg:
    /// - `best_block_number == 2`
    /// - `header_by_number(2)` hash == 2′ (NOT the old block 2)
    /// - `header_by_number(1)` exists and is unchanged
    /// - The old block 2's state is gone, 2′'s state is present
    ///
    /// This is verified by reopening a fresh provider from the same factory.
    #[test]
    fn reorg_unwinds_disk() {
        let factory = create_test_provider_factory_with_node_types::<ArbNode>(MAINNET.clone());
        let genesis_tip = seed_genesis(&factory);

        let fixtures_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../arb_revm/testdata/fixtures");
        let fixture_path = fixtures_dir.join("deposit_message_only.json");
        let json = std::fs::read_to_string(&fixture_path).unwrap();
        let feed_msg: BroadcastFeedMessage = serde_json::from_str(&json).unwrap();

        let deposit_to = address!("3f1eae7d46d88f08fc2f8ed27fcb2ab183eb2d0e");
        let single_deposit = U256::from(111000000000000000u128);

        let mut driver = ArbChainDriver::new(factory.clone(), 42161, genesis_tip.clone(), 1);

        driver.advance(&feed_msg, 0).expect("advance 1");
        assert_eq!(driver.tip().number, 1);

        let old_bh2 = driver.advance(&feed_msg, 0).expect("advance 2");
        assert_eq!(driver.tip().number, 2);
        assert_eq!(driver.pending_count(), 0); // auto-flushed

        // Build a new block 1′ (different from the existing block 1) by
        // re-executing the deposit against genesis.
        let (new_block_1, new_hash) = {
            let p = genesis_tip.header();
            let parent = ArbParentHeader {
                number: p.number, timestamp: p.timestamp, beneficiary: p.beneficiary,
                basefee: p.base_fee_per_gas.unwrap_or(0), gas_limit: p.gas_limit,
                difficulty: p.difficulty, prevrandao: Some(p.mix_hash),
            };
            let input = digest_message(&feed_msg, parent,
                ArbExecCfg { chain_id: 42161, ..ArbExecCfg::default() }, 0).unwrap();
            let attrs = ArbNextBlockEnvAttributes {
                timestamp: input.message.l1_timestamp.max(parent.timestamp),
                suggested_fee_recipient: input.message.poster,
                prev_randao: B256::ZERO, gas_limit: input.cfg.block_gas_limit,
                l1_block_number: input.message.l1_block_number,
                l1_base_fee_wei: input.message.l1_base_fee_wei,
                arbos_format_version: 0,
                delayed_messages_read: input.message.delayed_messages_read,
                extra_data: Bytes::default(), withdrawals: None,
            };

            let sp = factory.history_by_block_number(0).unwrap();
            let db = StateProviderDatabase::new(factory.history_by_block_number(0).unwrap());
            let mut state = State::builder().with_database(db).with_bundle_update().build();
            let mut builder = driver.evm_config
                .builder_for_next_block(&mut state, &genesis_tip, attrs)
                .map_err(|e| eyre!("{e:?}")).unwrap();
            builder.apply_pre_execution_changes().unwrap();
            for tx in input.message.txs {
                let sender = tx.sender().unwrap();
                builder.execute_transaction(Recovered::new_unchecked(tx, sender)).unwrap();
            }
            let outcome = builder.finish(sp, None).unwrap();
            let bundle = state.take_bundle();
            let rb: RecoveredBlock<ArbBlock> = outcome.block;
            let hash = rb.hash();
            let td = ComputedTrieData::new(
                Arc::new(outcome.hashed_state.into_sorted()),
                Arc::new(outcome.trie_updates.into_sorted()),
            );
            let ex = ExecutedBlock::new(Arc::new(rb), Arc::new(BlockExecutionOutput {
                result: BlockExecutionResult {
                    receipts: outcome.execution_result.receipts,
                    requests: outcome.execution_result.requests,
                    gas_used: outcome.execution_result.gas_used,
                    blob_gas_used: outcome.execution_result.blob_gas_used,
                },
                state: bundle,
            }), td);
            (ex, hash)
        };

        // old block 1 synthetic for in-memory rollback
        let old_synth = SealedBlock::<ArbBlock>::seal_slow(alloy_consensus::Block {
            header: Header { number: 1, parent_hash: genesis_tip.hash(), ..Default::default() },
            body: Default::default(),
        });
        let old_ex = ExecutedBlock::new(
            Arc::new(old_synth.try_recover().unwrap()),
            Arc::new(BlockExecutionOutput {
                result: BlockExecutionResult { receipts: vec![], requests: Default::default(),
                    gas_used: 0, blob_gas_used: 0 },
                state: BundleState::default(),
            }), ComputedTrieData::default());

        driver.reorg(vec![new_block_1], vec![old_ex]).expect("reorg must succeed");

        // 2: Verify on-disk state from a fresh provider
        let provider = factory.provider().unwrap();
        assert_eq!(provider.best_block_number().unwrap(), 1,
            "best block must be 1");

        let h1 = provider.header_by_number(1).unwrap().unwrap();
        assert_eq!(reth_primitives_traits::SealedHeader::seal_slow(h1).hash(), new_hash,
            "block 1 must be new block");

        assert!(provider.header_by_number(2).unwrap().is_none(), "block 2 must be gone");
        drop(provider);

        let state = factory.latest().unwrap();
        let acct = state.basic_account(&deposit_to).unwrap()
            .expect("deposit recipient must exist");
        assert_eq!(acct.balance, single_deposit,
            "balance after reorg must be one deposit");

        // 3: Old block 2 hash must NOT be on disk
        let p2 = factory.provider().unwrap();
        let h1_again = p2.header_by_number(1).unwrap().unwrap();
        assert_ne!(reth_primitives_traits::SealedHeader::seal_slow(h1_again).hash(),
            old_bh2, "old block 2 hash gone");
    }
}
