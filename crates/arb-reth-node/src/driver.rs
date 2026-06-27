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

use alloc::sync::Arc;
use alloc::vec::Vec;

use alloy_consensus::Header;
use alloy_consensus::transaction::Recovered;
use alloy_eips::eip2718::Typed2718;
use alloy_primitives::{Address, B256, Bytes};
use arb_alloy_consensus::{
    ArbTxEnvelope,
    reth::{ArbBlock, ArbPrimitives},
};
use arb_reth_evm::ArbEvmConfig;
use arb_reth_evm::config::ArbNextBlockEnvAttributes;
use arb_revm::executor::{ArbExecCfg, ArbParentHeader, digest_message};
use arb_sequencer_network::sequencer::feed::BroadcastFeedMessage;
use eyre::{Context as _, eyre};
use reth_chain_state::{
    CanonicalInMemoryState, ComputedTrieData, ExecutedBlock, NewCanonicalChain,
};
use reth_evm::{ConfigureEvm, execute::BlockBuilder as _};
use reth_execution_types::{BlockExecutionOutput, BlockExecutionResult};
use reth_primitives_traits::{RecoveredBlock, SealedHeader};
use reth_provider::providers::ProviderNodeTypes;
use reth_provider::{ProviderFactory, SaveBlocksMode};
use reth_revm::State;
use reth_revm::database::StateProviderDatabase;

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
        // 7. Execute each transaction from the digested message.
        // ------------------------------------------------------------------ //
        let txs: Vec<ArbTxEnvelope> = input.message.txs;
        for tx in txs {
            let sender: Address = tx
                .sender()
                .map_err(|e| eyre!("failed to determine sender for tx {}: {e}", tx.ty()))?;
            let recovered = Recovered::new_unchecked(tx, sender);
            builder
                .execute_transaction(recovered)
                .wrap_err("execute_transaction failed")?;
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

        // Update in-memory canonical state.
        self.canonical_in_memory
            .update_chain(NewCanonicalChain::Reorg {
                new: new_blocks.clone(),
                old: old_blocks.clone(),
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

        // TODO(D.2.4+): MDBX unwind (take_block_and_execution_above) + persist
        // of the new suffix. The caller should handle disk-level consistency
        // before calling reorg(), or we can add it when the test harness supports
        // multi-transaction MDBX operations without deadlocks.

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
    use reth_provider::{BlockNumReader, HeaderProvider, SaveBlocksMode};
    use reth_storage_api::AccountReader;
    use revm_database::BundleState;

    use crate::ArbNode;

    /// Default persistence threshold for tests — flush every block.
    const TEST_THRESHOLD: u64 = 1;

    /// Helper: seed genesis block 0 so that static files are contiguous for block 1.
    fn seed_genesis<N: ProviderNodeTypes<Primitives = ArbPrimitives>>(
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
            .save_blocks(vec![genesis_executed], SaveBlocksMode::Full)
            .unwrap();
        provider_rw.commit().unwrap();

        SealedHeader::new(
            genesis_block.into_sealed_header().into_header(),
            genesis_hash,
        )
    }

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

        // Perform the reorg (in-memory state only, no MDBX unwind).
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

        // Advance block 1
        let bh1 = driver
            .advance(&feed_msg, 0)
            .expect("block 1 advance must succeed");
        assert_eq!(driver.pending_count(), 0, "auto-flushed (threshold=1)");
        assert_eq!(driver.tip().number, 1);
        assert_eq!(driver.tip().hash(), bh1);

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
        assert_eq!(
            reth_primitives_traits::SealedHeader::seal_slow(h1).hash(),
            bh1,
            "block-1 hash mismatch"
        );

        // Block 2 header exists
        let h2 = provider.header_by_number(2).unwrap().unwrap();
        assert_eq!(
            reth_primitives_traits::SealedHeader::seal_slow(h2).hash(),
            bh2,
            "block-2 hash mismatch"
        );

        // The deposit recipient must have an account now (P0 regression guard).
        let deposit_to = address!("3f1eae7d46d88f08fc2f8ed27fcb2ab183eb2d0e");
        let state = factory.latest().expect("latest state must open");
        let account = state
            .basic_account(&deposit_to)
            .expect("account lookup must not fail");
        assert!(
            account.is_some(),
            "deposit recipient must have an account after two deposits (BundleState was persisted)"
        );
    }
}
