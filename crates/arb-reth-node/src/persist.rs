//! D.2.2 — durable "execute-once → persist" primitive for the Arbitrum node.
//!
//! The persist function takes an already-executed block (header carries the correct `state_root`
//! baked in by the caller) together with the post-state `BundleState` and writes everything to
//! MDBX via reth's [`save_blocks`](reth_provider::DatabaseProviderRW::save_blocks) path.
//!
//! It does **not** wire [`CanonicalInMemoryState`] — that lands with node assembly later.
//! D.2.2 is the durable disk path only.

use alloc::sync::Arc;

use alloy_primitives::Log;
use arb_alloy_consensus::{
    ArbReceiptEnvelope,
    reth::{ArbBlock, ArbPrimitives},
};
use eyre::eyre;
use reth_chain_state::{ComputedTrieData, ExecutedBlock};
use reth_execution_types::{BlockExecutionOutput, BlockExecutionResult};
use reth_primitives_traits::RecoveredBlock;
use reth_provider::providers::ProviderNodeTypes;
use reth_provider::{ProviderFactory, SaveBlocksMode};
use reth_storage_api::StateRootProvider;
use reth_trie_common::{HashedPostState, KeccakKeyHasher};
use revm_database::BundleState;

/// Persist an executed Arbitrum block to MDBX.
///
/// **Contract**: `recovered_block.header().state_root` must already reflect the post-state root
/// of `bundle_state`.  The function computes the root from the trie and asserts the two values
/// match — mismatches return an error.
///
/// # Steps
/// 1. Hash the post-state with Keccak: [`HashedPostState::from_bundle_state`].
/// 2. Compute `(root, trie_updates)` via [`StateRootProvider::state_root_with_updates`] against
///    the **parent** (current latest committed) state.
/// 3. Assert `root == block.header().state_root`.
/// 4. Build [`ExecutedBlock`] wrapping the output and pre-computed [`ComputedTrieData`].
/// 5. Write to MDBX + static files via `provider_rw.save_blocks(…, SaveBlocksMode::Full)` then
///    `provider_rw.commit()`.
pub fn persist_executed_block<N>(
    factory: &ProviderFactory<N>,
    recovered_block: RecoveredBlock<ArbBlock>,
    bundle_state: BundleState,
    receipts: Vec<ArbReceiptEnvelope<Log>>,
    gas_used: u64,
) -> eyre::Result<()>
where
    N: ProviderNodeTypes<Primitives = ArbPrimitives>,
{
    // 1. Build the hashed post-state from the bundle state.
    let hashed_state =
        HashedPostState::from_bundle_state::<KeccakKeyHasher>(bundle_state.state().iter());

    // 2. Open a read-only provider over the CURRENT (parent) committed state and compute the
    //    state root together with the trie updates needed to advance the trie.
    //
    //    `factory.latest()` returns a `Box<dyn StateProvider>` which implements
    //    `StateRootProvider` — we call `state_root_with_updates(hashed_state.clone())`.
    let state_provider = factory.latest()?;
    let (computed_root, trie_updates) = state_provider.state_root_with_updates(hashed_state.clone())?;

    // 3. Assert consistency — the caller baked the root into the header.
    let expected_root = recovered_block.header().state_root;
    if computed_root != expected_root {
        return Err(eyre!(
            "state root mismatch: computed {computed_root}, header has {expected_root}"
        ));
    }

    // 4. Build the execution output and the ComputedTrieData.
    let execution_output = Arc::new(BlockExecutionOutput {
        result: BlockExecutionResult {
            receipts,
            requests: Default::default(),
            gas_used,
            blob_gas_used: 0,
        },
        state: bundle_state,
    });

    let trie_data = ComputedTrieData::new(
        Arc::new(hashed_state.into_sorted()),
        Arc::new(trie_updates.into_sorted()),
    );

    let executed = ExecutedBlock::new(Arc::new(recovered_block), execution_output, trie_data);

    // 5. Persist to MDBX + static files, then commit.
    let provider_rw = factory.provider_rw()?;
    provider_rw.save_blocks(vec![executed], SaveBlocksMode::Full)?;
    provider_rw.commit()?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use alloc::vec;

    use alloy_consensus::Header;
    use alloy_primitives::{address, U256};
    use reth_chainspec::MAINNET;
    use reth_chain_state::ComputedTrieData;
    use reth_execution_types::{BlockExecutionOutput, BlockExecutionResult};
    use reth_primitives_traits::SealedBlock;
    use reth_provider::{
        HeaderProvider,
        test_utils::create_test_provider_factory_with_node_types,
    };
    use reth_storage_api::{AccountReader, BlockHashReader, StateRootProvider};
    use reth_trie_common::{HashedPostState, KeccakKeyHasher};
    use revm_database::BundleState;
    use revm_state::AccountInfo;

    use arb_alloy_consensus::reth::ArbBlock;

    use crate::ArbNode;

    /// D.2.2: persist a trivial executed block, reopen the factory from the same DB, and verify
    /// that the block is durably stored and the post-state account is readable.
    #[test]
    fn persist_block_roundtrip() {
        // ------------------------------------------------------------------ //
        // 1. Stand up an in-memory test factory (MDBX + static files).
        // ------------------------------------------------------------------ //
        let chain_spec = MAINNET.clone();
        let factory =
            create_test_provider_factory_with_node_types::<ArbNode>(chain_spec.clone());

        // ------------------------------------------------------------------ //
        // 2. Persist genesis (block 0) so static files are seeded.
        //    static files must be contiguous from genesis before save_blocks
        //    accepts block 1.
        // ------------------------------------------------------------------ //
        {
            let genesis_block = SealedBlock::<ArbBlock>::seal_slow(alloy_consensus::Block {
                header: Header { number: 0, ..Default::default() },
                body: Default::default(),
            });
            let genesis_executed = ExecutedBlock::new(
                Arc::new(genesis_block.try_recover().unwrap()),
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
            provider_rw.save_blocks(vec![genesis_executed], SaveBlocksMode::Full).unwrap();
            provider_rw.commit().unwrap();
        }

        // ------------------------------------------------------------------ //
        // 3. Build a trivial BundleState that creates one account.
        // ------------------------------------------------------------------ //
        const BLOCK_NUM: u64 = 1;
        let known_addr = address!("0000000000000000000000000000000000001234");
        let known_balance = U256::from(999_000_u64);

        let bundle = BundleState::builder(BLOCK_NUM..=BLOCK_NUM)
            .state_present_account_info(
                known_addr,
                AccountInfo {
                    nonce: 1,
                    balance: known_balance,
                    ..Default::default()
                },
            )
            .revert_account_info(BLOCK_NUM, known_addr, Some(None))
            .build();

        // ------------------------------------------------------------------ //
        // 4. Compute the state root against the current (genesis) state.
        //    Use factory.latest() which implements StateRootProvider.
        // ------------------------------------------------------------------ //
        let hashed_state =
            HashedPostState::from_bundle_state::<KeccakKeyHasher>(bundle.state().iter());
        let state_provider = factory.latest().unwrap();
        let (computed_root, _trie_updates) =
            state_provider.state_root_with_updates(hashed_state).unwrap();
        drop(state_provider); // release any read TX before taking write TX

        // ------------------------------------------------------------------ //
        // 5. Build the ArbBlock (block 1) with the computed state root.
        // ------------------------------------------------------------------ //
        let genesis_hash = factory
            .provider()
            .unwrap()
            .block_hash(0)
            .unwrap()
            .expect("genesis hash must exist");

        let block_header = Header {
            number: BLOCK_NUM,
            parent_hash: genesis_hash,
            state_root: computed_root,
            ..Default::default()
        };
        let sealed_block = SealedBlock::<ArbBlock>::seal_slow(
            alloy_consensus::Block {
                header: block_header,
                body: Default::default(),
            },
        );
        let recovered_block = sealed_block.try_recover().unwrap();

        // ------------------------------------------------------------------ //
        // 6. Persist via the D.2.2 primitive.
        // ------------------------------------------------------------------ //
        // Re-build bundle (moved into persist_executed_block).
        let bundle2 = BundleState::builder(BLOCK_NUM..=BLOCK_NUM)
            .state_present_account_info(
                known_addr,
                AccountInfo {
                    nonce: 1,
                    balance: known_balance,
                    ..Default::default()
                },
            )
            .revert_account_info(BLOCK_NUM, known_addr, Some(None))
            .build();

        persist_executed_block(&factory, recovered_block, bundle2, vec![], 0)
            .expect("persist_executed_block must succeed");

        // ------------------------------------------------------------------ //
        // 7. Reopen a fresh provider from the *same* factory and verify.
        // ------------------------------------------------------------------ //
        let provider = factory.provider().expect("fresh provider must open");

        use reth_provider::BlockNumReader;
        let best = provider.best_block_number().expect("best_block_number should succeed");
        assert_eq!(best, BLOCK_NUM, "best block should now be {BLOCK_NUM}");

        let header = provider
            .header_by_number(BLOCK_NUM)
            .expect("header lookup must not fail")
            .expect("header at block 1 must exist");
        assert_eq!(
            header.state_root, computed_root,
            "stored header's state_root must match what we computed"
        );

        drop(provider); // avoid holding a read TX alongside the state provider

        // Verify the new account is readable from the latest state.
        let state = factory.latest().expect("latest state provider must open");
        let account = state
            .basic_account(&known_addr)
            .expect("basic_account lookup must not fail")
            .expect("account must exist after persist");
        assert_eq!(account.balance, known_balance, "account balance must match");
    }
}
