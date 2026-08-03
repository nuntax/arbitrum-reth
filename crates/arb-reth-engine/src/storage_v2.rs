//! Storage V2 persistence support for legacy ArbOS storage wipes.
//!
//! Reth stores canonical state under hashed keys in Storage V2, while its unwind changesets keep
//! plain storage-slot keys. Staged sync bridges that gap through the native `SlotPreimages`
//! sidecar. Arb-reth uses engine-tree persistence instead, so this module enriches persistence
//! batches before delegating them to Reth's stock service.

use std::{
    sync::{Arc, OnceLock, mpsc},
    thread::JoinHandle,
    time::Instant,
};

use alloy_consensus::Header;
use alloy_primitives::{
    Address, B256, U256, keccak256,
    map::{B256Map, HashSet},
};
use arb_revm::ArbSpecId;
use arbitrum_alloy_consensus::{header::ArbHeaderInfo, reth::ArbPrimitives};
use eyre::{WrapErr as _, eyre};
use metrics::Histogram;
use rayon::slice::ParallelSliceMut;
use reth_chain_state::ExecutedBlock;
use reth_db_api::{
    cursor::{DbCursorRO, DbDupCursorRO},
    tables,
    transaction::DbTx,
};
use reth_engine_tree::persistence::{PersistenceAction, PersistenceHandle};
use reth_provider::{
    DatabaseProviderFactory, ProviderFactory, SaveBlocksInput, providers::ProviderNodeTypes,
};
use reth_prune::PrunerWithFactory;
use reth_revm::revm::database::states::RevertToSlot;
use reth_stages::stages::slot_preimages::{SlotPreimages, SlotPreimagesReader};
use reth_stages_api::{MetricEventsSender, StageId};
use reth_storage_api::{
    BlockNumReader, DBProvider, HeaderProvider, StageCheckpointReader, StoragePath,
    StorageSettingsCache,
};
use reth_tasks::spawn_os_thread;
use reth_trie::{ComputedTrieData, HashedPostStateSorted, HashedStorageSorted, LazyTrieData};

/// Joins the persistence proxy after the engine-tree request sender has been dropped.
pub(crate) struct PersistenceProxyGuard(Option<JoinHandle<()>>);

impl Drop for PersistenceProxyGuard {
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            let _ = handle.join();
        }
    }
}

/// Spawn a thin persistence-action proxy in front of Reth's stock persistence service.
pub(crate) fn spawn_persistence<N>(
    factory: ProviderFactory<N>,
    pruner: PrunerWithFactory<ProviderFactory<N>>,
    sync_metrics_tx: MetricEventsSender,
) -> (PersistenceHandle<ArbPrimitives>, PersistenceProxyGuard)
where
    N: ProviderNodeTypes<Primitives = ArbPrimitives>,
    <ProviderFactory<N> as DatabaseProviderFactory>::Provider: BlockNumReader
        + DBProvider
        + HeaderProvider<Header = Header>
        + StageCheckpointReader
        + StoragePath
        + StorageSettingsCache,
{
    let delegate = PersistenceHandle::<ArbPrimitives>::spawn_service::<N>(
        factory.clone(),
        pruner,
        sync_metrics_tx,
    );
    let (sender, receiver) = mpsc::channel();
    let handle = spawn_os_thread("arb-storage-v2-persistence", move || {
        let mut preimages = None;
        while let Ok(action) = receiver.recv() {
            match action {
                PersistenceAction::SaveBlocks(input, response) => {
                    let input = match prepare_storage_v2_batch(&factory, input, &mut preimages) {
                        Ok(input) => input,
                        Err(err) => {
                            tracing::error!(
                                target: "arb-reth::persistence",
                                %err,
                                "failed to prepare Storage V2 persistence batch",
                            );
                            // Dropping the response sender makes the engine tree surface the
                            // failure instead of persisting an unwind-unsafe batch.
                            break;
                        }
                    };

                    let (delegate_tx, delegate_rx) = crossbeam_channel::bounded(1);
                    if delegate.save_blocks(input, delegate_tx).is_err() {
                        break;
                    }
                    let Ok(result) = delegate_rx.recv() else {
                        break;
                    };
                    let _ = response.send(result);
                }
                PersistenceAction::RemoveBlocksAbove(block, response) => {
                    let (delegate_tx, delegate_rx) = crossbeam_channel::bounded(1);
                    if delegate.remove_blocks_above(block, delegate_tx).is_err() {
                        break;
                    }
                    let Ok(result) = delegate_rx.recv() else {
                        break;
                    };
                    let _ = response.send(result);
                }
                action => {
                    if delegate.send_action(action).is_err() {
                        break;
                    }
                }
            }
        }
    });

    (
        PersistenceHandle::new(sender),
        PersistenceProxyGuard(Some(handle)),
    )
}

fn prepare_storage_v2_batch<N>(
    factory: &ProviderFactory<N>,
    input: SaveBlocksInput<ArbPrimitives>,
    preimages: &mut Option<SlotPreimages>,
) -> eyre::Result<SaveBlocksInput<ArbPrimitives>>
where
    N: ProviderNodeTypes<Primitives = ArbPrimitives>,
    <ProviderFactory<N> as DatabaseProviderFactory>::Provider: BlockNumReader
        + DBProvider
        + HeaderProvider<Header = Header>
        + StageCheckpointReader
        + StoragePath
        + StorageSettingsCache,
{
    let prev_db_tip = input.prev_db_tip();
    let prev_partial_state_trie = input.prev_partial_state_trie();
    let new_db_tip = input.new_db_tip();
    let new_partial_state_trie = input.new_partial_state_trie();
    let mut blocks = input
        .state_trie_blocks()
        .iter()
        .chain(input.state_trie_masking_blocks())
        .cloned()
        .collect::<Vec<_>>();

    let provider = factory.database_provider_ro()?;
    if !provider.cached_storage_settings().use_hashed_state() {
        return Ok(input);
    }

    validate_persistence_batch(&provider, &blocks, prev_db_tip, prev_partial_state_trie)?;
    let legacy = legacy_block_flags(&provider, &blocks)?;
    if !legacy.iter().any(|legacy| *legacy) {
        return Ok(input);
    }

    let metrics = storage_v2_metrics();
    let started_at = Instant::now();
    if preimages.is_none() {
        let preimage_path = provider.storage_path().join("preimage");
        *preimages = Some(SlotPreimages::open(&preimage_path)?);
    }
    let preimages = preimages.as_ref().expect("initialized above");
    let mut entries = collect_slot_preimages(&blocks, &legacy);
    let mut preimage_candidates = 0usize;
    let mut new_preimages = 0usize;
    let mut preimage_commit = None;
    if !entries.is_empty() {
        entries.par_sort_unstable_by_key(|(hashed_slot, _)| *hashed_slot);
        for pair in entries.windows(2) {
            if pair[0].0 == pair[1].0 && pair[0].1 != pair[1].1 {
                return Err(eyre!(
                    "conflicting slot preimages for {:#x}: first={:#x}, second={:#x}",
                    pair[0].0,
                    pair[0].1,
                    pair[1].1,
                ));
            }
        }
        entries.dedup_by_key(|(hashed_slot, _)| *hashed_slot);
        preimage_candidates = entries.len();
        let missing = missing_preimages(preimages, entries)?;
        new_preimages = missing.len();
        if !missing.is_empty() {
            let commit_started_at = Instant::now();
            preimages.insert_preimages(&missing)?;
            preimage_commit = Some(commit_started_at.elapsed());
        }
    }

    let wipe_addresses = collect_wipe_addresses(&blocks, &legacy);
    let wipe_accounts = wipe_addresses.len();
    if wipe_addresses.is_empty() {
        let elapsed = started_at.elapsed();
        record_storage_v2_metrics(
            metrics,
            elapsed,
            preimage_commit,
            preimage_candidates,
            new_preimages,
            wipe_accounts,
            0,
        );
        return Ok(SaveBlocksInput::new(
            blocks,
            prev_db_tip,
            prev_partial_state_trie,
            new_db_tip,
            new_partial_state_trie,
        ));
    }

    let mut tracked = load_wiped_account_storage(&provider, wipe_addresses)?;
    let reader = preimages.reader()?;

    let mut injected_slots = 0usize;
    for (block, is_legacy) in blocks.iter_mut().zip(legacy) {
        if is_legacy && block_has_storage_wipe(block) {
            injected_slots += inject_plain_wipe_reverts(
                Arc::make_mut(&mut block.execution_output),
                &tracked,
                &reader,
            )?;
            mark_hashed_storage_wipes(block);
        }
        let trie_data = block.trie_data();
        apply_hashed_updates(&mut tracked, &trie_data.sorted.hashed_state);
    }

    let elapsed = started_at.elapsed();
    record_storage_v2_metrics(
        metrics,
        elapsed,
        preimage_commit,
        preimage_candidates,
        new_preimages,
        wipe_accounts,
        injected_slots,
    );
    Ok(SaveBlocksInput::new(
        blocks,
        prev_db_tip,
        prev_partial_state_trie,
        new_db_tip,
        new_partial_state_trie,
    ))
}

fn missing_preimages(
    preimages: &SlotPreimages,
    entries: Vec<(B256, B256)>,
) -> eyre::Result<Vec<(B256, B256)>> {
    let reader = preimages.reader()?;
    let mut missing = Vec::new();
    for (hashed_slot, plain_slot) in entries {
        match reader.get(&hashed_slot)? {
            None => missing.push((hashed_slot, plain_slot)),
            Some(existing) if existing == plain_slot => {}
            Some(existing) => {
                return Err(eyre!(
                    "corrupt slot preimage for {hashed_slot:#x}: expected {plain_slot:#x}, found {existing:#x}"
                ));
            }
        }
    }
    Ok(missing)
}

struct StorageV2Metrics {
    prepare_seconds: Histogram,
    preimage_commit_seconds: Histogram,
    preimage_candidates: Histogram,
    preimage_new: Histogram,
    wipe_accounts: Histogram,
    injected_slots: Histogram,
}

fn storage_v2_metrics() -> &'static StorageV2Metrics {
    static METRICS: OnceLock<StorageV2Metrics> = OnceLock::new();
    METRICS.get_or_init(|| StorageV2Metrics {
        prepare_seconds: metrics::histogram!("arb_reth.storage_v2_prepare_seconds"),
        preimage_commit_seconds: metrics::histogram!("arb_reth.storage_v2_preimage_commit_seconds"),
        preimage_candidates: metrics::histogram!(
            "arb_reth.storage_v2_preimage_candidates_per_batch"
        ),
        preimage_new: metrics::histogram!("arb_reth.storage_v2_new_preimages_per_batch"),
        wipe_accounts: metrics::histogram!("arb_reth.storage_v2_wipe_accounts_per_batch"),
        injected_slots: metrics::histogram!("arb_reth.storage_v2_injected_slots_per_batch"),
    })
}

fn record_storage_v2_metrics(
    metrics: &StorageV2Metrics,
    elapsed: std::time::Duration,
    preimage_commit: Option<std::time::Duration>,
    preimage_candidates: usize,
    new_preimages: usize,
    wipe_accounts: usize,
    injected_slots: usize,
) {
    metrics.prepare_seconds.record(elapsed.as_secs_f64());
    if let Some(elapsed) = preimage_commit {
        metrics
            .preimage_commit_seconds
            .record(elapsed.as_secs_f64());
    }
    metrics
        .preimage_candidates
        .record(preimage_candidates as f64);
    metrics.preimage_new.record(new_preimages as f64);
    metrics.wipe_accounts.record(wipe_accounts as f64);
    metrics.injected_slots.record(injected_slots as f64);
}

fn validate_persistence_batch<P>(
    provider: &P,
    blocks: &[ExecutedBlock<ArbPrimitives>],
    prev_db_tip: u64,
    prev_partial_state_trie: u64,
) -> eyre::Result<()>
where
    P: BlockNumReader + HeaderProvider<Header = Header> + StageCheckpointReader,
{
    let durable_tip = provider.last_block_number()?;
    if durable_tip != prev_db_tip {
        return Err(eyre!(
            "persistence input database tip {prev_db_tip} does not match durable tip {durable_tip}"
        ));
    }
    let (checkpoint_db_tip, checkpoint_state_trie_tip) = provider
        .get_stage_checkpoint(StageId::Finish)?
        .map(|checkpoint| {
            let state_trie_tip = checkpoint
                .finish_stage_checkpoint()
                .and_then(|finish| finish.partial_state_trie())
                .unwrap_or(checkpoint.block_number);
            (checkpoint.block_number, state_trie_tip)
        })
        .unwrap_or_default();
    if (checkpoint_db_tip, checkpoint_state_trie_tip)
        != (prev_db_tip, prev_partial_state_trie)
    {
        return Err(eyre!(
            "persistence input frontiers ({prev_db_tip}, {prev_partial_state_trie}) do not match Finish checkpoint ({checkpoint_db_tip}, {checkpoint_state_trie_tip})"
        ));
    }
    let mut expected_number = prev_partial_state_trie
        .checked_add(1)
        .ok_or_else(|| eyre!("durable block number overflow"))?;
    let mut expected_parent = provider
        .sealed_header(prev_partial_state_trie)?
        .ok_or_else(|| eyre!("missing state/trie frontier header {prev_partial_state_trie}"))?
        .hash();

    for block in blocks {
        if block.block_number() != expected_number {
            return Err(eyre!(
                "persistence batch starts or jumps at block {}, expected {} after durable tip {}",
                block.block_number(),
                expected_number,
                prev_partial_state_trie,
            ));
        }
        if block.recovered_block.header().parent_hash != expected_parent {
            return Err(eyre!(
                "persistence batch block {} has parent {}, expected {}",
                block.block_number(),
                block.recovered_block.header().parent_hash,
                expected_parent,
            ));
        }
        expected_number = expected_number
            .checked_add(1)
            .ok_or_else(|| eyre!("persistence block number overflow"))?;
        expected_parent = block.recovered_block.hash();
    }

    Ok(())
}

fn legacy_block_flags<P>(
    provider: &P,
    blocks: &[ExecutedBlock<ArbPrimitives>],
) -> eyre::Result<Vec<bool>>
where
    P: HeaderProvider<Header = Header>,
{
    let first_number = blocks[0].block_number();
    let parent_number = first_number
        .checked_sub(1)
        .ok_or_else(|| eyre!("cannot persist block zero as an executed child"))?;
    let mut parent = provider
        .header_by_number(parent_number)?
        .ok_or_else(|| eyre!("missing persisted parent header {parent_number}"))?;
    let mut legacy = Vec::with_capacity(blocks.len());

    for block in blocks {
        let parent_number = parent.number;
        let info = ArbHeaderInfo::decode_header(&parent)
            .wrap_err_with(|| format!("decode ArbOS parent header {parent_number}"))?;
        if !info.is_arbitrum() {
            return Err(eyre!(
                "parent header {parent_number} is not an Arbitrum header"
            ));
        }
        let spec = ArbSpecId::from_arbos_version(info.arbos_format_version);
        legacy.push(!spec.is_enabled_in(ArbSpecId::ARBOS_20));
        parent = block.recovered_block.header().clone();
    }

    Ok(legacy)
}

fn collect_slot_preimages(
    blocks: &[ExecutedBlock<ArbPrimitives>],
    legacy: &[bool],
) -> Vec<(B256, B256)> {
    let mut entries = Vec::new();
    for (block, is_legacy) in blocks.iter().zip(legacy) {
        if !is_legacy {
            continue;
        }
        let state = &block.execution_outcome().state;
        for account in state.state.values() {
            for &plain_slot in account.storage.keys() {
                push_preimage(&mut entries, plain_slot);
            }
        }
        for transition in state.reverts.iter() {
            for (_, revert) in transition {
                for &plain_slot in revert.storage.keys() {
                    push_preimage(&mut entries, plain_slot);
                }
            }
        }
    }
    entries
}

fn push_preimage(entries: &mut Vec<(B256, B256)>, plain_slot: U256) {
    let plain_slot = B256::from(plain_slot.to_be_bytes());
    entries.push((keccak256(plain_slot), plain_slot));
}

fn collect_wipe_addresses(
    blocks: &[ExecutedBlock<ArbPrimitives>],
    legacy: &[bool],
) -> HashSet<Address> {
    let mut addresses = HashSet::default();
    for (block, is_legacy) in blocks.iter().zip(legacy) {
        if !is_legacy {
            continue;
        }
        for transition in block.execution_outcome().state.reverts.iter() {
            for (address, revert) in transition {
                if revert.wipe_storage {
                    addresses.insert(*address);
                }
            }
        }
    }
    addresses
}

fn block_has_storage_wipe(block: &ExecutedBlock<ArbPrimitives>) -> bool {
    block
        .execution_outcome()
        .state
        .reverts
        .iter()
        .any(|transition| transition.iter().any(|(_, revert)| revert.wipe_storage))
}

fn mark_hashed_storage_wipes(block: &mut ExecutedBlock<ArbPrimitives>) {
    let wiped_addresses = block
        .execution_outcome()
        .state
        .reverts
        .iter()
        .flat_map(|transition| transition.iter())
        .filter_map(|(address, revert)| revert.wipe_storage.then_some(*address))
        .collect::<HashSet<_>>();
    if wiped_addresses.is_empty() {
        return;
    }

    let trie_data = block.trie_data().clone();
    let mut hashed_state = Arc::unwrap_or_clone(trie_data.sorted.hashed_state);
    for address in wiped_addresses {
        hashed_state
            .storages
            .entry(keccak256(address))
            .and_modify(|storage| storage.wiped = true)
            .or_insert_with(|| HashedStorageSorted {
                wiped: true,
                storage_slots: Vec::new(),
            });
    }
    block.trie_data = LazyTrieData::ready(ComputedTrieData::new(
        Arc::new(hashed_state),
        trie_data.sorted.trie_updates,
    ));
}

struct TrackedStorage {
    address: Address,
    slots: B256Map<U256>,
}

fn load_wiped_account_storage<P>(
    provider: &P,
    addresses: HashSet<Address>,
) -> eyre::Result<B256Map<TrackedStorage>>
where
    P: DBProvider,
{
    let mut tracked = B256Map::default();
    let mut cursor = provider
        .tx_ref()
        .cursor_dup_read::<tables::HashedStorages>()?;
    for address in addresses {
        let hashed_address = keccak256(address);
        let mut slots = B256Map::default();
        if let Some((_, entry)) = cursor.seek_exact(hashed_address)? {
            slots.insert(entry.key, entry.value);
            while let Some(entry) = cursor.next_dup_val()? {
                slots.insert(entry.key, entry.value);
            }
        }
        tracked.insert(hashed_address, TrackedStorage { address, slots });
    }
    Ok(tracked)
}

fn inject_plain_wipe_reverts<R>(
    output: &mut reth_execution_types::BlockExecutionOutput<R>,
    tracked: &B256Map<TrackedStorage>,
    preimages: &SlotPreimagesReader,
) -> eyre::Result<usize> {
    let mut injected = 0;
    for transition in output.state.reverts.iter_mut() {
        for (address, revert) in transition {
            if !revert.wipe_storage {
                continue;
            }
            let hashed_address = keccak256(*address);
            let storage = tracked
                .get(&hashed_address)
                .ok_or_else(|| eyre!("storage for wiped account {address:#x} was not loaded"))?;
            debug_assert_eq!(storage.address, *address);

            for (&hashed_slot, &value) in &storage.slots {
                let plain_slot = preimages.get(&hashed_slot)?.ok_or_else(|| {
                    eyre!(
                        "missing slot preimage for {hashed_slot:#x} in wiped account {address:#x}"
                    )
                })?;
                if keccak256(plain_slot) != hashed_slot {
                    return Err(eyre!(
                        "corrupt slot preimage for {hashed_slot:#x} in wiped account {address:#x}"
                    ));
                }
                let plain_key = U256::from_be_bytes(plain_slot.0);
                revert
                    .storage
                    .entry(plain_key)
                    .and_modify(|slot| {
                        if matches!(slot, RevertToSlot::Destroyed) {
                            *slot = RevertToSlot::Some(value);
                        }
                    })
                    .or_insert(RevertToSlot::Some(value));
                injected += 1;
            }
        }
    }
    Ok(injected)
}

fn apply_hashed_updates(tracked: &mut B256Map<TrackedStorage>, updates: &HashedPostStateSorted) {
    for (hashed_address, storage) in updates.account_storages() {
        let Some(current) = tracked.get_mut(hashed_address) else {
            continue;
        };
        if storage.is_wiped() {
            current.slots.clear();
        }
        for &(hashed_slot, value) in storage.storage_slots_ref() {
            if value.is_zero() {
                current.slots.remove(&hashed_slot);
            } else {
                current.slots.insert(hashed_slot, value);
            }
        }
    }

    for (hashed_address, account) in updates.accounts() {
        if account.is_none()
            && let Some(current) = tracked.get_mut(hashed_address)
        {
            current.slots.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use alloy_primitives::{address, b256, map::HashMap};
    use arbitrum_alloy_consensus::{
        ArbTxEnvelope,
        reth::{ArbBlock, ArbPrimitives},
    };
    use reth_chain_state::ExecutedBlock;
    use reth_chainspec::{ChainSpec, MAINNET};
    use reth_db_common::init::init_genesis_with_settings;
    use reth_execution_types::{BlockExecutionOutput, BlockExecutionResult};
    use reth_exex_types::FinishedExExHeight;
    use reth_node_types::NodeTypes;
    use reth_primitives_traits::SealedBlock;
    use reth_provider::{
        SaveBlocksInput, StaticFileProviderFactory, StorageSettings,
        test_utils::create_test_provider_factory_with_node_types,
    };
    use reth_prune::Pruner;
    use reth_revm::revm::{
        database::{
            BundleState,
            states::{AccountRevert, AccountStatus, RevertToSlot, reverts::Reverts},
        },
        state::AccountInfo,
    };
    use reth_storage_api::{
        AccountReader, EthStorage, StateProvider, StateRootProvider, StorageChangeSetReader,
    };
    use reth_trie::{
        ComputedTrieData, HashedPostState, HashedStorageSorted, KeccakKeyHasher,
        updates::TrieUpdates,
    };

    use super::*;

    #[derive(Debug, Clone, Default)]
    struct TestArbNode;

    impl NodeTypes for TestArbNode {
        type Primitives = ArbPrimitives;
        type ChainSpec = reth_chainspec::ChainSpec;
        type Storage = EthStorage<ArbTxEnvelope, Header>;
        type Payload = crate::ArbPayloadTypes;
    }

    fn arb_header(number: u64, parent_hash: B256, state_root: B256) -> Header {
        let mut header = Header {
            number,
            parent_hash,
            state_root,
            ..Default::default()
        };
        ArbHeaderInfo {
            arbos_format_version: 19,
            ..Default::default()
        }
        .update_header(&mut header);
        header
    }

    fn test_chain_spec() -> Arc<ChainSpec> {
        let mut genesis = MAINNET.genesis.clone();
        let info = ArbHeaderInfo {
            arbos_format_version: 19,
            ..Default::default()
        };
        genesis.extra_data = info.encode_extra_data();
        genesis.mix_hash = info.encode_mix_hash();
        Arc::new(ChainSpec::from_genesis(genesis))
    }

    fn executed_block(
        header: Header,
        state: BundleState,
        hashed_state: HashedPostState,
        trie_updates: TrieUpdates,
    ) -> ExecutedBlock<ArbPrimitives> {
        let block = SealedBlock::<ArbBlock>::seal_slow(alloy_consensus::Block {
            header,
            body: Default::default(),
        })
        .try_recover()
        .unwrap();
        ExecutedBlock::new(
            Arc::new(block),
            Arc::new(BlockExecutionOutput {
                result: BlockExecutionResult {
                    receipts: vec![],
                    requests: Default::default(),
                    gas_used: 0,
                    blob_gas_used: 0,
                },
                state,
            }),
            ComputedTrieData::new(
                Arc::new(hashed_state.into_sorted()),
                Arc::new(trie_updates.into_sorted()),
            ),
        )
    }

    #[test]
    fn wipe_reverts_include_untouched_plain_slots() -> eyre::Result<()> {
        let address = address!("0000000000000000000000000000000000001234");
        let plain_a = b256!("0000000000000000000000000000000000000000000000000000000000000042");
        let plain_b = b256!("0000000000000000000000000000000000000000000000000000000000000043");
        let value_a = U256::from(11);
        let value_b = U256::from(22);

        let temp = tempfile::tempdir()?;
        let preimages = SlotPreimages::open(temp.path())?;
        preimages
            .insert_preimages(&[(keccak256(plain_a), plain_a), (keccak256(plain_b), plain_b)])?;

        let mut slots = B256Map::default();
        slots.insert(keccak256(plain_a), value_a);
        slots.insert(keccak256(plain_b), value_b);
        let mut tracked = B256Map::default();
        tracked.insert(keccak256(address), TrackedStorage { address, slots });

        let mut revert = AccountRevert {
            wipe_storage: true,
            ..Default::default()
        };
        // Model destroy-and-recreate bookkeeping: Reth must replace `Destroyed` with the real
        // pre-block value while also adding the untouched slot.
        revert
            .storage
            .insert(U256::from_be_bytes(plain_b.0), RevertToSlot::Destroyed);
        let mut output = BlockExecutionOutput::<()>::default();
        output.state.reverts = Reverts::new(vec![vec![(address, revert)]]);

        inject_plain_wipe_reverts(&mut output, &tracked, &preimages.reader()?)?;

        let revert = &output.state.reverts[0][0].1;
        assert_eq!(
            revert.storage.get(&U256::from_be_bytes(plain_a.0)),
            Some(&RevertToSlot::Some(value_a))
        );
        assert_eq!(
            revert.storage.get(&U256::from_be_bytes(plain_b.0)),
            Some(&RevertToSlot::Some(value_b))
        );
        Ok(())
    }

    #[test]
    fn preimage_lookup_inserts_only_missing_and_rejects_corruption() -> eyre::Result<()> {
        let plain_a = b256!("0000000000000000000000000000000000000000000000000000000000000042");
        let plain_b = b256!("0000000000000000000000000000000000000000000000000000000000000043");
        let hashed_a = keccak256(plain_a);
        let hashed_b = keccak256(plain_b);

        let temp = tempfile::tempdir()?;
        let preimages = SlotPreimages::open(temp.path())?;
        preimages.insert_preimages(&[(hashed_a, plain_a)])?;

        assert_eq!(
            missing_preimages(&preimages, vec![(hashed_a, plain_a), (hashed_b, plain_b)])?,
            vec![(hashed_b, plain_b)]
        );

        let err = missing_preimages(&preimages, vec![(hashed_a, plain_b)]).unwrap_err();
        assert!(err.to_string().contains("corrupt slot preimage"));
        Ok(())
    }

    #[test]
    fn prior_unpersisted_updates_feed_a_later_wipe() {
        let address = address!("0000000000000000000000000000000000001234");
        let hashed_address = keccak256(address);
        let old_slot = keccak256(b256!(
            "0000000000000000000000000000000000000000000000000000000000000042"
        ));
        let new_slot = keccak256(b256!(
            "0000000000000000000000000000000000000000000000000000000000000043"
        ));
        let mut base = B256Map::default();
        base.insert(old_slot, U256::from(1));
        let mut tracked = B256Map::default();
        tracked.insert(
            hashed_address,
            TrackedStorage {
                address,
                slots: base,
            },
        );

        let mut storages = B256Map::default();
        storages.insert(
            hashed_address,
            HashedStorageSorted {
                storage_slots: vec![(old_slot, U256::ZERO), (new_slot, U256::from(2))],
                wiped: false,
            },
        );
        apply_hashed_updates(&mut tracked, &HashedPostStateSorted::new(vec![], storages));

        let storage = &tracked[&hashed_address].slots;
        assert!(!storage.contains_key(&old_slot));
        assert_eq!(storage.get(&new_slot), Some(&U256::from(2)));
    }

    #[test]
    fn arbos_twenty_uses_non_destructive_selfdestruct_semantics() {
        let mut legacy = Header::default();
        ArbHeaderInfo {
            arbos_format_version: 19,
            ..Default::default()
        }
        .update_header(&mut legacy);
        let mut cancun = Header::default();
        ArbHeaderInfo {
            arbos_format_version: 20,
            ..Default::default()
        }
        .update_header(&mut cancun);

        assert!(
            !ArbSpecId::from_arbos_version(
                ArbHeaderInfo::decode_header(&legacy)
                    .unwrap()
                    .arbos_format_version
            )
            .is_enabled_in(ArbSpecId::ARBOS_20)
        );
        assert!(
            ArbSpecId::from_arbos_version(
                ArbHeaderInfo::decode_header(&cancun)
                    .unwrap()
                    .arbos_format_version
            )
            .is_enabled_in(ArbSpecId::ARBOS_20)
        );
    }

    #[test]
    fn storage_v2_wipe_persists_plain_changeset_and_unwinds() -> eyre::Result<()> {
        let factory = create_test_provider_factory_with_node_types::<TestArbNode>(test_chain_spec());
        let genesis_hash = init_genesis_with_settings(&factory, StorageSettings::v2())?;

        let (_finished_exex_height_tx, finished_exex_height_rx) =
            tokio::sync::watch::channel(FinishedExExHeight::NoExExs);
        let pruner =
            Pruner::new_with_factory(factory.clone(), vec![], 5, 0, None, finished_exex_height_rx);
        let (metrics_tx, _metrics_rx) = tokio::sync::mpsc::unbounded_channel();
        let (persistence, persistence_guard) =
            spawn_persistence(factory.clone(), pruner, metrics_tx);

        let address = address!("0000000000000000000000000000000000001234");
        let slot_a = U256::from(0x42);
        let slot_b = U256::from(0x43);
        let value_a = U256::from(11);
        let value_b = U256::from(22);
        let info = AccountInfo {
            nonce: 1,
            balance: U256::from(1),
            ..Default::default()
        };

        let initial_state = BundleState::builder(1..=1)
            .state_present_account_info(address, info.clone())
            .state_storage(
                address,
                HashMap::from_iter([
                    (slot_a, (U256::ZERO, value_a)),
                    (slot_b, (U256::ZERO, value_b)),
                ]),
            )
            .revert_account_info(1, address, Some(None))
            .revert_storage(1, address, vec![(slot_a, U256::ZERO), (slot_b, U256::ZERO)])
            .build();
        let initial_hashed =
            HashedPostState::from_bundle_state::<KeccakKeyHasher>(initial_state.state.iter());
        let state_provider = factory.latest()?;
        let (initial_root, initial_updates) =
            state_provider.state_root_with_updates(initial_hashed.clone())?;
        drop(state_provider);
        let initial = executed_block(
            arb_header(1, genesis_hash, initial_root),
            initial_state,
            initial_hashed,
            initial_updates,
        );
        let initial_hash = initial.recovered_block.hash();
        let (response_tx, response_rx) = crossbeam_channel::bounded(1);
        persistence.save_blocks(
            SaveBlocksInput::new(vec![initial.clone()], 0, 0, 1, 1),
            response_tx,
        )?;
        response_rx.recv_timeout(std::time::Duration::from_secs(10))?;

        let mut destroyed_state = BundleState::builder(2..=2)
            .state_original_account_info(address, info.clone())
            .state_address(address)
            .revert_account_info(2, address, Some(Some(info.clone())))
            // Model revm only retaining one touched key. The untouched key must be recovered from
            // HashedStorages through the preimage sidecar.
            .revert_storage(2, address, vec![(slot_a, value_a)])
            .build();
        let destroyed = destroyed_state.state.get_mut(&address).unwrap();
        destroyed.info = None;
        destroyed.status = AccountStatus::Destroyed;
        let destroyed_revert = &mut destroyed_state.reverts[0][0].1;
        destroyed_revert.wipe_storage = true;
        destroyed_revert
            .storage
            .insert(slot_a, RevertToSlot::Destroyed);

        let destroyed_hashed =
            HashedPostState::from_bundle_state::<KeccakKeyHasher>(destroyed_state.state.iter());
        let state_provider = factory.latest()?;
        let (destroyed_root, destroyed_updates) =
            state_provider.state_root_with_updates(destroyed_hashed.clone())?;
        drop(state_provider);
        let destroyed = executed_block(
            arb_header(2, initial_hash, destroyed_root),
            destroyed_state,
            destroyed_hashed,
            destroyed_updates,
        );

        // Return to the pre-batch state, then persist create -> wipe together. The proxy must
        // reconstruct block 2's pre-state from block 1's unpersisted hashed updates.
        let (response_tx, response_rx) = crossbeam_channel::bounded(1);
        persistence.remove_blocks_above(0, response_tx)?;
        response_rx.recv_timeout(std::time::Duration::from_secs(10))?;

        let (response_tx, response_rx) = crossbeam_channel::bounded(1);
        persistence.save_blocks(
            SaveBlocksInput::new(vec![initial, destroyed], 0, 0, 2, 2),
            response_tx,
        )?;
        response_rx.recv_timeout(std::time::Duration::from_secs(10))?;

        let changeset = factory.static_file_provider().storage_changeset(2)?;
        assert!(changeset.iter().any(|(block_address, entry)| {
            block_address.address() == address
                && entry.key == B256::from(slot_a.to_be_bytes())
                && entry.value == value_a
        }));
        assert!(changeset.iter().any(|(block_address, entry)| {
            block_address.address() == address
                && entry.key == B256::from(slot_b.to_be_bytes())
                && entry.value == value_b
        }));

        let state = factory.latest()?;
        assert_eq!(state.basic_account(&address)?, None);
        assert_eq!(
            state.storage(address, B256::from(slot_a.to_be_bytes()))?,
            None
        );
        assert_eq!(
            state.storage(address, B256::from(slot_b.to_be_bytes()))?,
            None
        );
        drop(state);

        let (response_tx, response_rx) = crossbeam_channel::bounded(1);
        persistence.remove_blocks_above(1, response_tx)?;
        response_rx.recv_timeout(std::time::Duration::from_secs(10))?;

        let state = factory.latest()?;
        assert_eq!(
            state.basic_account(&address)?.map(|account| account.nonce),
            Some(1)
        );
        assert_eq!(
            state.storage(address, B256::from(slot_a.to_be_bytes()))?,
            Some(value_a)
        );
        assert_eq!(
            state.storage(address, B256::from(slot_b.to_be_bytes()))?,
            Some(value_b)
        );
        assert_eq!(state.state_root(HashedPostState::default())?, initial_root);
        drop(state);
        drop(persistence);
        drop(persistence_guard);
        Ok(())
    }

    #[test]
    fn storage_v2_wipe_fails_closed_when_snapshot_preimage_is_missing() -> eyre::Result<()> {
        let factory = create_test_provider_factory_with_node_types::<TestArbNode>(test_chain_spec());
        let genesis_hash = init_genesis_with_settings(&factory, StorageSettings::v2())?;

        let address = address!("0000000000000000000000000000000000001234");
        let slot_a = U256::from(0x42);
        let slot_b = U256::from(0x43);
        let value_a = U256::from(11);
        let value_b = U256::from(22);
        let info = AccountInfo {
            nonce: 1,
            balance: U256::from(1),
            ..Default::default()
        };
        let initial_state = BundleState::builder(1..=1)
            .state_present_account_info(address, info.clone())
            .state_storage(
                address,
                HashMap::from_iter([
                    (slot_a, (U256::ZERO, value_a)),
                    (slot_b, (U256::ZERO, value_b)),
                ]),
            )
            .revert_account_info(1, address, Some(None))
            .revert_storage(1, address, vec![(slot_a, U256::ZERO), (slot_b, U256::ZERO)])
            .build();
        let initial_hashed =
            HashedPostState::from_bundle_state::<KeccakKeyHasher>(initial_state.state.iter());
        let state_provider = factory.latest()?;
        let (initial_root, initial_updates) =
            state_provider.state_root_with_updates(initial_hashed.clone())?;
        drop(state_provider);
        let initial = executed_block(
            arb_header(1, genesis_hash, initial_root),
            initial_state,
            initial_hashed,
            initial_updates,
        );
        let initial_hash = initial.recovered_block.hash();

        // Persist the snapshot-like base state without the proxy, so its hashed slots exist while
        // the auxiliary plain-key sidecar remains empty.
        let provider = factory.database_provider_rw()?;
        let input = SaveBlocksInput::new(vec![initial], 0, 0, 1, 1);
        provider.save_blocks(&input)?;
        provider.commit()?;

        let mut destroyed_state = BundleState::builder(2..=2)
            .state_original_account_info(address, info.clone())
            .state_address(address)
            .revert_account_info(2, address, Some(Some(info)))
            // Only slot A is present in revm bookkeeping. Recovering untouched slot B must fail.
            .revert_storage(2, address, vec![(slot_a, value_a)])
            .build();
        let destroyed = destroyed_state.state.get_mut(&address).unwrap();
        destroyed.info = None;
        destroyed.status = AccountStatus::Destroyed;
        let destroyed_revert = &mut destroyed_state.reverts[0][0].1;
        destroyed_revert.wipe_storage = true;
        destroyed_revert
            .storage
            .insert(slot_a, RevertToSlot::Destroyed);
        let destroyed_hashed =
            HashedPostState::from_bundle_state::<KeccakKeyHasher>(destroyed_state.state.iter());
        let state_provider = factory.latest()?;
        let (destroyed_root, destroyed_updates) =
            state_provider.state_root_with_updates(destroyed_hashed.clone())?;
        drop(state_provider);
        let destroyed = executed_block(
            arb_header(2, initial_hash, destroyed_root),
            destroyed_state,
            destroyed_hashed,
            destroyed_updates,
        );

        let (_finished_exex_height_tx, finished_exex_height_rx) =
            tokio::sync::watch::channel(FinishedExExHeight::NoExExs);
        let pruner =
            Pruner::new_with_factory(factory.clone(), vec![], 5, 0, None, finished_exex_height_rx);
        let (metrics_tx, _metrics_rx) = tokio::sync::mpsc::unbounded_channel();
        let (persistence, persistence_guard) =
            spawn_persistence(factory.clone(), pruner, metrics_tx);

        let (response_tx, response_rx) = crossbeam_channel::bounded(1);
        persistence.save_blocks(
            SaveBlocksInput::new(vec![destroyed], 1, 1, 2, 2),
            response_tx,
        )?;
        assert!(matches!(
            response_rx.recv_timeout(std::time::Duration::from_secs(10)),
            Err(crossbeam_channel::RecvTimeoutError::Disconnected)
        ));

        assert_eq!(factory.provider()?.last_block_number()?, 1);
        let state = factory.latest()?;
        assert_eq!(
            state.storage(address, B256::from(slot_a.to_be_bytes()))?,
            Some(value_a)
        );
        assert_eq!(
            state.storage(address, B256::from(slot_b.to_be_bytes()))?,
            Some(value_b)
        );
        drop(state);
        drop(persistence);
        drop(persistence_guard);
        Ok(())
    }
}
