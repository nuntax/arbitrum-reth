//! `arb-reth snapshot import` / `arb-reth snapshot read`.
//!
//! ## `import`: import a Nitro state stream into reth MDBX.
//!
//! Reads a line-oriented state export produced by Nitro's state-dumper and writes
//! the accounts/bytecodes/storage directly into reth's `HashedAccounts`,
//! `HashedStorages`, and `Bytecodes` tables, then drives reth's state-root trie
//! computation to verify parity.
//!
//! ### Stream format
//!
//! ```text
//! A <accountHash:64hex> <nonce:dec> <balance:hex> <codeHash:64hex> <storageRoot:64hex>
//! C <codeHash:64hex> <code:hex>
//! S <slotHash:64hex> <value:hex>
//! ```
//!
//! - `A` lines start a new account; subsequent `S` lines belong to it.
//! - `C` lines appear anywhere and declare bytecode by its keccak hash.
//! - All hashes are 64-hex pre-keccak'ed keys (already the hashed representation).
//! - balance/value may be odd-length hex; parse with `U256::from_str_radix(tok, 16)`.
//!
//! ### Usage
//!
//! ```text
//! arb-reth snapshot import \
//!   --state /tmp/arb1_genesis_state.stream \
//!   --blocks /tmp/arb1_head_block.stream \
//!   --out   /tmp/arbreth-mdbx \
//!   --expect 0x7f2bfc4481d02bfcfc606ebb949384ef78d03a0f30a2dc9cccd652eb80926ae1
//! ```
//!
//! ## `read`: read hashed-state from a converted Arbitrum reth MDBX.
//!
//! Opens a read-only MDBX database (same layout as produced by `snapshot import`)
//! and, given an Ethereum address, prints account information read directly from the
//! hashed tables (`HashedAccounts`, `HashedStorages`, `Bytecodes`).
//!
//! ### Usage
//!
//! ```text
//! arb-reth snapshot read --db /tmp/arbreth-verify --addr 0xf124579b4d0a56cf720d601283f45d6ce4198279
//! arb-reth snapshot read --db /tmp/arbreth-verify --addr 0x0000000000000000000000000000000000000065
//! arb-reth snapshot read --db /tmp/arbreth-verify \
//!     --addr 0xe66092c38c2a56e63009946550407902934376da \
//!     --slot 0x0000000000000000000000000000000000000000000000000000000000000000
//! arb-reth snapshot read --db /tmp/arbreth-verify \
//!     --addr 0xe66092c38c2a56e63009946550407902934376da \
//!     --list-storage
//! ```

use std::{
    collections::HashSet,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use alloy_genesis::{ChainConfig, Genesis};
use alloy_primitives::{Address, B256, Bytes, U256, hex, keccak256};
use clap::Parser;
use reth_chainspec::ChainSpec;
#[cfg(test)]
use reth_chainspec::MAINNET;
use reth_db::{ClientVersion, init_db, mdbx::DatabaseArguments, open_db_read_only};
use reth_db_api::models::StorageSettings;
use reth_db_api::{
    cursor::{DbCursorRW, DbDupCursorRO},
    database::Database as RethDatabase,
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_node_types::NodeTypesWithDBAdapter;
use reth_primitives_traits::StorageEntry;
use reth_primitives_traits::{Account, Bytecode, SealedHeader};
use reth_provider::{
    DBProvider, MetadataWriter, ProviderFactory, StorageSettingsCache, TrieWriter,
    providers::{RocksDBProvider, StaticFileProvider},
};
use reth_prune_types::{PruneCheckpoint, PruneMode, PruneSegment};
use reth_tasks::Runtime;
use reth_trie::{IntermediateStateRootState, StateRoot as StateRootComputer, StateRootProgress};
use reth_trie_db::{
    DatabaseHashedCursorFactory, DatabaseStateRoot, DatabaseTrieCursorFactory, PackedKeyAdapter,
};

// Boot-wiring: write head header + checkpoints so ProviderFactory opens at the block.
use alloy_consensus::Header;
use alloy_rlp::Decodable;
use arb_revm::ArbSpecId;
use arbitrum_alloy_consensus::header::ArbHeaderInfo;
use reth_provider::{
    BlockNumReader, DatabaseProviderFactory, StageCheckpointWriter, StaticFileProviderFactory,
    StaticFileWriter,
};
use reth_stages::stages::slot_preimages::{SlotPreimages, SlotPreimagesReader};
use reth_stages_types::{StageCheckpoint, StageId};
use reth_static_file_types::StaticFileSegment;
use reth_storage_api::{HeaderProvider, PruneCheckpointReader, PruneCheckpointWriter};

use arb_reth_genesis::preimages::{MANIFEST_FILE, SlotPreimageManifest};

use crate::hashed_db::{
    KECCAK_EMPTY as HASHED_KECCAK_EMPTY, account_by_address, code_of, storage_at,
};

// Storage v2 keys trie nodes with `PackedKeyAdapter` (v1 used `LegacyKeyAdapter`). The state root
// is adapter-independent (the MPT hash of key→value), so the genesis root still validates; only the
// on-disk trie-node key encoding changes. The v2 flag must be cached on the factory *before* this
// runs, so `write_trie_updates` (which follows the cached settings) writes packed keys too.
type DbStateRoot<'a, TX> = StateRootComputer<
    DatabaseTrieCursorFactory<&'a TX, PackedKeyAdapter>,
    DatabaseHashedCursorFactory<&'a TX>,
>;

/// Number of storage writes (accounts + slots) to accumulate before committing
/// the MDBX transaction and opening a fresh one.  Bounds dirty-page growth on a
/// 2.6 GB stream.
const COMMIT_THRESHOLD: usize = 100_000;

/// Number of trie-update entries before we flush and restart with the saved
/// intermediate state (mirrors init.rs's STATE_ROOT_COMMIT_THRESHOLD).
const TRIE_COMMIT_THRESHOLD: u64 = 25_000;

/// keccak256 of the empty byte string.
/// If an account's codeHash equals this, bytecode_hash must be None.
const KECCAK_EMPTY: [u8; 32] =
    hex!("c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470");

use crate::ArbNode;
type ArbNodeTypesWithDB = NodeTypesWithDBAdapter<ArbNode, reth_db::DatabaseEnv>;

/// Number of preimages sorted and inserted in one auxiliary MDBX transaction.
const PREIMAGE_BATCH_SIZE: usize = 250_000;

const SNAPSHOT_IMPORT_MANIFEST_FILE: &str = "snapshot-import.json";
const SNAPSHOT_IMPORT_MANIFEST_VERSION: u64 = 1;

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
struct SnapshotImportManifest {
    version: u64,
    block_number: u64,
    block_hash: B256,
    state_root: B256,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SnapshotPreimagePolicy {
    /// Legacy destructive storage wipes are still possible. The current importer can prove a
    /// complete preimage set only for the canonical Arbitrum One Nitro genesis.
    CanonicalGenesisRequired,
    /// ArbOS 20 enables non-destructive selfdestruct semantics, so a forward sync cannot wipe
    /// storage that was inherited from the imported snapshot.
    NotRequired,
}

impl SnapshotPreimagePolicy {
    const fn requires_preimages(self) -> bool {
        matches!(self, Self::CanonicalGenesisRequired)
    }
}

/// Build Reth's native plaintext storage-slot preimage sidecar from a Nitro Classic export.
#[derive(Debug, Parser)]
#[command(
    name = "arb-snapshot-build-preimages",
    about = "Build the Storage V2 slot-preimage sidecar from a Nitro Classic state export"
)]
pub struct SnapshotBuildPreimagesArgs {
    /// Classic state export directory containing index.json and its referenced JSON files.
    #[arg(long, value_name = "DIR")]
    classic_state: PathBuf,

    /// Target reth datadir. The sidecar is written to `<out>/db/preimage`.
    #[arg(long, value_name = "DIR")]
    out: PathBuf,
}

/// Import a Nitro state stream into reth MDBX and verify the state root.
#[derive(Debug, Parser)]
#[command(
    name = "arb-snapshot-import",
    about = "Import a Nitro state stream into reth MDBX and verify the state root"
)]
pub struct SnapshotImportArgs {
    /// Path to the Nitro state stream file.
    #[arg(long, value_name = "FILE")]
    state: PathBuf,

    /// Output datadir (will be created if absent; `<out>/db`, `<out>/static_files`,
    /// `<out>/rocksdb` sub-directories are created automatically).
    #[arg(long, value_name = "DIR")]
    out: PathBuf,

    /// Expected state root (hex, with or without 0x prefix).
    #[arg(long, value_name = "HEX")]
    expect: String,

    /// Blocks stream (`H <num> <hash> <headerRLP>` records) containing the canonical snapshot
    /// head. Its block number and state root must match the Classic export and `--expect`.
    #[arg(long, value_name = "FILE")]
    blocks: PathBuf,
}

/// Read hashed state from a converted Arbitrum reth MDBX snapshot.
#[derive(Debug, Parser)]
#[command(
    name = "arb-snapshot-read",
    about = "Read hashed-state from a converted Arbitrum reth MDBX snapshot"
)]
pub struct SnapshotReadArgs {
    /// Path to the datadir (the directory that contains a `db/` sub-directory).
    #[arg(long, value_name = "DIR")]
    db: PathBuf,

    /// Ethereum address to look up (hex, with or without 0x prefix).
    #[arg(long, value_name = "ADDR")]
    addr: String,

    /// Optional storage slot to query (32-byte hex, with or without 0x prefix).
    #[arg(long, value_name = "SLOT")]
    slot: Option<String>,

    /// Enumerate all non-zero storage slots for this address and print their count.
    #[arg(long)]
    list_storage: bool,
}

/// Add missing history-boundary metadata to an existing snapshot-imported datadir.
#[derive(Debug, Parser)]
#[command(
    name = "repair-history",
    about = "Record the unavailable history prefix in a snapshot-imported datadir"
)]
pub struct SnapshotRepairHistoryArgs {
    /// Snapshot-imported datadir containing `db`, `static_files`, and `rocksdb`.
    #[arg(long, value_name = "DIR")]
    db: PathBuf,

    /// Blocks stream whose highest header is the imported snapshot head.
    #[arg(long, value_name = "FILE")]
    snapshot_head: PathBuf,
}

/// Construct the native `keccak256(slot) -> plain slot` sidecar used by Storage V2.
pub fn build_preimages(args: SnapshotBuildPreimagesArgs) -> eyre::Result<()> {
    let preimage_path = args.out.join("db").join("preimage");
    if preimage_path.exists() {
        eyre::bail!(
            "refusing to replace existing slot-preimage sidecar at {}",
            preimage_path.display()
        );
    }

    let db_path = args.out.join("db");
    std::fs::create_dir_all(&db_path)?;
    if let Some(stale) = find_staging_preimage_dir(&db_path)? {
        eyre::bail!(
            "incomplete slot-preimage build exists at {}; remove it after confirming no build is running",
            stale.display()
        );
    }
    let staging_path = db_path.join(".preimage.tmp");
    std::fs::create_dir(&staging_path)?;

    let build_result = (|| -> eyre::Result<_> {
        let store = SlotPreimages::open(&staging_path)?;
        let mut batch = Vec::with_capacity(PREIMAGE_BATCH_SIZE);
        let mut unique_mappings = 0u64;

        let stats = arb_reth_genesis::preimages::visit_arbitrum_one_slot_preimages(
            &args.classic_state,
            |hashed_slot, plain_slot| {
                batch.push((hashed_slot, plain_slot));
                if batch.len() == PREIMAGE_BATCH_SIZE {
                    unique_mappings += flush_preimage_batch(&store, &mut batch)? as u64;
                    tracing::info!(unique_mappings, "building slot-preimage sidecar");
                }
                Ok(())
            },
        )?;
        unique_mappings += flush_preimage_batch(&store, &mut batch)? as u64;

        let manifest = SlotPreimageManifest::new(stats, unique_mappings)?;
        drop(store);
        write_preimage_manifest(&staging_path, manifest)?;
        sync_directory(&staging_path)?;
        Ok((stats, unique_mappings))
    })();

    let (stats, unique_mappings) = match build_result {
        Ok(result) => result,
        Err(error) => {
            let _ = std::fs::remove_dir_all(&staging_path);
            return Err(error);
        }
    };

    if let Err(error) = std::fs::rename(&staging_path, &preimage_path) {
        let _ = std::fs::remove_dir_all(&staging_path);
        return Err(error.into());
    }
    sync_directory(&db_path)?;

    println!("preimage store       = {}", preimage_path.display());
    println!("next block           = {}", stats.next_block_number);
    println!("classic accounts     = {}", stats.classic_accounts);
    println!("classic storage slots= {}", stats.classic_slots);
    println!("address table entries= {}", stats.address_table_entries);
    println!("retryables           = {}", stats.retryables);
    println!("ArbOS accounts       = {}", stats.arbos_accounts);
    println!("ArbOS storage slots  = {}", stats.arbos_slots);
    println!("source slot mappings = {}", stats.total_slots());
    println!("unique slot mappings = {unique_mappings}");
    Ok(())
}

fn write_preimage_manifest(
    preimage_path: &Path,
    manifest: SlotPreimageManifest,
) -> eyre::Result<()> {
    let manifest_path = preimage_path.join(MANIFEST_FILE);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&manifest_path)?;
    serde_json::to_writer_pretty(&mut file, &manifest)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

fn read_preimage_manifest(preimage_path: &Path) -> eyre::Result<SlotPreimageManifest> {
    let manifest_path = preimage_path.join(MANIFEST_FILE);
    let manifest: SlotPreimageManifest =
        serde_json::from_reader(File::open(&manifest_path).map_err(|error| {
            eyre::eyre!(
                "slot-preimage completion manifest is missing at {}: {error}",
                manifest_path.display()
            )
        })?)?;
    manifest.validate()?;
    Ok(manifest)
}

fn sync_directory(path: &Path) -> eyre::Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn find_staging_preimage_dir(db_path: &Path) -> eyre::Result<Option<PathBuf>> {
    for entry in std::fs::read_dir(db_path)? {
        let entry = entry?;
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with(".preimage.tmp")
        {
            return Ok(Some(entry.path()));
        }
    }
    Ok(None)
}

fn flush_preimage_batch(
    store: &SlotPreimages,
    batch: &mut Vec<(B256, B256)>,
) -> eyre::Result<usize> {
    if batch.is_empty() {
        return Ok(0);
    }

    batch.sort_unstable_by_key(|(hashed_slot, _)| *hashed_slot);
    for pair in batch.windows(2) {
        if pair[0].0 == pair[1].0 && pair[0].1 != pair[1].1 {
            eyre::bail!(
                "conflicting slot preimages in batch for {:#x}: first={:#x}, second={:#x}",
                pair[0].0,
                pair[0].1,
                pair[1].1
            );
        }
    }
    batch.dedup_by_key(|(hashed_slot, _)| *hashed_slot);
    let reader = store.reader()?;
    let mut missing = Vec::with_capacity(batch.len());
    for &(hashed_slot, plain_slot) in batch.iter() {
        let actual_hash = keccak256(plain_slot);
        if actual_hash != hashed_slot {
            eyre::bail!(
                "invalid slot-preimage mapping: key={hashed_slot:#x}, plain={plain_slot:#x}, hash={actual_hash:#x}"
            );
        }
        match reader.get(&hashed_slot)? {
            Some(existing) if existing == plain_slot => {}
            Some(existing) => {
                eyre::bail!(
                    "conflicting slot preimage for {hashed_slot:#x}: existing={existing:#x}, new={plain_slot:#x}"
                );
            }
            None => missing.push((hashed_slot, plain_slot)),
        }
    }
    drop(reader);
    store.insert_preimages(&missing)?;
    let inserted = missing.len();
    batch.clear();
    Ok(inserted)
}

pub fn import(args: SnapshotImportArgs) -> eyre::Result<()> {
    let expected = parse_b256(&args.expect)
        .map_err(|error| eyre::eyre!("invalid --expect state root: {error}"))?;

    let db_path = args.out.join("db");
    let static_files_path = args.out.join("static_files");
    let rocksdb_path = args.out.join("rocksdb");
    let preimage_path = db_path.join("preimage");
    ensure_fresh_import_target(&args.out)?;

    let head = read_head_header(&args.blocks)?;
    let preimage_policy = validate_snapshot_identity(expected, &head)?;
    let preimage_manifest = if preimage_policy.requires_preimages() {
        if !preimage_path.join("mdbx.dat").is_file() {
            eyre::bail!(
                "slot-preimage sidecar is missing at {}; run `arb-reth snapshot build-preimages` first",
                preimage_path.display()
            );
        }
        Some(read_preimage_manifest(&preimage_path)?)
    } else {
        None
    };
    let preimages = preimage_policy
        .requires_preimages()
        .then(|| SlotPreimages::open(&preimage_path))
        .transpose()?;
    let preimage_reader = preimages.as_ref().map(SlotPreimages::reader).transpose()?;

    tracing::info!(path = ?args.state, "validating state stream before database creation");
    let state_stats = preflight_state_stream(&args.state, preimage_reader.as_ref())?;
    tracing::info!(
        accounts = state_stats.accounts,
        slots = state_stats.slots,
        bytecodes = state_stats.bytecodes,
        unique_slot_preimages = preimage_manifest.map(|manifest| manifest.unique_mappings),
        "state stream preflight complete"
    );

    std::fs::create_dir_all(&static_files_path)?;
    std::fs::create_dir_all(&rocksdb_path)?;

    tracing::info!(path = ?db_path, "opening MDBX");
    let db = init_db(&db_path, DatabaseArguments::new(ClientVersion::default()))?;

    // Inject the snapshot's real head header so genesis_hash() matches the DB and reth's launch
    // genesis-check passes.
    let chain_spec: Arc<ChainSpec> =
        arb_chain_spec_with_header(ARB_ONE_CHAIN_ID, head.2.clone(), head.1);

    let static_file_provider = StaticFileProvider::read_write(static_files_path.clone())?;
    let rocksdb_provider = RocksDBProvider::builder(&rocksdb_path)
        .with_default_tables()
        .build()
        .map_err(|e| eyre::eyre!("RocksDB open error: {e}"))?;
    let runtime = Runtime::test();

    let factory: ProviderFactory<ArbNodeTypesWithDB> = ProviderFactory::new(
        db,
        chain_spec,
        static_file_provider,
        rocksdb_provider,
        runtime,
    )
    .map_err(|e| eyre::eyre!("ProviderFactory::new: {e}"))?;

    // Emit a storage-v2 database (reth's default going forward; also the more natural fit for our
    // hashed-only import, since v2 treats the hashed-state tables as canonical). Cache the flag so
    // every provider (and `write_trie_updates`' `with_adapter!`) uses `PackedKeyAdapter`, and
    // persist it to metadata so the node reads v2 on boot (an unset flag defaults to v1).
    factory.set_storage_settings_cache(StorageSettings::v2());
    {
        let provider_rw = factory.database_provider_rw()?;
        provider_rw.write_storage_settings(StorageSettings::v2())?;
        provider_rw
            .commit()
            .map_err(|e| eyre::eyre!("persist storage settings: {e}"))?;
    }

    tracing::info!(path = ?args.state, "streaming state import (storage v2)");
    stream_import(&factory, &args.state, preimage_reader.as_ref())?;

    tracing::info!("computing state root (may take several minutes for large states)");
    let computed = compute_state_root_chunked(&factory)?;

    println!("computed  = {computed:#x}");
    println!("expected  = {expected:#x}");
    if computed != expected {
        eyre::bail!("state root mismatch: computed={computed:#x}, expected={expected:#x}");
    }
    println!("MATCH");

    tracing::info!(path = ?args.blocks, "writing head header + checkpoints");
    let (head_num, head_hash) = write_head_blocks(&factory, &args.blocks)?;
    verify_head(&factory, head_num, head_hash)?;
    // The injected-header chain spec means reth's launch genesis-check accepts this DB.
    verify_launch(&factory, head_hash)?;

    // The changeset segments were created in their fixed 500k slot (`_22000000_…`) but
    // `set_expected_block_start(head)` moved their header's expected range to start at `head`.
    // reth derives the on-disk filename from the header's expected range (via the index), so the
    // file must be renamed to match or every changeset read fails with a missing-file error. Do it
    // now, at the filesystem level, after all DB work: the factory is about to be dropped and the
    // node re-scans on boot.
    drop(factory);
    rename_changeset_files_to_header(&static_files_path)?;
    for path in [&db_path, &static_files_path, &rocksdb_path] {
        sync_directory(path)?;
    }
    write_snapshot_import_manifest(&args.out, &head)?;

    Ok(())
}

fn validate_snapshot_identity(
    expected: B256,
    head: &(u64, B256, Header),
) -> eyre::Result<SnapshotPreimagePolicy> {
    if head.2.number != head.0 || head.2.hash_slow() != head.1 {
        eyre::bail!("snapshot head contains an invalid number or block hash");
    }
    if head.2.state_root != expected {
        eyre::bail!(
            "snapshot head state root {:#x} does not match --expect {expected:#x}",
            head.2.state_root
        );
    }

    let info = ArbHeaderInfo::decode_header(&head.2)
        .map_err(|error| eyre::eyre!("decode snapshot head ArbOS version: {error}"))?;
    let spec = ArbSpecId::from_arbos_version(info.arbos_format_version);
    if spec.is_enabled_in(ArbSpecId::ARBOS_20) {
        return Ok(SnapshotPreimagePolicy::NotRequired);
    }

    if head.0 != arb_reth_genesis::arbitrum_one::GENESIS_BLOCK_NUMBER
        || head.1 != arb_reth_genesis::arbitrum_one::GENESIS_BLOCK_HASH
        || expected != arb_reth_genesis::arbitrum_one::GENESIS_STATE_ROOT
    {
        eyre::bail!(
            "pre-ArbOS 20 snapshot at block {} requires a complete slot-preimage set for that exact snapshot; currently only the canonical Arbitrum One Nitro genesis is supported",
            head.0
        );
    }
    Ok(SnapshotPreimagePolicy::CanonicalGenesisRequired)
}

fn write_snapshot_import_manifest(
    out: &Path,
    head: &(u64, B256, Header),
) -> eyre::Result<()> {
    let manifest = SnapshotImportManifest {
        version: SNAPSHOT_IMPORT_MANIFEST_VERSION,
        block_number: head.0,
        block_hash: head.1,
        state_root: head.2.state_root,
    };
    validate_snapshot_import_manifest(manifest, head)?;

    let path = out.join(SNAPSHOT_IMPORT_MANIFEST_FILE);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)?;
    serde_json::to_writer_pretty(&mut file, &manifest)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    sync_directory(out)?;
    Ok(())
}

fn validate_snapshot_import_manifest(
    manifest: SnapshotImportManifest,
    head: &(u64, B256, Header),
) -> eyre::Result<()> {
    if manifest.version != SNAPSHOT_IMPORT_MANIFEST_VERSION {
        eyre::bail!(
            "unsupported snapshot import manifest version {}, expected {SNAPSHOT_IMPORT_MANIFEST_VERSION}",
            manifest.version,
        );
    }
    if manifest.block_number != head.0
        || manifest.block_hash != head.1
        || manifest.state_root != head.2.state_root
    {
        eyre::bail!("snapshot import manifest does not match the supplied head stream");
    }
    if head.2.number != head.0 || head.2.hash_slow() != head.1 {
        eyre::bail!("snapshot head stream contains an invalid number or block hash");
    }
    Ok(())
}

/// Refuse to launch a new-format snapshot datadir unless its import completed successfully.
pub(crate) fn validate_snapshot_import_for_launch(
    out: &Path,
    head: &(u64, B256, Header),
) -> eyre::Result<()> {
    let preimage_path = out.join("db/preimage");
    let import_manifest_path = out.join(SNAPSHOT_IMPORT_MANIFEST_FILE);
    if !import_manifest_path.is_file() && !preimage_path.join(MANIFEST_FILE).is_file() {
        // Older imports predate completion manifests. Preserve their existing launch behavior.
        return Ok(());
    }

    let manifest: SnapshotImportManifest =
        serde_json::from_reader(File::open(&import_manifest_path).map_err(|error| {
            eyre::eyre!(
                "snapshot import is incomplete: missing completion manifest at {}: {error}",
                import_manifest_path.display()
            )
        })?)?;
    validate_snapshot_import_manifest(manifest, head)?;

    let preimage_policy = validate_snapshot_identity(head.2.state_root, head)?;
    if preimage_policy.requires_preimages() {
        if !preimage_path.join("mdbx.dat").is_file() {
            eyre::bail!(
                "snapshot slot-preimage database is missing at {}",
                preimage_path.display()
            );
        }
        read_preimage_manifest(&preimage_path)?;
    }
    Ok(())
}

pub(crate) fn ensure_fresh_import_target(out: &Path) -> eyre::Result<()> {
    let import_manifest = out.join(SNAPSHOT_IMPORT_MANIFEST_FILE);
    if import_manifest.exists() {
        eyre::bail!(
            "snapshot import requires a fresh target; completion manifest already exists at {}",
            import_manifest.display()
        );
    }
    let db_path = out.join("db");
    if db_path.exists() {
        for entry in std::fs::read_dir(&db_path)? {
            let entry = entry?;
            if entry.file_name() != "preimage" {
                eyre::bail!(
                    "snapshot import requires a fresh target; unexpected path exists at {}",
                    entry.path().display()
                );
            }
        }
    }

    for path in [out.join("static_files"), out.join("rocksdb")] {
        if path.exists() {
            eyre::bail!(
                "snapshot import requires a fresh target; remove the previous import at {}",
                path.display()
            );
        }
    }
    Ok(())
}

/// Repair an older import that predates snapshot history-boundary checkpoints.
///
/// This only writes two prune-checkpoint rows. It does not modify state, blocks, trie data, or the
/// snapshot header, and is idempotent for the same snapshot head.
pub fn repair_history(args: SnapshotRepairHistoryArgs) -> eyre::Result<()> {
    let (head_num, head_hash, header) = read_head_header(&args.snapshot_head)?;
    let db = init_db(
        args.db.join("db"),
        DatabaseArguments::new(ClientVersion::default()),
    )?;
    let static_files = StaticFileProvider::read_write(args.db.join("static_files"))?;
    let rocksdb = RocksDBProvider::builder(args.db.join("rocksdb"))
        .with_default_tables()
        .build()
        .map_err(|error| eyre::eyre!("RocksDB open error: {error}"))?;
    let factory: ProviderFactory<ArbNodeTypesWithDB> = ProviderFactory::new(
        db,
        arb_chain_spec_with_header(ARB_ONE_CHAIN_ID, header, head_hash),
        static_files,
        rocksdb,
        Runtime::test(),
    )?;
    factory.set_storage_settings_cache(StorageSettings::v2());

    {
        let provider = factory.provider()?;
        let best = provider.best_block_number()?;
        if best < head_num {
            return Err(eyre::eyre!(
                "database head {best} is below snapshot head {head_num}"
            ));
        }
        let actual_hash = provider
            .sealed_header(head_num)?
            .ok_or_else(|| eyre::eyre!("snapshot header {head_num} is missing from the database"))?
            .hash();
        if actual_hash != head_hash {
            return Err(eyre::eyre!(
                "snapshot header hash mismatch at {head_num}: database={actual_hash:#x}, stream={head_hash:#x}"
            ));
        }

        for segment in [PruneSegment::AccountHistory, PruneSegment::StorageHistory] {
            if let Some(existing) = provider.get_prune_checkpoint(segment)?
                && existing.block_number.is_some_and(|block| block > head_num)
            {
                return Err(eyre::eyre!(
                    "refusing to move {segment} checkpoint backward from {:?} to {head_num}",
                    existing.block_number
                ));
            }
        }
    }

    let provider = factory.database_provider_rw()?;
    write_snapshot_history_boundaries(&provider, head_num)?;
    provider.commit()?;

    println!("snapshot history boundary = {head_num}");
    println!("account history checkpoint: OK");
    println!("storage history checkpoint: OK");
    Ok(())
}

/// Rename the changeset static-file segments so their on-disk name matches the `expected_block_range`
/// recorded in their header. The import creates them in the fixed 500k slot (e.g. `_22000000_…`) and
/// then `set_expected_block_start(head)` shifts the header's expected start to `head`; reth resolves
/// the file path from the header's expected range, so the name must agree or reads miss the file.
/// Idempotent: only renames when the name doesn't already match the header.
fn rename_changeset_files_to_header(static_files: &std::path::Path) -> eyre::Result<()> {
    for seg in ["account-change-sets", "storage-change-sets"] {
        let prefix = format!("static_file_{seg}_");
        let data_name = std::fs::read_dir(static_files)?
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .find(|n| n.starts_with(&prefix) && !n.contains('.'));
        let Some(data_name) = data_name else { continue };
        let conf = std::fs::read(static_files.join(format!("{data_name}.conf")))?;
        if conf.len() < 24 {
            continue;
        }
        let exp_start = u64::from_le_bytes(conf[8..16].try_into().unwrap());
        let exp_end = u64::from_le_bytes(conf[16..24].try_into().unwrap());
        let want = format!("static_file_{seg}_{exp_start}_{exp_end}");
        if data_name == want {
            continue; // already matches header
        }
        for ext in ["", ".conf", ".off", ".csoff"] {
            let src = static_files.join(format!("{data_name}{ext}"));
            let dst = static_files.join(format!("{want}{ext}"));
            if src.exists() && src != dst {
                std::fs::rename(&src, &dst)?;
            }
        }
        tracing::info!(seg, from = %data_name, to = %want, "renamed changeset file to match header expected range");
    }
    Ok(())
}

/// Arbitrum One chain id.
const ARB_ONE_CHAIN_ID: u64 = 42161;

/// Build a `ChainSpec` whose genesis header IS the snapshot's head header (number/hash/stateRoot),
/// so `chain_spec.genesis_hash()` equals the DB's genesis block hash and reth's launch
/// genesis-validation passes. We can't use the alloc-based `from_genesis` path (we have hashed
/// state, no alloc), so we override the public `genesis_header` field directly.
fn arb_chain_spec_with_header(chain_id: u64, header: Header, hash: B256) -> Arc<ChainSpec> {
    // London-format, all pre-London forks at 0 (post-London EVM features are ArbOS-version-gated
    // via the header mixHash, not chain-spec forks). Mirrors `genesis::arb_chain_spec`.
    let config = ChainConfig {
        chain_id,
        homestead_block: Some(0),
        dao_fork_support: false,
        eip150_block: Some(0),
        eip155_block: Some(0),
        eip158_block: Some(0),
        byzantium_block: Some(0),
        constantinople_block: Some(0),
        petersburg_block: Some(0),
        istanbul_block: Some(0),
        muir_glacier_block: Some(0),
        berlin_block: Some(0),
        london_block: Some(0),
        ..Default::default()
    };
    let genesis = Genesis {
        config,
        number: Some(header.number),
        ..Default::default()
    };
    let mut spec = ChainSpec::from_genesis(genesis);
    // Override the computed (alloc-derived, wrong) genesis header with the real one.
    spec.genesis_header = SealedHeader::new(header, hash);
    Arc::new(spec)
}

/// Read the highest-numbered `H <num> <hash> <headerRLP>` record (the head/genesis header).
fn read_head_header(path: &Path) -> eyre::Result<(u64, B256, Header)> {
    let reader = std::io::BufReader::new(File::open(path)?);
    let mut best: Option<(u64, B256, Header)> = None;
    for (line_index, line) in reader.lines().enumerate() {
        let Some((num, hash, header)) = parse_header_record(&line?, line_index + 1)? else {
            continue;
        };
        if best.as_ref().map(|(n, ..)| num >= *n).unwrap_or(true) {
            best = Some((num, hash, header));
        }
    }
    best.ok_or_else(|| eyre::eyre!("no H records in {path:?}"))
}

fn parse_header_record(
    line: &str,
    line_number: usize,
) -> eyre::Result<Option<(u64, B256, Header)>> {
    let mut parts = line.split_whitespace();
    let Some(tag) = parts.next() else {
        return Ok(None);
    };
    if matches!(tag, "B" | "R") {
        let num: u64 = parts
            .next()
            .ok_or_else(|| eyre::eyre!("{tag}: missing number at line {line_number}"))?
            .parse()
            .map_err(|error| {
                eyre::eyre!("{tag}: bad number at line {line_number}: {error}")
            })?;
        let encoded = hex::decode(
            parts
                .next()
                .ok_or_else(|| eyre::eyre!("{tag}: missing RLP at line {line_number}"))?,
        )?;
        if parts.next().is_some() {
            eyre::bail!("{tag}: unexpected trailing fields at line {line_number}");
        }

        let mut input = encoded.as_slice();
        let rlp_header = alloy_rlp::Header::decode(&mut input).map_err(|error| {
            eyre::eyre!("decode {tag} record for block {num} at line {line_number}: {error}")
        })?;
        if !rlp_header.list || input.len() != rlp_header.payload_length {
            eyre::bail!("invalid {tag} RLP for block {num} at line {line_number}");
        }
        return Ok(None);
    }
    if tag != "H" {
        eyre::bail!("unknown block record {tag:?} at line {line_number}");
    }
    let num: u64 = parts
        .next()
        .ok_or_else(|| eyre::eyre!("H: missing number at line {line_number}"))?
        .parse()
        .map_err(|error| eyre::eyre!("H: bad number at line {line_number}: {error}"))?;
    let hash = parse_b256(
        parts
            .next()
            .ok_or_else(|| eyre::eyre!("H: missing hash at line {line_number}"))?,
    )?;
    let rlp = hex::decode(
        parts
            .next()
            .ok_or_else(|| eyre::eyre!("H: missing headerRLP at line {line_number}"))?,
    )?;
    if parts.next().is_some() {
        eyre::bail!("H: unexpected trailing fields at line {line_number}");
    }
    let mut input = rlp.as_slice();
    let header = Header::decode(&mut input)
        .map_err(|error| eyre::eyre!("decode header {num} at line {line_number}: {error}"))?;
    if !input.is_empty() {
        eyre::bail!("trailing bytes after header RLP at line {line_number}");
    }
    if header.number != num {
        eyre::bail!(
            "header number mismatch: record={num}, decoded={}",
            header.number
        );
    }
    let computed_hash = header.hash_slow();
    if computed_hash != hash {
        eyre::bail!("header hash mismatch at {num}: record={hash:#x}, decoded={computed_hash:#x}");
    }
    Ok(Some((num, hash, header)))
}

/// Launch-acceptance gate: runs `init_genesis` with validation against the converted DB.
/// With the injected-header chain spec it must find the genesis present (no GenesisHashMismatch,
/// no re-write), confirming a node would open this DB cleanly.
fn verify_launch(
    factory: &ProviderFactory<ArbNodeTypesWithDB>,
    head_hash: B256,
) -> eyre::Result<()> {
    use reth_db_common::init::init_genesis_with_settings_and_validate;
    let got = init_genesis_with_settings_and_validate(factory, StorageSettings::v2(), true)
        .map_err(|e| eyre::eyre!("init_genesis (launch genesis check) rejected the DB: {e}"))?;
    println!("init_genesis (validate=true) = {got:#x}");
    if got == head_hash {
        println!("LAUNCH OK");
        Ok(())
    } else {
        Err(eyre::eyre!(
            "init_genesis returned {got:#x}, expected {head_hash:#x}"
        ))
    }
}

/// Write every `H <num> <hash> <headerRLP>` record into the static-file Headers segment plus
/// `HeaderNumbers`/`BlockBodyIndices`, then set all stage checkpoints to the highest block so a
/// `ProviderFactory` reports it as the head. Returns `(head_number, head_hash)`.
fn write_head_blocks(
    factory: &ProviderFactory<ArbNodeTypesWithDB>,
    path: &Path,
) -> eyre::Result<(u64, B256)> {
    let provider_rw = factory.database_provider_rw()?;
    let sfp = provider_rw.static_file_provider();

    let reader = std::io::BufReader::new(File::open(path)?);
    let mut head_num = 0u64;
    let mut head_hash = B256::ZERO;
    let mut count = 0u64;

    for (line_index, line) in reader.lines().enumerate() {
        let Some((num, hash, header)) = parse_header_record(&line?, line_index + 1)? else {
            continue;
        };

        // Genesis TD == difficulty for the first block (Arbitrum difficulty is 1).
        let mut writer = sfp.get_writer(num, StaticFileSegment::Headers)?;
        if num > 0 {
            writer.user_header_mut().set_block_range(num, num);
            writer.append_header_direct(&header, header.difficulty, &hash)?;
        } else {
            writer.append_header(&header, &hash)?;
        }
        writer.commit()?;

        provider_rw
            .tx_ref()
            .put::<tables::HeaderNumbers>(hash, num)?;
        provider_rw
            .tx_ref()
            .put::<tables::BlockBodyIndices>(num, Default::default())?;

        if num >= head_num {
            head_num = num;
            head_hash = hash;
        }
        count += 1;
    }

    // Initialize the per-block static-file segments to the head block. Without this, reth's launch
    // `check_consistency` sees those segments empty (highest block None) while the stage checkpoints
    // say `head_num`, and unwinds to block 0. The head block has no txs/receipts, so the segments
    // stay empty; only the block range / expected start needs setting. Mirrors reth `init_genesis`'s
    // non-zero-genesis v2 path (db-common init.rs): Receipts/Transactions/TransactionSenders use
    // `set_block_range`; the changeset segments use `set_expected_block_start` (their block range is
    // established lazily on the first append, but `next_block_number` must start at `head_num`, else
    // the first per-block append during sync tries to write block 0).
    sfp.get_writer(head_num, StaticFileSegment::Receipts)?
        .user_header_mut()
        .set_block_range(head_num, head_num);
    sfp.get_writer(head_num, StaticFileSegment::Transactions)?
        .user_header_mut()
        .set_block_range(head_num, head_num);
    sfp.get_writer(head_num, StaticFileSegment::TransactionSenders)?
        .user_header_mut()
        .set_block_range(head_num, head_num);
    // Changeset segments need all three of these to be true, or the DB is broken for stock reth's
    // v2 unwind/rewind (all invisible to forward sync; hashed state / state root are unaffected):
    //   (a) highest_static_file_block == head, or launch `check_consistency` sees highest=None while
    //       the Execution checkpoint says head and unwinds to block 0 (panic).
    //   (b) expected_block_start == the actual data start, or `truncate_changesets` (which keys off
    //       expected_block_start, = the fixed 500k slot 22000000) over-counts and corrupts the
    //       offset map on every unwind.
    //   (c) csoff[0] must map to `head`, or `changeset_offset_index(N) = N - block_range.start` is
    //       shifted (genesis carries no changeset, so a naive first-append lands csoff[0] at head+1).
    // We satisfy all three by giving genesis an explicit empty changeset entry (matching reth's
    // init_genesis model): `set_expected_block_start(head)` aligns (b), and appending an empty
    // changeset for `head` sets block_range=[head,head] with csoff[0]=head, giving highest=head (a)
    // and an aligned map (c). The file is then renamed to match its new expected range.
    for seg in [
        StaticFileSegment::AccountChangeSets,
        StaticFileSegment::StorageChangeSets,
    ] {
        let mut w = sfp.get_writer(head_num, seg)?;
        w.user_header_mut().set_expected_block_start(head_num);
        match seg {
            StaticFileSegment::AccountChangeSets => {
                w.append_account_changeset(Vec::new(), head_num)?
            }
            StaticFileSegment::StorageChangeSets => {
                w.append_storage_changeset(Vec::new(), head_num)?
            }
            _ => unreachable!(),
        }
        w.commit()?;
    }

    // Mark every stage complete at the head so reth treats the DB as synced to that block.
    let cp = StageCheckpoint::new(head_num);
    for stage in StageId::ALL {
        provider_rw.save_stage_checkpoint(stage, cp)?;
    }
    write_snapshot_history_boundaries(&provider_rw, head_num)?;
    provider_rw.commit()?;
    tracing::info!(count, head_num, ?head_hash, "wrote headers + checkpoints");
    Ok((head_num, head_hash))
}

/// Record that account and storage history before the imported snapshot head is unavailable.
///
/// The imported hashed state is the complete state at `head_num`, but the import contains no
/// changesets or history index for earlier blocks. Without these checkpoints, a storage-v2
/// historical lookup can mistake an imported account for an account first created after its MDBX
/// snapshot when RocksDB is one persistence commit ahead. Marking the missing prefix makes that
/// lookup fall back to the imported hashed state.
fn write_snapshot_history_boundaries(
    provider: &impl PruneCheckpointWriter,
    head_num: u64,
) -> eyre::Result<()> {
    let checkpoint = PruneCheckpoint {
        block_number: Some(head_num),
        tx_number: None,
        prune_mode: PruneMode::before_inclusive(head_num),
    };

    for segment in [PruneSegment::AccountHistory, PruneSegment::StorageHistory] {
        provider.save_prune_checkpoint(segment, checkpoint)?;
    }

    Ok(())
}

/// Re-open the DB and assert the head is wired correctly (the boot-wiring gate).
fn verify_head(
    factory: &ProviderFactory<ArbNodeTypesWithDB>,
    head_num: u64,
    head_hash: B256,
) -> eyre::Result<()> {
    let provider = factory.provider()?;
    let best = provider.best_block_number()?;
    let sealed = HeaderProvider::sealed_header(&provider, head_num)?
        .ok_or_else(|| eyre::eyre!("no sealed header at {head_num}"))?;
    println!("best_block_number = {best}");
    println!("sealed_header({head_num}).hash() = {:#x}", sealed.hash());
    if best == head_num && sealed.hash() == head_hash {
        println!("HEAD OK");
        Ok(())
    } else {
        Err(eyre::eyre!(
            "head mismatch: best={best} (want {head_num}), hash={:#x} (want {head_hash:#x})",
            sealed.hash()
        ))
    }
}

#[derive(Debug)]
enum StateRecord {
    Account {
        account_hash: B256,
        account: Account,
    },
    Code {
        code_hash: B256,
        bytecode: Bytecode,
    },
    Storage {
        slot_hash: B256,
        value: U256,
    },
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct StateStreamStats {
    accounts: u64,
    slots: u64,
    bytecodes: u64,
}

fn parse_state_record(line: &str, line_number: usize) -> eyre::Result<Option<StateRecord>> {
    let line = line.trim();
    if line.is_empty() {
        return Ok(None);
    }

    let mut parts = line.split_whitespace();
    let tag = parts.next().expect("non-empty line");
    let mut next = |field: &str| {
        parts
            .next()
            .ok_or_else(|| eyre::eyre!("{tag}: missing {field} at line {line_number}"))
    };

    let record = match tag {
        "A" => {
            let account_hash = parse_b256(next("accountHash")?).map_err(|error| {
                eyre::eyre!("A: bad accountHash at line {line_number}: {error}")
            })?;
            let nonce = next("nonce")?
                .parse()
                .map_err(|error| eyre::eyre!("A: bad nonce at line {line_number}: {error}"))?;
            let balance = U256::from_str_radix(next("balance")?.trim_start_matches("0x"), 16)
                .map_err(|error| eyre::eyre!("A: bad balance at line {line_number}: {error}"))?;
            let code_hash = parse_b256(next("codeHash")?)
                .map_err(|error| eyre::eyre!("A: bad codeHash at line {line_number}: {error}"))?;
            parse_b256(next("storageRoot")?).map_err(|error| {
                eyre::eyre!("A: bad storageRoot at line {line_number}: {error}")
            })?;

            let bytecode_hash = (code_hash.0 != KECCAK_EMPTY).then_some(code_hash);
            StateRecord::Account {
                account_hash,
                account: Account {
                    nonce,
                    balance,
                    bytecode_hash,
                },
            }
        }
        "C" => {
            let code_hash = parse_b256(next("codeHash")?)
                .map_err(|error| eyre::eyre!("C: bad codeHash at line {line_number}: {error}"))?;
            let code = hex::decode(next("code")?)
                .map_err(|error| eyre::eyre!("C: bad code hex at line {line_number}: {error}"))?;
            let actual_hash = keccak256(&code);
            if actual_hash != code_hash {
                eyre::bail!(
                    "C: bytecode hash mismatch at line {line_number}: declared={code_hash:#x}, actual={actual_hash:#x}"
                );
            }
            StateRecord::Code {
                code_hash,
                bytecode: Bytecode::new_raw(Bytes::from(code)),
            }
        }
        "S" => {
            let slot_hash = parse_b256(next("slotHash")?)
                .map_err(|error| eyre::eyre!("S: bad slotHash at line {line_number}: {error}"))?;
            let value = U256::from_str_radix(next("value")?.trim_start_matches("0x"), 16)
                .map_err(|error| eyre::eyre!("S: bad value at line {line_number}: {error}"))?;
            StateRecord::Storage { slot_hash, value }
        }
        _ => {
            eyre::bail!("unknown state record {tag:?} at line {line_number}");
        }
    };

    if parts.next().is_some() {
        eyre::bail!("{tag}: unexpected trailing fields at line {line_number}");
    }
    Ok(Some(record))
}

fn preflight_state_stream(
    path: &Path,
    preimages: Option<&SlotPreimagesReader>,
) -> eyre::Result<StateStreamStats> {
    let reader = BufReader::with_capacity(4 * 1024 * 1024, File::open(path)?);
    let mut stats = StateStreamStats::default();
    let mut saw_account = false;
    let mut required_code_hashes: HashSet<B256> = HashSet::new();
    let mut provided_code_hashes: HashSet<B256> = HashSet::new();

    for (line_index, line) in reader.lines().enumerate() {
        let Some(record) = parse_state_record(&line?, line_index + 1)? else {
            continue;
        };
        match record {
            StateRecord::Account { account, .. } => {
                saw_account = true;
                stats.accounts += 1;
                if let Some(code_hash) = account.bytecode_hash {
                    required_code_hashes.insert(code_hash);
                }
            }
            StateRecord::Code { code_hash, .. } => {
                stats.bytecodes += 1;
                provided_code_hashes.insert(code_hash);
            }
            StateRecord::Storage { slot_hash, value } => {
                if !saw_account {
                    eyre::bail!("S record before any A record at line {}", line_index + 1);
                }
                if value.is_zero() {
                    continue;
                }
                if let Some(preimages) = preimages {
                    require_slot_preimage(preimages, slot_hash).map_err(|error| {
                        eyre::eyre!(
                            "S: invalid slot preimage at line {}: {error}",
                            line_index + 1
                        )
                    })?;
                }
                stats.slots += 1;
            }
        }
    }

    if stats.accounts == 0 {
        eyre::bail!("state stream contains no accounts");
    }
    if let Some(missing) = required_code_hashes
        .difference(&provided_code_hashes)
        .next()
    {
        eyre::bail!("state stream is missing bytecode record {missing:#x}");
    }
    Ok(stats)
}

fn stream_import<PF>(
    factory: &PF,
    path: &PathBuf,
    preimages: Option<&SlotPreimagesReader>,
) -> eyre::Result<()>
where
    PF: reth_provider::DatabaseProviderFactory<ProviderRW: DBProvider<Tx: DbTxMut>>,
{
    let file = File::open(path)?;
    let reader = BufReader::with_capacity(4 * 1024 * 1024, file);

    let mut provider_rw = factory.database_provider_rw()?;

    // Track progress
    let mut total_accounts: usize = 0;
    let mut total_slots: usize = 0;
    let mut total_bytecodes: usize = 0;
    let mut storage_units: usize = 0;

    // Flush storage when the next A/C line arrives.
    let mut current_account_hash: Option<B256> = None;

    for (line_index, line) in reader.lines().enumerate() {
        let Some(record) = parse_state_record(&line?, line_index + 1)? else {
            continue;
        };

        match record {
            StateRecord::Account {
                account_hash,
                account,
            } => {
                // Commit if threshold reached (before this account pushes us over).
                if storage_units >= COMMIT_THRESHOLD {
                    provider_rw.commit()?;
                    provider_rw = factory.database_provider_rw()?;
                    tracing::info!(
                        total_accounts,
                        total_slots,
                        total_bytecodes,
                        storage_units,
                        "committed chunk"
                    );
                    storage_units = 0;
                }

                // Write hashed account.
                provider_rw
                    .tx_ref()
                    .put::<tables::HashedAccounts>(account_hash, account)?;
                current_account_hash = Some(account_hash);
                total_accounts += 1;
                storage_units += 1;

                if total_accounts.is_multiple_of(100_000) {
                    tracing::info!(total_accounts, total_slots, "writing accounts...");
                }
            }
            StateRecord::Code {
                code_hash,
                bytecode,
            } => {
                // Commit if threshold reached.
                if storage_units >= COMMIT_THRESHOLD {
                    provider_rw.commit()?;
                    provider_rw = factory.database_provider_rw()?;
                    tracing::info!(
                        total_accounts,
                        total_slots,
                        total_bytecodes,
                        storage_units,
                        "committed chunk"
                    );
                    storage_units = 0;
                }

                provider_rw
                    .tx_ref()
                    .put::<tables::Bytecodes>(code_hash, bytecode)?;
                total_bytecodes += 1;
                storage_units += 1;
            }
            StateRecord::Storage { slot_hash, value } => {
                let acct_hash = match current_account_hash {
                    Some(h) => h,
                    None => {
                        return Err(eyre::eyre!(
                            "S record before any A record at line {}",
                            line_index + 1
                        ));
                    }
                };

                if value.is_zero() {
                    // Zero slots have no effect on the trie.
                    continue;
                }

                if let Some(preimages) = preimages {
                    require_slot_preimage(preimages, slot_hash).map_err(|e| {
                        eyre::eyre!("S: invalid slot preimage at line {}: {e}", line_index + 1)
                    })?;
                }

                // Commit if threshold reached.
                if storage_units >= COMMIT_THRESHOLD {
                    provider_rw.commit()?;
                    provider_rw = factory.database_provider_rw()?;
                    tracing::info!(
                        total_accounts,
                        total_slots,
                        total_bytecodes,
                        storage_units,
                        "committed chunk"
                    );
                    storage_units = 0;
                }

                let entry = StorageEntry {
                    key: slot_hash,
                    value,
                };
                let tx = provider_rw.tx_ref();
                let mut cursor = tx.cursor_dup_write::<tables::HashedStorages>()?;
                cursor.upsert(acct_hash, &entry)?;

                total_slots += 1;
                storage_units += 1;
            }
        }
    }

    // Final commit.
    provider_rw.commit()?;
    tracing::info!(
        total_accounts,
        total_slots,
        total_bytecodes,
        "all data written to MDBX"
    );

    Ok(())
}

fn require_slot_preimage(preimages: &SlotPreimagesReader, hashed_slot: B256) -> eyre::Result<B256> {
    let plain_slot = preimages
        .get(&hashed_slot)?
        .ok_or_else(|| eyre::eyre!("missing preimage for slot {hashed_slot:#x}"))?;
    let actual_hash = keccak256(plain_slot);
    if actual_hash != hashed_slot {
        eyre::bail!(
            "corrupt preimage for slot {hashed_slot:#x}: plain={plain_slot:#x}, hash={actual_hash:#x}"
        );
    }
    Ok(plain_slot)
}

fn compute_state_root_chunked<PF>(factory: &PF) -> eyre::Result<B256>
where
    PF: reth_provider::DatabaseProviderFactory<
            ProviderRW: DBProvider<Tx: DbTxMut> + TrieWriter + StorageSettingsCache,
        >,
{
    let mut intermediate_state: Option<IntermediateStateRootState> = None;
    let mut total_flushed: usize = 0;

    loop {
        let provider_rw = factory.database_provider_rw()?;

        // Borrow tx for the root computation, then drop the borrow before commit.
        let (root_result, state_opt, updates_opt) = {
            let tx = provider_rw.tx_ref();
            let state_root = DbStateRoot::from_tx(tx)
                .with_intermediate_state(intermediate_state.take())
                .with_threshold(TRIE_COMMIT_THRESHOLD);

            match state_root.root_with_progress()? {
                StateRootProgress::Progress(state, _, updates) => {
                    (None, Some(*state), Some(updates))
                }
                StateRootProgress::Complete(root, _, updates) => (Some(root), None, Some(updates)),
            }
        };

        let n = provider_rw.write_trie_updates(updates_opt.unwrap())?;
        total_flushed += n;

        if let Some(state) = state_opt {
            tracing::info!(
                last_key = %state.account_root_state.last_hashed_key,
                flushed = n,
                total_flushed,
                "trie progress: committing to free dirty pages"
            );
            intermediate_state = Some(state);
            provider_rw
                .commit()
                .map_err(|e| eyre::eyre!("trie progress commit: {e}"))?;
        } else if let Some(root) = root_result {
            tracing::info!(%root, flushed = n, total_flushed, "state root computation complete");
            provider_rw
                .commit()
                .map_err(|e| eyre::eyre!("trie final commit: {e}"))?;
            return Ok(root);
        }
    }
}

fn parse_b256(hex_str: &str) -> eyre::Result<B256> {
    let s = hex_str.trim_start_matches("0x");
    if s.len() != 64 {
        return Err(eyre::eyre!(
            "expected 64 hex chars, got {}: {:?}",
            s.len(),
            &hex_str[..s.len().min(20)]
        ));
    }
    let bytes = hex::decode(s)?;
    Ok(B256::from_slice(&bytes))
}

pub fn read(args: SnapshotReadArgs) -> eyre::Result<()> {
    // Parse the address.
    let addr_str = args.addr.trim_start_matches("0x");
    if addr_str.len() != 40 {
        return Err(eyre::eyre!(
            "--addr must be 40 hex chars (20 bytes), got {}",
            args.addr
        ));
    }
    let addr_bytes = hex::decode(addr_str)?;
    let address = Address::from_slice(&addr_bytes);

    // Compute the keccak hash of the address (the hashed-state key).
    let hashed_key = keccak256(address);

    // Open the MDBX read-only.
    // arb-snapshot-import stores MDBX in <out>/db.
    let db_path = args.db.join("db");

    // Pick the actual MDBX directory: prefer <dir>/db, fall back to <dir>.
    let mdbx_path = if db_path.exists() {
        db_path
    } else {
        args.db.clone()
    };

    let db = open_db_read_only(
        mdbx_path.as_path(),
        DatabaseArguments::new(ClientVersion::default()),
    )?;

    let tx = db.tx()?;

    let maybe_account = account_by_address(&tx, address);

    let (nonce, balance, code_hash) = match &maybe_account {
        Some(acct) => {
            let ch = acct.bytecode_hash.unwrap_or(HASHED_KECCAK_EMPTY);
            (acct.nonce, acct.balance, ch)
        }
        None => (0u64, U256::ZERO, HASHED_KECCAK_EMPTY),
    };

    let code_len = if code_hash == HASHED_KECCAK_EMPTY {
        0usize
    } else {
        match code_of(&tx, code_hash) {
            Some(bytecode) => bytecode.0.len(),
            None => 0,
        }
    };

    println!(
        "addr {} keccak {} nonce {} balance {} codeHash {} codeLen {}",
        address, hashed_key, nonce, balance, code_hash, code_len,
    );

    if let Some(slot_str) = &args.slot {
        let slot_hex = slot_str.trim_start_matches("0x");
        // Pad to 64 hex chars if shorter.
        let padded = format!("{:0>64}", slot_hex);
        if padded.len() != 64 {
            return Err(eyre::eyre!(
                "--slot must be at most 32 bytes (64 hex chars), got {}",
                slot_str
            ));
        }
        let slot_bytes = hex::decode(&padded)?;
        let slot = B256::from_slice(&slot_bytes);

        let value = storage_at(&tx, address, slot);
        println!("slot {} value {}", slot, value);
    }

    if args.list_storage {
        let ak = keccak256(address);
        let mut cursor = tx.cursor_dup_read::<tables::HashedStorages>()?;

        // Walk all dup values for this account key.
        let walker = cursor.walk_dup(Some(ak), None)?;
        let mut count = 0usize;
        for entry_result in walker {
            let (_key, entry) = entry_result?;
            if !entry.value.is_zero() {
                println!("  storage hashed_slot {} value {}", entry.key, entry.value);
                count += 1;
            }
        }
        println!("storage non-zero slot count: {}", count);
    }

    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{address, b256};
    use reth_db_api::{
        BlockNumberList,
        models::{ShardedKey, storage_sharded_key::StorageShardedKey},
    };
    use reth_storage_api::{
        AccountReader, PruneCheckpointReader, StateProvider, TryIntoHistoricalStateProvider,
    };

    #[test]
    fn preimage_batches_are_deduplicated_and_native_store_roundtrips() -> eyre::Result<()> {
        let temp = tempfile::tempdir()?;
        let store = SlotPreimages::open(temp.path())?;
        let plain_a = b256!("0000000000000000000000000000000000000000000000000000000000000042");
        let plain_b = b256!("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
        let mut batch = vec![
            (keccak256(plain_b), plain_b),
            (keccak256(plain_a), plain_a),
            (keccak256(plain_a), plain_a),
        ];

        assert_eq!(flush_preimage_batch(&store, &mut batch)?, 2);
        assert!(batch.is_empty());

        let reader = store.reader()?;
        assert_eq!(reader.get(&keccak256(plain_a))?, Some(plain_a));
        assert_eq!(reader.get(&keccak256(plain_b))?, Some(plain_b));
        drop(reader);

        let mut duplicate_batch = vec![(keccak256(plain_a), plain_a)];
        assert_eq!(flush_preimage_batch(&store, &mut duplicate_batch)?, 0);

        let corrupt_temp = tempfile::tempdir()?;
        let corrupt_store = SlotPreimages::open(corrupt_temp.path())?;
        corrupt_store.insert_preimages(&[(keccak256(plain_a), plain_b)])?;
        let mut conflicting_batch = vec![(keccak256(plain_a), plain_a)];
        let error = flush_preimage_batch(&corrupt_store, &mut conflicting_batch).unwrap_err();
        assert!(error.to_string().contains("conflicting slot preimage"));

        let mut internally_conflicting = vec![(B256::ZERO, plain_a), (B256::ZERO, plain_b)];
        let error = flush_preimage_batch(&store, &mut internally_conflicting).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("conflicting slot preimages in batch")
        );
        Ok(())
    }

    #[test]
    fn imported_slot_requires_a_matching_preimage() -> eyre::Result<()> {
        let missing_temp = tempfile::tempdir()?;
        let missing_store = SlotPreimages::open(missing_temp.path())?;
        let plain = b256!("0000000000000000000000000000000000000000000000000000000000000042");
        let hashed = keccak256(plain);
        let error = require_slot_preimage(&missing_store.reader()?, hashed).unwrap_err();
        assert!(error.to_string().contains("missing preimage"));

        let corrupt_temp = tempfile::tempdir()?;
        let corrupt_store = SlotPreimages::open(corrupt_temp.path())?;
        let wrong_plain = b256!("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
        corrupt_store.insert_preimages(&[(hashed, wrong_plain)])?;
        let error = require_slot_preimage(&corrupt_store.reader()?, hashed).unwrap_err();
        assert!(error.to_string().contains("corrupt preimage"));
        Ok(())
    }

    #[test]
    fn state_stream_preflight_checks_code_and_slot_preimages() -> eyre::Result<()> {
        let temp = tempfile::tempdir()?;
        let store = SlotPreimages::open(&temp.path().join("preimages"))?;
        let state_path = temp.path().join("state.stream");
        let account_hash = keccak256(address!("0000000000000000000000000000000000001234"));
        let plain_slot = b256!("0000000000000000000000000000000000000000000000000000000000000042");
        let slot_hash = keccak256(plain_slot);
        store.insert_preimages(&[(slot_hash, plain_slot)])?;
        let code = [0x60, 0x00, 0x56];
        let code_hash = keccak256(code);

        std::fs::write(
            &state_path,
            format!(
                "A {account_hash:#x} 7 2a {code_hash:#x} {:#x}\nS {slot_hash:#x} 01\nC {code_hash:#x} {}\n",
                B256::ZERO,
                hex::encode(code),
            ),
        )?;
        assert_eq!(
            preflight_state_stream(&state_path, Some(&store.reader()?))?,
            StateStreamStats {
                accounts: 1,
                slots: 1,
                bytecodes: 1,
            }
        );
        assert_eq!(
            preflight_state_stream(&state_path, None)?,
            StateStreamStats {
                accounts: 1,
                slots: 1,
                bytecodes: 1,
            },
            "post-ArbOS 20 snapshots do not require slot preimages"
        );

        std::fs::write(
            &state_path,
            format!(
                "A {account_hash:#x} 7 2a {code_hash:#x} {:#x}\n",
                B256::ZERO
            ),
        )?;
        let error = preflight_state_stream(&state_path, Some(&store.reader()?)).unwrap_err();
        assert!(error.to_string().contains("missing bytecode record"));

        std::fs::write(&state_path, format!("C {code_hash:#x} 00\n"))?;
        let error = preflight_state_stream(&state_path, Some(&store.reader()?)).unwrap_err();
        assert!(error.to_string().contains("bytecode hash mismatch"));
        Ok(())
    }

    #[test]
    fn snapshot_import_requires_a_fresh_target() -> eyre::Result<()> {
        let temp = tempfile::tempdir()?;
        std::fs::create_dir_all(temp.path().join("db/preimage"))?;
        ensure_fresh_import_target(temp.path())?;

        std::fs::create_dir(temp.path().join("db/.preimage.tmp"))?;
        assert!(find_staging_preimage_dir(&temp.path().join("db"))?.is_some());
        std::fs::remove_dir(temp.path().join("db/.preimage.tmp"))?;

        std::fs::create_dir(temp.path().join("static_files"))?;
        let error = ensure_fresh_import_target(temp.path()).unwrap_err();
        assert!(error.to_string().contains("fresh target"));

        let other = tempfile::tempdir()?;
        std::fs::create_dir_all(other.path().join("db"))?;
        std::fs::write(other.path().join("db/mdbx.dat"), [])?;
        let error = ensure_fresh_import_target(other.path()).unwrap_err();
        assert!(error.to_string().contains("unexpected path"));
        Ok(())
    }

    #[test]
    fn snapshot_head_record_is_self_authenticating() -> eyre::Result<()> {
        use alloy_rlp::Encodable;

        let temp = tempfile::tempdir()?;
        let path = temp.path().join("head.stream");
        let header = Header {
            number: 42,
            state_root: b256!("1111111111111111111111111111111111111111111111111111111111111111"),
            ..Default::default()
        };
        let hash = header.hash_slow();
        let mut encoded = Vec::new();
        header.encode(&mut encoded);
        std::fs::write(&path, format!("H 42 {hash:#x} {}\n", hex::encode(encoded)))?;
        let (number, decoded_hash, decoded) = read_head_header(&path)?;
        assert_eq!(number, 42);
        assert_eq!(decoded_hash, hash);
        assert_eq!(decoded.state_root, header.state_root);

        std::fs::write(
            &path,
            format!(
                "H 42 {hash:#x} {}\nB 42 c2c0c0\nR 42 c0\n",
                hex::encode(alloy_rlp::encode(header.clone()))
            ),
        )?;
        assert_eq!(read_head_header(&path)?.0, 42);

        let bad_path = temp.path().join("bad-head.stream");
        std::fs::write(
            &bad_path,
            format!(
                "H 42 {:#x} {}\n",
                B256::ZERO,
                hex::encode(alloy_rlp::encode(header.clone()))
            ),
        )?;
        let error = read_head_header(&bad_path).unwrap_err();
        assert!(error.to_string().contains("header hash mismatch"));

        let trailing_path = temp.path().join("trailing-head.stream");
        let mut encoded = alloy_rlp::encode(header.clone());
        encoded.push(0);
        std::fs::write(
            &trailing_path,
            format!("H 42 {hash:#x} {}\n", hex::encode(encoded)),
        )?;
        let error = read_head_header(&trailing_path).unwrap_err();
        assert!(error.to_string().contains("trailing bytes"));

        let invalid_body_path = temp.path().join("invalid-body.stream");
        std::fs::write(
            &invalid_body_path,
            format!(
                "H 42 {hash:#x} {}\nB 42 80\n",
                hex::encode(alloy_rlp::encode(header))
            ),
        )?;
        let error = read_head_header(&invalid_body_path).unwrap_err();
        assert!(error.to_string().contains("invalid B RLP"));
        Ok(())
    }

    #[test]
    fn snapshot_identity_binds_export_header_and_state_root() {
        let head = canonical_test_head();
        assert_eq!(
            validate_snapshot_identity(
                arb_reth_genesis::arbitrum_one::GENESIS_STATE_ROOT,
                &head,
            )
            .unwrap(),
            SnapshotPreimagePolicy::CanonicalGenesisRequired
        );

        let wrong_number = (head.0 + 1, head.1, head.2.clone());
        assert!(
            validate_snapshot_identity(
                arb_reth_genesis::arbitrum_one::GENESIS_STATE_ROOT,
                &wrong_number,
            )
            .is_err()
        );

        let mut wrong_root_header = head.2.clone();
        wrong_root_header.state_root = B256::ZERO;
        assert!(
            validate_snapshot_identity(
                arb_reth_genesis::arbitrum_one::GENESIS_STATE_ROOT,
                &(head.0, head.1, wrong_root_header),
            )
            .is_err()
        );
        assert!(validate_snapshot_identity(B256::ZERO, &head).is_err());

        let mut alternate_header = head.2.clone();
        alternate_header.timestamp += 1;
        let alternate = (head.0, alternate_header.hash_slow(), alternate_header);
        let error = validate_snapshot_identity(
            arb_reth_genesis::arbitrum_one::GENESIS_STATE_ROOT,
            &alternate,
        )
        .unwrap_err();
        assert!(error.to_string().contains("pre-ArbOS 20 snapshot"));

        let mut post_arbos_twenty = Header {
            number: 500_000_000,
            state_root: b256!("1111111111111111111111111111111111111111111111111111111111111111"),
            ..Default::default()
        };
        ArbHeaderInfo {
            arbos_format_version: 20,
            ..Default::default()
        }
        .update_header(&mut post_arbos_twenty);
        let post_arbos_twenty = (
            post_arbos_twenty.number,
            post_arbos_twenty.hash_slow(),
            post_arbos_twenty,
        );
        assert_eq!(
            validate_snapshot_identity(post_arbos_twenty.2.state_root, &post_arbos_twenty).unwrap(),
            SnapshotPreimagePolicy::NotRequired
        );
    }

    #[test]
    fn new_snapshot_format_requires_a_matching_completion_manifest() -> eyre::Result<()> {
        let temp = tempfile::tempdir()?;
        let preimage_path = temp.path().join("db/preimage");
        std::fs::create_dir_all(&preimage_path)?;
        drop(SlotPreimages::open(&preimage_path)?);
        write_preimage_manifest(&preimage_path, canonical_test_manifest())?;
        let head = canonical_test_head();

        let error = validate_snapshot_import_for_launch(temp.path(), &head).unwrap_err();
        assert!(error.to_string().contains("snapshot import is incomplete"));

        write_snapshot_import_manifest(temp.path(), &head)?;
        validate_snapshot_import_for_launch(temp.path(), &head)?;

        let mut altered_header = head.2.clone();
        altered_header.timestamp += 1;
        let altered = (head.0, altered_header.hash_slow(), altered_header);
        assert!(validate_snapshot_import_for_launch(temp.path(), &altered).is_err());

        let post_temp = tempfile::tempdir()?;
        let mut post_header = Header {
            number: 500_000_000,
            state_root: b256!("2222222222222222222222222222222222222222222222222222222222222222"),
            ..Default::default()
        };
        ArbHeaderInfo {
            arbos_format_version: 20,
            ..Default::default()
        }
        .update_header(&mut post_header);
        let post_head = (post_header.number, post_header.hash_slow(), post_header);
        write_snapshot_import_manifest(post_temp.path(), &post_head)?;
        validate_snapshot_import_for_launch(post_temp.path(), &post_head)?;
        Ok(())
    }

    #[test]
    fn invalid_snapshot_identity_does_not_create_database() -> eyre::Result<()> {
        let temp = tempfile::tempdir()?;
        let out = temp.path().join("out");
        let preimage_path = out.join("db/preimage");
        std::fs::create_dir_all(&preimage_path)?;
        drop(SlotPreimages::open(&preimage_path)?);
        write_preimage_manifest(&preimage_path, canonical_test_manifest())?;

        let mut header = Header {
            number: arb_reth_genesis::arbitrum_one::GENESIS_BLOCK_NUMBER + 1,
            state_root: arb_reth_genesis::arbitrum_one::GENESIS_STATE_ROOT,
            ..Default::default()
        };
        ArbHeaderInfo {
            arbos_format_version: 19,
            ..Default::default()
        }
        .update_header(&mut header);
        let blocks = temp.path().join("head.stream");
        std::fs::write(
            &blocks,
            format!(
                "H {} {:#x} {}\n",
                header.number,
                header.hash_slow(),
                hex::encode(alloy_rlp::encode(header.clone())),
            ),
        )?;

        let error = import(SnapshotImportArgs {
            state: temp.path().join("unused-state.stream"),
            out: out.clone(),
            expect: format!("{:#x}", arb_reth_genesis::arbitrum_one::GENESIS_STATE_ROOT),
            blocks,
        })
        .unwrap_err();
        assert!(error.to_string().contains("pre-ArbOS 20 snapshot"));
        assert!(!out.join("db/mdbx.dat").exists());
        assert!(!out.join("static_files").exists());
        assert!(!out.join("rocksdb").exists());
        Ok(())
    }

    fn canonical_test_manifest() -> SlotPreimageManifest {
        SlotPreimageManifest::new(
            arb_reth_genesis::preimages::SlotPreimageStats {
                next_block_number: arb_reth_genesis::arbitrum_one::GENESIS_BLOCK_NUMBER,
                classic_accounts: 1_294_583,
                classic_slots: 24_491_013,
                arbos_accounts: 15,
                arbos_slots: 1_410_458,
                address_table_entries: 680_046,
                retryables: 16_206,
            },
            18_784_532,
        )
        .unwrap()
    }

    fn canonical_test_head() -> (u64, B256, Header) {
        let line = include_str!("../../tests/fixtures/arb1_nitro_genesis_head.stream")
            .lines()
            .next()
            .unwrap();
        parse_header_record(line, 1).unwrap().unwrap()
    }

    #[test]
    fn snapshot_history_boundaries_preserve_imported_state_during_rocks_ahead_window()
    -> eyre::Result<()> {
        const SNAPSHOT_HEAD: u64 = 22_207_817;
        let address = address!("0000000000000000000000000000000000001234");
        let storage_key = b256!("0000000000000000000000000000000000000000000000000000000000000042");
        let account = Account {
            nonce: 7,
            balance: U256::from(123_456u64),
            bytecode_hash: None,
        };
        let storage_value = U256::from(987_654u64);

        let temp = tempfile::tempdir()?;
        let db = init_db(
            temp.path().join("db"),
            DatabaseArguments::new(ClientVersion::default()),
        )?;
        let static_files = StaticFileProvider::read_write(temp.path().join("static_files"))?;
        let rocksdb = RocksDBProvider::builder(temp.path().join("rocksdb"))
            .with_default_tables()
            .build()
            .map_err(|error| eyre::eyre!("RocksDB open error: {error}"))?;
        let factory: ProviderFactory<ArbNodeTypesWithDB> = ProviderFactory::new(
            db,
            Arc::new(MAINNET.as_ref().clone()),
            static_files,
            rocksdb.clone(),
            Runtime::test(),
        )?;
        factory.set_storage_settings_cache(StorageSettings::v2());

        // Model an imported snapshot account and storage slot. The Finish checkpoint is one block
        // ahead of the snapshot so requesting SNAPSHOT_HEAD takes the historical-provider path.
        {
            let provider = factory.database_provider_rw()?;
            provider.write_storage_settings(StorageSettings::v2())?;
            provider
                .tx_ref()
                .put::<tables::HashedAccounts>(keccak256(address), account)?;
            let mut storage = provider
                .tx_ref()
                .cursor_dup_write::<tables::HashedStorages>()?;
            storage.upsert(
                keccak256(address),
                &StorageEntry {
                    key: keccak256(storage_key),
                    value: storage_value,
                },
            )?;
            provider
                .save_stage_checkpoint(StageId::Finish, StageCheckpoint::new(SNAPSHOT_HEAD + 1))?;
            provider.commit()?;
        }

        // Model the normal storage-v2 commit window: RocksDB history for the next block is visible
        // while the companion MDBX snapshot still reports the previous visible tip.
        rocksdb.put::<tables::AccountsHistory>(
            ShardedKey::new(address, u64::MAX),
            &BlockNumberList::new([SNAPSHOT_HEAD + 2]).expect("valid history list"),
        )?;
        rocksdb.put::<tables::StoragesHistory>(
            StorageShardedKey::new(address, storage_key, u64::MAX),
            &BlockNumberList::new([SNAPSHOT_HEAD + 2]).expect("valid history list"),
        )?;

        // Without a snapshot boundary, Reth interprets the first history entry as the account and
        // slot not existing yet, even though both are present in the imported hashed state.
        let state = factory
            .provider()?
            .try_into_history_at_block(SNAPSHOT_HEAD)?;
        assert_eq!(state.basic_account(&address)?, None);
        assert_eq!(state.storage(address, storage_key)?, None);

        {
            let provider = factory.database_provider_rw()?;
            write_snapshot_history_boundaries(&provider, SNAPSHOT_HEAD)?;
            provider.commit()?;
        }

        for segment in [PruneSegment::AccountHistory, PruneSegment::StorageHistory] {
            let checkpoint = factory
                .provider()?
                .get_prune_checkpoint(segment)?
                .expect("snapshot history checkpoint");
            assert_eq!(checkpoint.block_number, Some(SNAPSHOT_HEAD));
            assert_eq!(checkpoint.prune_mode, PruneMode::Before(SNAPSHOT_HEAD + 1));
        }

        // The boundary changes an ambiguous no-history result into a fallback to the imported
        // hashed state, preserving both the account and storage value during the same skew window.
        let state = factory
            .provider()?
            .try_into_history_at_block(SNAPSHOT_HEAD)?;
        assert_eq!(state.basic_account(&address)?, Some(account));
        assert_eq!(state.storage(address, storage_key)?, Some(storage_value));

        Ok(())
    }
}
