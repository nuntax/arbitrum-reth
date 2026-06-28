//! `arb-snapshot-import`: import a Nitro genesis-state stream into reth MDBX.
//!
//! Reads a line-oriented state export produced by Nitro's state-dumper and writes
//! the accounts/bytecodes/storage directly into reth's `HashedAccounts`,
//! `HashedStorages`, and `Bytecodes` tables, then drives reth's state-root trie
//! computation to verify parity.
//!
//! ## Stream format
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
//! ## Usage
//!
//! ```text
//! arb-snapshot-import \
//!   --state /tmp/arb1_genesis_state.stream \
//!   --out   /tmp/arbreth-mdbx \
//!   --expect 0x7f2bfc4481d02bfcfc606ebb949384ef78d03a0f30a2dc9cccd652eb80926ae1
//! ```

#![allow(missing_docs)]

use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
    sync::Arc,
};
use reth_tracing::{RethTracer, Tracer};

use alloy_primitives::{hex, B256, U256};
use clap::Parser;
use alloy_genesis::{ChainConfig, Genesis};
use reth_chainspec::{ChainSpec, MAINNET};
use reth_db::{init_db, mdbx::DatabaseArguments, ClientVersion};
use reth_db_api::{
    cursor::{DbCursorRW, DbDupCursorRW},
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_primitives_traits::{Account, Bytecode, SealedHeader};
use reth_provider::{
    providers::{RocksDBProvider, StaticFileProvider},
    DBProvider, ProviderFactory, StorageSettingsCache, TrieWriter,
};
use reth_node_types::NodeTypesWithDBAdapter;
use reth_primitives_traits::StorageEntry;
use reth_tasks::Runtime;
use reth_trie::{IntermediateStateRootState, StateRoot as StateRootComputer, StateRootProgress};
use reth_trie_db::{DatabaseHashedCursorFactory, DatabaseStateRoot, DatabaseTrieCursorFactory, LegacyKeyAdapter};

// Boot-wiring (#52): write head header + checkpoints so ProviderFactory opens at the block.
use alloy_consensus::Header;
use alloy_rlp::Decodable;
use reth_provider::{
    BlockNumReader, DatabaseProviderFactory, StageCheckpointWriter, StaticFileProviderFactory,
    StaticFileWriter,
};
use reth_stages_types::{StageCheckpoint, StageId};
use reth_static_file_types::StaticFileSegment;
use reth_storage_api::HeaderProvider;

/// Stack-probe shim for x86_64 (same as arb-reth.rs).
#[cfg(target_arch = "x86_64")]
#[no_mangle]
pub unsafe extern "C" fn __rust_probestack() {}

// Arbitrum One Nitro uses LegacyKeyAdapter (storage v1) for genesis state.
type DbStateRoot<'a, TX> = StateRootComputer<
    DatabaseTrieCursorFactory<&'a TX, LegacyKeyAdapter>,
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

use arb_reth_node::ArbNode;
type ArbNodeTypesWithDB = NodeTypesWithDBAdapter<ArbNode, reth_db::DatabaseEnv>;

/// Import a Nitro genesis state stream into reth MDBX and verify the state root.
#[derive(Debug, Parser)]
#[command(
    name = "arb-snapshot-import",
    about = "Import a Nitro genesis state stream into reth MDBX and verify the state root"
)]
struct Args {
    /// Path to the Nitro genesis state stream file.
    #[arg(long, value_name = "FILE")]
    state: PathBuf,

    /// Output datadir (will be created if absent; `<out>/db`, `<out>/static_files`,
    /// `<out>/rocksdb` sub-directories are created automatically).
    #[arg(long, value_name = "DIR")]
    out: PathBuf,

    /// Expected state root (hex, with or without 0x prefix).
    #[arg(long, value_name = "HEX")]
    expect: String,

    /// Optional blocks stream (`H <num> <hash> <headerRLP>` records). When given, the head
    /// header is written (static-file Headers + HeaderNumbers + BlockBodyIndices) and all stage
    /// checkpoints are set to the head block, making the DB openable at that block.
    #[arg(long, value_name = "FILE")]
    blocks: Option<PathBuf>,
}

fn main() -> eyre::Result<()> {
    let _guard = RethTracer::new().init()?;
    let args = Args::parse();
    run(args)
}

fn run(args: Args) -> eyre::Result<()> {
    let expect_str = args.expect.trim_start_matches("0x");
    let expected: B256 = B256::from_slice(&hex::decode(expect_str)?);

    let db_path = args.out.join("db");
    let static_files_path = args.out.join("static_files");
    let rocksdb_path = args.out.join("rocksdb");

    std::fs::create_dir_all(&db_path)?;
    std::fs::create_dir_all(&static_files_path)?;
    std::fs::create_dir_all(&rocksdb_path)?;

    tracing::info!(path = ?db_path, "opening MDBX");
    let db = init_db(&db_path, DatabaseArguments::new(ClientVersion::default()))?;

    // With --blocks we inject the snapshot's real head header so genesis_hash() matches the DB
    // and reth's launch genesis-check passes. Without --blocks we fall back to MAINNET (fine
    // for the state-root gate, which is chain-spec-independent).
    let chain_spec: Arc<ChainSpec> = match &args.blocks {
        Some(bp) => {
            let (_num, hash, header) = read_head_header(bp)?;
            arb_chain_spec_with_header(ARB_ONE_CHAIN_ID, header, hash)
        }
        None => Arc::new(MAINNET.as_ref().clone()),
    };

    let static_file_provider =
        StaticFileProvider::read_write(static_files_path)?;
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

    tracing::info!(path = ?args.state, "streaming state import");
    stream_import(&factory, &args.state)?;

    tracing::info!("computing state root (may take several minutes for large states)");
    let computed = compute_state_root_chunked(&factory)?;

    println!("computed  = {computed:#x}");
    println!("expected  = {expected:#x}");
    if computed == expected {
        println!("MATCH");
    } else {
        println!("MISMATCH");
    }

    if let Some(blocks_path) = &args.blocks {
        tracing::info!(path = ?blocks_path, "writing head header + checkpoints");
        let (head_num, head_hash) = write_head_blocks(&factory, blocks_path)?;
        verify_head(&factory, head_num, head_hash)?;
        // The injected-header chain spec means reth's launch genesis-check accepts this DB.
        verify_launch(&factory, head_hash)?;
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
    let genesis = Genesis { config, number: Some(header.number), ..Default::default() };
    let mut spec = ChainSpec::from_genesis(genesis);
    // Override the computed (alloc-derived, wrong) genesis header with the real one.
    spec.genesis_header = SealedHeader::new(header, hash);
    Arc::new(spec)
}

/// Read the highest-numbered `H <num> <hash> <headerRLP>` record (the head/genesis header).
fn read_head_header(path: &PathBuf) -> eyre::Result<(u64, B256, Header)> {
    let reader = std::io::BufReader::new(File::open(path)?);
    let mut best: Option<(u64, B256, Header)> = None;
    for line in reader.lines() {
        let line = line?;
        let mut p = line.splitn(4, ' ');
        if p.next() != Some("H") {
            continue;
        }
        let num: u64 = p.next().ok_or_else(|| eyre::eyre!("H: missing number"))?.parse()?;
        let hash = parse_b256(p.next().ok_or_else(|| eyre::eyre!("H: missing hash"))?)?;
        let rlp = hex::decode(p.next().ok_or_else(|| eyre::eyre!("H: missing headerRLP"))?)?;
        let header = Header::decode(&mut rlp.as_slice())
            .map_err(|e| eyre::eyre!("decode header {num}: {e}"))?;
        if best.as_ref().map(|(n, ..)| num >= *n).unwrap_or(true) {
            best = Some((num, hash, header));
        }
    }
    best.ok_or_else(|| eyre::eyre!("no H records in {path:?}"))
}

/// Launch-acceptance gate: runs `init_genesis` with validation against the converted DB.
/// With the injected-header chain spec it must find the genesis present (no GenesisHashMismatch,
/// no re-write), confirming a node would open this DB cleanly.
fn verify_launch(factory: &ProviderFactory<ArbNodeTypesWithDB>, head_hash: B256) -> eyre::Result<()> {
    use reth_db_common::init::init_genesis_with_settings_and_validate;
    use reth_db_api::models::StorageSettings;
    let got = init_genesis_with_settings_and_validate(factory, StorageSettings::v1(), true)
        .map_err(|e| eyre::eyre!("init_genesis (launch genesis check) rejected the DB: {e}"))?;
    println!("init_genesis (validate=true) = {got:#x}");
    if got == head_hash {
        println!("LAUNCH OK");
        Ok(())
    } else {
        Err(eyre::eyre!("init_genesis returned {got:#x}, expected {head_hash:#x}"))
    }
}

/// Write every `H <num> <hash> <headerRLP>` record into the static-file Headers segment plus
/// `HeaderNumbers`/`BlockBodyIndices`, then set all stage checkpoints to the highest block so a
/// `ProviderFactory` reports it as the head. Returns `(head_number, head_hash)`.
fn write_head_blocks(
    factory: &ProviderFactory<ArbNodeTypesWithDB>,
    path: &PathBuf,
) -> eyre::Result<(u64, B256)> {
    let provider_rw = factory.database_provider_rw()?;
    let sfp = provider_rw.static_file_provider();

    let reader = std::io::BufReader::new(File::open(path)?);
    let mut head_num = 0u64;
    let mut head_hash = B256::ZERO;
    let mut count = 0u64;

    for line in reader.lines() {
        let line = line?;
        let mut p = line.splitn(4, ' ');
        if p.next() != Some("H") {
            continue;
        }
        let num: u64 = p.next().ok_or_else(|| eyre::eyre!("H: missing number"))?.parse()?;
        let hash = parse_b256(p.next().ok_or_else(|| eyre::eyre!("H: missing hash"))?)?;
        let rlp = hex::decode(p.next().ok_or_else(|| eyre::eyre!("H: missing headerRLP"))?)?;
        let header = Header::decode(&mut rlp.as_slice())
            .map_err(|e| eyre::eyre!("decode header {num}: {e}"))?;

        // Genesis TD == difficulty for the first block (Arbitrum difficulty is 1).
        let mut writer = sfp.get_writer(num, StaticFileSegment::Headers)?;
        if num > 0 {
            writer.user_header_mut().set_block_range(num, num);
            writer.append_header_direct(&header, header.difficulty, &hash)?;
        } else {
            writer.append_header(&header, &hash)?;
        }
        writer.commit()?;

        provider_rw.tx_ref().put::<tables::HeaderNumbers>(hash, num)?;
        provider_rw.tx_ref().put::<tables::BlockBodyIndices>(num, Default::default())?;

        if num >= head_num {
            head_num = num;
            head_hash = hash;
        }
        count += 1;
    }

    // Initialize the Transactions + Receipts static-file segments to the head block. Without
    // this, reth's launch `check_consistency` sees those segments empty (highest block None)
    // while the stage checkpoints say `head_num`, and unwinds to block 0. The head block has no
    // txs/receipts, so the segments stay empty — only the block range needs setting. Mirrors
    // reth `init_genesis`'s non-zero-genesis path (db-common init.rs).
    sfp.get_writer(head_num, StaticFileSegment::Receipts)?
        .user_header_mut()
        .set_block_range(head_num, head_num);
    sfp.get_writer(head_num, StaticFileSegment::Transactions)?
        .user_header_mut()
        .set_block_range(head_num, head_num);

    // Mark every stage complete at the head so reth treats the DB as synced to that block.
    let cp = StageCheckpoint::new(head_num);
    for stage in StageId::ALL {
        provider_rw.save_stage_checkpoint(stage, cp)?;
    }
    provider_rw.commit()?;
    tracing::info!(count, head_num, ?head_hash, "wrote headers + checkpoints");
    Ok((head_num, head_hash))
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

fn stream_import<PF>(factory: &PF, path: &PathBuf) -> eyre::Result<()>
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

    for (line_no, line_result) in reader.lines().enumerate() {
        let line = line_result?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let mut parts = line.splitn(7, ' ');
        let tag = parts.next().unwrap_or("");

        match tag {
            "A" => {
                // A <accountHash> <nonce> <balance> <codeHash> <storageRoot>
                let acct_hash_hex = parts.next().ok_or_else(|| eyre::eyre!("A: missing accountHash at line {line_no}"))?;
                let nonce_str     = parts.next().ok_or_else(|| eyre::eyre!("A: missing nonce at line {line_no}"))?;
                let balance_str   = parts.next().ok_or_else(|| eyre::eyre!("A: missing balance at line {line_no}"))?;
                let code_hash_hex = parts.next().ok_or_else(|| eyre::eyre!("A: missing codeHash at line {line_no}"))?;
                // storageRoot is ignored; we use the S lines directly.

                let acct_hash = parse_b256(acct_hash_hex)
                    .map_err(|e| eyre::eyre!("A: bad accountHash at line {line_no}: {e}"))?;
                let nonce: u64 = nonce_str.parse()
                    .map_err(|e| eyre::eyre!("A: bad nonce at line {line_no}: {e}"))?;
                let balance = U256::from_str_radix(balance_str.trim_start_matches("0x"), 16)
                    .map_err(|e| eyre::eyre!("A: bad balance at line {line_no}: {e}"))?;
                let code_hash = parse_b256(code_hash_hex)
                    .map_err(|e| eyre::eyre!("A: bad codeHash at line {line_no}: {e}"))?;

                let bytecode_hash = if code_hash.0 == KECCAK_EMPTY { None } else { Some(code_hash) };

                let account = Account { nonce, balance, bytecode_hash };

                // Commit if threshold reached (before this account pushes us over).
                if storage_units > 0 && storage_units >= COMMIT_THRESHOLD {
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
                provider_rw.tx_ref().put::<tables::HashedAccounts>(acct_hash, account)?;
                current_account_hash = Some(acct_hash);
                total_accounts += 1;
                storage_units += 1;

                if total_accounts % 100_000 == 0 {
                    tracing::info!(total_accounts, total_slots, "writing accounts...");
                }
            }

            "C" => {
                // C <codeHash> <code:hex>
                let code_hash_hex = parts.next().ok_or_else(|| eyre::eyre!("C: missing codeHash at line {line_no}"))?;
                let code_hex      = parts.next().ok_or_else(|| eyre::eyre!("C: missing code at line {line_no}"))?;

                let code_hash = parse_b256(code_hash_hex)
                    .map_err(|e| eyre::eyre!("C: bad codeHash at line {line_no}: {e}"))?;

                let code_bytes = hex::decode(code_hex)
                    .map_err(|e| eyre::eyre!("C: bad code hex at line {line_no}: {e}"))?;

                // Use new_raw to avoid revalidation; the hash is already the pre-image.
                let bytecode = Bytecode::new_raw(alloy_primitives::Bytes::from(code_bytes));

                // Commit if threshold reached.
                if storage_units > 0 && storage_units >= COMMIT_THRESHOLD {
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

                provider_rw.tx_ref().put::<tables::Bytecodes>(code_hash, bytecode)?;
                total_bytecodes += 1;
                storage_units += 1;
            }

            "S" => {
                // S <slotHash> <value:hex>
                let acct_hash = match current_account_hash {
                    Some(h) => h,
                    None => return Err(eyre::eyre!("S line at {line_no} before any A line")),
                };
                let slot_hash_hex = parts.next().ok_or_else(|| eyre::eyre!("S: missing slotHash at line {line_no}"))?;
                let value_str     = parts.next().ok_or_else(|| eyre::eyre!("S: missing value at line {line_no}"))?;

                let slot_hash = parse_b256(slot_hash_hex)
                    .map_err(|e| eyre::eyre!("S: bad slotHash at line {line_no}: {e}"))?;
                let value = U256::from_str_radix(value_str.trim_start_matches("0x"), 16)
                    .map_err(|e| eyre::eyre!("S: bad value at line {line_no}: {e}"))?;

                if value.is_zero() {
                    // Zero slots have no effect on the trie.
                    continue;
                }

                // Commit if threshold reached.
                if storage_units > 0 && storage_units >= COMMIT_THRESHOLD {
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

                let entry = StorageEntry { key: slot_hash, value };
                let tx = provider_rw.tx_ref();
                let mut cursor = tx.cursor_dup_write::<tables::HashedStorages>()?;
                cursor.upsert(acct_hash, &entry)?;

                total_slots += 1;
                storage_units += 1;
            }

            _ => {
                // Unknown tag: skip silently (comments, blank lines, etc.)
            }
        }
    }

    // Final commit.
    provider_rw.commit()?;
    tracing::info!(total_accounts, total_slots, total_bytecodes, "all data written to MDBX");

    Ok(())
}

fn compute_state_root_chunked<PF>(factory: &PF) -> eyre::Result<B256>
where
    PF: reth_provider::DatabaseProviderFactory<ProviderRW: DBProvider<Tx: DbTxMut> + TrieWriter + StorageSettingsCache>,
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
                StateRootProgress::Complete(root, _, updates) => {
                    (Some(root), None, Some(updates))
                }
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
            provider_rw.commit().map_err(|e| eyre::eyre!("trie progress commit: {e}"))?;
        } else if let Some(root) = root_result {
            tracing::info!(%root, flushed = n, total_flushed, "state root computation complete");
            provider_rw.commit().map_err(|e| eyre::eyre!("trie final commit: {e}"))?;
            return Ok(root);
        }
    }
}

fn parse_b256(hex_str: &str) -> eyre::Result<B256> {
    let s = hex_str.trim_start_matches("0x");
    if s.len() != 64 {
        return Err(eyre::eyre!("expected 64 hex chars, got {}: {:?}", s.len(), &hex_str[..s.len().min(20)]));
    }
    let bytes = hex::decode(s)?;
    Ok(B256::from_slice(&bytes))
}
