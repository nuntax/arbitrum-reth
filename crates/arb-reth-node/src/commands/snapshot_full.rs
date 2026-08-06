//! `arb-reth snapshot import-full`: turn a `reth-export --mode full-snapshot` stream into a reth
//! datadir that carries block history and per-block state history, not just head state.
//!
//! The stream holds a manifest, then blocks, then state history, then the state trie, all ending at
//! the same block. Sections are written in that order, which is also append-only order for reth's
//! static files. Invariants and their reasoning live in
//! `arb-kb/decisions/ADR-004-snapshot-conversion-invariants.md`; the checks below name the ones they
//! enforce.
//!
//! Nothing here writes the completion manifest, so an interrupted or failed run leaves a datadir the
//! node refuses to boot. That is deliberate: a partial conversion that boots would answer historical
//! queries with silence rather than an error.

use std::{
    collections::HashSet,
    fs::File,
    io::{BufReader, Read},
    path::PathBuf,
    sync::Arc,
};

use alloy_consensus::{
    Header,
    proofs::{calculate_receipt_root, calculate_transaction_root},
};
use alloy_primitives::B256;
use alloy_rlp::Decodable;
use arb_reth_genesis::snapshot_stream::{HistoryObject, Manifest, Record, SnapshotStream};
use arbitrum_alloy_consensus::reth::ArbBlockBody;
use clap::Parser;
use reth_chainspec::ChainSpec;
use reth_db::{ClientVersion, init_db, mdbx::DatabaseArguments};
use reth_db_api::{
    cursor::DbCursorRW,
    database::Database,
    models::{AccountBeforeTx, StorageBeforeTx, StorageSettings},
    tables,
    transaction::DbTxMut,
};
use reth_node_types::NodeTypesWithDBAdapter;
use reth_primitives_traits::{Account, Bytecode, StorageEntry};
use reth_provider::{
    BlockWriter, DBProvider, DatabaseProviderFactory, EitherWriter, MetadataWriter,
    ProviderFactory, StaticFileProviderFactory, StaticFileWriter, StorageSettingsCache,
    providers::{RocksDBProvider, StaticFileProvider},
};
use reth_prune_types::{PruneCheckpoint, PruneMode, PruneSegment};
use reth_stages::stages::{
    IndexAccountHistoryStage, IndexStorageHistoryStage, SenderRecoveryStage, TransactionLookupStage,
};
use reth_stages_api::{ExecInput, Stage};
use reth_stages_types::{StageCheckpoint, StageId};
use reth_static_file_types::StaticFileSegment;
use reth_storage_api::{BlockBodyIndicesProvider, StageCheckpointWriter};
use reth_tasks::Runtime;
use reth_tracing::tracing::info;

use crate::{ArbNode, L1ResumeCheckpoint, L1ResumeLog, stored_receipt::decode_stored_receipts};

type ArbNodeTypesWithDB = NodeTypesWithDBAdapter<ArbNode, reth_db::DatabaseEnv>;

/// A database a [`ProviderFactory`] can be built over: the real MDBX environment when importing, a
/// temporary one under test. The section writers are generic over it so the tests exercise the same
/// code the command runs.
pub trait SnapshotDb:
    Database + reth_db_api::database_metrics::DatabaseMetrics + Clone + Unpin + 'static
{
}

impl<T> SnapshotDb for T where
    T: Database + reth_db_api::database_metrics::DatabaseMetrics + Clone + Unpin + 'static
{
}

/// Read-ahead over the stream. It is read strictly forward, so a large buffer is all that matters.
const STREAM_BUFFER: usize = 16 * 1024 * 1024;

/// Blocks accumulated before a database transaction is committed. Bounds dirty-page growth over a
/// chain of tens of millions of blocks.
const BLOCK_BATCH: usize = 4_000;

/// Transactions accumulated before committing early, for chains whose blocks are much fuller than
/// Arbitrum's average of roughly one transaction each.
const TX_BATCH: usize = 50_000;

/// Changeset entries (accounts plus slots) accumulated before a commit. Blocks are batched by
/// entries rather than by count because a single block's diff can be very large.
const CHANGESET_BATCH: usize = 250_000;

/// Hashed-state writes accumulated before a commit, to bound dirty-page growth.
const STATE_COMMIT_THRESHOLD: usize = 250_000;

/// Convert a full-snapshot stream into a reth datadir.
#[derive(Debug, Parser)]
#[command(
    name = "arb-snapshot-import-full",
    about = "Convert a reth-export --mode full-snapshot stream into a reth datadir"
)]
pub struct SnapshotImportFullArgs {
    /// Stream produced by `reth-export --mode full-snapshot`.
    #[arg(long, value_name = "FILE")]
    stream: PathBuf,

    /// Output datadir. Must be empty; `db`, `static_files` and `rocksdb` are created inside it.
    #[arg(long, value_name = "DIR")]
    out: PathBuf,

    /// Nitro `chaininfo.json` for the chain the snapshot came from.
    #[arg(long = "chain-info", value_name = "PATH")]
    chain_info: PathBuf,

    /// Nitro `genesis.json` for the same chain.
    #[arg(long = "genesis", value_name = "PATH")]
    genesis_json: PathBuf,
}

/// Finish a datadir whose sections were imported but which never reached finalisation.
#[derive(Debug, Parser)]
#[command(
    name = "arb-snapshot-finalize",
    about = "Finish a converted datadir that stopped after its state root"
)]
pub struct SnapshotFinalizeArgs {
    /// Datadir produced by an `import-full` run that did not complete.
    #[arg(long, value_name = "DIR")]
    datadir: PathBuf,

    /// Nitro `chaininfo.json` for the chain the snapshot came from.
    #[arg(long = "chain-info", value_name = "PATH")]
    chain_info: PathBuf,

    /// Nitro `genesis.json` for the same chain.
    #[arg(long = "genesis", value_name = "PATH")]
    genesis_json: PathBuf,
}

/// Finish a datadir that was imported up to its state root but never finalised.
///
/// The sections take most of an hour on a real chain and finalisation is the last and longest step,
/// so making it re-runnable on its own is the difference between losing an afternoon and losing a
/// few minutes. Everything it needs is recoverable from the datadir: the convert point is the
/// highest header, its root and hash come from that header, and `S_lo` is where the changeset
/// segments begin.
pub fn finalize_datadir(args: SnapshotFinalizeArgs) -> eyre::Result<()> {
    let manifest_path = args
        .datadir
        .join(super::snapshot::SNAPSHOT_IMPORT_MANIFEST_FILE);
    if manifest_path.exists() {
        return Err(eyre::eyre!(
            "{} is already finished; its completion manifest is at {}",
            args.datadir.display(),
            manifest_path.display()
        ));
    }

    let static_files_path = args.datadir.join("static_files");
    let history_from = lowest_changeset_block(&static_files_path)?;

    let chain_info = std::fs::read(&args.chain_info)
        .map_err(|error| eyre::eyre!("read {}: {error}", args.chain_info.display()))?;
    let genesis = std::fs::read(&args.genesis_json)
        .map_err(|error| eyre::eyre!("read {}: {error}", args.genesis_json.display()))?;
    let (chain_spec, _init, _info) = crate::orbit_chain_from_files(&chain_info, &genesis)?;

    let factory = open_factory(
        &args.datadir.join("db"),
        &static_files_path,
        &args.datadir.join("rocksdb"),
        Arc::new(chain_spec),
    )?;

    // The convert point is wherever the blocks section stopped, and its header carries the root the
    // trie was already checked against.
    let provider = factory.provider()?;
    let block = provider
        .static_file_provider()
        .get_highest_static_file_block(StaticFileSegment::Headers)
        .ok_or_else(|| eyre::eyre!("no headers in {}", args.datadir.display()))?;
    let head = reth_storage_api::HeaderProvider::sealed_header(&provider, block)?
        .ok_or_else(|| eyre::eyre!("block {block} is missing its header"))?;
    let manifest = Manifest {
        block,
        root: head.state_root,
        state_id: 0,
        hash: head.hash(),
        resume: None,
    };
    drop(provider);

    info!(
        target: "arb-snapshot",
        block,
        root = %manifest.root,
        history_from,
        "finishing a datadir that stopped after its state root"
    );

    finalize(&factory, &manifest, history_from, &args.datadir)?;
    println!(
        "finished {} at block {block} root {:#x}",
        args.datadir.display(),
        manifest.root
    );
    Ok(())
}

/// `S_lo`, read from where the account-changeset segments begin.
///
/// reth resolves a segment's path from the block range in its name, and the import renames those
/// files to agree with their headers, so the lowest name is the lowest block with history.
fn lowest_changeset_block(static_files: &std::path::Path) -> eyre::Result<u64> {
    const PREFIX: &str = "static_file_account-change-sets_";
    let mut lowest: Option<u64> = None;
    for entry in std::fs::read_dir(static_files)
        .map_err(|error| eyre::eyre!("read {}: {error}", static_files.display()))?
    {
        let name = entry?.file_name().to_string_lossy().into_owned();
        let Some(range) = name.strip_prefix(PREFIX) else {
            continue;
        };
        // Skip the sidecars, which share the prefix but carry an extension.
        if range.contains('.') {
            continue;
        }
        let Some((start, _)) = range.split_once('_') else {
            continue;
        };
        if let Ok(start) = start.parse::<u64>() {
            lowest = Some(lowest.map_or(start, |current: u64| current.min(start)));
        }
    }
    lowest.ok_or_else(|| {
        eyre::eyre!(
            "no account-changeset segments in {}; this datadir has no state history to finish",
            static_files.display()
        )
    })
}

/// What a section wrote, for the run's summary and for cross-checking against the exporter's own
/// counts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlockSectionStats {
    pub blocks: u64,
    pub bodies: u64,
    pub transactions: u64,
    pub receipt_sets: u64,
    pub receipts: u64,
    pub first_block: u64,
    pub last_block: u64,
}

pub fn import_full(args: SnapshotImportFullArgs) -> eyre::Result<()> {
    super::snapshot::ensure_fresh_import_target(&args.out)?;

    let file = File::open(&args.stream)
        .map_err(|error| eyre::eyre!("open {}: {error}", args.stream.display()))?;
    let mut stream = SnapshotStream::open(BufReader::with_capacity(STREAM_BUFFER, file))?;
    let manifest = stream.manifest().clone();
    info!(
        target: "arb-snapshot",
        block = manifest.block,
        root = %manifest.root,
        state_id = manifest.state_id,
        "opened full-snapshot stream"
    );

    let chain_info = std::fs::read(&args.chain_info)
        .map_err(|error| eyre::eyre!("read {}: {error}", args.chain_info.display()))?;
    let genesis = std::fs::read(&args.genesis_json)
        .map_err(|error| eyre::eyre!("read {}: {error}", args.genesis_json.display()))?;
    let (chain_spec, _init, _info) = crate::orbit_chain_from_files(&chain_info, &genesis)?;
    let chain_spec = Arc::new(chain_spec);

    let db_path = args.out.join("db");
    let static_files_path = args.out.join("static_files");
    let rocksdb_path = args.out.join("rocksdb");
    std::fs::create_dir_all(&static_files_path)?;
    std::fs::create_dir_all(&rocksdb_path)?;

    let factory = open_factory(&db_path, &static_files_path, &rocksdb_path, chain_spec)?;

    let blocks = write_blocks(&factory, &mut stream, &manifest)?;
    info!(
        target: "arb-snapshot",
        blocks = blocks.blocks,
        transactions = blocks.transactions,
        receipts = blocks.receipts,
        range = format!("{}..={}", blocks.first_block, blocks.last_block),
        "blocks section imported"
    );

    let history = write_history(&factory, &mut stream, &manifest)?;
    info!(
        target: "arb-snapshot",
        objects = history.objects,
        accounts = history.accounts,
        slots = history.slots,
        range = format!("{}..={}", history.first_block, history.last_block),
        "state history imported"
    );

    if history.first_block < blocks.first_block {
        return Err(eyre::eyre!(
            "history starts at {} but blocks start at {}; there would be changesets for blocks the \
             datadir has no header for",
            history.first_block,
            blocks.first_block
        ));
    }

    let state = write_state(&factory, &mut stream)?;
    info!(
        target: "arb-snapshot",
        accounts = state.accounts,
        slots = state.slots,
        bytecodes = state.bytecodes,
        "state imported; building the trie"
    );

    let root = super::snapshot::compute_state_root_chunked(&factory)?;
    if root != manifest.root {
        return Err(eyre::eyre!(
            "state root is {root:#x}, but the snapshot converted at {:#x}",
            manifest.root
        ));
    }
    info!(target: "arb-snapshot", %root, "state root matches the convert point");

    // The changeset files have to carry their real names before anything reads them back, and the
    // index stages below do exactly that.
    factory.static_file_provider().commit()?;
    rename_changeset_files_to_header(&static_files_path)?;

    finalize(&factory, &manifest, history.first_block, &args.out)?;

    println!(
        "converted {} blocks ({} transactions, {} receipts), {} history objects, {} accounts at \
         block {} root {root:#x}",
        blocks.blocks,
        blocks.transactions,
        blocks.receipts,
        history.objects,
        state.accounts,
        manifest.block,
    );
    Ok(())
}

/// Everything the datadir needs beyond the data itself: the indices reth derives from what was
/// imported, the checkpoints that say how far it is synced, the boundary below which historical
/// state is unavailable, and the manifest that marks the conversion complete.
///
/// The indices are built by running reth's own stages rather than by hand. They read exactly the
/// changesets, bodies and transactions just written, so what they produce is what a forward sync
/// would have produced, which is the property the whole conversion is trying to hold.
fn finalize<DB: SnapshotDb>(
    factory: &ProviderFactory<NodeTypesWithDBAdapter<ArbNode, DB>>,
    manifest: &Manifest,
    history_from: u64,
    out: &std::path::Path,
) -> eyre::Result<()> {
    run_stage(
        factory,
        SenderRecoveryStage::default(),
        manifest.block,
        "sender recovery",
    )?;
    run_stage(
        factory,
        TransactionLookupStage::default(),
        manifest.block,
        "transaction lookup",
    )?;
    run_stage(
        factory,
        IndexAccountHistoryStage::default(),
        manifest.block,
        "account history index",
    )?;
    run_stage(
        factory,
        IndexStorageHistoryStage::default(),
        manifest.block,
        "storage history index",
    )?;

    let provider = factory.database_provider_rw()?;

    // Read the convert point's header back rather than carrying it here, which also proves the
    // blocks section actually landed it.
    let head = reth_storage_api::HeaderProvider::sealed_header(&provider, manifest.block)?
        .ok_or_else(|| eyre::eyre!("block {} has no header after the import", manifest.block))?;
    if head.hash() != manifest.hash {
        return Err(eyre::eyre!(
            "block {} hashes to {:#x}, but the manifest says {:#x}",
            manifest.block,
            head.hash(),
            manifest.hash
        ));
    }

    // Stages must all name the convert point, since reth treats the database as synced only as far
    // as the lowest of them.
    let checkpoint = StageCheckpoint::new(manifest.block);
    for stage in StageId::ALL {
        provider.save_stage_checkpoint(stage, checkpoint)?;
    }

    write_history_boundary(&provider, history_from)?;
    provider
        .commit()
        .map_err(|error| eyre::eyre!("commit finalisation: {error}"))?;

    write_resume_log(manifest, out)?;

    // Last, and only once everything above held. Without it the node refuses to boot, so a run that
    // dies anywhere earlier leaves a datadir that cannot be mistaken for a finished one.
    super::snapshot::write_snapshot_import_manifest(
        out,
        &(manifest.block, head.hash(), head.header().clone()),
    )?;
    Ok(())
}

/// Drive one of reth's stages to completion over `[.., target]`.
///
/// A stage may return before reaching the target when it has done a batch's worth of work, so it is
/// re-entered from its own checkpoint until it reports done.
fn run_stage<DB, S>(
    factory: &ProviderFactory<NodeTypesWithDBAdapter<ArbNode, DB>>,
    mut stage: S,
    target: u64,
    what: &str,
) -> eyre::Result<()>
where
    DB: SnapshotDb,
    S: Stage<
        reth_provider::DatabaseProvider<
            <DB as Database>::TXMut,
            NodeTypesWithDBAdapter<ArbNode, DB>,
        >,
    >,
{
    let started = std::time::Instant::now();
    let mut checkpoint = None;
    loop {
        let provider = factory.database_provider_rw()?;
        let output = stage
            .execute(
                &provider,
                ExecInput {
                    target: Some(target),
                    checkpoint,
                },
            )
            .map_err(|error| eyre::eyre!("{what}: {error}"))?;
        provider
            .commit()
            .map_err(|error| eyre::eyre!("{what}: commit: {error}"))?;

        checkpoint = Some(output.checkpoint);
        if output.done {
            break;
        }
        info!(target: "arb-snapshot", stage = what, at = output.checkpoint.block_number, "building index");
    }
    info!(target: "arb-snapshot", stage = what, elapsed = ?started.elapsed(), "index built");
    Ok(())
}

/// Give the node its L1-derivation cursor, so it does not re-derive from batch 0.
///
/// Nothing in the L2 state says where derivation left off, so without this the node falls back to
/// re-deriving the whole chain: on Robinhood that was 685,718 parent-chain blocks of `getLogs` and
/// blob fetches, every result discarded, before a single new block. The exporter reads the answer
/// out of the snapshot's own `arbitrumdata` and carries it in the manifest.
fn write_resume_log(manifest: &Manifest, out: &std::path::Path) -> eyre::Result<()> {
    let Some(resume) = manifest.resume else {
        info!(
            target: "arb-snapshot",
            "the stream carries no resume point, so the node will re-derive from batch 0; \
             re-export with --arbitrumdata to avoid that"
        );
        return Ok(());
    };
    // The boundary sits at or below the convert point, never above it: derivation would otherwise
    // start after blocks the datadir does not have and leave a gap.
    if resume.l2_block > manifest.block {
        return Err(eyre::eyre!(
            "resume point names L2 block {}, above the convert point {}",
            resume.l2_block,
            manifest.block
        ));
    }
    let log = L1ResumeLog {
        checkpoints: vec![L1ResumeCheckpoint {
            l1_block: resume.l1_block,
            delayed_count: resume.delayed_count,
            l2_block: resume.l2_block,
        }],
    };
    let path = L1ResumeLog::path_in(out);
    std::fs::write(&path, serde_json::to_vec(&log)?)
        .map_err(|error| eyre::eyre!("write {}: {error}", path.display()))?;
    info!(
        target: "arb-snapshot",
        l1_block = resume.l1_block,
        delayed = resume.delayed_count,
        l2_block = resume.l2_block,
        "wrote the L1 derivation resume point"
    );
    Ok(())
}

/// Record that historical state below `history_from` is unavailable, and nothing else.
///
/// The converted datadir has changesets from `S_lo` upward, so a historical lookup below it has no
/// answer. Marking the missing prefix pruned makes reth fall back rather than infer, for example
/// reading an imported account as one first created after the snapshot. The existing head-state
/// importer marks everything below its head, which for a full conversion would be a lie about the
/// history that is actually present (ADR-004 D4).
fn write_history_boundary(
    provider: &impl reth_storage_api::PruneCheckpointWriter,
    history_from: u64,
) -> eyre::Result<()> {
    let Some(last_missing) = history_from.checked_sub(1) else {
        // History reaches the first block, so nothing is missing.
        return Ok(());
    };
    let checkpoint = PruneCheckpoint {
        block_number: Some(last_missing),
        tx_number: None,
        prune_mode: PruneMode::before_inclusive(last_missing),
    };
    for segment in [PruneSegment::AccountHistory, PruneSegment::StorageHistory] {
        provider.save_prune_checkpoint(segment, checkpoint)?;
    }
    Ok(())
}

fn open_factory(
    db_path: &std::path::Path,
    static_files_path: &std::path::Path,
    rocksdb_path: &std::path::Path,
    chain_spec: Arc<ChainSpec>,
) -> eyre::Result<ProviderFactory<ArbNodeTypesWithDB>> {
    let db = init_db(db_path, DatabaseArguments::new(ClientVersion::default()))?;
    let static_file_provider = StaticFileProvider::read_write(static_files_path)?;
    let rocksdb_provider = RocksDBProvider::builder(rocksdb_path)
        .with_default_tables()
        .build()
        .map_err(|error| eyre::eyre!("RocksDB open error: {error}"))?;

    let factory: ProviderFactory<ArbNodeTypesWithDB> = ProviderFactory::new(
        db,
        chain_spec,
        static_file_provider,
        rocksdb_provider,
        Runtime::test(),
    )
    .map_err(|error| eyre::eyre!("ProviderFactory::new: {error}"))?;

    // Storage v2 is where changesets live in static files and history indices in RocksDB, which is
    // what a converted archive datadir needs. Cache it before any write so every provider agrees,
    // and persist it so the node reads v2 on boot rather than defaulting to v1.
    factory.set_storage_settings_cache(StorageSettings::v2());
    let provider_rw = factory.database_provider_rw()?;
    provider_rw.write_storage_settings(StorageSettings::v2())?;
    provider_rw
        .commit()
        .map_err(|error| eyre::eyre!("persist storage settings: {error}"))?;

    Ok(factory)
}

/// One block's records, gathered until the next header shows the block is complete.
struct PendingBlock {
    number: u64,
    hash: B256,
    header: Header,
    body: Option<ArbBlockBody>,
    receipts: Option<Vec<arbitrum_alloy_consensus::receipt::ArbReceiptEnvelope>>,
}

/// Write the blocks section: headers, bodies and receipts for `[B_lo, P]`.
///
/// Every block is checked against its own header before it is written (ADR-004 B2). Both roots are
/// recomputed from the decoded values rather than copied, so a match also proves the decode is
/// faithful: the transactions re-encode to the committed `transactionsRoot`, and the receipts, whose
/// stored form drops the bloom and the transaction type, re-encode to the committed `receiptsRoot`.
fn write_blocks<R: Read, DB: SnapshotDb>(
    factory: &ProviderFactory<NodeTypesWithDBAdapter<ArbNode, DB>>,
    stream: &mut SnapshotStream<R>,
    manifest: &Manifest,
) -> eyre::Result<BlockSectionStats> {
    let mut stats = BlockSectionStats::default();
    let mut batch: Vec<PendingBlock> = Vec::with_capacity(BLOCK_BATCH);
    let mut pending: Option<PendingBlock> = None;
    let mut batch_txs = 0usize;
    let mut next_tx_num = 0u64;
    let mut first = true;

    loop {
        let record = stream.next_record()?;
        match record {
            Some(Record::Header { block, hash, rlp }) => {
                if let Some(done) = pending.take() {
                    batch_txs += done.body.as_ref().map_or(0, |b| b.transactions.len());
                    batch.push(done);
                }
                if batch.len() >= BLOCK_BATCH || batch_txs >= TX_BATCH {
                    flush_blocks(
                        factory,
                        &mut batch,
                        &mut next_tx_num,
                        &mut first,
                        &mut stats,
                    )?;
                    batch_txs = 0;
                }

                let mut input = rlp.as_slice();
                let header = Header::decode(&mut input)
                    .map_err(|error| eyre::eyre!("block {block}: decode header: {error}"))?;
                if !input.is_empty() {
                    return Err(eyre::eyre!(
                        "block {block}: trailing bytes after the header"
                    ));
                }
                if header.number != block {
                    return Err(eyre::eyre!(
                        "block {block}: header says it is block {}",
                        header.number
                    ));
                }
                pending = Some(PendingBlock {
                    number: block,
                    hash,
                    header,
                    body: None,
                    receipts: None,
                });
            }
            Some(Record::Body { block, rlp }) => {
                let target = expect_pending(&mut pending, block, "body")?;
                let mut input = rlp.as_slice();
                let body = ArbBlockBody::decode(&mut input)
                    .map_err(|error| eyre::eyre!("block {block}: decode body: {error}"))?;
                if !input.is_empty() {
                    return Err(eyre::eyre!("block {block}: trailing bytes after the body"));
                }
                let root = calculate_transaction_root(&body.transactions);
                if root != target.header.transactions_root {
                    return Err(eyre::eyre!(
                        "block {block}: transactions root is {root:#x}, header commits to {:#x}",
                        target.header.transactions_root
                    ));
                }
                target.body = Some(body);
            }
            Some(Record::Receipts { block, rlp }) => {
                let target = expect_pending(&mut pending, block, "receipts")?;
                // The stored form carries no transaction type, so the body has to have arrived
                // first. The exporter writes them in that order for every block.
                let body = target
                    .body
                    .as_ref()
                    .ok_or_else(|| eyre::eyre!("block {block}: receipts arrived without a body"))?;
                let tx_types: Vec<u8> = body.transactions.iter().map(tx_type_byte).collect();
                let receipts = decode_stored_receipts(&rlp, &tx_types)
                    .map_err(|error| eyre::eyre!("block {block}: {error}"))?;
                let root = calculate_receipt_root(&receipts);
                if root != target.header.receipts_root {
                    return Err(eyre::eyre!(
                        "block {block}: receipts root is {root:#x}, header commits to {:#x}",
                        target.header.receipts_root
                    ));
                }
                target.receipts = Some(receipts);
            }
            other => {
                if let Some(done) = pending.take() {
                    batch.push(done);
                }
                flush_blocks(
                    factory,
                    &mut batch,
                    &mut next_tx_num,
                    &mut first,
                    &mut stats,
                )?;
                if let Some(record) = other {
                    // The blocks section ends where the next section's first record begins.
                    stream.unread(record);
                }
                break;
            }
        }
    }

    if stats.blocks == 0 {
        return Err(eyre::eyre!("the stream's blocks section is empty"));
    }
    // The exporter stops the blocks section at the convert point, and so must the datadir: state,
    // blocks and history all have to end at the same block (ADR-004 P3).
    if stats.last_block != manifest.block {
        return Err(eyre::eyre!(
            "blocks end at {}, but the convert point is {}",
            stats.last_block,
            manifest.block
        ));
    }
    Ok(stats)
}

fn expect_pending<'a>(
    pending: &'a mut Option<PendingBlock>,
    block: u64,
    what: &str,
) -> eyre::Result<&'a mut PendingBlock> {
    let target = pending
        .as_mut()
        .ok_or_else(|| eyre::eyre!("block {block}: {what} arrived before any header"))?;
    if target.number != block {
        return Err(eyre::eyre!(
            "block {block}: {what} does not belong to the open block {}",
            target.number
        ));
    }
    Ok(target)
}

fn tx_type_byte(tx: &arbitrum_alloy_consensus::transactions::ArbTxEnvelope) -> u8 {
    use alloy_eips::Typed2718;
    tx.ty()
}

/// Commit one batch of complete blocks.
fn flush_blocks<DB: SnapshotDb>(
    factory: &ProviderFactory<NodeTypesWithDBAdapter<ArbNode, DB>>,
    batch: &mut Vec<PendingBlock>,
    next_tx_num: &mut u64,
    first: &mut bool,
    stats: &mut BlockSectionStats,
) -> eyre::Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let provider = factory.database_provider_rw()?;
    let sfp = provider.static_file_provider();
    let from_block = batch[0].number;

    {
        let mut writer = sfp.get_writer(from_block, StaticFileSegment::Headers)?;
        for block in batch.iter() {
            if *first && block.number > 0 {
                // A chain whose history starts above zero: seed the segment's range rather than
                // letting `increment_block` walk up from block 0.
                writer
                    .user_header_mut()
                    .set_block_range(block.number, block.number);
                writer.append_header_direct(&block.header, block.header.difficulty, &block.hash)?;
            } else {
                writer.append_header(&block.header, &block.hash)?;
            }
            *first = false;
        }
        // Not committed here: dropping the writer returns it to the pool, and `provider.commit()`
        // finalizes static files before RocksDB and MDBX, which is the order reth recovers from.
    }

    for block in batch.iter() {
        provider
            .tx_ref()
            .put::<tables::HeaderNumbers>(block.hash, block.number)?;
    }

    let bodies: Vec<_> = batch
        .iter()
        .map(|block| (block.number, block.body.as_ref()))
        .collect();
    provider.append_block_bodies(bodies)?;

    {
        let mut writer = EitherWriter::new_receipts(&provider, from_block)?;
        let mut tx_num = *next_tx_num;
        for block in batch.iter() {
            writer.increment_block(block.number)?;
            let count = block.body.as_ref().map_or(0, |b| b.transactions.len());
            match &block.receipts {
                Some(receipts) => {
                    for receipt in receipts {
                        writer.append_receipt(tx_num, receipt)?;
                        tx_num += 1;
                    }
                }
                // Blocks with transactions but no receipts would silently lose them.
                None if count > 0 => {
                    return Err(eyre::eyre!(
                        "block {} has {count} transactions but no receipts",
                        block.number
                    ));
                }
                None => {}
            }
        }
    }

    for block in batch.iter() {
        stats.blocks += 1;
        if stats.blocks == 1 {
            stats.first_block = block.number;
        }
        stats.last_block = block.number;
        if let Some(body) = &block.body {
            stats.bodies += 1;
            stats.transactions += body.transactions.len() as u64;
            *next_tx_num += body.transactions.len() as u64;
        }
        if let Some(receipts) = &block.receipts {
            stats.receipt_sets += 1;
            stats.receipts += receipts.len() as u64;
        }
    }

    // ADR-004 B5: reth's own view of where transactions end must agree with the counter driving the
    // receipt and sender numbering. If they ever part, every later block's receipts are misfiled.
    let last = batch[batch.len() - 1].number;
    let indices = provider.block_body_indices(last)?.ok_or_else(|| {
        eyre::eyre!("block {last}: body indices missing right after writing them")
    })?;
    if indices.next_tx_num() != *next_tx_num {
        return Err(eyre::eyre!(
            "block {last}: transaction numbering diverged; reth says the next is {}, the import \
             says {}",
            indices.next_tx_num(),
            *next_tx_num
        ));
    }

    provider
        .commit()
        .map_err(|error| eyre::eyre!("commit blocks up to {last}: {error}"))?;
    batch.clear();
    info!(target: "arb-snapshot", blocks = stats.blocks, transactions = stats.transactions, at = last, "wrote blocks");
    Ok(())
}

/// What the history section wrote.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HistorySectionStats {
    /// History objects, which is the number of blocks that actually changed state.
    pub objects: u64,
    /// Blocks given a changeset entry, including the empty ones for blocks that changed nothing.
    pub blocks: u64,
    pub accounts: u64,
    pub slots: u64,
    /// `S_lo`, the first block with history. Below it, historical state is unavailable.
    pub first_block: u64,
    pub last_block: u64,
}

/// Write the state-history section as reth changesets.
///
/// geth's reverse diffs and reth's changesets mean the same thing, the values from before the block,
/// and this snapshot's history identifies both accounts and storage slots by raw key, so the mapping
/// is direct.
///
/// A block that changed nothing produces no history object, but the changeset segments are indexed
/// by `block - block_range.start()`, so every block from `S_lo` to `P` still needs an entry. Those
/// blocks get an empty one, which is also what they mean (ADR-004 D1, S6).
fn write_history<R: Read, DB: SnapshotDb>(
    factory: &ProviderFactory<NodeTypesWithDBAdapter<ArbNode, DB>>,
    stream: &mut SnapshotStream<R>,
    manifest: &Manifest,
) -> eyre::Result<HistorySectionStats> {
    let mut stats = HistorySectionStats::default();
    let mut batch: Vec<HistoryObject> = Vec::new();
    let mut batch_entries = 0usize;
    // The next block the changeset segments expect, so gaps can be filled across batches.
    let mut cursor: Option<u64> = None;

    loop {
        match stream.next_record()? {
            Some(Record::History(object)) => {
                batch_entries += object
                    .accounts
                    .iter()
                    .map(|a| 1 + a.storage.len())
                    .sum::<usize>();
                batch.push(object);
                if batch_entries >= CHANGESET_BATCH {
                    flush_history(factory, &mut batch, &mut cursor, &mut stats)?;
                    batch_entries = 0;
                }
            }
            other => {
                flush_history(factory, &mut batch, &mut cursor, &mut stats)?;
                if let Some(record) = other {
                    stream.unread(record);
                }
                break;
            }
        }
    }

    if stats.objects == 0 {
        return Err(eyre::eyre!("the stream's history section is empty"));
    }
    // The last object's post root is the state the state section is about to describe, and the
    // reader has already checked the chain of roots that leads to it (ADR-004 S2, S3).
    stream.check_history_meets_state()?;

    // Nothing above `P` may carry a changeset, or an unwind would walk into a block whose state the
    // datadir does not have.
    if stats.last_block != manifest.block {
        return Err(eyre::eyre!(
            "history ends at {}, but the convert point is {}",
            stats.last_block,
            manifest.block
        ));
    }
    Ok(stats)
}

fn flush_history<DB: SnapshotDb>(
    factory: &ProviderFactory<NodeTypesWithDBAdapter<ArbNode, DB>>,
    batch: &mut Vec<HistoryObject>,
    cursor: &mut Option<u64>,
    stats: &mut HistorySectionStats,
) -> eyre::Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let provider = factory.database_provider_rw()?;
    let from_block = cursor.unwrap_or(batch[0].block);

    if cursor.is_none() {
        // The segments would otherwise start at their 500k file boundary, so the first block with
        // history would be written at the wrong offset and every read after it shifted. The files
        // are renamed to match once the import is done, since reth resolves their path from this.
        stats.first_block = from_block;
        let sfp = provider.static_file_provider();
        for segment in [
            StaticFileSegment::AccountChangeSets,
            StaticFileSegment::StorageChangeSets,
        ] {
            sfp.get_writer(from_block, segment)?
                .user_header_mut()
                .set_expected_block_start(from_block);
        }
    }

    let mut accounts_writer = EitherWriter::new_account_changesets(&provider, from_block)?;
    let mut storage_writer = EitherWriter::new_storage_changesets(&provider, from_block)?;
    let mut at = from_block;

    for object in batch.iter() {
        if object.block < at {
            return Err(eyre::eyre!(
                "history object at block {} is at or below the previous one ({})",
                object.block,
                at.saturating_sub(1)
            ));
        }
        // Blocks between two objects changed no state, so their changeset is empty.
        for empty in at..object.block {
            accounts_writer.append_account_changeset(empty, Vec::new())?;
            storage_writer.append_storage_changeset(empty, Vec::new())?;
            stats.blocks += 1;
        }

        let mut accounts = Vec::with_capacity(object.accounts.len());
        let mut slots = Vec::new();
        for account in &object.accounts {
            accounts.push(AccountBeforeTx {
                address: account.address,
                info: account
                    .previous
                    .map(|(nonce, balance, bytecode_hash)| Account {
                        nonce,
                        balance,
                        bytecode_hash,
                    }),
            });
            for (key, value) in &account.storage {
                slots.push(StorageBeforeTx {
                    address: account.address,
                    key: *key,
                    value: *value,
                });
            }
        }
        stats.accounts += accounts.len() as u64;
        stats.slots += slots.len() as u64;
        accounts_writer.append_account_changeset(object.block, accounts)?;
        storage_writer.append_storage_changeset(object.block, slots)?;

        stats.objects += 1;
        stats.blocks += 1;
        stats.last_block = object.block;
        at = object.block + 1;
    }

    drop(accounts_writer);
    drop(storage_writer);
    *cursor = Some(at);
    provider
        .commit()
        .map_err(|error| eyre::eyre!("commit history up to {}: {error}", at - 1))?;
    batch.clear();
    info!(
        target: "arb-snapshot",
        objects = stats.objects,
        accounts = stats.accounts,
        slots = stats.slots,
        at = at - 1,
        "wrote state history"
    );
    Ok(())
}

/// What the state section wrote.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StateSectionStats {
    pub accounts: u64,
    pub slots: u64,
    pub bytecodes: u64,
}

/// Write the state at the convert point into the hashed-state tables.
///
/// The keys are already hashed, because a pruned snapshot has no preimage for them. Storage v2
/// treats the hashed tables as canonical, which is why this is enough to serve current state; the
/// trie is then built over it and its root checked against the manifest (ADR-004 P2).
fn write_state<R: Read, DB: SnapshotDb>(
    factory: &ProviderFactory<NodeTypesWithDBAdapter<ArbNode, DB>>,
    stream: &mut SnapshotStream<R>,
) -> eyre::Result<StateSectionStats> {
    let mut stats = StateSectionStats::default();
    let mut provider = factory.database_provider_rw()?;
    let mut written = 0usize;
    // Storage records belong to the account that most recently went past. A code record can sit
    // between an account and its slots, so it must not disturb this.
    let mut account: Option<B256> = None;
    // Every code hash an account referenced, against every code blob the stream carried.
    let mut wanted: HashSet<B256> = HashSet::new();
    let mut provided: HashSet<B256> = HashSet::new();

    loop {
        if written >= STATE_COMMIT_THRESHOLD {
            provider
                .commit()
                .map_err(|error| eyre::eyre!("commit state: {error}"))?;
            provider = factory.database_provider_rw()?;
            written = 0;
            info!(
                target: "arb-snapshot",
                accounts = stats.accounts,
                slots = stats.slots,
                bytecodes = stats.bytecodes,
                "wrote state"
            );
        }

        match stream.next_record()? {
            Some(Record::Account {
                hashed_address,
                nonce,
                balance,
                code_hash,
            }) => {
                if let Some(hash) = code_hash {
                    wanted.insert(hash);
                }
                provider.tx_ref().put::<tables::HashedAccounts>(
                    hashed_address,
                    Account {
                        nonce,
                        balance,
                        bytecode_hash: code_hash,
                    },
                )?;
                account = Some(hashed_address);
                stats.accounts += 1;
                written += 1;
            }
            Some(Record::Storage { hashed_slot, value }) => {
                let owner = account.ok_or_else(|| {
                    eyre::eyre!("storage slot {hashed_slot:#x} arrived before any account")
                })?;
                // A zero slot is absent from the trie, so writing one would change the root.
                if value.is_zero() {
                    continue;
                }
                provider
                    .tx_ref()
                    .cursor_dup_write::<tables::HashedStorages>()?
                    .upsert(
                        owner,
                        &StorageEntry {
                            key: hashed_slot,
                            value,
                        },
                    )?;
                stats.slots += 1;
                written += 1;
            }
            Some(Record::Code { hash, code }) => {
                // The stream reader already checked the blob hashes to its key.
                provided.insert(hash);
                provider
                    .tx_ref()
                    .put::<tables::Bytecodes>(hash, Bytecode::new_raw(code.into()))?;
                stats.bytecodes += 1;
                written += 1;
            }
            other => {
                if let Some(record) = other {
                    return Err(eyre::eyre!(
                        "unexpected {record:?} after the state section; it is the last one"
                    ));
                }
                break;
            }
        }
    }

    provider
        .commit()
        .map_err(|error| eyre::eyre!("commit state: {error}"))?;

    if stats.accounts == 0 {
        return Err(eyre::eyre!("the stream's state section is empty"));
    }
    if let Some(missing) = wanted.difference(&provided).next() {
        return Err(eyre::eyre!(
            "an account references code {missing:#x}, which the stream does not carry"
        ));
    }
    Ok(stats)
}

/// Rename changeset static files whose name disagrees with the expected range in their header.
///
/// reth resolves a segment file's path from that range, and the first changeset file's start was
/// moved to `S_lo` after it was created in its fixed 500k slot, so its name is stale. Every other
/// file already agrees; this checks them all rather than assuming which one moved.
fn rename_changeset_files_to_header(static_files: &std::path::Path) -> eyre::Result<()> {
    for segment in ["account-change-sets", "storage-change-sets"] {
        let prefix = format!("static_file_{segment}_");
        let names: Vec<String> = std::fs::read_dir(static_files)?
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with(&prefix) && !name.contains('.'))
            .collect();

        for name in names {
            let conf = std::fs::read(static_files.join(format!("{name}.conf")))?;
            if conf.len() < 24 {
                continue;
            }
            let start = u64::from_le_bytes(conf[8..16].try_into().expect("8 bytes"));
            let end = u64::from_le_bytes(conf[16..24].try_into().expect("8 bytes"));
            let want = format!("static_file_{segment}_{start}_{end}");
            if name == want {
                continue;
            }
            for extension in ["", ".conf", ".off", ".csoff"] {
                let from = static_files.join(format!("{name}{extension}"));
                let to = static_files.join(format!("{want}{extension}"));
                if from.exists() && from != to {
                    std::fs::rename(&from, &to)?;
                }
            }
            info!(target: "arb-snapshot", segment, from = %name, to = %want, "renamed changeset file to match its header");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{Eip658Value, Receipt, ReceiptWithBloom, TxEip1559};
    use alloy_eips::Decodable2718;
    use alloy_primitives::{
        Bytes, Log, LogData, Signature, TxKind, U256, address, b256, keccak256, logs_bloom,
    };
    use alloy_rlp::Encodable;
    use arb_reth_genesis::snapshot_stream::{HistoryAccount, HistoryObject};
    use arb_reth_genesis::snapshot_stream::{Manifest, StreamBuilder};
    use arbitrum_alloy_consensus::{
        receipt::{ArbReceipt, ArbReceiptEnvelope},
        transactions::{ArbTxEnvelope, deposit::TxDeposit},
    };
    use reth_db_api::transaction::DbTx;
    use reth_provider::test_utils::create_test_provider_factory_with_node_types;
    use reth_storage_api::{
        ChangeSetReader, PruneCheckpointReader, ReceiptProvider, StageCheckpointReader,
        StorageChangeSetReader, TransactionsProvider,
    };

    use super::*;

    /// A factory over a temporary MDBX, static files and RocksDB, in storage v2 like the import.
    fn v2_factory() -> reth_provider::ProviderFactory<
        NodeTypesWithDBAdapter<
            ArbNode,
            Arc<reth_db::test_utils::TempDatabase<reth_db::DatabaseEnv>>,
        >,
    > {
        let factory = create_test_provider_factory_with_node_types::<ArbNode>(spec());
        factory.set_storage_settings_cache(StorageSettings::v2());
        let provider = factory.database_provider_rw().unwrap();
        provider
            .write_storage_settings(StorageSettings::v2())
            .unwrap();
        provider.commit().unwrap();
        factory
    }

    fn spec() -> Arc<ChainSpec> {
        use arb_revm::arbos_init::ArbosInitConfig;
        const CHAIN_CONFIG: &[u8] =
            include_bytes!("../../tests/fixtures/testnode_l2_chain_config.json");
        let init = ArbosInitConfig {
            initial_arbos_version: 40,
            initial_chain_owner: address!("5E1497dD1f08C87b2d8FE23e9AAB6c1De833D927"),
            chain_id: U256::from(412346u64),
            genesis_block_number: 0,
            initial_l1_base_fee: U256::from(167u64),
            serialized_chain_config: CHAIN_CONFIG.to_vec(),
            debug_precompiles: true,
        };
        Arc::new(crate::arb_chain_spec(&init).expect("build chain spec"))
    }

    /// A signed EIP-1559 transaction and an Arbitrum deposit, so the test covers both a type the
    /// body wraps as a typed envelope and an Arbitrum-only type.
    fn transactions() -> Vec<ArbTxEnvelope> {
        use alloy_consensus::Signed;
        let signed = Signed::new_unhashed(
            TxEip1559 {
                chain_id: 412346,
                nonce: 3,
                gas_limit: 40_000,
                max_fee_per_gas: 2_000_000_000,
                max_priority_fee_per_gas: 1_000_000_000,
                to: TxKind::Call(address!("2222222222222222222222222222222222222222")),
                value: U256::from(9u64),
                access_list: Default::default(),
                input: Bytes::from_static(&[0xde, 0xad]),
            },
            Signature::test_signature(),
        );
        vec![
            ArbTxEnvelope::Eip1559(signed),
            ArbTxEnvelope::from(TxDeposit {
                chain_id: U256::from(412346u64),
                request_id: b256!(
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                ),
                from: address!("1111111111111111111111111111111111111111"),
                to: address!("2222222222222222222222222222222222222222"),
                value: U256::from(123u64),
            }),
        ]
    }

    fn log() -> Log {
        Log {
            address: address!("00000000000000000000000000000000000000aa"),
            data: LogData::new_unchecked(
                vec![b256!(
                    "1111111111111111111111111111111111111111111111111111111111111111"
                )],
                Bytes::from_static(&[0xbe, 0xef]),
            ),
        }
    }

    /// One receipt described once, so the stored bytes and the expected envelope cannot drift.
    struct ReceiptSpec {
        tx_type: u8,
        success: bool,
        cumulative_gas_used: u64,
        gas_used_for_l1: u64,
        logs: Vec<Log>,
    }

    fn receipt_specs() -> Vec<ReceiptSpec> {
        vec![
            ReceiptSpec {
                tx_type: 0x02,
                success: true,
                cumulative_gas_used: 21_000,
                gas_used_for_l1: 4_321,
                logs: vec![log()],
            },
            ReceiptSpec {
                tx_type: 0x64,
                success: false,
                cumulative_gas_used: 50_000,
                gas_used_for_l1: 0,
                logs: vec![],
            },
        ]
    }

    /// The receipts as reth models them: bloom present, type carried by the envelope.
    fn receipts() -> Vec<ArbReceiptEnvelope> {
        receipt_specs()
            .into_iter()
            .map(|spec| {
                let receipt = ArbReceipt {
                    inner: Receipt {
                        status: Eip658Value::Eip658(spec.success),
                        cumulative_gas_used: spec.cumulative_gas_used,
                        logs: spec.logs,
                    },
                    gas_used_for_l1: spec.gas_used_for_l1,
                };
                arb_reth_evm::block::receipt_envelope_for_type(
                    spec.tx_type,
                    ReceiptWithBloom {
                        logs_bloom: logs_bloom(receipt.inner.logs.iter()),
                        receipt,
                    },
                )
            })
            .collect()
    }

    /// The same receipts in Nitro's storage form: no bloom, no type, plus `l1GasUsed`. This is what
    /// the exporter copies out of the freezer, so the test drives the real decode path.
    fn stored_receipts_rlp(specs: &[ReceiptSpec]) -> Vec<u8> {
        let mut items = Vec::new();
        for spec in specs {
            let mut fields = Vec::new();
            if spec.success {
                Bytes::from_static(&[1]).encode(&mut fields);
            } else {
                Bytes::new().encode(&mut fields);
            }
            spec.cumulative_gas_used.encode(&mut fields);
            spec.gas_used_for_l1.encode(&mut fields);
            alloy_rlp::encode_list(&spec.logs, &mut fields);
            alloy_rlp::Header {
                list: true,
                payload_length: fields.len(),
            }
            .encode(&mut items);
            items.extend_from_slice(&fields);
        }
        let mut out = Vec::new();
        alloy_rlp::Header {
            list: true,
            payload_length: items.len(),
        }
        .encode(&mut out);
        out.extend_from_slice(&items);
        out
    }

    fn body_rlp(transactions: &[ArbTxEnvelope]) -> Vec<u8> {
        let body = ArbBlockBody {
            transactions: transactions.to_vec(),
            ommers: Vec::new(),
            withdrawals: None,
        };
        alloy_rlp::encode(&body)
    }

    fn header(
        number: u64,
        parent_hash: B256,
        body: &[ArbTxEnvelope],
        rcpts: &[ArbReceiptEnvelope],
    ) -> Header {
        Header {
            number,
            parent_hash,
            difficulty: U256::from(1u64),
            transactions_root: calculate_transaction_root(body),
            receipts_root: calculate_receipt_root(rcpts),
            ..Default::default()
        }
    }

    /// Blocks 0..=2, where block 1 carries transactions and receipts and the others are empty, then
    /// the whole thing read back out of the database.
    fn three_block_stream() -> (Vec<u8>, Manifest, Vec<B256>) {
        let txs = transactions();
        let rcpts = receipts();

        let h0 = header(0, B256::ZERO, &[], &[]);
        let hash0 = h0.hash_slow();
        let h1 = header(1, hash0, &txs, &rcpts);
        let hash1 = h1.hash_slow();
        let h2 = header(2, hash1, &[], &[]);
        let hash2 = h2.hash_slow();

        let manifest = Manifest {
            block: 2,
            root: B256::repeat_byte(0xee),
            state_id: 3,
            hash: hash2,
            resume: None,
        };
        let bytes = StreamBuilder::new(&manifest)
            .blocks()
            .header(0, &alloy_rlp::encode(&h0))
            .body(0, &body_rlp(&[]))
            .header(1, &alloy_rlp::encode(&h1))
            .body(1, &body_rlp(&txs))
            .receipts(1, &stored_receipts_rlp(&receipt_specs()))
            .header(2, &alloy_rlp::encode(&h2))
            .body(2, &body_rlp(&[]))
            .end_section()
            .history_section()
            .end_section()
            .state()
            .end_section()
            .finish();
        (bytes, manifest, vec![hash0, hash1, hash2])
    }

    #[test]
    fn writes_headers_bodies_and_receipts_into_a_readable_database() {
        let factory = create_test_provider_factory_with_node_types::<ArbNode>(spec());
        factory.set_storage_settings_cache(StorageSettings::v2());
        {
            let provider = factory.database_provider_rw().unwrap();
            provider
                .write_storage_settings(StorageSettings::v2())
                .unwrap();
            provider.commit().unwrap();
        }

        let (bytes, manifest, hashes) = three_block_stream();
        let mut stream = SnapshotStream::open(bytes.as_slice()).unwrap();
        let stats = write_blocks(&factory, &mut stream, &manifest).unwrap();

        assert_eq!(stats.blocks, 3);
        assert_eq!(stats.bodies, 3);
        assert_eq!(stats.transactions, 2);
        assert_eq!(stats.receipts, 2);
        assert_eq!((stats.first_block, stats.last_block), (0, 2));

        let provider = factory.provider().unwrap();
        for (number, hash) in hashes.iter().enumerate() {
            let sealed = reth_storage_api::HeaderProvider::sealed_header(&provider, number as u64)
                .unwrap()
                .unwrap_or_else(|| panic!("no header at {number}"));
            assert_eq!(sealed.hash(), *hash, "block {number} hash");
        }

        // Only block 1 carries transactions, and they must land under its own numbering.
        assert_eq!(provider.block_body_indices(0).unwrap().unwrap().tx_count, 0);
        let indices = provider.block_body_indices(1).unwrap().unwrap();
        assert_eq!((indices.first_tx_num, indices.tx_count), (0, 2));
        assert_eq!(
            provider
                .block_body_indices(2)
                .unwrap()
                .unwrap()
                .first_tx_num,
            2
        );

        let stored_txs = provider
            .transactions_by_block(1u64.into())
            .unwrap()
            .unwrap();
        assert_eq!(stored_txs, transactions());
        let stored_receipts = provider.receipts_by_block(1u64.into()).unwrap().unwrap();
        assert_eq!(stored_receipts, receipts());
        assert!(
            provider
                .receipts_by_block(0u64.into())
                .unwrap()
                .unwrap()
                .is_empty(),
            "an empty block keeps an empty receipt list, not a missing one"
        );
    }

    const ACCOUNT_A: alloy_primitives::Address =
        address!("00000000000000000000000000000000000000a1");
    const ACCOUNT_B: alloy_primitives::Address =
        address!("00000000000000000000000000000000000000b2");

    /// Blocks 0..=4 with history at 2 and 4 only. That covers the three cases the writer has to get
    /// right: history starting above the chain's first block, a block in between that changed
    /// nothing, and history ending exactly at the convert point.
    fn history_stream(root: B256) -> (Vec<u8>, Manifest, Vec<HistoryObject>) {
        let mut builder = StreamBuilder::new(&Manifest {
            block: 4,
            root,
            state_id: 5,
            hash: B256::repeat_byte(0xaa),
            resume: None,
        })
        .blocks();
        let mut parent = B256::ZERO;
        for number in 0..=4u64 {
            let h = header(number, parent, &[], &[]);
            parent = h.hash_slow();
            builder = builder
                .header(number, &alloy_rlp::encode(&h))
                .body(number, &body_rlp(&[]));
        }
        let manifest = Manifest {
            block: 4,
            root,
            state_id: 5,
            hash: parent,
            resume: None,
        };

        let objects = vec![
            HistoryObject {
                state_id: 3,
                block: 2,
                parent_root: B256::repeat_byte(0x11),
                post_root: B256::repeat_byte(0x22),
                accounts: vec![
                    HistoryAccount {
                        address: ACCOUNT_A,
                        previous: Some((7, U256::from(1234u64), Some(B256::repeat_byte(0x9)))),
                        storage: vec![
                            (B256::repeat_byte(0x01), U256::from(5u64)),
                            (B256::repeat_byte(0x02), U256::ZERO),
                        ],
                    },
                    // Did not exist before this block.
                    HistoryAccount {
                        address: ACCOUNT_B,
                        previous: None,
                        storage: vec![],
                    },
                ],
            },
            HistoryObject {
                state_id: 5,
                block: 4,
                parent_root: B256::repeat_byte(0x22),
                post_root: root,
                accounts: vec![HistoryAccount {
                    address: ACCOUNT_B,
                    previous: Some((1, U256::from(9u64), None)),
                    storage: vec![(B256::repeat_byte(0x03), U256::from(77u64))],
                }],
            },
        ];

        let mut builder = builder.end_section().history_section();
        for object in &objects {
            builder = builder.history(object);
        }
        let bytes = builder.end_section().state().end_section().finish();
        (bytes, manifest, objects)
    }

    #[test]
    fn writes_state_history_as_changesets_reth_can_read() {
        let factory = v2_factory();
        let root = B256::repeat_byte(0xee);
        let (bytes, manifest, objects) = history_stream(root);

        let mut stream = SnapshotStream::open(bytes.as_slice()).unwrap();
        write_blocks(&factory, &mut stream, &manifest).unwrap();
        let stats = write_history(&factory, &mut stream, &manifest).unwrap();

        assert_eq!(stats.objects, 2);
        assert_eq!(stats.accounts, 3);
        assert_eq!(stats.slots, 3);
        // Blocks 2, 3 and 4 all get an entry; block 3 changed nothing so its entry is empty.
        assert_eq!(stats.blocks, 3);
        assert_eq!((stats.first_block, stats.last_block), (2, 4));

        let provider = factory.provider().unwrap();

        let changed = provider.account_block_changeset(2).unwrap();
        assert_eq!(
            changed,
            vec![
                AccountBeforeTx {
                    address: ACCOUNT_A,
                    info: Some(Account {
                        nonce: 7,
                        balance: U256::from(1234u64),
                        bytecode_hash: Some(B256::repeat_byte(0x9)),
                    }),
                },
                AccountBeforeTx {
                    address: ACCOUNT_B,
                    info: None,
                },
            ],
            "pre-block values, with a missing account kept as missing"
        );

        let slots = provider.storage_changeset(2).unwrap();
        assert_eq!(slots.len(), 2);
        assert_eq!(slots[0].0.address(), ACCOUNT_A);
        assert_eq!(slots[0].1.key, B256::repeat_byte(0x01));
        assert_eq!(slots[0].1.value, U256::from(5u64));
        // A slot that was zero before the block is still a change, and has to stay in the set.
        assert_eq!(slots[1].1.key, B256::repeat_byte(0x02));
        assert_eq!(slots[1].1.value, U256::ZERO);

        assert!(
            provider.account_block_changeset(3).unwrap().is_empty(),
            "a block that changed nothing has an empty changeset, not a missing one"
        );
        assert!(provider.storage_changeset(3).unwrap().is_empty());

        assert_eq!(
            provider.account_block_changeset(4).unwrap(),
            vec![AccountBeforeTx {
                address: ACCOUNT_B,
                info: Some(Account {
                    nonce: 1,
                    balance: U256::from(9u64),
                    bytecode_hash: None,
                }),
            }]
        );
        assert_eq!(objects[1].post_root, root);
    }

    /// Blocks, history and state in one stream, with the state root the trie actually produces so
    /// the manifest check passes.
    fn full_stream(root: B256) -> (Vec<u8>, Manifest) {
        // Same blocks and history, but with a real state section in place of the empty one.
        let (_, manifest, objects) = history_stream(root);
        let mut builder = StreamBuilder::new(&manifest).blocks();
        let mut parent = B256::ZERO;
        for number in 0..=4u64 {
            let h = header(number, parent, &[], &[]);
            parent = h.hash_slow();
            builder = builder
                .header(number, &alloy_rlp::encode(&h))
                .body(number, &body_rlp(&[]));
        }
        builder = builder.end_section().history_section();
        for object in &objects {
            builder = builder.history(object);
        }
        let code: &[u8] = &[0x60, 0x00, 0x56];
        let bytes = builder
            .end_section()
            .state()
            .account(
                keccak256(ACCOUNT_A),
                7,
                U256::from(1234u64),
                Some(keccak256(code)),
            )
            .code(code)
            .storage(keccak256(B256::repeat_byte(0x01)), U256::from(5u64))
            // A zero slot is absent from the trie, so it must not reach the database.
            .storage(keccak256(B256::repeat_byte(0x02)), U256::ZERO)
            .account(keccak256(ACCOUNT_B), 3, U256::from(42u64), None)
            .end_section()
            .finish();
        (bytes, manifest)
    }

    /// A stream carrying a resume point must leave the node able to skip re-deriving the whole
    /// chain. Without this the conversion is correct but unusable: the node re-derives from batch 0,
    /// which on a real chain is hundreds of thousands of L1 blocks fetched and discarded.
    #[test]
    fn a_resume_point_in_the_stream_becomes_the_node_s_derivation_cursor() {
        let factory = v2_factory();
        let root = B256::repeat_byte(0xee);
        let (bytes, mut manifest) = full_stream(root);
        // Rebuild the stream with a resume point in its manifest.
        manifest.resume = Some(arb_reth_genesis::snapshot_stream::ResumePoint {
            l1_block: 25_679_956,
            delayed_count: 91_588,
            l2_block: 3,
        });
        let _ = bytes;
        let mut rebuilt = StreamBuilder::new(&manifest).blocks();
        let mut parent = B256::ZERO;
        for number in 0..=4u64 {
            let h = header(number, parent, &[], &[]);
            parent = h.hash_slow();
            rebuilt = rebuilt
                .header(number, &alloy_rlp::encode(&h))
                .body(number, &body_rlp(&[]));
        }
        let bytes = rebuilt
            .end_section()
            .history_section()
            .end_section()
            .state()
            .account(keccak256(ACCOUNT_A), 1, U256::from(1u64), None)
            .end_section()
            .finish();

        let mut stream = SnapshotStream::open(bytes.as_slice()).unwrap();
        assert_eq!(stream.manifest().resume, manifest.resume);
        write_blocks(&factory, &mut stream, &manifest).unwrap();

        let out = tempfile::tempdir().unwrap();
        write_resume_log(&manifest, out.path()).unwrap();

        let log = L1ResumeLog::load(&L1ResumeLog::path_in(out.path())).expect("resume log");
        assert_eq!(log.checkpoints.len(), 1);
        assert_eq!(log.checkpoints[0].l1_block, 25_679_956);
        assert_eq!(log.checkpoints[0].delayed_count, 91_588);
        // The node resolves it for its own tip, which is what makes a non-boundary convert point work.
        assert_eq!(log.resume_for(4).map(|c| c.l2_block), Some(3));
    }

    /// A cursor above the convert point would start derivation after blocks the datadir does not
    /// have, leaving a gap that nothing downstream detects.
    #[test]
    fn rejects_a_resume_point_above_the_convert_point() {
        let out = tempfile::tempdir().unwrap();
        let manifest = Manifest {
            block: 100,
            root: B256::repeat_byte(0xee),
            state_id: 1,
            hash: B256::repeat_byte(0xaa),
            resume: Some(arb_reth_genesis::snapshot_stream::ResumePoint {
                l1_block: 5,
                delayed_count: 0,
                l2_block: 101,
            }),
        };
        let error = write_resume_log(&manifest, out.path())
            .unwrap_err()
            .to_string();
        assert!(error.contains("above the convert point"), "{error}");
    }

    /// `S_lo` has to come back out of the datadir for finalisation to be re-runnable on its own,
    /// and the sidecars share the segment prefix, so they must not be mistaken for segments.
    #[test]
    fn reads_the_history_floor_back_from_the_changeset_segments() {
        let dir = tempfile::tempdir().unwrap();
        for name in [
            "static_file_account-change-sets_1_499999",
            "static_file_account-change-sets_1_499999.conf",
            "static_file_account-change-sets_1_499999.csoff",
            "static_file_account-change-sets_500000_999999",
            "static_file_storage-change-sets_1_499999",
            "static_file_headers_0_499999",
        ] {
            std::fs::write(dir.path().join(name), []).unwrap();
        }
        assert_eq!(lowest_changeset_block(dir.path()).unwrap(), 1);

        let empty = tempfile::tempdir().unwrap();
        let error = lowest_changeset_block(empty.path())
            .unwrap_err()
            .to_string();
        assert!(error.contains("no state history to finish"), "{error}");
    }

    /// Finalisation on top of a converted datadir: reth's own stages build the indices from what
    /// was imported, the checkpoints say how far it is synced, and the boundary says how far back
    /// history goes.
    #[test]
    fn finalisation_builds_the_indices_and_marks_the_history_boundary() {
        let factory = v2_factory();
        let root = B256::repeat_byte(0xee);
        let (bytes, manifest) = full_stream(root);
        let mut stream = SnapshotStream::open(bytes.as_slice()).unwrap();
        write_blocks(&factory, &mut stream, &manifest).unwrap();
        let history = write_history(&factory, &mut stream, &manifest).unwrap();
        write_state(&factory, &mut stream).unwrap();

        factory.static_file_provider().commit().unwrap();
        rename_changeset_files_to_header(factory.static_file_provider().directory()).unwrap();

        let out = tempfile::tempdir().unwrap();
        finalize(&factory, &manifest, history.first_block, out.path()).unwrap();

        let provider = factory.provider().unwrap();

        // Every stage names the convert point, or reth reads the database as synced to the lowest.
        for stage in StageId::ALL {
            assert_eq!(
                provider
                    .get_stage_checkpoint(stage)
                    .unwrap()
                    .map(|c| c.block_number),
                Some(manifest.block),
                "{stage} checkpoint"
            );
        }

        // History starts at S_lo = 2, so blocks 0 and 1 are the unavailable prefix and nothing else.
        for segment in [PruneSegment::AccountHistory, PruneSegment::StorageHistory] {
            assert_eq!(
                provider
                    .get_prune_checkpoint(segment)
                    .unwrap()
                    .and_then(|c| c.block_number),
                Some(history.first_block - 1),
                "{segment:?} boundary"
            );
        }

        drop(provider);

        // The query the whole feature exists for, and the one the head-state importer cannot
        // answer: account B's balance as of block 3, which only the changesets know. The head
        // state holds different values, so a lookup that quietly fell through to it would fail
        // here rather than pass by coincidence.
        use reth_storage_api::AccountReader;
        let historical = factory.history_by_block_number(3).unwrap();
        let before = historical
            .basic_account(&ACCOUNT_B)
            .unwrap()
            .expect("account B at block 3");
        assert_eq!(
            (before.nonce, before.balance),
            (1, U256::from(9u64)),
            "pre-block-4 values, taken from the changeset"
        );

        let latest = factory.latest().unwrap();
        let head = latest
            .basic_account(&ACCOUNT_B)
            .unwrap()
            .expect("account B at head");
        assert_eq!(
            (head.nonce, head.balance),
            (3, U256::from(42u64)),
            "head state, taken from the state section"
        );

        // The completion manifest is written last and only on success.
        assert!(out.path().join("snapshot-import.json").is_file());

        // Absent from this stream, so no cursor is written and the node falls back to batch 0.
        assert!(!out.path().join("arb-l1-resume.json").is_file());
    }

    /// The whole conversion, end to end: every section written, then the trie built over the
    /// imported state and its root compared with the manifest.
    #[test]
    fn imports_all_three_sections_and_reproduces_the_state_root() {
        let factory = v2_factory();
        // The root the imported state actually hashes to is only known after the fact, so run once
        // with a placeholder to learn it, then assert the check accepts the real one and rejects
        // the placeholder.
        let (bytes, manifest) = full_stream(B256::repeat_byte(0xee));
        let mut stream = SnapshotStream::open(bytes.as_slice()).unwrap();
        write_blocks(&factory, &mut stream, &manifest).unwrap();
        write_history(&factory, &mut stream, &manifest).unwrap();
        let state = write_state(&factory, &mut stream).unwrap();

        assert_eq!(state.accounts, 2);
        assert_eq!(state.slots, 1, "the zero slot is not written");
        assert_eq!(state.bytecodes, 1);

        let root = super::super::snapshot::compute_state_root_chunked(&factory).unwrap();
        assert_ne!(root, B256::ZERO);
        assert_ne!(
            root, manifest.root,
            "the placeholder root must not accidentally match"
        );

        let provider = factory.provider().unwrap();
        let account = provider
            .tx_ref()
            .get::<tables::HashedAccounts>(keccak256(ACCOUNT_A))
            .unwrap()
            .expect("account A");
        assert_eq!(account.nonce, 7);
        assert_eq!(account.balance, U256::from(1234u64));
        assert_eq!(
            account.bytecode_hash,
            Some(keccak256([0x60u8, 0x00, 0x56].as_slice()))
        );
        assert!(
            provider
                .tx_ref()
                .get::<tables::Bytecodes>(keccak256([0x60u8, 0x00, 0x56].as_slice()))
                .unwrap()
                .is_some()
        );
        // Account B has no code, so it must be stored as an EOA rather than as empty code.
        assert_eq!(
            provider
                .tx_ref()
                .get::<tables::HashedAccounts>(keccak256(ACCOUNT_B))
                .unwrap()
                .expect("account B")
                .bytecode_hash,
            None
        );
    }

    /// An account whose code the stream never carried would leave the datadir unable to execute
    /// against it, and nothing else would notice.
    #[test]
    fn rejects_state_that_references_missing_code() {
        let factory = v2_factory();
        let manifest = Manifest {
            block: 0,
            root: B256::repeat_byte(0xee),
            state_id: 1,
            hash: B256::repeat_byte(0xaa),
            resume: None,
        };
        let bytes = StreamBuilder::new(&manifest)
            .blocks()
            .end_section()
            .history_section()
            .end_section()
            .state()
            .account(
                keccak256(ACCOUNT_A),
                0,
                U256::ZERO,
                Some(B256::repeat_byte(0x77)),
            )
            .end_section()
            .finish();

        let mut stream = SnapshotStream::open(bytes.as_slice()).unwrap();
        let error = write_state(&factory, &mut stream).unwrap_err().to_string();
        assert!(error.contains("does not carry"), "{error}");
    }

    /// The first changeset file is created in its fixed 500k slot and then has its expected start
    /// moved to `S_lo`, so its name no longer matches the range reth resolves paths from. Until it
    /// is renamed, a freshly opened provider cannot find it at all.
    #[test]
    fn renaming_makes_the_changeset_files_findable_by_a_fresh_provider() {
        let factory = v2_factory();
        let root = B256::repeat_byte(0xee);
        let (bytes, manifest, _) = history_stream(root);

        let mut stream = SnapshotStream::open(bytes.as_slice()).unwrap();
        write_blocks(&factory, &mut stream, &manifest).unwrap();
        write_history(&factory, &mut stream, &manifest).unwrap();

        let directory = factory.static_file_provider().directory().to_path_buf();
        let names = |dir: &std::path::Path| -> Vec<String> {
            let mut found: Vec<String> = std::fs::read_dir(dir)
                .unwrap()
                .filter_map(Result::ok)
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n.starts_with("static_file_account-change-sets_") && !n.contains('.'))
                .collect();
            found.sort();
            found
        };
        assert_eq!(
            names(&directory),
            vec!["static_file_account-change-sets_0_499999".to_string()],
            "created in the fixed slot, while its header now expects to start at 2"
        );

        rename_changeset_files_to_header(&directory).unwrap();
        assert_eq!(
            names(&directory),
            vec!["static_file_account-change-sets_2_499999".to_string()],
        );

        // A provider that has just opened the directory reads the changesets back at the right
        // blocks, which is the case the running node is in.
        let reopened =
            StaticFileProvider::<<ArbNode as reth_node_types::NodeTypes>::Primitives>::read_only(
                &directory,
            )
            .unwrap();
        assert_eq!(reopened.account_block_changeset(2).unwrap().len(), 2);
        assert!(reopened.account_block_changeset(3).unwrap().is_empty());
        assert_eq!(reopened.account_block_changeset(4).unwrap().len(), 1);
    }

    /// History has to reach the convert point, or the datadir would claim to unwind to a block
    /// whose state it cannot reconstruct.
    #[test]
    fn rejects_history_that_stops_below_the_convert_point() {
        let factory = v2_factory();
        let root = B256::repeat_byte(0xee);
        let (_, manifest, _) = history_stream(root);

        let mut builder = StreamBuilder::new(&manifest).blocks();
        let mut parent = B256::ZERO;
        for number in 0..=4u64 {
            let h = header(number, parent, &[], &[]);
            parent = h.hash_slow();
            builder = builder
                .header(number, &alloy_rlp::encode(&h))
                .body(number, &body_rlp(&[]));
        }
        let bytes = builder
            .end_section()
            .history_section()
            .history(&HistoryObject {
                state_id: 3,
                block: 2,
                parent_root: B256::repeat_byte(0x11),
                post_root: root,
                accounts: vec![],
            })
            .end_section()
            .state()
            .end_section()
            .finish();

        let mut stream = SnapshotStream::open(bytes.as_slice()).unwrap();
        write_blocks(&factory, &mut stream, &manifest).unwrap();
        let error = write_history(&factory, &mut stream, &manifest)
            .unwrap_err()
            .to_string();
        assert!(error.contains("history ends at block 2"), "{error}");
    }

    /// The transactions root is recomputed from the decoded transactions, so a body that does not
    /// belong to its header is caught at that block rather than at the end of the run.
    #[test]
    fn rejects_a_body_that_does_not_match_its_header() {
        let factory = create_test_provider_factory_with_node_types::<ArbNode>(spec());
        factory.set_storage_settings_cache(StorageSettings::v2());

        let h0 = header(0, B256::ZERO, &[], &[]);
        let manifest = Manifest {
            block: 0,
            root: B256::repeat_byte(0xee),
            state_id: 1,
            hash: h0.hash_slow(),
            resume: None,
        };
        // The header commits to no transactions; the body carries two.
        let bytes = StreamBuilder::new(&manifest)
            .blocks()
            .header(0, &alloy_rlp::encode(&h0))
            .body(0, &body_rlp(&transactions()))
            .end_section()
            .finish();

        let mut stream = SnapshotStream::open(bytes.as_slice()).unwrap();
        let error = write_blocks(&factory, &mut stream, &manifest)
            .unwrap_err()
            .to_string();
        assert!(error.contains("transactions root is"), "{error}");
    }

    /// The receipts root is recomputed from the decoded receipts, which is what proves the storage
    /// form was read correctly: the bloom and the transaction type are both absent from it and are
    /// reconstructed here, and either being wrong changes the root.
    #[test]
    fn rejects_receipts_that_do_not_match_their_header() {
        let factory = create_test_provider_factory_with_node_types::<ArbNode>(spec());
        factory.set_storage_settings_cache(StorageSettings::v2());

        let txs = transactions();
        let mut wrong = receipt_specs();
        wrong[0].cumulative_gas_used += 1;

        let h0 = header(0, B256::ZERO, &txs, &receipts());
        let manifest = Manifest {
            block: 0,
            root: B256::repeat_byte(0xee),
            state_id: 1,
            hash: h0.hash_slow(),
            resume: None,
        };
        let bytes = StreamBuilder::new(&manifest)
            .blocks()
            .header(0, &alloy_rlp::encode(&h0))
            .body(0, &body_rlp(&txs))
            .receipts(0, &stored_receipts_rlp(&wrong))
            .end_section()
            .finish();

        let mut stream = SnapshotStream::open(bytes.as_slice()).unwrap();
        let error = write_blocks(&factory, &mut stream, &manifest)
            .unwrap_err()
            .to_string();
        assert!(error.contains("receipts root is"), "{error}");
    }

    /// State, blocks and history have to end at the same block, or the datadir has a hole nothing
    /// else would surface.
    #[test]
    fn rejects_blocks_that_stop_below_the_convert_point() {
        let factory = create_test_provider_factory_with_node_types::<ArbNode>(spec());
        factory.set_storage_settings_cache(StorageSettings::v2());
        {
            let provider = factory.database_provider_rw().unwrap();
            provider
                .write_storage_settings(StorageSettings::v2())
                .unwrap();
            provider.commit().unwrap();
        }

        let h0 = header(0, B256::ZERO, &[], &[]);
        let manifest = Manifest {
            block: 7,
            root: B256::repeat_byte(0xee),
            state_id: 8,
            hash: B256::repeat_byte(0xaa),
            resume: None,
        };
        let bytes = StreamBuilder::new(&manifest)
            .blocks()
            .header(0, &alloy_rlp::encode(&h0))
            .body(0, &body_rlp(&[]))
            .end_section()
            .history_section()
            .end_section()
            .state()
            .end_section()
            .finish();

        let mut stream = SnapshotStream::open(bytes.as_slice()).unwrap();
        let error = write_blocks(&factory, &mut stream, &manifest)
            .unwrap_err()
            .to_string();
        assert!(error.contains("blocks end at 0"), "{error}");
    }

    /// Every Arbitrum transaction type has to survive the body decode, because the receipt decoder
    /// takes its type from there and a wrong type produces a wrong receipts root.
    #[test]
    fn body_decode_preserves_transaction_types() {
        let txs = transactions();
        let encoded = body_rlp(&txs);
        let decoded = ArbBlockBody::decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(decoded.transactions, txs);
        assert_eq!(
            decoded
                .transactions
                .iter()
                .map(tx_type_byte)
                .collect::<Vec<_>>(),
            vec![0x02, 0x64]
        );
        // And the 2718 form round-trips, which is what the static file stores.
        for tx in &decoded.transactions {
            use alloy_eips::Encodable2718;
            let bytes = tx.encoded_2718();
            assert_eq!(
                &ArbTxEnvelope::decode_2718(&mut bytes.as_slice()).unwrap(),
                tx
            );
        }
    }
}
