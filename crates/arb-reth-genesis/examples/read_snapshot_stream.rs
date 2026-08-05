//! Validate a `reth-export --mode full-snapshot` stream.
//!
//! Streams the whole file through [`SnapshotStream`], which enforces the invariants that need no
//! database, then checks that the history reaches the exported state. Exits non-zero on any failure
//! so this can gate an import.
//!
//! Usage: `read_snapshot_stream <stream-file>`

use std::time::Instant;

use alloy_primitives::B256;
use arb_reth_genesis::snapshot_stream::{header_field, transactions_root, Record, SnapshotStream};

fn main() -> eyre::Result<()> {
    let path = std::env::args()
        .nth(1)
        .ok_or_else(|| eyre::eyre!("usage: read_snapshot_stream <file>"))?;
    let file = std::fs::File::open(&path)?;
    let total = file.metadata()?.len();
    let mut stream = SnapshotStream::open(std::io::BufReader::with_capacity(1 << 22, file))?;

    let m = stream.manifest().clone();
    println!("manifest: block {} state id {}", m.block, m.state_id);
    println!("  root {:#x}", m.root);
    println!("  hash {:#x}", m.hash);

    let started = Instant::now();
    let (mut headers, mut bodies, mut receipts) = (0u64, 0u64, 0u64);
    let (mut histories, mut hist_accounts, mut hist_slots) = (0u64, 0u64, 0u64);
    let (mut accounts, mut slots, mut codes) = (0u64, 0u64, 0u64);
    let mut first_history_block = None;
    // Header by block, so a body can be checked against the header that precedes it.
    let mut pending_header: Option<(u64, B256)> = None;
    let mut tx_roots_checked = 0u64;

    while let Some(rec) = stream.next_record()? {
        match rec {
            Record::Header { block, rlp, .. } => {
                headers += 1;
                pending_header = Some((block, B256::from_slice(header_field(&rlp, 4)?)));
            }
            Record::Body { block, rlp } => {
                bodies += 1;
                match pending_header {
                    Some((n, want)) if n == block => {
                        let got = transactions_root(&rlp)?;
                        if got != want {
                            return Err(eyre::eyre!(
                                "block {block}: transactions root {got:#x}, header says {want:#x}"
                            ));
                        }
                        tx_roots_checked += 1;
                    }
                    other => {
                        return Err(eyre::eyre!(
                            "block {block}: body without its header (pending {other:?})"
                        ));
                    }
                }
            }
            Record::Receipts { .. } => receipts += 1,
            Record::History(h) => {
                first_history_block.get_or_insert(h.block);
                histories += 1;
                hist_accounts += h.accounts.len() as u64;
                hist_slots += h.accounts.iter().map(|a| a.storage.len() as u64).sum::<u64>();
                if histories % 5_000_000 == 0 {
                    println!("  .. {histories} history objects ({:?})", started.elapsed());
                }
            }
            Record::Account { .. } => accounts += 1,
            Record::Storage { .. } => slots += 1,
            Record::Code { .. } => codes += 1,
        }
    }

    stream.check_history_meets_state()?;

    println!("\nblocks   {headers} headers, {bodies} bodies, {receipts} receipt sets");
    println!("history  {histories} objects, {hist_accounts} accounts, {hist_slots} slots");
    println!("state    {accounts} accounts, {slots} slots, {codes} code blobs");
    println!("first history block: {first_history_block:?}");
    println!("highest block: {:?}", stream.last_block());
    println!(
        "\nvalidated {:.1} GB in {:?}",
        total as f64 / 1e9,
        started.elapsed()
    );
    Ok(())
}
