//! Read a `reth-export --mode full-snapshot` stream and report what it contains.
//!
//! Usage: `read_snapshot_stream <stream-file>`. A truncated stream is expected to end in an error,
//! which is the point: the reader should notice rather than stop quietly.

use arb_reth_genesis::snapshot_stream::{Record, SnapshotStream};

fn main() -> eyre::Result<()> {
    let path = std::env::args().nth(1).ok_or_else(|| eyre::eyre!("usage: read_snapshot_stream <file>"))?;
    let file = std::fs::File::open(&path)?;
    let mut stream = SnapshotStream::open(std::io::BufReader::new(file))?;

    let m = stream.manifest().clone();
    println!("manifest: block {} state id {}", m.block, m.state_id);
    println!("  root {:#x}", m.root);
    println!("  hash {:#x}", m.hash);

    let (mut headers, mut bodies, mut receipts, mut histories, mut accounts, mut slots, mut codes) =
        (0u64, 0u64, 0u64, 0u64, 0u64, 0u64, 0u64);
    loop {
        match stream.next_record() {
            Ok(None) => break,
            Ok(Some(rec)) => match rec {
                Record::Header { .. } => headers += 1,
                Record::Body { .. } => bodies += 1,
                Record::Receipts { .. } => receipts += 1,
                Record::History(h) => {
                    histories += 1;
                    accounts += h.accounts.len() as u64;
                    slots += h.accounts.iter().map(|a| a.storage.len() as u64).sum::<u64>();
                }
                Record::Account { .. } => accounts += 1,
                Record::Storage { .. } => slots += 1,
                Record::Code { .. } => codes += 1,
            },
            Err(e) => {
                println!("\nstopped: {e}");
                break;
            }
        }
    }
    println!(
        "\nheaders {headers}, bodies {bodies}, receipt sets {receipts}, histories {histories}, \
         accounts {accounts}, slots {slots}, code blobs {codes}"
    );
    println!("highest block seen: {:?}", stream.last_block());
    Ok(())
}
