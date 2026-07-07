//! `arb-snapshot-read`: read hashed-state from a converted Arbitrum reth MDBX.
//!
//! Opens a read-only MDBX database (same layout as produced by `arb-snapshot-import`)
//! and, given an Ethereum address, prints account information read directly from the
//! hashed tables (`HashedAccounts`, `HashedStorages`, `Bytecodes`).
//!
//! ## Usage
//!
//! ```text
//! arb-snapshot-read --db /tmp/arbreth-verify --addr 0xf124579b4d0a56cf720d601283f45d6ce4198279
//! arb-snapshot-read --db /tmp/arbreth-verify --addr 0x0000000000000000000000000000000000000065
//! arb-snapshot-read --db /tmp/arbreth-verify \
//!     --addr 0xe66092c38c2a56e63009946550407902934376da \
//!     --slot 0x0000000000000000000000000000000000000000000000000000000000000000
//! arb-snapshot-read --db /tmp/arbreth-verify \
//!     --addr 0xe66092c38c2a56e63009946550407902934376da \
//!     --list-storage
//! ```

#![allow(missing_docs)]

use std::path::PathBuf;

use alloy_primitives::{hex, keccak256, Address, B256, U256};
use clap::Parser;
use reth_db::{mdbx::DatabaseArguments, open_db_read_only, ClientVersion};
use reth_db_api::{
    cursor::DbDupCursorRO,
    database::Database as RethDatabase,
    tables,
    transaction::DbTx,
};

use arb_reth_node::hashed_db::{account_by_address, code_of, storage_at, KECCAK_EMPTY};

/// Stack-probe shim for x86_64 (same as other binaries in this crate).
#[cfg(target_arch = "x86_64")]
#[no_mangle]
pub unsafe extern "C" fn __rust_probestack() {}

/// Read hashed state from a converted Arbitrum reth MDBX snapshot.
#[derive(Debug, Parser)]
#[command(
    name = "arb-snapshot-read",
    about = "Read hashed-state from a converted Arbitrum reth MDBX snapshot"
)]
struct Args {
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

fn main() -> eyre::Result<()> {
    let args = Args::parse();

    // Parse the address.
    let addr_str = args.addr.trim_start_matches("0x");
    if addr_str.len() != 40 {
        return Err(eyre::eyre!("--addr must be 40 hex chars (20 bytes), got {}", args.addr));
    }
    let addr_bytes = hex::decode(addr_str)?;
    let address = Address::from_slice(&addr_bytes);

    // Compute the keccak hash of the address (the hashed-state key).
    let hashed_key = keccak256(address);

    // Open the MDBX read-only.
    // arb-snapshot-import stores MDBX in <out>/db.
    let db_path = args.db.join("db");

    // Pick the actual MDBX directory: prefer <dir>/db, fall back to <dir>.
    let mdbx_path = if db_path.exists() { db_path } else { args.db.clone() };

    let db = open_db_read_only(
        mdbx_path.as_path(),
        DatabaseArguments::new(ClientVersion::default()),
    )?;

    let tx = db.tx()?;

    let maybe_account = account_by_address(&tx, address);

    let (nonce, balance, code_hash) = match &maybe_account {
        Some(acct) => {
            let ch = acct.bytecode_hash.unwrap_or(KECCAK_EMPTY);
            (acct.nonce, acct.balance, ch)
        }
        None => (0u64, U256::ZERO, KECCAK_EMPTY),
    };

    let code_len = if code_hash == KECCAK_EMPTY {
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
