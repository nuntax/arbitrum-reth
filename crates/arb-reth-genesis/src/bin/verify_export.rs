//! Read the `reth-export --mode state` stream (A/C/S records) on stdin, recompute the hashed
//! state-trie root, and assert it equals the expected stateRoot (the end-to-end proof that the
//! geth-DB → reth conversion preserves state exactly). Also cross-checks each account's storage
//! root (recomputed from its S records) against the storage root in its A record.
//!
//! Usage:  reth-export --mode state <dir> | verify-export <expectedStateRoot>

use std::io::{BufRead, Write};

use alloy_primitives::{hex, B256, U256};
use alloy_trie::{TrieAccount, EMPTY_ROOT_HASH};
use arb_reth_genesis::verify;

fn b256(tok: &str) -> B256 {
    B256::from_slice(&hex::decode(tok).expect("hex b256"))
}
fn u256(tok: &str) -> U256 {
    U256::from_str_radix(tok, 16).expect("hex u256")
}

fn main() -> eyre::Result<()> {
    let expected: Option<B256> = std::env::args().nth(1).map(|s| {
        B256::from_slice(&hex::decode(s.trim_start_matches("0x")).expect("expected root hex"))
    });

    let stdin = std::io::stdin();
    let mut accounts: Vec<(B256, TrieAccount)> = Vec::with_capacity(1_300_000);

    // Current account's storage cross-check state.
    let mut cur_addr_hash = B256::ZERO;
    let mut cur_storage_root = EMPTY_ROOT_HASH;
    let mut cur_slots: Vec<(B256, U256)> = Vec::new();
    let mut storage_checked: u64 = 0;

    let flush_storage = |addr: B256, expect: B256, slots: &mut Vec<(B256, U256)>, checked: &mut u64| {
        if slots.is_empty() {
            assert_eq!(expect, EMPTY_ROOT_HASH, "account {addr} has storageRoot but no S records");
            return;
        }
        let got = verify::storage_root_hashed(slots.iter().copied());
        assert_eq!(got, expect, "storage root mismatch for account {addr}");
        *checked += 1;
        slots.clear();
    };

    let mut n_acc = 0u64;
    let mut n_code = 0u64;
    let mut n_slot = 0u64;
    for line in stdin.lock().lines() {
        let line = line?;
        let mut it = line.split_whitespace();
        match it.next() {
            Some("A") => {
                // Flush the previous account's storage before starting a new one.
                flush_storage(cur_addr_hash, cur_storage_root, &mut cur_slots, &mut storage_checked);
                let addr_hash = b256(it.next().expect("A hash"));
                let nonce: u64 = it.next().expect("A nonce").parse()?;
                let balance = u256(it.next().expect("A balance"));
                let code_hash = b256(it.next().expect("A codeHash"));
                let storage_root = b256(it.next().expect("A storageRoot"));
                accounts.push((
                    addr_hash,
                    TrieAccount { nonce, balance, storage_root, code_hash },
                ));
                cur_addr_hash = addr_hash;
                cur_storage_root = storage_root;
                n_acc += 1;
            }
            Some("S") => {
                let slot = b256(it.next().expect("S slot"));
                let val = u256(it.next().expect("S value"));
                cur_slots.push((slot, val));
                n_slot += 1;
            }
            Some("C") => n_code += 1,
            Some("H") | Some("B") | Some("R") | None => {}
            Some(other) => eyre::bail!("unknown record type {other:?}"),
        }
    }
    flush_storage(cur_addr_hash, cur_storage_root, &mut cur_slots, &mut storage_checked);

    eprintln!(
        "parsed {n_acc} accounts, {n_code} codes, {n_slot} slots; storage roots checked: {storage_checked}"
    );

    let root = verify::state_root_hashed(accounts);
    let mut out = std::io::stdout();
    writeln!(out, "state root: {root:#x}")?;
    if let Some(exp) = expected {
        if root == exp {
            writeln!(out, "✅ MATCH {exp:#x}")?;
        } else {
            eyre::bail!("MISMATCH: got {root:#x}, expected {exp:#x}");
        }
    }
    Ok(())
}
