//! Stage H: convert the Nitro classic-state export (the Arbitrum One Nitro-genesis snapshot)
//! into a reth-readable genesis and verify the resulting block-22207817 state root against the
//! live chain.
//!
//! Usage:
//!   arb-genesis-import <export-dir> [--arbos-only]
//!
//! `<export-dir>` holds `index.json`, `accounts.json`, `addresstable.json`, `retryables.json`
//! (the `state/0x152dd48/` directory from the snapshot tar). With `--arbos-only` the 1.29M classic
//! accounts are skipped and only the ArbOS state account (0xa4b05…) storage-trie root is checked
//! against the eth_getProof sub-oracle (a fast loop for the ArbOS init / address-table / retryable
//! paths). Without it, the full genesis state root is computed and checked.

use std::path::PathBuf;
use std::time::Instant;

use alloy_primitives::{address, Address, U256};
use arb_revm::arbos_init::{build_mainnet_genesis_accounts, ArbosInitConfig};
use arb_reth_genesis::{readers, verify};

/// ArbOS state account (Nitro constant 0xa4b05ff…).
const ARBOS_STATE: Address = address!("0xA4B05FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");

/// Arbitrum One Nitro-genesis oracles (block 22207817), read from the live chain.
const GENESIS_STATE_ROOT: &str =
    "0x7f2bfc4481d02bfcfc606ebb949384ef78d03a0f30a2dc9cccd652eb80926ae1";
const ARBOS_STORAGE_ROOT: &str =
    "0x95d4357ce7baf56bfdcc4f01b594b8f071c588adf58fd79e322ea6d029748573";
/// `prevHeader.Time` at the migration == genesis block timestamp; the retryable expiry cutoff.
const GENESIS_TIMESTAMP: u64 = 1661956342;

/// Exact ArbOS init parameters for the Arbitrum One genesis, all verified via eth_getProof on
/// 0xa4b05… @ block 22207817 (the response's storageHash matched the sub-oracle).
fn arb_one_init() -> ArbosInitConfig {
    ArbosInitConfig {
        initial_arbos_version: 6,
        initial_chain_owner: address!("0xd345e41ae2cb00311956aa7109fc801ae8c81a52"),
        chain_id: U256::from(42161u64),
        genesis_block_number: 22207817,
        initial_l1_base_fee: U256::from(50_000_000_000u64), // 50 GWei
        // chainConfig StorageBytes length == 0 at the v6 genesis (added in a later ArbOS version);
        // an empty Vec makes our unconditional `chain_config.set` a no-op, matching the chain.
        serialized_chain_config: Vec::new(),
        debug_precompiles: false,
    }
}

fn main() -> eyre::Result<()> {
    let mut args = std::env::args().skip(1);
    let dir = PathBuf::from(
        args.next()
            .unwrap_or_else(|| "genesis-export/state/0x152dd48".to_string()),
    );
    let arbos_only = std::env::args().any(|a| a == "--arbos-only");

    let index = readers::read_index(&dir.join("index.json"))?;
    println!(
        "index: NextBlockNumber={} accounts={} addressTable={} retryables={}",
        index.next_block_number, index.accounts_path, index.address_table_path, index.retryable_path
    );

    let t = Instant::now();
    let address_table = readers::address_table(&dir.join(&index.address_table_path))?;
    let retryables = readers::retryables(&dir.join(&index.retryable_path))?;
    println!(
        "loaded {} address-table entries, {} retryables in {:.1}s",
        address_table.len(),
        retryables.len(),
        t.elapsed().as_secs_f64()
    );

    let config = arb_one_init();

    let built = if arbos_only {
        println!("building ArbOS state only (no classic accounts)…");
        let t = Instant::now();
        let accounts = build_mainnet_genesis_accounts(
            &config,
            address_table,
            retryables,
            std::iter::empty(),
            GENESIS_TIMESTAMP,
        )
        .map_err(|e| eyre::eyre!(e))?;
        println!("built {} ArbOS accounts in {:.1}s", accounts.len(), t.elapsed().as_secs_f64());
        if accounts.len() < 100 {
            for a in &accounts {
                let sr = verify::storage_root_of(a);
                println!(
                    "  {} nonce={} bal={} codeLen={} slots={} storageRoot={sr:#x}",
                    a.address, a.nonce, a.balance, a.code.len(), a.storage.len()
                );
            }
        }
        accounts
    } else {
        println!("streaming classic accounts + building full genesis…");
        let t = Instant::now();
        let mut n = 0u64;
        let accounts_path = dir.join(&index.accounts_path);
        let acct_iter = readers::accounts(&accounts_path)?.map(move |r| {
            n += 1;
            if n % 200_000 == 0 {
                eprintln!("  …{n} accounts parsed");
            }
            r.expect("account parse error")
        });
        let accounts = build_mainnet_genesis_accounts(
            &config,
            address_table,
            retryables,
            acct_iter,
            GENESIS_TIMESTAMP,
        )
        .map_err(|e| eyre::eyre!(e))?;
        println!(
            "built {} total genesis accounts in {:.1}s",
            accounts.len(),
            t.elapsed().as_secs_f64()
        );
        accounts
    };

    // Optional: dump exact built leaves for `--dump 0xaddr,0xaddr,...` (differential debugging).
    if let Some(list) = std::env::args()
        .skip_while(|a| a != "--dump")
        .nth(1)
    {
        use alloy_primitives::keccak256;
        use alloy_trie::{EMPTY_ROOT_HASH, KECCAK_EMPTY};
        println!("--- built leaves ({} total accounts) ---", built.len());
        for want in list.split(',') {
            let addr: Address = want.parse().expect("bad --dump address");
            match built.iter().find(|a| a.address == addr) {
                None => println!("{addr}  ABSENT from built genesis"),
                Some(a) => {
                    let sr = verify::storage_root_of(a);
                    let ch = if a.code.is_empty() { KECCAK_EMPTY } else { keccak256(a.code.as_ref()) };
                    println!(
                        "{addr}  nonce={} balance={} codeHash={ch:#x} storageRoot={sr:#x} (codeLen={} slots={}{})",
                        a.nonce, a.balance, a.code.len(), a.storage.len(),
                        if sr == EMPTY_ROOT_HASH { " EMPTYROOT" } else { "" }
                    );
                }
            }
        }
        return Ok(());
    }

    // ArbOS storage sub-root check (always available).
    let arbos = built
        .iter()
        .find(|a| a.address == ARBOS_STATE)
        .ok_or_else(|| eyre::eyre!("ArbOS state account 0xa4b05… not found in built genesis"))?;
    let arbos_root = verify::storage_root_of(arbos);
    let arbos_ok = format!("{arbos_root:#x}") == ARBOS_STORAGE_ROOT;
    println!(
        "ArbOS storage root: {arbos_root:#x}  [{}]  ({} storage slots)",
        if arbos_ok { "MATCH" } else { "MISMATCH" },
        arbos.storage.len()
    );

    if arbos_only {
        if !arbos_ok {
            eyre::bail!("ArbOS storage root mismatch (expected {ARBOS_STORAGE_ROOT})");
        }
        println!("\n✅ ArbOS sub-oracle MATCH");
        return Ok(());
    }

    let t = Instant::now();
    let root = verify::state_root(&built);
    let ok = format!("{root:#x}") == GENESIS_STATE_ROOT;
    println!(
        "full genesis state root: {root:#x}  [{}]  (computed in {:.1}s)",
        if ok { "MATCH" } else { "MISMATCH" },
        t.elapsed().as_secs_f64()
    );
    if !ok {
        eyre::bail!("genesis state root mismatch (expected {GENESIS_STATE_ROOT})");
    }
    println!("\n✅ Arbitrum One genesis state root MATCH (block 22207817)");
    Ok(())
}
