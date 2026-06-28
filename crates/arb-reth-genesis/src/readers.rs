//! Streaming JSONL readers for the Nitro classic-state export
//! (`state/0x152dd48/{accounts,addresstable,retryables}.json`).
//!
//! The real accounts file is ~4.37 GB, so [`accounts`] returns a lazy iterator
//! that owns the open file handle and parses one line at a time.

use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

use alloy_primitives::{Address, Bytes, B256, U256};
use arb_revm::arbos_init::{GenesisAccountInput, GenesisRetryableInput};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct RawIndex {
    #[serde(rename = "NextBlockNumber")]
    next_block_number: u64,
    #[serde(rename = "AccountsPath")]
    accounts_path: String,
    #[serde(rename = "AddressTableContentsPath")]
    address_table_path: String,
    #[serde(rename = "RetryableDataPath")]
    retryable_path: String,
}

/// Parsed contents of `index.json`.
#[derive(Debug)]
pub struct IndexManifest {
    pub next_block_number: u64,
    pub accounts_path: String,
    pub address_table_path: String,
    pub retryable_path: String,
}

/// Parse the `index.json` manifest file.
pub fn read_index(path: &Path) -> eyre::Result<IndexManifest> {
    let text = std::fs::read_to_string(path)?;
    let raw: RawIndex = serde_json::from_str(&text)?;
    Ok(IndexManifest {
        next_block_number: raw.next_block_number,
        accounts_path: raw.accounts_path,
        address_table_path: raw.address_table_path,
        retryable_path: raw.retryable_path,
    })
}

#[derive(Debug, Deserialize)]
struct RawContractInfo {
    #[serde(rename = "Code")]
    code: Vec<u8>,
    // A contract with code but no storage serializes `ContractStorage` as JSON `null`
    // (e.g. EIP-1167 minimal proxies), so this must tolerate null/absent.
    #[serde(rename = "ContractStorage", default)]
    contract_storage: Option<BTreeMap<B256, B256>>,
}

#[derive(Debug, Deserialize)]
struct RawAccount {
    #[serde(rename = "Addr")]
    addr: Address,
    #[serde(rename = "Balance")]
    balance: String,
    #[serde(rename = "Nonce")]
    nonce: u64,
    #[serde(rename = "ContractInfo")]
    contract_info: Option<RawContractInfo>,
}

fn parse_account_line(line: &str) -> eyre::Result<GenesisAccountInput> {
    let raw: RawAccount = serde_json::from_str(line)?;
    let balance = raw
        .balance
        .parse::<U256>()
        .map_err(|e| eyre::eyre!("bad balance {:?}: {e}", raw.balance))?;
    let (code, storage) = match raw.contract_info {
        Some(ci) => (
            Bytes::from(ci.code),
            ci.contract_storage
                .unwrap_or_default()
                .into_iter()
                .collect::<Vec<_>>(),
        ),
        None => (Bytes::new(), Vec::new()),
    };
    Ok(GenesisAccountInput {
        address: raw.addr,
        balance,
        nonce: raw.nonce,
        code,
        storage,
    })
}

/// Open `path` and return a lazy iterator of parsed [`GenesisAccountInput`]s.
///
/// The iterator owns the open file handle; blank lines are skipped.
/// Parse errors surface as `Err` items rather than panicking.
pub fn accounts(
    path: &Path,
) -> eyre::Result<impl Iterator<Item = eyre::Result<GenesisAccountInput>>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let iter = reader.lines().filter_map(|line_result| {
        match line_result {
            Err(e) => Some(Err(eyre::eyre!(e))),
            Ok(line) => {
                if line.trim().is_empty() {
                    None
                } else {
                    Some(parse_account_line(&line))
                }
            }
        }
    });
    Ok(iter)
}

/// Read the address-table JSONL file and return addresses in file order.
///
/// Each line is a bare JSON string `"0x<40hex>"`.  The slot index is the
/// position in the returned `Vec`.
pub fn address_table(path: &Path) -> eyre::Result<Vec<Address>> {
    let text = std::fs::read_to_string(path)?;
    let mut out = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let addr: Address = serde_json::from_str(trimmed)?;
        out.push(addr);
    }
    Ok(out)
}

#[derive(Debug, Deserialize)]
struct RawRetryable {
    #[serde(rename = "Id")]
    id: B256,
    #[serde(rename = "Timeout")]
    timeout: u64,
    #[serde(rename = "From")]
    from: Address,
    #[serde(rename = "To")]
    to: Address,
    #[serde(rename = "Callvalue")]
    callvalue: String,
    #[serde(rename = "Beneficiary")]
    beneficiary: Address,
    #[serde(rename = "Calldata")]
    calldata: Vec<u8>,
}

/// Read the retryables JSONL file and return all records.
pub fn retryables(path: &Path) -> eyre::Result<Vec<GenesisRetryableInput>> {
    let text = std::fs::read_to_string(path)?;
    let mut out = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let raw: RawRetryable = serde_json::from_str(trimmed)?;
        let callvalue = raw
            .callvalue
            .parse::<U256>()
            .map_err(|e| eyre::eyre!("bad callvalue {:?}: {e}", raw.callvalue))?;
        // Nitro treats the zero address as a nil destination (contract-creation).
        let to = if raw.to == Address::ZERO {
            None
        } else {
            Some(raw.to)
        };
        out.push(GenesisRetryableInput {
            id: raw.id,
            timeout: raw.timeout,
            from: raw.from,
            to,
            callvalue,
            beneficiary: raw.beneficiary,
            calldata: Bytes::from(raw.calldata),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;

    const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");

    fn fixture(name: &str) -> std::path::PathBuf {
        std::path::Path::new(FIXTURES).join(name)
    }

    #[test]
    fn test_read_index() {
        let manifest = read_index(&fixture("index.json")).unwrap();
        assert_eq!(manifest.next_block_number, 22207817);
        assert_eq!(manifest.accounts_path, "accounts.json");
        assert_eq!(manifest.address_table_path, "addresstable.json");
        assert_eq!(manifest.retryable_path, "retryables.json");
    }

    #[test]
    fn test_address_table() {
        let addrs = address_table(&fixture("addresstable_head.jsonl")).unwrap();
        assert_eq!(addrs.len(), 8);
        // First entry is the zero address.
        assert_eq!(addrs[0], Address::ZERO);
        // Fourth entry (index 3).
        assert_eq!(
            addrs[3],
            address!("a4b1c60011605b133c4e9734860131ef2ce3c4b9")
        );
    }

    #[test]
    fn test_accounts() {
        let all: Vec<_> = accounts(&fixture("accounts_head.jsonl"))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();

        // EOA check: first record.
        let boa = &all[0];
        assert_eq!(
            boa.address,
            address!("f124579b4d0a56cf720d601283f45d6ce4198279")
        );
        assert_eq!(boa.nonce, 1);
        assert_eq!(
            boa.balance,
            "114024661353161".parse::<U256>().unwrap()
        );
        assert!(boa.code.is_empty(), "EOA must have empty code");
        assert!(boa.storage.is_empty(), "EOA must have empty storage");

        // Contract check: seventh record (index 6).
        let contract = &all[6];
        assert_eq!(
            contract.address,
            address!("e66092c38c2a56e63009946550407902934376da")
        );
        // EIP-1167 proxy prefix: 0x36 0x3d 0x3d 0x37
        assert!(
            contract.code.len() >= 4,
            "contract code should be non-empty"
        );
        assert_eq!(&contract.code[..4], &[0x36, 0x3d, 0x3d, 0x37]);
        // Exactly one storage slot whose value has last byte == 1.
        assert_eq!(contract.storage.len(), 1);
        let (_, val) = contract.storage[0];
        assert_eq!(val.as_slice()[31], 1u8);
    }

    #[test]
    fn test_retryables() {
        let all = retryables(&fixture("retryables_head.jsonl")).unwrap();
        assert_eq!(all.len(), 5);

        // All five have callvalue == 0.
        for r in &all {
            assert_eq!(r.callvalue, U256::ZERO);
        }

        // Records 1-4: From == To == Beneficiary, To is non-zero → Some.
        for r in &all[..4] {
            assert!(r.to.is_some(), "expected Some(addr) for non-zero To");
            assert_eq!(r.to.unwrap(), r.from);
            assert_eq!(r.to.unwrap(), r.beneficiary);
        }

        // Fifth record has non-empty calldata.
        let fifth = &all[4];
        assert!(!fifth.calldata.is_empty(), "fifth retryable must have calldata");
    }
}
