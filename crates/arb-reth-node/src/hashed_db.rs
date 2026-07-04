//! `hashed_db`: helpers for reading hashed-state tables directly.
//!
//! An Arbitrum genesis snapshot imported via `arb-snapshot-import` contains only the
//! *hashed* state tables (`HashedAccounts`, `HashedStorages`, `Bytecodes`, trie tables)
//! with no `PlainAccountState` or `PlainStorageState` rows. This module provides free
//! helpers [`account_by_address`], [`code_of`], [`storage_at`] for reading that shape
//! directly from the `arb-snapshot-read` binary and tests.

use alloy_primitives::{keccak256, Address, B256, U256};
use reth_db_api::{cursor::DbDupCursorRO, tables, transaction::DbTx};
use reth_primitives_traits::{Account, Bytecode};

/// `keccak256` of the empty byte string; re-exported from `alloy_consensus::constants`.
pub use alloy_consensus::constants::KECCAK_EMPTY;

/// Look up the reth [`Account`] for `address` using the hashed-state table.
///
/// Returns `None` if the account does not exist in the snapshot.
pub fn account_by_address<TX: DbTx>(tx: &TX, address: Address) -> Option<Account> {
    let hk = keccak256(address);
    tx.get::<tables::HashedAccounts>(hk).ok().flatten()
}

/// Fetch the reth [`Bytecode`] for `code_hash` from `Bytecodes`.
///
/// Returns `None` if the hash is not stored (e.g., `KECCAK_EMPTY`).
pub fn code_of<TX: DbTx>(tx: &TX, code_hash: B256) -> Option<Bytecode> {
    tx.get::<tables::Bytecodes>(code_hash).ok().flatten()
}

/// Read a single storage slot for `address` at `slot` (a 32-byte key,
/// **not** yet keccak-hashed; this function hashes both).
///
/// Returns [`U256::ZERO`] when the slot is absent.
pub fn storage_at<TX: DbTx>(tx: &TX, address: Address, slot: B256) -> U256 {
    let ak = keccak256(address);
    let sk = keccak256(slot);
    tx.cursor_dup_read::<tables::HashedStorages>()
        .ok()
        .and_then(|mut cursor| cursor.seek_by_key_subkey(ak, sk).ok().flatten())
        .filter(|entry| entry.key == sk)
        .map(|entry| entry.value)
        .unwrap_or(U256::ZERO)
}
