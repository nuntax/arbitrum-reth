//! `hashed_db`: a revm [`Database`] that reads hashed state tables directly.
//!
//! An Arbitrum genesis snapshot imported via `arb-snapshot-import` contains only the
//! *hashed* state tables (`HashedAccounts`, `HashedStorages`, `Bytecodes`, trie tables)
//! with no `PlainAccountState` or `PlainStorageState` rows. This module provides:
//!
//! * [`HashedStateDb`]: a thin wrapper around any [`DbTx`] that implements revm's
//!   [`Database`] trait by reading those hashed tables.
//! * Free helpers [`account_by_address`], [`code_of`], [`storage_at`] for direct use
//!   from the `arb-snapshot-read` binary and tests.

use alloy_primitives::{keccak256, Address, B256, U256};
use reth_db_api::{
    cursor::DbDupCursorRO,
    tables,
    transaction::DbTx,
};
use reth_primitives_traits::{Account, Bytecode};
use reth_provider::ProviderError;
use revm::database_interface::Database;
use revm::state::{AccountInfo, Bytecode as RevmBytecode};

/// `keccak256` of the empty byte string; re-exported from `alloy_consensus::constants`.
pub use alloy_consensus::constants::KECCAK_EMPTY;

/// A revm [`Database`] that reads from the reth hashed-state tables.
///
/// Useful when the MDBX database contains only `HashedAccounts`, `HashedStorages`, and
/// `Bytecodes` (no `PlainAccountState`). An Arbitrum genesis snapshot imported by
/// `arb-snapshot-import` is exactly this shape.
///
/// `TX` must implement [`DbTx`]. The struct takes ownership; call
/// [`HashedStateDb::into_tx`] to reclaim it.
#[derive(Debug)]
pub struct HashedStateDb<TX> {
    tx: TX,
}

impl<TX: DbTx + Send + Sync + std::fmt::Debug> HashedStateDb<TX> {
    /// Create a new [`HashedStateDb`] wrapping `tx`.
    pub fn new(tx: TX) -> Self {
        Self { tx }
    }

    /// Consume this wrapper and return the inner transaction.
    pub fn into_tx(self) -> TX {
        self.tx
    }

    /// Borrow the inner transaction.
    pub fn tx_ref(&self) -> &TX {
        &self.tx
    }
}

impl<TX: DbTx + Send + Sync + std::fmt::Debug> Database for HashedStateDb<TX> {
    type Error = ProviderError;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let hk = keccak256(address);
        let maybe_acct: Option<Account> = self.tx.get::<tables::HashedAccounts>(hk)?;
        Ok(maybe_acct.map(|a| AccountInfo {
            balance: a.balance,
            nonce: a.nonce,
            code_hash: a.bytecode_hash.unwrap_or(KECCAK_EMPTY),
            code: None,
            account_id: None,
        }))
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let ak = keccak256(address);
        let sk = keccak256(B256::from(index.to_be_bytes::<32>()));
        let mut cursor = self.tx.cursor_dup_read::<tables::HashedStorages>()?;
        let maybe = cursor.seek_by_key_subkey(ak, sk)?;
        if let Some(entry) = maybe {
            if entry.key == sk {
                return Ok(entry.value);
            }
        }
        Ok(U256::ZERO)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<RevmBytecode, Self::Error> {
        let maybe: Option<Bytecode> = self.tx.get::<tables::Bytecodes>(code_hash)?;
        Ok(match maybe {
            Some(b) => b.0, // Bytecode newtype wraps RevmBytecode
            None => RevmBytecode::default(),
        })
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        Ok(self.tx.get::<tables::CanonicalHeaders>(number)?.unwrap_or_default())
    }
}

/// Execution [`Database`] for a hashed-only snapshot.
///
/// Account/storage/code reads come from [`HashedStateDb`] (the only state a converted snapshot
/// has). `block_hash` is delegated to `blocks` (a header-aware DB such as
/// `reth_revm::database::StateProviderDatabase`), because block headers live in reth **static
/// files**, not the MDBX `CanonicalHeaders` table. Reading the table directly returns zero, which
/// would corrupt EIP-2935 `ProcessParentBlockHash`.
#[derive(Debug)]
pub struct HashedExecDb<TX, B> {
    /// Hashed-state reads (basic/storage/code).
    pub state: HashedStateDb<TX>,
    /// Header-aware DB used only for `block_hash`.
    pub blocks: B,
}

impl<TX, B> Database for HashedExecDb<TX, B>
where
    TX: DbTx + Send + Sync + std::fmt::Debug,
    B: Database<Error = ProviderError>,
{
    type Error = ProviderError;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.state.basic(address)
    }
    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        self.state.storage(address, index)
    }
    fn code_by_hash(&mut self, code_hash: B256) -> Result<RevmBytecode, Self::Error> {
        self.state.code_by_hash(code_hash)
    }
    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        self.blocks.block_hash(number)
    }
}

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
