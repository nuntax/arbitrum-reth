//! In-memory Ethereum state-root computation over the ArbOS-built genesis accounts, using
//! `alloy-trie` directly (no reth/MDBX needed, keeping the verification loop fast).
//!
//! Mirrors what `reth_chainspec::ChainSpec::from_genesis(..).genesis_header().state_root` computes
//! for an alloc, which is already validated against the nitro-testnode genesis. Here we compute it
//! straight from [`ArbGenesisAccount`]s so the Arbitrum One mainnet genesis root can be checked
//! against the live chain's value without materializing an `alloy_genesis::Genesis`.

use alloy_primitives::{keccak256, B256, U256};
use alloy_trie::{
    root::{state_root_unhashed, storage_root_unhashed},
    TrieAccount, EMPTY_ROOT_HASH, KECCAK_EMPTY,
};
use arb_revm::arbos_init::ArbGenesisAccount;

/// Storage trie root for a single account (`EMPTY_ROOT_HASH` if it has no storage).
pub fn storage_root_of(account: &ArbGenesisAccount) -> B256 {
    if account.storage.is_empty() {
        return EMPTY_ROOT_HASH;
    }
    storage_root_unhashed(
        account
            .storage
            .iter()
            .map(|(slot, value)| (*slot, U256::from_be_bytes(value.0))),
    )
}

/// The account's MPT leaf (`TrieAccount`): nonce, balance, storage root, code hash.
pub fn trie_account(account: &ArbGenesisAccount) -> TrieAccount {
    TrieAccount {
        nonce: account.nonce,
        balance: account.balance,
        storage_root: storage_root_of(account),
        code_hash: if account.code.is_empty() {
            KECCAK_EMPTY
        } else {
            keccak256(account.code.as_ref())
        },
    }
}

/// Full state-trie root over all genesis accounts (geth `deriveHash`).
pub fn state_root(accounts: &[ArbGenesisAccount]) -> B256 {
    state_root_unhashed(accounts.iter().map(|a| (a.address, trie_account(a))))
}

/// State root over leaves whose keys are ALREADY hashed (`keccak(addr) -> TrieAccount`).
/// Used by the geth-DB converter, where the source state is hash-keyed (no preimages).
pub fn state_root_hashed(accounts: impl IntoIterator<Item = (B256, TrieAccount)>) -> B256 {
    alloy_trie::root::state_root_unsorted(accounts)
}

/// Storage root over leaves whose keys are ALREADY hashed (`keccak(slot) -> value`).
pub fn storage_root_hashed(slots: impl IntoIterator<Item = (B256, U256)>) -> B256 {
    alloy_trie::root::storage_root_unsorted(slots)
}
