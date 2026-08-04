//! Plain storage-slot preimages for a snapshot-seeded Storage V2 database.

use std::path::Path;

use alloy_primitives::{B256, keccak256};
use arb_revm::arbos_init::build_mainnet_genesis_accounts;
use serde::{Deserialize, Serialize};

use crate::{arbitrum_one, readers};

/// Completion marker written into a fully-built slot-preimage sidecar.
pub const MANIFEST_FILE: &str = "manifest.json";

const MANIFEST_VERSION: u64 = 1;
const CLASSIC_ACCOUNTS: u64 = 1_294_583;
const CLASSIC_STORAGE_SLOTS: u64 = 24_491_013;
const ADDRESS_TABLE_ENTRIES: u64 = 680_046;
const RETRYABLES: u64 = 16_206;
const ARBOS_ACCOUNTS: u64 = 15;
const ARBOS_STORAGE_SLOTS: u64 = 1_410_458;
const UNIQUE_SLOT_PREIMAGES: u64 = 18_784_532;

/// Counts collected while enumerating an Arbitrum One genesis export.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotPreimageStats {
    /// Next block recorded by the Classic export manifest.
    pub next_block_number: u64,
    /// Classic accounts read from `accounts.json`.
    pub classic_accounts: u64,
    /// Non-zero Classic account storage entries visited.
    pub classic_slots: u64,
    /// Accounts produced by ArbOS initialization.
    pub arbos_accounts: u64,
    /// Non-zero ArbOS/address-table/retryable storage entries visited.
    pub arbos_slots: u64,
    /// Entries restored into ArbOS's address table.
    pub address_table_entries: u64,
    /// Live retryables restored into ArbOS state.
    pub retryables: u64,
}

impl SlotPreimageStats {
    /// Total number of account-slot entries visited before global slot-key deduplication.
    pub const fn total_slots(self) -> u64 {
        self.classic_slots + self.arbos_slots
    }

    /// Require the exact canonical Arbitrum One Classic export shape.
    pub fn validate_canonical(self) -> eyre::Result<()> {
        let expected = Self {
            next_block_number: arbitrum_one::GENESIS_BLOCK_NUMBER,
            classic_accounts: CLASSIC_ACCOUNTS,
            classic_slots: CLASSIC_STORAGE_SLOTS,
            arbos_accounts: ARBOS_ACCOUNTS,
            arbos_slots: ARBOS_STORAGE_SLOTS,
            address_table_entries: ADDRESS_TABLE_ENTRIES,
            retryables: RETRYABLES,
        };
        if self != expected {
            eyre::bail!(
                "Classic export does not match canonical Arbitrum One Nitro genesis: got {self:?}, expected {expected:?}"
            );
        }
        Ok(())
    }
}

/// Provenance and completion record for the native slot-preimage store.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotPreimageManifest {
    /// Manifest schema version.
    pub version: u64,
    /// Source and generated record counts.
    pub stats: SlotPreimageStats,
    /// Number of globally unique `keccak256(slot) -> slot` mappings written.
    pub unique_mappings: u64,
}

impl SlotPreimageManifest {
    /// Construct and validate a completed canonical manifest.
    pub fn new(stats: SlotPreimageStats, unique_mappings: u64) -> eyre::Result<Self> {
        let manifest = Self {
            version: MANIFEST_VERSION,
            stats,
            unique_mappings,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    /// Validate the schema, canonical source counts, and global mapping count.
    pub fn validate(self) -> eyre::Result<()> {
        if self.version != MANIFEST_VERSION {
            eyre::bail!(
                "unsupported slot-preimage manifest version {}, expected {MANIFEST_VERSION}",
                self.version
            );
        }
        self.stats.validate_canonical()?;
        if self.unique_mappings != UNIQUE_SLOT_PREIMAGES {
            eyre::bail!(
                "slot-preimage mapping count mismatch: got {}, expected {UNIQUE_SLOT_PREIMAGES}",
                self.unique_mappings
            );
        }
        Ok(())
    }
}

/// Visit every plain storage-slot key needed by the Arbitrum One Nitro-genesis state.
///
/// The Classic export contains plaintext keys for Classic accounts. ArbOS initialization,
/// address-table restoration, and retryable restoration create additional slots, so those are
/// reproduced separately using the same initialization code as the canonical genesis builder.
/// The visitor receives the global Reth mapping `keccak256(slot) -> slot`.
///
/// Duplicate mappings are expected because different accounts often use the same slot key. The
/// destination preimage store should deduplicate by the hashed key.
pub fn visit_arbitrum_one_slot_preimages(
    export_dir: &Path,
    mut visit: impl FnMut(B256, B256) -> eyre::Result<()>,
) -> eyre::Result<SlotPreimageStats> {
    let index = readers::read_index(&export_dir.join("index.json"))?;
    let mut stats = SlotPreimageStats {
        next_block_number: index.next_block_number,
        ..Default::default()
    };

    for account in readers::accounts(&export_dir.join(&index.accounts_path))? {
        let account = account?;
        stats.classic_accounts += 1;
        visit_storage(&account.storage, &mut stats.classic_slots, &mut visit)?;
    }

    let address_table = readers::address_table(&export_dir.join(&index.address_table_path))?;
    let retryables = readers::retryables(&export_dir.join(&index.retryable_path))?;
    stats.address_table_entries = address_table.len() as u64;
    stats.retryables = retryables.len() as u64;
    let arbos_accounts = build_mainnet_genesis_accounts(
        &arbitrum_one::init_config(),
        address_table,
        retryables,
        std::iter::empty(),
        arbitrum_one::GENESIS_TIMESTAMP,
    )
    .map_err(eyre::Report::msg)?;

    stats.arbos_accounts = arbos_accounts.len() as u64;
    for account in arbos_accounts {
        visit_storage(&account.storage, &mut stats.arbos_slots, &mut visit)?;
    }

    Ok(stats)
}

fn visit_storage(
    storage: &[(B256, B256)],
    count: &mut u64,
    visit: &mut impl FnMut(B256, B256) -> eyre::Result<()>,
) -> eyre::Result<()> {
    for &(plain_slot, value) in storage {
        if value == B256::ZERO {
            continue;
        }
        visit(keccak256(plain_slot), plain_slot)?;
        *count += 1;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use alloy_primitives::{b256, keccak256};

    use super::*;

    const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");

    #[test]
    fn enumerates_classic_and_generated_arbos_slots() {
        let export = tempfile::tempdir().unwrap();
        std::fs::copy(
            Path::new(FIXTURES).join("index.json"),
            export.path().join("index.json"),
        )
        .unwrap();
        for (source, destination) in [
            ("accounts_head.jsonl", "accounts.json"),
            ("addresstable_head.jsonl", "addresstable.json"),
            ("retryables_head.jsonl", "retryables.json"),
        ] {
            std::fs::copy(
                Path::new(FIXTURES).join(source),
                export.path().join(destination),
            )
            .unwrap();
        }

        let mut mappings = HashMap::new();
        let stats = visit_arbitrum_one_slot_preimages(export.path(), |hashed_slot, plain_slot| {
            assert_eq!(hashed_slot, keccak256(plain_slot));
            mappings.insert(hashed_slot, plain_slot);
            Ok(())
        })
        .unwrap();

        let classic_slot =
            b256!("0631038f468f2276d9a272e30fc10dc70c868e349eda452a58680e3420363b34");
        assert_eq!(mappings.get(&keccak256(classic_slot)), Some(&classic_slot));
        assert!(stats.classic_accounts > 0);
        assert!(stats.classic_slots > 0);
        assert!(stats.arbos_accounts > 0);
        assert!(stats.arbos_slots > 0);
        assert!(stats.total_slots() > stats.classic_slots);
    }

    #[test]
    fn manifest_requires_the_complete_canonical_export() {
        let stats = SlotPreimageStats {
            next_block_number: arbitrum_one::GENESIS_BLOCK_NUMBER,
            classic_accounts: CLASSIC_ACCOUNTS,
            classic_slots: CLASSIC_STORAGE_SLOTS,
            arbos_accounts: ARBOS_ACCOUNTS,
            arbos_slots: ARBOS_STORAGE_SLOTS,
            address_table_entries: ADDRESS_TABLE_ENTRIES,
            retryables: RETRYABLES,
        };

        let manifest = SlotPreimageManifest::new(stats, UNIQUE_SLOT_PREIMAGES).unwrap();
        let mut unsupported = manifest;
        unsupported.version += 1;
        assert!(unsupported.validate().is_err());
        assert!(SlotPreimageManifest::new(stats, UNIQUE_SLOT_PREIMAGES - 1).is_err());

        let mut incomplete = stats;
        incomplete.classic_slots -= 1;
        assert!(SlotPreimageManifest::new(incomplete, UNIQUE_SLOT_PREIMAGES).is_err());
    }
}
