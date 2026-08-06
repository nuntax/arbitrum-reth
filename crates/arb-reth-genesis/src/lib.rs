//! Convert the Nitro classic-state export into a reth-readable Arbitrum One genesis.
//!
//! See [`readers`] for the streaming JSONL parsers over the export
//! (`state/0x152dd48/{accounts,addresstable,retryables}.json`), which yield the
//! `arb_revm::arbos_init` input types consumed by `build_mainnet_genesis_accounts`.

pub mod arbitrum_one;
pub mod preimages;
pub mod readers;
pub mod snapshot_stream;
pub mod verify;
