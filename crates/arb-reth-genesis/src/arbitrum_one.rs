//! Constants and initialization parameters for the Arbitrum One Nitro genesis.

use alloy_primitives::{Address, B256, U256, address, b256};
use arb_revm::arbos_init::ArbosInitConfig;

/// ArbOS state account.
pub const ARBOS_STATE: Address = address!("A4B05FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");

/// Canonical state root at the Arbitrum One Nitro genesis block.
pub const GENESIS_STATE_ROOT: B256 =
    b256!("7f2bfc4481d02bfcfc606ebb949384ef78d03a0f30a2dc9cccd652eb80926ae1");

/// Canonical ArbOS account storage root at the Arbitrum One Nitro genesis block.
pub const ARBOS_STORAGE_ROOT: B256 =
    b256!("95d4357ce7baf56bfdcc4f01b594b8f071c588adf58fd79e322ea6d029748573");

/// Arbitrum One Nitro genesis block number.
pub const GENESIS_BLOCK_NUMBER: u64 = 22_207_817;

/// Timestamp of the final Classic block and Nitro genesis block.
pub const GENESIS_TIMESTAMP: u64 = 1_661_956_342;

/// Exact ArbOS initialization parameters for the Arbitrum One Nitro genesis.
pub fn init_config() -> ArbosInitConfig {
    ArbosInitConfig {
        initial_arbos_version: 6,
        initial_chain_owner: address!("d345e41ae2cb00311956aa7109fc801ae8c81a52"),
        chain_id: U256::from(42_161u64),
        genesis_block_number: GENESIS_BLOCK_NUMBER,
        initial_l1_base_fee: U256::from(50_000_000_000u64),
        // `chainConfig` was introduced after the v6 genesis. An empty value makes the
        // initialization write a no-op, matching the canonical state.
        serialized_chain_config: Vec::new(),
        debug_precompiles: false,
    }
}
