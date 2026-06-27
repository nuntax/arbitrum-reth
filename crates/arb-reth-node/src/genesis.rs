//! ArbOS genesis chain-spec builder (Stage D.5 / genesis wiring).
//!
//! Converts an [`arb_revm::arbos_init::ArbosInitConfig`] into a
//! [`reth_chainspec::ChainSpec`] whose genesis allocation is the trie over the
//! ArbOS-initialized state (block 0's state root).
//!
//! Two additional helpers parse the chain config from the sources that supply
//! the init parameters:
//! - [`arbos_init_from_chain_config_json`] — parse an Arbitrum chain-config JSON
//!   blob (the `ArbChainConfig` Go type, same JSON embedded in the init message).
//! - [`arbos_init_from_parsed`] — extract from a [`ParsedInitMessage`] that has
//!   already been decoded from an L1 Initialize message.
//!
//! # Genesis header
//!
//! [`arb_chain_spec`] reproduces Nitro's `arbosState.MakeGenesisBlock` header exactly (London
//! format, nonce=1, gasLimit=1<<50, baseFee=0.1gwei, difficulty=1, extraData=32 zero bytes,
//! mixHash encoding the ArbOS version) so both the genesis state root AND the genesis block hash
//! match the real chain — validated byte-for-byte against the nitro-testnode (block 0 hash
//! `0xb88471…`, state root `0xff8927…`). The genesis timestamp is currently 0 (testnode);
//! Arbitrum One uses its Nitro-migration time (see TODO in `arb_chain_spec`).

use std::collections::BTreeMap;

use alloy_genesis::{ChainConfig, Genesis, GenesisAccount};
use alloy_primitives::{Address, B256, U256};
use arb_revm::arbos_init::{arb_genesis_accounts, ArbosInitConfig};
use arb_sequencer_network::init_message::{ArbChainConfig, ParsedInitMessage};
use reth_chainspec::ChainSpec;

/// Build a [`ChainSpec`] from an [`ArbosInitConfig`].
///
/// The genesis allocation is produced by running [`arb_genesis_accounts`], which
/// re-executes the ArbOS init procedure against an empty state to enumerate every
/// account written to block 0's state trie.
///
/// All EVM hard-forks through Prague are activated at genesis so reth derives a
/// Prague-class execution spec. The genesis header fields (gas_limit, timestamp)
/// are placeholder values; see the module-level TODO for testnode-parity.
pub fn arb_chain_spec(init: &ArbosInitConfig) -> eyre::Result<ChainSpec> {
    let accounts = arb_genesis_accounts(init).map_err(|e| eyre::eyre!(e))?;

    // Build the genesis alloc from the ArbOS-produced account list.
    let alloc: BTreeMap<Address, GenesisAccount> = accounts
        .into_iter()
        .map(|a| {
            let ga = GenesisAccount {
                nonce: Some(a.nonce),
                balance: a.balance,
                code: (!a.code.is_empty()).then(|| a.code.clone()),
                storage: (!a.storage.is_empty())
                    .then(|| a.storage.iter().copied().collect::<BTreeMap<B256, B256>>()),
                ..Default::default()
            };
            (a.address, ga)
        })
        .collect();

    // Arbitrum's geth chain config is LONDON-format: forks activate through london only, with NO
    // shanghai/cancun/prague (those would make reth add withdrawalsRoot / blob / requests fields to
    // the genesis header, diverging from Nitro's London-format header). The post-london EVM features
    // are gated on the ArbOS version (decoded from the header mixHash by `ArbEvmConfig`), not on
    // these chainspec forks — so a London-only config is both correct for execution and required for
    // genesis block-hash parity. Mirrors the testnode's `l2_chain_config.json`.
    let config = ChainConfig {
        chain_id: init.chain_id.to::<u64>(),
        homestead_block: Some(0),
        dao_fork_support: false,
        eip150_block: Some(0),
        eip155_block: Some(0),
        eip158_block: Some(0),
        byzantium_block: Some(0),
        constantinople_block: Some(0),
        petersburg_block: Some(0),
        istanbul_block: Some(0),
        muir_glacier_block: Some(0),
        berlin_block: Some(0),
        london_block: Some(0),
        ..Default::default()
    };

    // Genesis header — reproduces Nitro `arbosState.MakeGenesisBlock` exactly so the block hash
    // matches (validated against the nitro-testnode genesis, 0xb88471…). All values are Nitro
    // constants except the ArbOS version (encoded into mixHash) and the timestamp.
    //   - nonce      = 1  (EncodeNonce(1): "the genesis block reads the init message")
    //   - gasLimit   = l2pricing.GethBlockGasLimit = 1 << 50
    //   - baseFee    = l2pricing.InitialBaseFeeWei = 0.1 gwei
    //   - difficulty = 1
    //   - extraData  = SendRoot = 32 zero bytes at genesis (HeaderInfo.extra())
    //   - mixHash    = pack(SendCount=0[0:8], L1BlockNumber=0[8:16], ArbOSFormatVersion[16:24])
    //     (HeaderInfo.mixDigest()); ArbEvmConfig reads the version back out of bytes [16:24].
    // TODO(arb-one): Arbitrum One's genesis timestamp is the Nitro-migration time, not 0.
    let mut mix = [0u8; 32];
    mix[16..24].copy_from_slice(&init.initial_arbos_version.to_be_bytes());

    let genesis = Genesis {
        config,
        alloc,
        nonce: 1,
        timestamp: 0,
        gas_limit: 1 << 50,
        difficulty: U256::from(1u64),
        mix_hash: B256::from(mix),
        coinbase: Address::ZERO,
        extra_data: alloy_primitives::Bytes::from(vec![0u8; 32]),
        base_fee_per_gas: Some(100_000_000),
        ..Default::default()
    };

    Ok(ChainSpec::from_genesis(genesis))
}

/// Parse an Arbitrum chain-config JSON blob into an [`ArbosInitConfig`].
///
/// The JSON must be the Go `ChainConfig` format (top-level `"chainId"` +
/// `"arbitrum"` object) — the same JSON embedded in the ArbOS Initialize message.
/// The initial L1 base fee defaults to 50 GWei (`DefaultInitialL1BaseFee`); the
/// serialized chain config is the raw JSON bytes passed in.
pub fn arbos_init_from_chain_config_json(json: &[u8]) -> eyre::Result<ArbosInitConfig> {
    let cfg: ArbChainConfig = serde_json::from_slice(json)
        .map_err(|e| eyre::eyre!("failed to parse ArbChainConfig JSON: {}", e))?;

    Ok(ArbosInitConfig {
        initial_arbos_version: cfg.arbitrum.initial_arbos_version,
        initial_chain_owner: cfg.arbitrum.initial_chain_owner,
        chain_id: U256::from(cfg.chain_id),
        genesis_block_number: cfg.arbitrum.genesis_block_num,
        // Nitro DefaultInitialL1BaseFee = 50 GWei
        initial_l1_base_fee: U256::from(50_000_000_000u64),
        serialized_chain_config: json.to_vec(),
        debug_precompiles: cfg.arbitrum.allow_debug_precompiles,
    })
}

/// Build an [`ArbosInitConfig`] from a [`ParsedInitMessage`].
///
/// Requires `parsed.chain_config` to be `Some` (the Initialize message must have
/// included a chain-config JSON payload).
pub fn arbos_init_from_parsed(p: &ParsedInitMessage) -> eyre::Result<ArbosInitConfig> {
    let cfg = p
        .chain_config
        .as_ref()
        .ok_or_else(|| eyre::eyre!("ParsedInitMessage has no chain_config (32-byte-only message)"))?;

    Ok(ArbosInitConfig {
        initial_arbos_version: cfg.arbitrum.initial_arbos_version,
        initial_chain_owner: cfg.arbitrum.initial_chain_owner,
        chain_id: p.chain_id,
        genesis_block_number: cfg.arbitrum.genesis_block_num,
        initial_l1_base_fee: p.initial_l1_base_fee,
        serialized_chain_config: p.serialized_chain_config.clone(),
        debug_precompiles: cfg.arbitrum.allow_debug_precompiles,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;

    /// ArbSys precompile address — 0x0000...0064.
    const ARB_SYS: Address = address!("0x0000000000000000000000000000000000000064");
    /// ArbOS state account address (Nitro constant).
    const ARBOS_STATE: Address = address!("0xA4B05FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");

    fn test_chain_config_json() -> Vec<u8> {
        br#"{"chainId":412346,"arbitrum":{"InitialArbOSVersion":32,"InitialChainOwner":"0x00000000000000000000000000000000000a11ce","GenesisBlockNum":0,"AllowDebugPrecompiles":false}}"#.to_vec()
    }

    /// Round-trip through JSON parse → chain spec, check chain id and key accounts.
    #[test]
    fn arb_chain_spec_from_json_has_correct_chain_id() {
        let json = test_chain_config_json();
        let init = arbos_init_from_chain_config_json(&json).expect("parse chain config");
        let spec = arb_chain_spec(&init).expect("build chain spec");

        assert_eq!(spec.chain().id(), 412346, "chain id must be 412346");
    }

    /// ArbSys (0x64) must be in genesis alloc with code `0xfe`.
    #[test]
    fn arb_chain_spec_genesis_has_arb_sys_with_code() {
        let json = test_chain_config_json();
        let init = arbos_init_from_chain_config_json(&json).expect("parse chain config");
        let spec = arb_chain_spec(&init).expect("build chain spec");

        let genesis = spec.genesis();
        let arb_sys = genesis
            .alloc
            .get(&ARB_SYS)
            .expect("ArbSys 0x64 must be present in genesis alloc");

        let code = arb_sys.code.as_ref().expect("ArbSys must have code");
        assert_eq!(code.as_ref(), &[0xfe_u8][..], "ArbSys code must be [0xfe]");
    }

    /// ArbOS state account must have non-empty storage.
    #[test]
    fn arb_chain_spec_genesis_has_arbos_state_with_storage() {
        let json = test_chain_config_json();
        let init = arbos_init_from_chain_config_json(&json).expect("parse chain config");
        let spec = arb_chain_spec(&init).expect("build chain spec");

        let genesis = spec.genesis();
        let state_acct = genesis
            .alloc
            .get(&ARBOS_STATE)
            .expect("ArbOS state account 0xa4b05fffff... must be present");

        let storage = state_acct.storage.as_ref().expect("ArbOS state must have storage");
        assert!(!storage.is_empty(), "ArbOS state storage must be non-empty");
    }

    /// `arbos_init_from_parsed` with a full ParsedInitMessage.
    #[test]
    fn arbos_init_from_parsed_works() {
        use arb_sequencer_network::init_message::{ArbitrumChainParams, ArbChainConfig, ParsedInitMessage};

        let chain_config = ArbChainConfig {
            chain_id: 412346,
            arbitrum: ArbitrumChainParams {
                initial_arbos_version: 32,
                initial_chain_owner: address!("0x00000000000000000000000000000000000a11ce"),
                genesis_block_num: 0,
                allow_debug_precompiles: false,
            },
        };
        let p = ParsedInitMessage {
            chain_id: U256::from(412346u64),
            initial_l1_base_fee: U256::from(70_000_000_000u64),
            serialized_chain_config: br#"{"chainId":412346}"#.to_vec(),
            chain_config: Some(chain_config),
        };

        let init = arbos_init_from_parsed(&p).expect("arbos_init_from_parsed");
        assert_eq!(init.chain_id, U256::from(412346u64));
        assert_eq!(init.initial_l1_base_fee, U256::from(70_000_000_000u64));
        assert_eq!(init.initial_arbos_version, 32);
    }

    /// Missing chain_config in ParsedInitMessage returns an error.
    #[test]
    fn arbos_init_from_parsed_requires_chain_config() {
        use arb_sequencer_network::init_message::ParsedInitMessage;

        let p = ParsedInitMessage {
            chain_id: U256::from(1u64),
            initial_l1_base_fee: U256::from(50_000_000_000u64),
            serialized_chain_config: vec![],
            chain_config: None,
        };
        assert!(arbos_init_from_parsed(&p).is_err());
    }
}

#[cfg(test)]
mod testnode_genesis_parity {
    use super::*;
    use arb_revm::arbos_init::ArbosInitConfig;
    use revm::primitives::{Address, U256};
    use std::str::FromStr;

    /// The nitro-testnode's L2 chain config (ArbOS v40, debug precompiles on), vendored verbatim
    /// from its `config` docker volume (`l2_chain_config.json`). Its byte content is stored in the
    /// ArbOS `chainConfig` subspace, so it must match Nitro's exactly for genesis parity.
    const TESTNODE_CHAIN_CONFIG: &[u8] =
        include_bytes!("../tests/fixtures/testnode_l2_chain_config.json");

    /// GENESIS PARITY: our ArbOS genesis must reproduce the real nitro-testnode's L2 block-0 state
    /// root exactly. The expected root was captured from a live nitro-testnode (nitro v3.9.6) via
    /// `eth_getBlockByNumber("0x0").stateRoot`; the ArbOS storage was additionally verified
    /// slot-for-slot against the live node (`eth_getStorageAt`, 49/49 match).
    ///
    /// This locks in the ArbOS genesis init (Stage G.1/G.2) including the two parity fixes found
    /// by this very comparison: the v6 firstTime pricing overrides (equilibrationUnits=160e6,
    /// speedLimit=7e6, perBlockGasLimit=32e6) and the ArbOS-state-account nonce=1. The L2 genesis
    /// state is exactly the ArbOS accounts (prefunded EOAs in the testnode's `geth_genesis.json`
    /// belong to the L1 chain; L2 accounts are funded by deposits in later blocks).
    /// Same as below, but for the 2026-06-27 capture instance used by the per-block replay-parity
    /// test (`driver::tests::replay_feed_matches_testnode_per_block`). That testnode init read a
    /// live L1 base fee of 167 wei (not 147), giving a different genesis — this locks the genesis
    /// inputs that the per-block fixtures depend on.
    #[test]
    fn matches_capture_instance_genesis() {
        let init = ArbosInitConfig {
            initial_arbos_version: 40,
            initial_chain_owner: Address::from_str("0x5E1497dD1f08C87b2d8FE23e9AAB6c1De833D927")
                .unwrap(),
            chain_id: U256::from(412346u64),
            genesis_block_number: 0,
            initial_l1_base_fee: U256::from(167u64),
            serialized_chain_config: TESTNODE_CHAIN_CONFIG.to_vec(),
            debug_precompiles: true,
        };
        let spec = arb_chain_spec(&init).expect("build ArbOS chain spec");
        assert_eq!(
            format!("{:#x}", spec.genesis_header().state_root),
            "0xbff172125e1230f576de2d8bc223af9371bb4dfe1020203cefc21149dd81f23a",
        );
        assert_eq!(
            format!("{:#x}", spec.genesis_hash()),
            "0x300d0b71fac429fbb9dd25a7473637522a9d5bfd3b927a5a5b7a33f66738f936",
        );
    }

    #[test]
    fn matches_real_testnode_genesis_state_root() {
        let init = ArbosInitConfig {
            initial_arbos_version: 40,
            initial_chain_owner: Address::from_str("0x5E1497dD1f08C87b2d8FE23e9AAB6c1De833D927")
                .unwrap(),
            chain_id: U256::from(412346u64),
            genesis_block_number: 0,
            initial_l1_base_fee: U256::from(147u64), // the testnode's InitialL1BaseFee
            serialized_chain_config: TESTNODE_CHAIN_CONFIG.to_vec(),
            debug_precompiles: true,
        };
        let spec = arb_chain_spec(&init).expect("build ArbOS chain spec");
        let root = spec.genesis_header().state_root;
        assert_eq!(
            format!("{root:#x}"),
            "0xff8927407d6cd2703a5e65285970bd4da3b3b20b48861a62583a159795dc37bf",
            "ArbOS genesis state root must match the real nitro-testnode L2 block 0"
        );
        // Full block-hash parity: the genesis header (London format, nonce=1, gasLimit=1<<50,
        // baseFee=0.1gwei, difficulty=1, extraData=32 zeros, mixHash encoding ArbOS v40) must hash
        // to the testnode's actual block-0 hash.
        let hash = spec.genesis_hash();
        assert_eq!(
            format!("{hash:#x}"),
            "0xb88471684cde5f972dcf47e3fae8f87a5bb690c6b05873843e8549eee18eecf0",
            "ArbOS genesis block hash must match the real nitro-testnode L2 block 0"
        );
    }
}
