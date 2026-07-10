//! ArbOS genesis chain-spec builder.
//!
//! Converts an [`arb_revm::arbos_init::ArbosInitConfig`] into a
//! [`reth_chainspec::ChainSpec`] whose genesis allocation is the trie over the
//! ArbOS-initialized state (block 0's state root).
//!
//! Two additional helpers parse the chain config from the sources that supply the init parameters:
//! - [`arbos_init_from_chain_config_json`]: parse an Arbitrum chain-config JSON blob (the
//!   `ArbChainConfig` Go type, same JSON embedded in the init message).
//! - [`arbos_init_from_parsed`]: extract from a [`ParsedInitMessage`] decoded from an L1
//!   Initialize message.
//!
//! # Genesis header
//!
//! [`arb_chain_spec`] reproduces Nitro's `arbosState.MakeGenesisBlock` header exactly (London
//! format, nonce=1, gasLimit=1<<50, baseFee=0.1gwei, difficulty=1, extraData=32 zero bytes,
//! mixHash encoding the ArbOS version) so both the genesis state root and the genesis block hash
//! match the real chain. Validated byte-for-byte against the nitro-testnode (block 0 hash
//! `0xb88471...`, state root `0xff8927...`). The genesis timestamp is currently 0 (testnode);
//! Arbitrum One uses its Nitro-migration time (see TODO in `arb_chain_spec`).

use std::collections::BTreeMap;

use alloy_genesis::{ChainConfig, Genesis, GenesisAccount};
use alloy_primitives::{Address, Bytes, B256, U256};
use arb_revm::arbos_init::{arb_genesis_accounts, ArbosInitConfig};
use arbitrum_alloy_sequencer::init_message::{ArbChainConfig, ParsedInitMessage};
use reth_chainspec::ChainSpec;
use serde::Deserialize;

/// Build a [`ChainSpec`] from an [`ArbosInitConfig`].
///
/// The genesis allocation is produced by running [`arb_genesis_accounts`], which
/// re-executes the ArbOS init procedure against an empty state to enumerate every
/// account written to block 0's state trie.
pub fn arb_chain_spec(init: &ArbosInitConfig) -> eyre::Result<ChainSpec> {
    arb_chain_spec_with_alloc(init, BTreeMap::new())
}

/// Like [`arb_chain_spec`] but merges an additional genesis allocation (the user `alloc` from an
/// Orbit chain's genesis.json) under the ArbOS-init accounts. Nitro applies the prealloc first and
/// then runs ArbOS init, so on an address conflict the ArbOS-written account wins.
pub fn arb_chain_spec_with_alloc(
    init: &ArbosInitConfig,
    extra_alloc: BTreeMap<Address, GenesisAccount>,
) -> eyre::Result<ChainSpec> {
    let accounts = arb_genesis_accounts(init).map_err(|e| eyre::eyre!(e))?;

    // User prealloc first; ArbOS-produced accounts override on conflict.
    let mut alloc: BTreeMap<Address, GenesisAccount> = extra_alloc;
    for a in accounts {
        let ga = GenesisAccount {
            nonce: Some(a.nonce),
            balance: a.balance,
            code: (!a.code.is_empty()).then(|| a.code.clone()),
            storage: (!a.storage.is_empty())
                .then(|| a.storage.iter().copied().collect::<BTreeMap<B256, B256>>()),
            ..Default::default()
        };
        alloc.insert(a.address, ga);
    }

    // Arbitrum's geth chain config is LONDON-format: forks activate through London only, with no
    // Shanghai/Cancun/Prague. Adding those forks would cause reth to add withdrawalsRoot/blob/
    // requests fields to the genesis header, diverging from Nitro's London-format header. Post-London
    // EVM features are gated on the ArbOS version (decoded from the header mixHash by `ArbEvmConfig`),
    // not on chain-spec forks. Mirrors the testnode's `l2_chain_config.json`.
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

    // Genesis header reproduces Nitro `arbosState.MakeGenesisBlock` exactly (validated against
    // nitro-testnode genesis `0xb88471...`). Nitro constants:
    //   nonce=1 (EncodeNonce(1): "the genesis block reads the init message")
    //   gasLimit=l2pricing.GethBlockGasLimit=1<<50, baseFee=l2pricing.InitialBaseFeeWei=0.1gwei
    //   difficulty=1, extraData=SendRoot=32 zero bytes (HeaderInfo.extra())
    //   mixHash=pack(SendCount=0[0:8], L1BlockNumber=0[8:16], ArbOSFormatVersion[16:24])
    //     (HeaderInfo.mixDigest()); ArbEvmConfig reads the version from bytes [16:24].
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
/// The JSON must be the Go `ChainConfig` format (top-level `"chainId"` + `"arbitrum"` object),
/// the same JSON embedded in the ArbOS Initialize message. The initial L1 base fee defaults to
/// 50 GWei (`DefaultInitialL1BaseFee`); the serialized chain config is the raw JSON bytes.
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
/// `parsed.chain_config` must be `Some` (the Initialize message must include a chain-config JSON).
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

/// Build a [`ChainSpec`] whose genesis header **is** an imported snapshot's head header.
///
/// A snapshot-imported DB has its head (e.g. the Arbitrum One Nitro-genesis block 22207817) as
/// reth's "genesis"; for the node to open that DB, `chain_spec.genesis_hash()` must equal the
/// stored head hash. We can't take the alloc-based [`arb_chain_spec`] path (we have hashed state,
/// no preimage alloc), so we override the public `genesis_header` field directly. Mirrors
/// `arb-snapshot-import`'s launch-gate spec.
pub fn arb_chain_spec_with_header(
    chain_id: u64,
    header: alloy_consensus::Header,
    hash: B256,
) -> std::sync::Arc<ChainSpec> {
    let config = ChainConfig {
        chain_id,
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
    let genesis = Genesis { config, number: Some(header.number), ..Default::default() };
    let mut spec = ChainSpec::from_genesis(genesis);
    spec.genesis_header = reth_primitives_traits::SealedHeader::new(header, hash);
    std::sync::Arc::new(spec)
}

/// Read the highest-numbered `H <num> <hash> <headerRLP>` record (the head/genesis header) from a
/// `reth-export --mode blocks` stream. Used to build the chain spec for booting on a snapshot DB.
pub fn read_head_header(path: &std::path::Path) -> eyre::Result<(u64, B256, alloy_consensus::Header)> {
    use alloy_rlp::Decodable;
    use std::io::BufRead;
    let reader = std::io::BufReader::new(std::fs::File::open(path)?);
    let mut best: Option<(u64, B256, alloy_consensus::Header)> = None;
    for line in reader.lines() {
        let line = line?;
        let mut p = line.splitn(4, ' ');
        if p.next() != Some("H") {
            continue;
        }
        let num: u64 = p.next().ok_or_else(|| eyre::eyre!("H: missing number"))?.parse()?;
        let hash: B256 = p.next().ok_or_else(|| eyre::eyre!("H: missing hash"))?.parse()?;
        let rlp = alloy_primitives::hex::decode(p.next().ok_or_else(|| eyre::eyre!("H: missing headerRLP"))?)?;
        let header = alloy_consensus::Header::decode(&mut rlp.as_slice())
            .map_err(|e| eyre::eyre!("decode header {num}: {e}"))?;
        if best.as_ref().map(|(n, ..)| num >= *n).unwrap_or(true) {
            best = Some((num, hash, header));
        }
    }
    best.ok_or_else(|| eyre::eyre!("no H records in {path:?}"))
}

// ---------------------------------------------------------------------------
// Orbit chain files: Nitro `chaininfo.json` + geth-style `genesis.json`.
// ---------------------------------------------------------------------------

/// One entry of a Nitro `chaininfo.json` (the file is a JSON array of these). Carries the chain
/// config plus the L1 rollup-deployment addresses, exactly what Nitro's `chaininfo` package holds.
#[derive(Debug, Clone, Deserialize)]
pub struct ChainInfo {
    /// L2 chain id.
    #[serde(rename = "chain-id")]
    pub chain_id: u64,
    /// Parent (settlement) chain id: 1 = Ethereum mainnet for an L2 Orbit chain.
    #[serde(rename = "parent-chain-id", default)]
    pub parent_chain_id: u64,
    /// Whether the parent chain is itself an Arbitrum chain (i.e. this is an L3).
    #[serde(rename = "parent-chain-is-arbitrum", default)]
    pub parent_chain_is_arbitrum: bool,
    /// Human-readable chain name.
    #[serde(rename = "chain-name", default)]
    pub chain_name: String,
    /// The Arbitrum chain config (`{"chainId":..,"arbitrum":{..}}`).
    #[serde(rename = "chain-config")]
    pub chain_config: ArbChainConfig,
    /// L1 rollup-deployment contract addresses.
    pub rollup: RollupInfo,
}

/// Rollup-deployment addresses from a `chaininfo.json` entry (the subset the node needs to derive
/// from L1: the sequencer inbox, the bridge, and the deploy block that anchors batch 0).
#[derive(Debug, Clone, Deserialize)]
pub struct RollupInfo {
    #[serde(default)]
    pub bridge: Address,
    #[serde(default)]
    pub inbox: Address,
    #[serde(rename = "sequencer-inbox", default)]
    pub sequencer_inbox: Address,
    #[serde(default)]
    pub rollup: Address,
    /// L1 block the rollup contracts were deployed at (anchors reading batch 0 + the Initialize
    /// message). Nitro's `RollupAddresses.DeployedAt`.
    #[serde(rename = "deployed-at", default)]
    pub deployed_at: u64,
}

/// Parse a Nitro `chaininfo.json` (an array of [`ChainInfo`]). Returns the entry whose `chain-id`
/// matches `chain_id`, or the sole entry when `chain_id` is `None`.
pub fn parse_chain_info(json: &[u8], chain_id: Option<u64>) -> eyre::Result<ChainInfo> {
    let chains: Vec<ChainInfo> =
        serde_json::from_slice(json).map_err(|e| eyre::eyre!("parse chaininfo JSON: {e}"))?;
    match chain_id {
        Some(id) => chains
            .into_iter()
            .find(|c| c.chain_id == id)
            .ok_or_else(|| eyre::eyre!("chaininfo has no entry for chain-id {id}")),
        None => {
            let n = chains.len();
            let mut it = chains.into_iter();
            let first = it.next().ok_or_else(|| eyre::eyre!("chaininfo array is empty"))?;
            if n > 1 {
                return Err(eyre::eyre!(
                    "chaininfo has {n} entries; specify --chain-id to pick one"
                ));
            }
            Ok(first)
        }
    }
}

/// A parsed Nitro `genesis.json`: the user prealloc, the exact serialized chain config bytes
/// (needed byte-for-byte for the ArbOS genesis root), and the initial L1 base fee.
#[derive(Debug, Clone)]
pub struct NitroGenesisFile {
    /// Genesis prealloc accounts (contracts + funded accounts) to layer under the ArbOS state.
    pub alloc: BTreeMap<Address, GenesisAccount>,
    /// Raw bytes of the `serializedChainConfig` string (the canonical serialization; used both as
    /// `ArbosInitConfig::serialized_chain_config` and to derive the structured config).
    pub serialized_chain_config: Vec<u8>,
    /// `arbOSInit.initialL1BaseFee`, if present.
    pub initial_l1_base_fee: Option<U256>,
}

#[derive(Deserialize)]
struct RawNitroGenesis {
    #[serde(default)]
    alloc: BTreeMap<Address, RawAllocAccount>,
    #[serde(rename = "arbOSInit", default)]
    arbos_init: Option<RawArbOsInit>,
    #[serde(rename = "serializedChainConfig")]
    serialized_chain_config: String,
}

#[derive(Deserialize)]
struct RawArbOsInit {
    #[serde(rename = "initialL1BaseFee", default)]
    initial_l1_base_fee: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct RawAllocAccount {
    #[serde(default)]
    balance: Option<serde_json::Value>,
    #[serde(default)]
    nonce: Option<serde_json::Value>,
    #[serde(default)]
    code: Option<Bytes>,
    #[serde(default)]
    storage: Option<BTreeMap<B256, B256>>,
}

/// Coerce a genesis JSON scalar (a bare number OR a hex/decimal string) into a `U256`. Nitro's
/// genesis.json writes balances/fees as bare JSON numbers, not the quoted hex geth normally uses.
fn json_to_u256(v: &serde_json::Value) -> eyre::Result<U256> {
    match v {
        serde_json::Value::Number(n) => U256::try_from(n.as_u128().ok_or_else(|| {
            eyre::eyre!("numeric value {n} out of u128 range; encode large balances as a string")
        })?)
        .map_err(|e| eyre::eyre!("u256 from number: {e}")),
        serde_json::Value::String(s) => {
            let s = s.trim();
            match s.strip_prefix("0x") {
                Some(hex) => U256::from_str_radix(hex, 16),
                None => U256::from_str_radix(s, 10),
            }
            .map_err(|e| eyre::eyre!("u256 from string {s:?}: {e}"))
        }
        other => Err(eyre::eyre!("expected number or string for U256, got {other}")),
    }
}

/// Parse a Nitro `genesis.json` into a [`NitroGenesisFile`].
pub fn parse_nitro_genesis(json: &[u8]) -> eyre::Result<NitroGenesisFile> {
    let raw: RawNitroGenesis =
        serde_json::from_slice(json).map_err(|e| eyre::eyre!("parse genesis.json: {e}"))?;

    let mut alloc = BTreeMap::new();
    for (addr, a) in raw.alloc {
        let balance = a.balance.as_ref().map(json_to_u256).transpose()?.unwrap_or_default();
        let nonce = a
            .nonce
            .as_ref()
            .map(|v| json_to_u256(v).map(|u| u.saturating_to::<u64>()))
            .transpose()?;
        alloc.insert(
            addr,
            GenesisAccount { nonce, balance, code: a.code, storage: a.storage, ..Default::default() },
        );
    }

    let initial_l1_base_fee = raw
        .arbos_init
        .and_then(|i| i.initial_l1_base_fee)
        .as_ref()
        .map(json_to_u256)
        .transpose()?;

    Ok(NitroGenesisFile {
        alloc,
        serialized_chain_config: raw.serialized_chain_config.into_bytes(),
        initial_l1_base_fee,
    })
}

/// Build everything the node needs to boot an Orbit chain from its `chaininfo.json` +
/// `genesis.json`: the [`ChainSpec`] (ArbOS state under the genesis prealloc), the
/// [`ArbosInitConfig`], and the [`ChainInfo`] (rollup addresses for L1 derivation).
///
/// The structured ArbOS init is parsed from the genesis.json's `serializedChainConfig` so the
/// bytes that go into the genesis root are exactly the chain's own; the initial L1 base fee is
/// taken from `arbOSInit` when present (else Nitro's 50 GWei default). The `chaininfo` chain-config
/// is only used to sanity-check the chain id and to carry the rollup deployment.
pub fn orbit_chain_from_files(
    chain_info_json: &[u8],
    genesis_json: &[u8],
) -> eyre::Result<(ChainSpec, ArbosInitConfig, ChainInfo)> {
    let genesis = parse_nitro_genesis(genesis_json)?;
    let mut init = arbos_init_from_chain_config_json(&genesis.serialized_chain_config)?;
    if let Some(fee) = genesis.initial_l1_base_fee {
        init.initial_l1_base_fee = fee;
    }
    let chain_id = init.chain_id.to::<u64>();
    let info = parse_chain_info(chain_info_json, Some(chain_id))?;
    let spec = arb_chain_spec_with_alloc(&init, genesis.alloc)?;
    Ok((spec, init, info))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;

    const ROBINHOOD_CHAIN_INFO: &[u8] = include_bytes!("../tests/fixtures/robinhood-chain-info.json");
    const ROBINHOOD_GENESIS: &[u8] = include_bytes!("../tests/fixtures/robinhood-genesis.json");

    #[test]
    fn parse_robinhood_chain_info() {
        let info = parse_chain_info(ROBINHOOD_CHAIN_INFO, Some(4663)).expect("parse chaininfo");
        assert_eq!(info.chain_id, 4663);
        assert_eq!(info.parent_chain_id, 1);
        assert!(!info.parent_chain_is_arbitrum, "Robinhood is an L2 (parent = Ethereum)");
        assert_eq!(info.chain_config.arbitrum.initial_arbos_version, 51);
        assert_eq!(
            info.rollup.sequencer_inbox,
            address!("0xBd0D173EEb87D57A09521c24388a12789F33ba96")
        );
        assert_eq!(info.rollup.deployed_at, 24994238);
        // picking a non-present chain id must fail rather than silently return the wrong chain
        assert!(parse_chain_info(ROBINHOOD_CHAIN_INFO, Some(42161)).is_err());
    }

    #[test]
    fn parse_robinhood_genesis() {
        let g = parse_nitro_genesis(ROBINHOOD_GENESIS).expect("parse genesis.json");
        assert_eq!(g.alloc.len(), 38, "all 38 prealloc accounts");
        assert!(g.alloc.values().all(|a| a.code.is_some()), "every prealloc is a contract");
        assert_eq!(g.initial_l1_base_fee, Some(U256::from(613218601u64)));
        // serializedChainConfig is the canonical chain-config bytes; it must parse back.
        let init = arbos_init_from_chain_config_json(&g.serialized_chain_config).unwrap();
        assert_eq!(init.chain_id, U256::from(4663u64));
        assert_eq!(init.initial_arbos_version, 51);
    }

    #[test]
    fn orbit_chain_from_files_builds_spec() {
        let (spec, init, info) =
            orbit_chain_from_files(ROBINHOOD_CHAIN_INFO, ROBINHOOD_GENESIS).expect("build orbit spec");
        assert_eq!(init.chain_id, U256::from(4663u64));
        // real initial L1 base fee from arbOSInit wins over the 50 GWei default
        assert_eq!(init.initial_l1_base_fee, U256::from(613218601u64));
        assert_eq!(info.rollup.sequencer_inbox, address!("0xBd0D173EEb87D57A09521c24388a12789F33ba96"));
        // genesis alloc (38) + ArbOS accounts are both present, and the genesis root computed.
        let genesis = &spec.genesis;
        assert!(genesis.alloc.len() > 38, "ArbOS accounts layered over the 38 prealloc");
        assert!(genesis.alloc.contains_key(&address!("0x000000000022D473030F116dDEE9F6B43aC78BA3")));
        assert_ne!(spec.genesis_header().state_root, B256::ZERO);
    }

    /// ArbSys precompile address (0x0000...0064).
    const ARB_SYS: Address = address!("0x0000000000000000000000000000000000000064");
    /// ArbOS state account address (Nitro constant).
    const ARBOS_STATE: Address = address!("0xA4B05FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");

    fn test_chain_config_json() -> Vec<u8> {
        br#"{"chainId":412346,"arbitrum":{"InitialArbOSVersion":32,"InitialChainOwner":"0x00000000000000000000000000000000000a11ce","GenesisBlockNum":0,"AllowDebugPrecompiles":false}}"#.to_vec()
    }

    /// Round-trip through JSON parse to chain spec; check chain id and key accounts.
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

    /// `arbos_init_from_parsed` with a full `ParsedInitMessage`.
    #[test]
    fn arbos_init_from_parsed_works() {
        use arbitrum_alloy_sequencer::init_message::{ArbitrumChainParams, ArbChainConfig, ParsedInitMessage};

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
        use arbitrum_alloy_sequencer::init_message::ParsedInitMessage;

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
    /// from its `config` docker volume (`l2_chain_config.json`). Its byte content is stored in
    /// the ArbOS `chainConfig` subspace, so it must match Nitro's exactly for genesis parity.
    const TESTNODE_CHAIN_CONFIG: &[u8] =
        include_bytes!("../tests/fixtures/testnode_l2_chain_config.json");

    /// Genesis parity for the capture instance used by the per-block replay-parity
    /// test (`driver::tests::replay_feed_matches_testnode_per_block`). That testnode init read a
    /// live L1 base fee of 167 wei (not 147), giving a different genesis than the live-testnode
    /// fixture below. This test locks the genesis inputs that the per-block fixtures depend on.
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
        // Full block-hash parity: London format, nonce=1, gasLimit=1<<50, baseFee=0.1gwei,
        // difficulty=1, extraData=32 zeros, mixHash encoding ArbOS v40.
        let hash = spec.genesis_hash();
        assert_eq!(
            format!("{hash:#x}"),
            "0xb88471684cde5f972dcf47e3fae8f87a5bb690c6b05873843e8549eee18eecf0",
            "ArbOS genesis block hash must match the real nitro-testnode L2 block 0"
        );
    }
}
