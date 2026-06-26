//! arb-reth-node — the Arbitrum node skeleton (Stage D.2).
//!
//! # Why this is not a reth sync `Stage`
//!
//! reth's staged pipeline is *download-then-execute*: `HeaderStage`/`BodyStage` download a
//! trustless header+body, `ExecutionStage` executes the stored body, and `MerkleStage` computes the
//! state root and **validates it against the stored header's `state_root`** (`BodyStateRootDiff` on
//! mismatch). That model exists to check a header you *downloaded* from a peer.
//!
//! An Arbitrum node has no such header. It is **execute-to-derive**: a sequencer message is the
//! input, and the block — including its state root — is the *output* of executing that message.
//! There is no pre-existing trusted root to validate against; we mint it. So we follow the path
//! reth itself uses for locally-produced (payload/engine) blocks: produce → execute → compute the
//! state root over the trie → seal the header → persist the executed block via the provider's
//! block-writer (`save_blocks` / `ExecutedBlock`), bypassing the download pipeline.
//!
//! # Pipeline (per block)
//!
//! ```text
//!  L1 inbox / feed  ──(arb-reth-derive, Stage F)──▶  DerivedMessage
//!         │
//!         ▼  digest (Stage E)
//!  arb_revm::executor::digest_message(msg, parent_tip, cfg)  ──▶  ArbExecutionInput
//!         │
//!         ▼  execute (the STF)
//!  arb-reth-evm::ArbBlockExecutor  ──▶  receipts + write-set (BundleState)
//!         │
//!         ▼  state root (alloy_trie HashBuilder — mainnet-validated witness math)
//!  post-state root
//!         │
//!         ▼  assemble + seal (Stage C)
//!  arb-reth-evm::ArbBlockAssembler  ──▶  SealedBlock<ArbTxEnvelope>
//!         │
//!         ▼  persist
//!  reth ProviderFactory::save_blocks (ExecutedBlock)  ──▶  MDBX
//! ```
//!
//! # Increment status
//!
//! - **D.2.1** state-root bridge — executor write-set → canonical post-state root (the linchpin;
//!   root math already exists & is mainnet-validated for the witness path).
//! - **D.2.2** block builder — `digest → execute → root → assemble → seal`, header parity vs a real
//!   mainnet block.
//! - **D.2.3** persistence — `ProviderFactory`/MDBX + `save_blocks`; import N blocks, reopen, verify.
//! - **D.2.4** `ArbChainSpec`/`NodeTypes` + driver loop fed by [`arb-reth-derive`] / the feed.
//!
//! Nothing here yet wires MDBX — the heavy reth storage surface lands in D.2.3 once the
//! produce→execute→seal core (D.2.1/D.2.2) is proven against mainnet fixtures.

/// Arbitrum One mainnet chain id.
pub const ARB_ONE_CHAIN_ID: u64 = 42161;
