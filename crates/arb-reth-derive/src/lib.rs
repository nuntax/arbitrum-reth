//! `arb-reth-derive`: Stage F, L1 inbox derivation.
//!
//! Decodes Arbitrum L1 `SequencerInbox` batches into the canonical
//! [`MessageWithMetadata`] stream (the trustless-sync half that `arbitrum-reth`
//! lacks). First milestone: the **blob** path (EIP-4844), then calldata.
//!
//! Pipeline (see `docs/stage-f-handoff.md` + the blob-decode addendum):
//! ```text
//! blob sidecars --[field-element unpack]--> batch bytes
//!   --[40-byte timeBounds header + 0x00 brotli flag]--> RLP segment list
//!   --[multiplexer Pop]--> Vec<MessageWithMetadata>
//! ```

pub mod batch;
pub mod blob;
pub mod delayed;
pub mod l2message;
pub mod message;
pub mod multiplexer;

use arb_sequencer_network as _;
