//! `arb-reth-derive` — Stage F: L1 inbox derivation.
//!
//! Decodes Arbitrum L1 `SequencerInbox` batches into the canonical
//! [`MessageWithMetadata`] stream — the trustless-sync half that `arbitrum-reth`
//! lacks. First milestone: the **blob** path (EIP-4844), then calldata.
//!
//! Pipeline (see `docs/stage-f-handoff.md` + the blob-decode addendum):
//! ```text
//! blob sidecars --[field-element unpack]--> batch bytes
//!   --[40-byte timeBounds header + 0x00 brotli flag]--> RLP segment list
//!   --[multiplexer Pop]--> Vec<MessageWithMetadata>
//! ```
//! This is a scaffold; the decode modules land next as the blob-format research
//! completes.

pub mod batch;
pub mod blob;
pub mod delayed;
pub mod l2message;
pub mod message;
pub mod multiplexer;

// Establish the message-type dependency (reused, not redefined) and prove it
// unifies on reth's alloy 1.8.3 in this workspace.
use arb_sequencer_network as _;
