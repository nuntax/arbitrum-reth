//! `arb-reth-derive`: L1 inbox derivation.
//!
//! Decodes Arbitrum L1 `SequencerInbox` batches into the canonical
//! [`MessageWithMetadata`] stream: the trustless-sync half that lets the node reach
//! tip standalone from L1. Covers the blob path (EIP-4844) and calldata.
//!
//! Pipeline:
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

use arbitrum_alloy_sequencer as _;
