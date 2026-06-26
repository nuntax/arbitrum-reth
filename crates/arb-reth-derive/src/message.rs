//! Canonical (binary) Arbitrum L1 incoming message produced by derivation.
//!
//! NB (open design point): `arb-sequencer-network` currently models these as
//! JSON *feed* DTOs (hex-string fields). This is the binary representation the
//! decode pipeline naturally produces; the canonical type + `serialize`/`hash`
//! (Nitro 113-byte header / RLP hash) will be unified into arb-sequencer-network
//! once validated against a real fixture.

use alloy_primitives::{address, Address, B256, U256};

/// Virtual batch-poster address stamped on sequencer L2 messages
/// (`nitro arbos/l1pricing.BatchPosterAddress`; trailing bytes spell "sequencer").
pub const BATCH_POSTER_ADDRESS: Address = address!("A4B000000000000000000073657175656e636572");

/// L1 message kind for an L2 message envelope (`L1MessageType_L2Message`).
pub const KIND_L2_MESSAGE: u8 = 3;

/// Header of an L1 incoming message (binary form).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct L1IncomingMessageHeader {
    pub kind: u8,
    pub poster: Address,
    pub block_number: u64,
    pub timestamp: u64,
    /// `None` for sequencer L2 messages; `Some` for delayed-inbox messages.
    pub request_id: Option<B256>,
    pub l1_base_fee: U256,
}

/// An L1 incoming message plus its delayed-inbox cursor — the unit a batch
/// decodes into (mirrors Nitro `MessageWithMetadata`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedMessage {
    pub header: L1IncomingMessageHeader,
    pub l2_msg: Vec<u8>,
    pub delayed_messages_read: u64,
}
