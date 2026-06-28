//! Adapt a derived L1 message into the sequencer-feed [`BroadcastFeedMessage`]
//! shape, so messages recovered from L1 can be driven through the same validated
//! `digest_message` + executor path the node uses for live feed replay.
//!
//! The feed types are stringly/JSON-typed (`sender` hex string, `requestId`/`baseFeeL1`
//! as JSON values, `l2Msg` base64); this re-encodes the already-typed
//! [`DerivedMessage`] fields into that shape. `parse_message` base64-decodes `l2Msg`
//! for every message kind, so the body is always base64-encoded here.
//!
//! Note: `BatchPostingReport` (kind 13) additionally needs `batchDataTokens` /
//! `batchGasCost`, which are not carried on a [`DerivedMessage`] and are left `None`;
//! reconstructing those from L1 is a separate step.

use arb_reth_derive::message::DerivedMessage;
use arb_sequencer_network::sequencer::feed::{
    BatchDataStats, BroadcastFeedMessage, Header, L1IncomingMessage, MessageWithMetadata,
};
use base64::prelude::*;
use serde_json::Value;

/// Convert a [`DerivedMessage`] into a [`BroadcastFeedMessage`] with the given feed
/// sequence number.
pub fn derived_to_feed_message(derived: &DerivedMessage, sequence_number: u64) -> BroadcastFeedMessage {
    derived_to_feed_message_with_stats(derived, sequence_number, None)
}

/// As [`derived_to_feed_message`], but attaches batch data stats. `BatchPostingReport`
/// (kind 13) messages need these for gas accounting; see
/// [`crate::batch_serialize::batch_data_stats`].
pub fn derived_to_feed_message_with_stats(
    derived: &DerivedMessage,
    sequence_number: u64,
    batch_data_stats: Option<BatchDataStats>,
) -> BroadcastFeedMessage {
    let h = &derived.header;
    let header = Header {
        kind: h.kind,
        sender: format!("{:#x}", h.poster),
        block_number: h.block_number,
        timestamp: h.timestamp,
        request_id: match h.request_id {
            Some(id) => Value::String(format!("{id:#x}")),
            None => Value::Null,
        },
        // `L1Header::from_header` reads this via `as_u64`; L1 base fee per gas fits u64.
        base_fee_l1: Value::Number(u64::try_from(h.l1_base_fee).unwrap_or(0).into()),
    };
    BroadcastFeedMessage {
        sequence_number,
        message_with_meta_data: MessageWithMetadata {
            l1_incoming_message: L1IncomingMessage {
                header,
                l2msg: BASE64_STANDARD.encode(&derived.l2_msg),
                legacy_batch_gas_cost: None,
                batch_data_stats,
            },
            delayed_messages_read: derived.delayed_messages_read,
        },
    }
}
