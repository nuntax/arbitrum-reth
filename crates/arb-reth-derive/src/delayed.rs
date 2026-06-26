//! Delayed-inbox messages: reconstruction from L1 (`MessageDelivered` +
//! `InboxMessageDelivered`) and the on-chain accumulator, plus the source the
//! multiplexer pulls from when it hits a `DelayedMessages` segment.
//!
//! Format from `nitro/arbnode/delayed.go` (header build) and
//! `nitro/contracts/src/bridge/Messages.sol` (accumulator).

use std::collections::BTreeMap;

use alloy_primitives::{keccak256, Address, B256, U256};

use crate::message::{DerivedMessage, L1IncomingMessageHeader};

/// A delayed-inbox message reconstructed from L1.
///
/// Header mapping (`delayed.go:230`): `Kind`, `Poster = sender`,
/// `BlockNumber = l1 block`, `Timestamp`, `RequestId = BigToHash(messageIndex)`,
/// `L1BaseFee`. `L2msg = data` (with `keccak256(data) == messageDataHash`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelayedMessage {
    pub kind: u8,
    pub sender: Address,
    pub block_number: u64,
    pub timestamp: u64,
    /// Global delayed-message index (the inbox sequence number).
    pub inbox_seq_num: u64,
    pub base_fee_l1: U256,
    pub data: Vec<u8>,
    /// The inbox accumulator *before* this message — for chain verification.
    pub before_inbox_acc: B256,
}

impl DelayedMessage {
    /// `keccak256(data)` — must equal the event's `messageDataHash`.
    pub fn message_data_hash(&self) -> B256 {
        keccak256(&self.data)
    }

    /// Per-message hash (`Messages.sol`): `keccak256(kind ++ sender(20) ++
    /// blockNumber(8) ++ timestamp(8) ++ inboxSeqNum(32) ++ baseFeeL1(32) ++
    /// messageDataHash(32))`.
    pub fn message_hash(&self) -> B256 {
        delayed_message_hash(
            self.kind,
            self.sender,
            self.block_number,
            self.timestamp,
            self.inbox_seq_num,
            self.base_fee_l1,
            self.message_data_hash(),
        )
    }

    /// The accumulator after this message: `keccak256(beforeAcc ++ messageHash)`.
    pub fn accumulator(&self) -> B256 {
        keccak256([self.before_inbox_acc.as_slice(), self.message_hash().as_slice()].concat())
    }

    /// The request id stamped on the message header: `BigToHash(messageIndex)`.
    pub fn request_id(&self) -> B256 {
        B256::from(U256::from(self.inbox_seq_num).to_be_bytes::<32>())
    }

    /// Convert to the canonical [`DerivedMessage`] emitted by the multiplexer.
    pub fn to_derived(&self, delayed_messages_read: u64) -> DerivedMessage {
        DerivedMessage {
            header: L1IncomingMessageHeader {
                kind: self.kind,
                poster: self.sender,
                block_number: self.block_number,
                timestamp: self.timestamp,
                request_id: Some(self.request_id()),
                l1_base_fee: self.base_fee_l1,
            },
            l2_msg: self.data.clone(),
            delayed_messages_read,
        }
    }
}

/// Per-delayed-message hash (`nitro/contracts/src/bridge/Messages.sol`):
/// `keccak256(kind(1) ++ sender(20) ++ blockNumber(8 BE) ++ timestamp(8 BE) ++
/// inboxSeqNum(32 BE) ++ baseFeeL1(32 BE) ++ messageDataHash(32))`. Exposed as a
/// free function so it can be checked directly against a `MessageDelivered`
/// event's `messageDataHash` without the message body.
pub fn delayed_message_hash(
    kind: u8,
    sender: Address,
    block_number: u64,
    timestamp: u64,
    inbox_seq_num: u64,
    base_fee_l1: U256,
    message_data_hash: B256,
) -> B256 {
    let mut buf = Vec::with_capacity(1 + 20 + 8 + 8 + 32 + 32 + 32);
    buf.push(kind);
    buf.extend_from_slice(sender.as_slice());
    buf.extend_from_slice(&block_number.to_be_bytes());
    buf.extend_from_slice(&timestamp.to_be_bytes());
    buf.extend_from_slice(&U256::from(inbox_seq_num).to_be_bytes::<32>());
    buf.extend_from_slice(&base_fee_l1.to_be_bytes::<32>());
    buf.extend_from_slice(message_data_hash.as_slice());
    keccak256(buf)
}

/// Accumulator step (`Messages.sol`): `keccak256(beforeAcc ++ messageHash)`.
pub fn accumulate(before_acc: B256, message_hash: B256) -> B256 {
    keccak256([before_acc.as_slice(), message_hash.as_slice()].concat())
}

/// A source of reconstructed delayed messages, keyed by global delayed index.
pub trait DelayedSource {
    fn message(&self, index: u64) -> Option<&DelayedMessage>;
}

/// Empty source — sequencer-only batches never query it.
pub struct NoDelayed;

impl DelayedSource for NoDelayed {
    fn message(&self, _index: u64) -> Option<&DelayedMessage> {
        None
    }
}

/// In-memory source backed by messages keyed by their `inbox_seq_num`.
#[derive(Debug, Default)]
pub struct DelayedMap(pub BTreeMap<u64, DelayedMessage>);

impl DelayedMap {
    pub fn from_messages(msgs: impl IntoIterator<Item = DelayedMessage>) -> Self {
        Self(msgs.into_iter().map(|m| (m.inbox_seq_num, m)).collect())
    }
}

impl DelayedSource for DelayedMap {
    fn message(&self, index: u64) -> Option<&DelayedMessage> {
        self.0.get(&index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(idx: u64, before: B256) -> DelayedMessage {
        DelayedMessage {
            kind: 12, // EthDeposit
            sender: Address::repeat_byte(0xab),
            block_number: 25_000_000,
            timestamp: 1_700_000_000,
            inbox_seq_num: idx,
            base_fee_l1: U256::from(7_000_000_000u64),
            data: vec![0xde, 0xad, 0xbe, 0xef],
            before_inbox_acc: before,
        }
    }

    #[test]
    fn hashes_are_deterministic_and_chain() {
        let m0 = sample(0, B256::ZERO);
        let h0 = m0.message_hash();
        assert_eq!(h0, m0.message_hash(), "message_hash deterministic");
        let acc0 = m0.accumulator();
        // next message chains off the prior accumulator
        let m1 = sample(1, acc0);
        assert_ne!(m1.accumulator(), acc0);
        // data hash sanity
        assert_eq!(m0.message_data_hash(), keccak256(&m0.data));
    }

    #[test]
    fn to_derived_sets_request_id_and_kind() {
        let m = sample(5, B256::ZERO);
        let d = m.to_derived(6);
        assert_eq!(d.header.kind, 12);
        assert_eq!(d.delayed_messages_read, 6);
        assert_eq!(d.header.request_id, Some(m.request_id()));
        assert_eq!(d.l2_msg, m.data);
    }
}
