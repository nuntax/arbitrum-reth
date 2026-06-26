//! Batch multiplexer: walk a decoded batch's segments and emit the sequence of
//! [`DerivedMessage`]s. Mirrors `nitro/arbstate/inbox.go` `inboxMultiplexer`.
//!
//! Each segment emits at most one message: an L2Message/L2MessageBrotli emits a
//! sequencer message; a DelayedMessages segment pulls exactly one message from
//! the [`DelayedSource`] (at the running cursor) and advances it; Advance*
//! segments only update the running timestamp / L1-block.

use alloy_primitives::U256;
use alloy_rlp::Decodable;

use crate::batch::{segment_kind, BatchHeader, Segment};
use crate::delayed::DelayedSource;
use crate::message::{
    DerivedMessage, L1IncomingMessageHeader, BATCH_POSTER_ADDRESS, KIND_L2_MESSAGE,
};

/// Nitro `arbostypes.MaxL2MessageSize`.
const MAX_L2_MESSAGE_SIZE: usize = 262_144;

/// Errors from [`extract_messages`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultiplexerError {
    /// An Advance* segment carried a malformed RLP u64 delta.
    AdvanceDelta(&'static str),
    /// An L2MessageBrotli segment failed to decompress.
    Brotli,
    /// An over-large L2MessageBrotli payload (exceeds `MaxL2MessageSize`).
    OversizeL2Message,
    /// A DelayedMessages segment referenced an index the source couldn't provide.
    DelayedMissing(u64),
    /// A DelayedMessages segment tried to read past the batch's delayed count.
    DelayedPastCount { read: u64, after: u64 },
}

#[inline]
fn clamp(v: u64, lo: u64, hi: u64) -> u64 {
    v.max(lo).min(hi)
}

/// Decode a batch's segments into its full message list.
///
/// `before_delayed_count` is this batch's starting delayed cursor (the previous
/// batch's `afterDelayedMessages`). `delayed` supplies reconstructed delayed
/// messages by global index; sequencer-only batches may pass
/// [`crate::delayed::NoDelayed`].
pub fn extract_messages(
    header: &BatchHeader,
    segments: &[Segment],
    before_delayed_count: u64,
    delayed: &dyn DelayedSource,
) -> Result<Vec<DerivedMessage>, MultiplexerError> {
    let mut timestamp = header.min_timestamp;
    let mut block = header.min_l1_block;
    let mut delayed_read = before_delayed_count;
    let mut out = Vec::new();

    for seg in segments {
        match seg.kind {
            segment_kind::ADVANCE_TIMESTAMP => {
                let d = decode_u64(&seg.data).map_err(|_| MultiplexerError::AdvanceDelta("timestamp"))?;
                timestamp = timestamp.saturating_add(d);
            }
            segment_kind::ADVANCE_L1_BLOCK => {
                let d = decode_u64(&seg.data).map_err(|_| MultiplexerError::AdvanceDelta("l1block"))?;
                block = block.saturating_add(d);
            }
            segment_kind::L2_MESSAGE => {
                out.push(make_l2_message(header, seg.data.clone(), timestamp, block, delayed_read));
            }
            segment_kind::L2_MESSAGE_BROTLI => {
                let l2 = brotli::decompress(&seg.data, brotli::Dictionary::Empty)
                    .map_err(|_| MultiplexerError::Brotli)?;
                if l2.len() > MAX_L2_MESSAGE_SIZE {
                    return Err(MultiplexerError::OversizeL2Message);
                }
                out.push(make_l2_message(header, l2, timestamp, block, delayed_read));
            }
            segment_kind::DELAYED_MESSAGES => {
                if delayed_read >= header.after_delayed_messages {
                    return Err(MultiplexerError::DelayedPastCount {
                        read: delayed_read,
                        after: header.after_delayed_messages,
                    });
                }
                let dm = delayed
                    .message(delayed_read)
                    .ok_or(MultiplexerError::DelayedMissing(delayed_read))?;
                delayed_read += 1;
                out.push(dm.to_derived(delayed_read));
            }
            // Unknown segment kinds are skipped, matching Nitro.
            _ => {}
        }
    }
    Ok(out)
}

fn decode_u64(mut data: &[u8]) -> Result<u64, alloy_rlp::Error> {
    u64::decode(&mut data)
}

fn make_l2_message(
    header: &BatchHeader,
    l2_msg: Vec<u8>,
    timestamp: u64,
    block: u64,
    delayed: u64,
) -> DerivedMessage {
    DerivedMessage {
        header: L1IncomingMessageHeader {
            kind: KIND_L2_MESSAGE,
            poster: BATCH_POSTER_ADDRESS,
            block_number: clamp(block, header.min_l1_block, header.max_l1_block),
            timestamp: clamp(timestamp, header.min_timestamp, header.max_timestamp),
            request_id: None,
            l1_base_fee: U256::ZERO,
        },
        l2_msg,
        delayed_messages_read: delayed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delayed::{DelayedMap, DelayedMessage, NoDelayed};
    use alloy_primitives::{Address, B256};

    fn header() -> BatchHeader {
        BatchHeader {
            min_timestamp: 1_000,
            max_timestamp: 2_000,
            min_l1_block: 100,
            max_l1_block: 200,
            after_delayed_messages: 8,
        }
    }

    fn advance(kind: u8, delta: u64) -> Segment {
        Segment { kind, data: alloy_rlp::encode(delta) }
    }

    #[test]
    fn emits_l2_messages_with_running_timestamp_and_poster() {
        let h = header();
        let segs = vec![
            advance(segment_kind::ADVANCE_TIMESTAMP, 250),
            Segment { kind: segment_kind::L2_MESSAGE, data: b"tx-a".to_vec() },
            advance(segment_kind::ADVANCE_L1_BLOCK, 5),
            Segment { kind: segment_kind::L2_MESSAGE, data: b"tx-b".to_vec() },
        ];
        let msgs = extract_messages(&h, &segs, 7, &NoDelayed).unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].header.poster, BATCH_POSTER_ADDRESS);
        assert_eq!(msgs[0].header.timestamp, 1_250);
        assert_eq!(msgs[0].header.block_number, 100);
        assert_eq!(msgs[0].delayed_messages_read, 7);
        assert!(msgs[0].header.request_id.is_none());
        assert_eq!(msgs[1].header.block_number, 105);
    }

    #[test]
    fn timestamp_clamped_to_max() {
        let h = header();
        let segs = vec![
            advance(segment_kind::ADVANCE_TIMESTAMP, 10_000),
            Segment { kind: segment_kind::L2_MESSAGE, data: b"x".to_vec() },
        ];
        let msgs = extract_messages(&h, &segs, 0, &NoDelayed).unwrap();
        assert_eq!(msgs[0].header.timestamp, 2_000);
    }

    #[test]
    fn l2_message_brotli_is_decompressed() {
        let h = header();
        let inner = b"decompressed-l2-message".to_vec();
        let compressed =
            brotli::compress(&inner, 11, brotli::DEFAULT_WINDOW_SIZE, brotli::Dictionary::Empty).unwrap();
        let segs = vec![Segment { kind: segment_kind::L2_MESSAGE_BROTLI, data: compressed }];
        let msgs = extract_messages(&h, &segs, 0, &NoDelayed).unwrap();
        assert_eq!(msgs[0].l2_msg, inner);
    }

    #[test]
    fn delayed_segment_pulls_from_source_and_advances_cursor() {
        let h = header();
        let dm = DelayedMessage {
            kind: 12,
            sender: Address::repeat_byte(0xcd),
            block_number: 150,
            timestamp: 1_500,
            inbox_seq_num: 7,
            base_fee_l1: alloy_primitives::U256::from(9u64),
            data: vec![0x11, 0x22],
            before_inbox_acc: B256::ZERO,
        };
        let src = DelayedMap::from_messages([dm.clone()]);
        let segs = vec![
            Segment { kind: segment_kind::L2_MESSAGE, data: b"seq".to_vec() },
            Segment { kind: segment_kind::DELAYED_MESSAGES, data: vec![] },
        ];
        let msgs = extract_messages(&h, &segs, 7, &src).unwrap();
        assert_eq!(msgs.len(), 2);
        // sequencer message carries the pre-read cursor
        assert_eq!(msgs[0].delayed_messages_read, 7);
        // delayed message: reconstructed, cursor advanced to 8
        assert_eq!(msgs[1].header.kind, 12);
        assert_eq!(msgs[1].header.poster, dm.sender);
        assert_eq!(msgs[1].l2_msg, dm.data);
        assert_eq!(msgs[1].delayed_messages_read, 8);
        assert_eq!(msgs[1].header.request_id, Some(dm.request_id()));
    }

    #[test]
    fn delayed_segment_missing_source_errors() {
        let h = header();
        let segs = vec![Segment { kind: segment_kind::DELAYED_MESSAGES, data: vec![] }];
        assert_eq!(
            extract_messages(&h, &segs, 0, &NoDelayed),
            Err(MultiplexerError::DelayedMissing(0))
        );
    }
}
