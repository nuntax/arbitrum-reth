//! Sequencer batch framing: the 40-byte timeBounds header, header-flag dispatch,
//! brotli decompression, and the RLP segment stream. Mirrors
//! `nitro/arbstate/inbox.go` (`ParseSequencerMessage`) and `daprovider/util.go`.
//!
//! Also provides [`parse_sequencer_batch_delivered`] which decodes the non-indexed
//! log data of the `SequencerBatchDelivered` event into a [`BatchHeader`], freeing
//! callers from ABI-layout arithmetic.

use alloy_primitives::B256;
use alloy_rlp::Header;

/// Header-flag bytes (`nitro/daprovider/util.go`). The flag is the first byte of
/// the post-timeBounds payload.
pub mod flag {
    pub const BROTLI: u8 = 0x00;
    pub const DA_CERTIFICATE: u8 = 0x01;
    pub const ANYTRUST_TREE: u8 = 0x08;
    pub const ZEROHEAVY: u8 = 0x20;
    pub const L1_AUTHENTICATED: u8 = 0x40;
    pub const BLOB_HASHES: u8 = 0x50; // L1_AUTHENTICATED | 0x10
    pub const ANYTRUST: u8 = 0x80;
}

/// Batch segment kinds (`nitro/arbstate/inbox.go`).
pub mod segment_kind {
    pub const L2_MESSAGE: u8 = 0;
    pub const L2_MESSAGE_BROTLI: u8 = 1;
    pub const DELAYED_MESSAGES: u8 = 2;
    pub const ADVANCE_TIMESTAMP: u8 = 3;
    pub const ADVANCE_L1_BLOCK: u8 = 4;
}

/// Length of the big-endian timeBounds header prepended to every sequencer batch.
pub const BATCH_HEADER_LEN: usize = 40;

/// The 40-byte timeBounds header (5 × big-endian u64).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchHeader {
    pub min_timestamp: u64,
    pub max_timestamp: u64,
    pub min_l1_block: u64,
    pub max_l1_block: u64,
    pub after_delayed_messages: u64,
}

impl BatchHeader {
    /// Parse the 40-byte header; returns `(header, remaining_payload)`.
    pub fn parse(data: &[u8]) -> Result<(Self, &[u8]), BatchError> {
        if data.len() < BATCH_HEADER_LEN {
            return Err(BatchError::Truncated);
        }
        let r = |o: usize| u64::from_be_bytes(data[o..o + 8].try_into().unwrap());
        let header = Self {
            min_timestamp: r(0),
            max_timestamp: r(8),
            min_l1_block: r(16),
            max_l1_block: r(24),
            after_delayed_messages: r(32),
        };
        Ok((header, &data[BATCH_HEADER_LEN..]))
    }
}

/// Errors from batch framing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchError {
    /// Data shorter than the 40-byte header (or an empty payload).
    Truncated,
    /// Header flag not handled at this layer (blob/DA/zeroheavy resolved elsewhere).
    UnsupportedFlag(u8),
    /// Brotli decompression failed.
    Brotli,
    /// Malformed RLP in the segment stream.
    Rlp(&'static str),
    /// `SequencerBatchDelivered` event log data has wrong length (expected 224).
    EventDataWrongLen(usize),
}

/// Parsed fields from the non-indexed data of a `SequencerBatchDelivered` L1 event.
///
/// Layout: 7 × 32-byte ABI words —
/// `[delayedAcc(32), afterDelayedMessagesRead(32), minTimestamp(32), maxTimestamp(32),
///   minL1Block(32), maxL1Block(32), dataLocation(32)]`.
/// Each timeBounds field is a `uint64` right-aligned in its 32-byte word.
/// `dataLocation` enum: `TxInput=0, SeparateBatchEvent=1, NoData=2, Blob=3`.
///
/// The `BatchHeader` fields (`min_timestamp`, `max_timestamp`, `min_l1_block`,
/// `max_l1_block`, `after_delayed_messages`) match the 40-byte prepend used by
/// `nitro/arbnode/sequencer_inbox.go SerializeSequencerInboxBatch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequencerBatchDeliveredData {
    /// `delayedAcc`: the delayed-inbox accumulator after all delayed messages in this batch.
    pub delayed_acc: B256,
    /// `afterDelayedMessagesRead`: the batch header field.
    pub after_delayed_messages_read: u64,
    /// timeBounds minimum timestamp.
    pub min_timestamp: u64,
    /// timeBounds maximum timestamp.
    pub max_timestamp: u64,
    /// timeBounds minimum L1 block number.
    pub min_l1_block: u64,
    /// timeBounds maximum L1 block number.
    pub max_l1_block: u64,
    /// `dataLocation` enum value (0=TxInput, 1=SeparateBatchEvent, 2=NoData, 3=Blob).
    pub data_location: u8,
}

/// `dataLocation` enum values in `SequencerBatchDelivered`.
pub mod data_location {
    pub const TX_INPUT: u8 = 0;
    pub const SEPARATE_BATCH_EVENT: u8 = 1;
    pub const NO_DATA: u8 = 2;
    pub const BLOB_HASHES: u8 = 3;
}

impl SequencerBatchDeliveredData {
    /// Build a [`BatchHeader`] from the event fields (the 40-byte timeBounds prepend).
    pub fn batch_header(&self) -> BatchHeader {
        BatchHeader {
            min_timestamp: self.min_timestamp,
            max_timestamp: self.max_timestamp,
            min_l1_block: self.min_l1_block,
            max_l1_block: self.max_l1_block,
            after_delayed_messages: self.after_delayed_messages_read,
        }
    }
}

/// Decode the non-indexed data bytes of a `SequencerBatchDelivered` event.
///
/// `log_data` must be exactly 224 bytes (7 × 32-byte ABI words). Returns
/// [`BatchError::EventDataWrongLen`] otherwise.
///
/// The three indexed topics (`batchSeqNum`, `beforeAcc`, `afterAcc`) are already
/// present in the log's `topics` field and are not part of `log_data`.
pub fn parse_sequencer_batch_delivered(log_data: &[u8]) -> Result<SequencerBatchDeliveredData, BatchError> {
    const EXPECTED: usize = 7 * 32; // 224 bytes
    if log_data.len() != EXPECTED {
        return Err(BatchError::EventDataWrongLen(log_data.len()));
    }

    // Each word is 32 bytes big-endian. For uint64 values we take the low 8 bytes (bytes 24..32).
    let word = |i: usize| &log_data[i * 32..(i + 1) * 32];
    let u64_word = |i: usize| u64::from_be_bytes(word(i)[24..32].try_into().unwrap());

    let delayed_acc = B256::from_slice(word(0));
    let after_delayed_messages_read = u64_word(1);
    let min_timestamp = u64_word(2);
    let max_timestamp = u64_word(3);
    let min_l1_block = u64_word(4);
    let max_l1_block = u64_word(5);
    let data_location = word(6)[31]; // lowest byte of the uint8-in-32-byte word

    Ok(SequencerBatchDeliveredData {
        delayed_acc,
        after_delayed_messages_read,
        min_timestamp,
        max_timestamp,
        min_l1_block,
        max_l1_block,
        data_location,
    })
}

/// Decompress a header-flagged payload into the raw RLP segment stream.
///
/// Handles the brotli (`0x00`) path. The blob (`0x50`), zeroheavy (`0x20`) and DA
/// flags are resolved upstream (blob hashes need external blob retrieval first);
/// after that resolution the recovered bytes re-enter here starting at their own
/// flag byte — which for mainnet sequencer batches is `0x00` brotli.
pub fn decompress_payload(payload: &[u8]) -> Result<Vec<u8>, BatchError> {
    let (&flag, body) = payload.split_first().ok_or(BatchError::Truncated)?;
    match flag {
        flag::BROTLI => {
            brotli::decompress(body, brotli::Dictionary::Empty).map_err(|_| BatchError::Brotli)
        }
        other => Err(BatchError::UnsupportedFlag(other)),
    }
}

/// A decoded batch segment: the `kind` byte plus its payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment {
    pub kind: u8,
    pub data: Vec<u8>,
}

/// Parse a decompressed batch body — a stream of consecutive RLP byte strings,
/// each one segment whose first byte is the [`segment_kind`]. Matches Nitro's
/// `rlp.NewStream(...).Decode(&segment)` loop. Empty (zero-length) items are
/// skipped, as in Nitro.
pub fn parse_segments(mut buf: &[u8]) -> Result<Vec<Segment>, BatchError> {
    let mut segs = Vec::new();
    while !buf.is_empty() {
        let header = Header::decode(&mut buf).map_err(|_| BatchError::Rlp("segment header"))?;
        if header.list {
            return Err(BatchError::Rlp("segment is a list, expected string"));
        }
        if header.payload_length > buf.len() {
            return Err(BatchError::Rlp("segment length exceeds data"));
        }
        let (seg, rest) = buf.split_at(header.payload_length);
        buf = rest;
        if let Some((&kind, data)) = seg.split_first() {
            segs.push(Segment { kind, data: data.to_vec() });
        }
    }
    Ok(segs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;

    fn rlp_string(data: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        Header { list: false, payload_length: data.len() }.encode(&mut out);
        out.extend_from_slice(data);
        out
    }

    #[test]
    fn header_parses_big_endian_fields() {
        let mut data = Vec::new();
        for v in [1u64, 2, 3, 4, 5] {
            data.extend_from_slice(&v.to_be_bytes());
        }
        data.extend_from_slice(b"payload");
        let (h, rest) = BatchHeader::parse(&data).unwrap();
        assert_eq!(h.min_timestamp, 1);
        assert_eq!(h.max_timestamp, 2);
        assert_eq!(h.min_l1_block, 3);
        assert_eq!(h.max_l1_block, 4);
        assert_eq!(h.after_delayed_messages, 5);
        assert_eq!(rest, b"payload");
    }

    #[test]
    fn header_rejects_truncated() {
        assert_eq!(BatchHeader::parse(&[0u8; 39]), Err(BatchError::Truncated));
    }

    #[test]
    fn brotli_payload_roundtrips() {
        let segments_body = b"the quick brown fox jumps over the lazy dog, repeatedly.".repeat(10);
        let compressed =
            brotli::compress(&segments_body, 11, brotli::DEFAULT_WINDOW_SIZE, brotli::Dictionary::Empty)
                .unwrap();
        let mut payload = vec![flag::BROTLI];
        payload.extend_from_slice(&compressed);
        assert_eq!(decompress_payload(&payload).unwrap(), segments_body);
    }

    #[test]
    fn unsupported_flag_reported() {
        assert_eq!(
            decompress_payload(&[flag::ANYTRUST, 0xaa]),
            Err(BatchError::UnsupportedFlag(flag::ANYTRUST))
        );
    }

    #[test]
    fn segments_stream_parses_kinds_and_data() {
        // An AdvanceTimestamp segment (kind 3 + rlp(u64) delta) then an L2Message (kind 0 + body).
        let advance = {
            let mut s = vec![segment_kind::ADVANCE_TIMESTAMP];
            s.extend_from_slice(&rlp_string(&[]) ); // placeholder inner; multiplexer decodes data itself
            s
        };
        let l2 = {
            let mut s = vec![segment_kind::L2_MESSAGE];
            s.extend_from_slice(b"\x03some-l2-message-bytes");
            s
        };
        let mut stream = Vec::new();
        stream.extend_from_slice(&rlp_string(&advance));
        stream.extend_from_slice(&rlp_string(&l2));

        let segs = parse_segments(&stream).unwrap();
        assert_eq!(segs.len(), 2);
        assert_eq!(segs[0].kind, segment_kind::ADVANCE_TIMESTAMP);
        assert_eq!(segs[1].kind, segment_kind::L2_MESSAGE);
        assert_eq!(segs[1].data, b"\x03some-l2-message-bytes");
    }

    #[test]
    fn segments_skip_empty_items() {
        let mut stream = Vec::new();
        stream.extend_from_slice(&rlp_string(&[])); // empty -> skipped
        stream.extend_from_slice(&rlp_string(&[segment_kind::L2_MESSAGE, 0xff]));
        let segs = parse_segments(&stream).unwrap();
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].kind, segment_kind::L2_MESSAGE);
        assert_eq!(segs[0].data, vec![0xff]);
    }

    // ──────────────────────────────────────────────────────────────────────
    // parse_sequencer_batch_delivered tests
    // ──────────────────────────────────────────────────────────────────────

    /// Verify wrong-length rejection.
    #[test]
    fn event_parse_rejects_wrong_len() {
        assert_eq!(
            parse_sequencer_batch_delivered(&[0u8; 223]),
            Err(BatchError::EventDataWrongLen(223))
        );
        assert_eq!(
            parse_sequencer_batch_delivered(&[0u8; 225]),
            Err(BatchError::EventDataWrongLen(225))
        );
    }

    /// Parse the real `SequencerBatchDelivered` log data from Arbitrum One blob batch
    /// seq 1277861 (L1 block 25398052, tx 0x20eae1f4…). This is the same batch used
    /// by `blob_batch_fixture.rs` — the fixture `*_meta.json` was captured from this
    /// event. Verified 2026-06-26 via `eth_getTransactionReceipt`.
    ///
    /// Non-indexed log data (224 bytes / 7 × 32 words):
    ///   word0 delayedAcc              36dcb569…
    ///   word1 afterDelayedMessagesRead  2484028 (0x25e73c)
    ///   word2 minTimestamp           1782345239 (0x6a3c6e17)
    ///   word3 maxTimestamp           1782432407 (0x6a3dc297)
    ///   word4 minL1Block               25390852 (0x1836f04)
    ///   word5 maxL1Block               25398116 (0x1838b64)
    ///   word6 dataLocation                    3 (Blob)
    #[test]
    fn event_parse_blob_batch_1277861() {
        let hex = concat!(
            "36dcb569736ad44a19d929cc88ad1490ceebd85afcc0e94a0d60c9a47fa60c05",
            "000000000000000000000000000000000000000000000000000000000025e73c",
            "000000000000000000000000000000000000000000000000000000006a3c6e17",
            "000000000000000000000000000000000000000000000000000000006a3dc297",
            "0000000000000000000000000000000000000000000000000000000001836f04",
            "0000000000000000000000000000000000000000000000000000000001838b64",
            "0000000000000000000000000000000000000000000000000000000000000003",
        );
        let data: Vec<u8> = (0..hex.len() / 2)
            .map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap())
            .collect();
        let parsed = parse_sequencer_batch_delivered(&data).unwrap();

        assert_eq!(parsed.after_delayed_messages_read, 2_484_028);
        assert_eq!(parsed.min_timestamp, 1_782_345_239);
        assert_eq!(parsed.max_timestamp, 1_782_432_407);
        assert_eq!(parsed.min_l1_block, 25_390_852);
        assert_eq!(parsed.max_l1_block, 25_398_116);
        assert_eq!(parsed.data_location, data_location::BLOB_HASHES);
        assert_eq!(
            parsed.delayed_acc,
            "0x36dcb569736ad44a19d929cc88ad1490ceebd85afcc0e94a0d60c9a47fa60c05"
                .parse::<B256>()
                .unwrap()
        );

        // batch_header() conversion
        let hdr = parsed.batch_header();
        assert_eq!(hdr.min_timestamp, parsed.min_timestamp);
        assert_eq!(hdr.after_delayed_messages, parsed.after_delayed_messages_read);
    }

    /// Parse the `SequencerBatchDelivered` log data for the calldata batch
    /// (seq 497980, L1 block 19000015, dataLocation=0 TxInput). Verified 2026-06-26.
    #[test]
    fn event_parse_calldata_batch_497980() {
        let hex = concat!(
            "03d3a37ee159851c98b8fa4fac1abc1c573754c09961daf0937f375127501a6d",
            "00000000000000000000000000000000000000000000000000000000001432cf",
            "0000000000000000000000000000000000000000000000000000000065a190f7",
            "0000000000000000000000000000000000000000000000000000000065a2f087",
            "000000000000000000000000000000000000000000000000000000000121d44f",
            "000000000000000000000000000000000000000000000000000000000121eadb",
            "0000000000000000000000000000000000000000000000000000000000000000",
        );
        let data: Vec<u8> = (0..hex.len() / 2)
            .map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap())
            .collect();
        let parsed = parse_sequencer_batch_delivered(&data).unwrap();

        assert_eq!(parsed.after_delayed_messages_read, 1_323_727);
        assert_eq!(parsed.min_timestamp, 1_705_087_223);
        assert_eq!(parsed.max_timestamp, 1_705_177_223);
        assert_eq!(parsed.min_l1_block, 18_994_255);
        assert_eq!(parsed.max_l1_block, 19_000_027);
        assert_eq!(parsed.data_location, data_location::TX_INPUT);
    }
}
