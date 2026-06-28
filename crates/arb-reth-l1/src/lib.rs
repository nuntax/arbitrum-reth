//! `arb-reth-l1`: Stage F, the L1-derivation fetch layer.
//!
//! Reads `SequencerInbox` batches from an L1 RPC and turns them into the
//! [`DerivedMessage`](arb_reth_derive::message::DerivedMessage) stream that the
//! decoders in `arb-reth-derive` produce. This is the trustless-sync source the
//! node uses to catch up from a snapshot height to the L1 head before following
//! the live feed for the tip.
//!
//! What lives where:
//! * [`contracts`] - the `SequencerInbox` ABI surface (event topic + call selectors).
//! * [`reader`] - [`SequencerInboxReader`], which fetches batch logs and resolves
//!   each batch's payload (calldata now; blob sidecars in a later slice).
//! * [`extract_calldata_payload`] / [`decode_batch_messages`] - the pure decode glue
//!   bridging recovered calldata to the derive pipeline.

pub mod batch_serialize;
pub mod beacon;
pub mod contracts;
pub mod delayed;
pub mod feed;
pub mod reader;

use alloy_sol_types::SolCall;
use arb_reth_derive::batch::{self, data_location, BatchError};
use arb_reth_derive::delayed::DelayedSource;
use arb_reth_derive::message::DerivedMessage;
use arb_reth_derive::multiplexer::{extract_messages, MultiplexerError};

pub use beacon::BeaconClient;
pub use contracts::{BRIDGE_MAINNET, SEQUENCER_INBOX_MAINNET};
pub use batch_serialize::{batch_data_hash, batch_data_stats, report_data_hash, serialize_batch};
pub use delayed::{verify_accumulator_chain, DelayedInboxReader};
pub use feed::{derived_to_feed_message, derived_to_feed_message_with_stats};
pub use reader::{BatchPayload, DeliveredBatch, SequencerInboxReader};

/// Errors from the L1 fetch + decode glue.
#[derive(Debug)]
pub enum L1Error {
    /// Transaction input shorter than a 4-byte selector.
    CalldataTooShort(usize),
    /// Batch-poster selector not recognised (likely a blob/delay-proof variant not
    /// yet wired).
    UnknownSelector([u8; 4]),
    /// ABI decode of the batch-poster call failed.
    Abi(alloy_sol_types::Error),
    /// Batch framing/decompression error from arb-reth-derive.
    Batch(BatchError),
    /// Multiplexer error from arb-reth-derive.
    Mux(MultiplexerError),
    /// Blob field-element decode failed.
    Blob(String),
    /// `dataLocation` enum value with no decode path here yet (e.g. blobs).
    UnsupportedDataLocation(u8),
    /// A log/transaction lacked a field needed to resolve the batch.
    Missing(&'static str),
    /// Transport/RPC failure (stringified to avoid leaking the provider error type).
    Rpc(String),
}

impl core::fmt::Display for L1Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            L1Error::CalldataTooShort(n) => write!(f, "calldata too short: {n} bytes"),
            L1Error::UnknownSelector(s) => {
                write!(f, "unknown batch-poster selector: 0x{}", alloy_primitives::hex::encode(s))
            }
            L1Error::Abi(e) => write!(f, "abi decode: {e}"),
            L1Error::Batch(e) => write!(f, "batch decode: {e:?}"),
            L1Error::Mux(e) => write!(f, "multiplexer: {e:?}"),
            L1Error::Blob(e) => write!(f, "blob decode: {e}"),
            L1Error::UnsupportedDataLocation(d) => write!(f, "unsupported dataLocation: {d}"),
            L1Error::Missing(what) => write!(f, "missing {what}"),
            L1Error::Rpc(e) => write!(f, "rpc: {e}"),
        }
    }
}

impl std::error::Error for L1Error {}

impl From<alloy_sol_types::Error> for L1Error {
    fn from(e: alloy_sol_types::Error) -> Self {
        L1Error::Abi(e)
    }
}

/// Recover the batch payload (header-flag byte + compressed segments) from a
/// batch-poster transaction's calldata.
///
/// Dispatches on the 4-byte selector. Returns the `data` argument for the calldata
/// posters; blob/delay-proof variants are not handled here and surface as
/// [`L1Error::UnknownSelector`].
pub fn extract_calldata_payload(input: &[u8]) -> Result<Vec<u8>, L1Error> {
    if input.len() < 4 {
        return Err(L1Error::CalldataTooShort(input.len()));
    }
    let selector: [u8; 4] = input[..4].try_into().unwrap();

    if selector == contracts::origin::addSequencerL2BatchFromOriginCall::SELECTOR {
        let call = contracts::origin::addSequencerL2BatchFromOriginCall::abi_decode(input)?;
        return Ok(call.data.to_vec());
    }
    if selector == contracts::origin_legacy::addSequencerL2BatchFromOriginCall::SELECTOR {
        let call = contracts::origin_legacy::addSequencerL2BatchFromOriginCall::abi_decode(input)?;
        return Ok(call.data.to_vec());
    }
    Err(L1Error::UnknownSelector(selector))
}

/// Decode a resolved batch payload (the brotli-flagged byte stream from either the
/// calldata or blob path) into its `DerivedMessage` stream, given the batch header.
///
/// `before_delayed_count` is the number of delayed messages read before this batch
/// (the previous batch's `afterDelayedMessagesRead`); the multiplexer needs it to
/// index `DelayedMessages` segments. For batches with no delayed-message segments
/// the value is unused.
pub fn decode_payload_messages(
    header: &arb_reth_derive::batch::BatchHeader,
    payload: &[u8],
    before_delayed_count: u64,
    delayed: &dyn DelayedSource,
) -> Result<Vec<DerivedMessage>, L1Error> {
    let seg_bytes = batch::decompress_payload(payload).map_err(L1Error::Batch)?;
    let segments = batch::parse_segments(&seg_bytes).map_err(L1Error::Batch)?;
    extract_messages(header, &segments, before_delayed_count, delayed).map_err(L1Error::Mux)
}

/// Decode a resolved calldata batch into its `DerivedMessage` stream.
///
/// Blob batches must first be resolved to a payload via
/// [`SequencerInboxReader::resolve_blob_payload`](reader::SequencerInboxReader) and
/// then decoded with [`decode_payload_messages`].
pub fn decode_batch_messages(
    batch: &DeliveredBatch,
    before_delayed_count: u64,
    delayed: &dyn DelayedSource,
) -> Result<Vec<DerivedMessage>, L1Error> {
    let payload = match &batch.payload {
        BatchPayload::Calldata(p) => p.as_slice(),
        BatchPayload::None => return Ok(Vec::new()),
        BatchPayload::Blob { .. } => {
            return Err(L1Error::UnsupportedDataLocation(data_location::BLOB_HASHES))
        }
    };
    decode_payload_messages(&batch.event.batch_header(), payload, before_delayed_count, delayed)
}
