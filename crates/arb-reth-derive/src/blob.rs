//! EIP-4844 blob decoding for Arbitrum sequencer batches.
//!
//! Faithful port of `nitro/util/blobs/blobs.go::DecodeBlobs`. Each 32-byte field
//! element carries 31 payload bytes in bytes `1..32`, plus 6 spare low bits in
//! byte `0` (the BLS modulus is ~254 bits, so only 254 of each 256 bits are
//! usable). The reassembled byte stream is `RLP(payload)`.
//!
//! IMPORTANT (verified against the Go source): the two extraction passes are
//! **interleaved per blob** — for each blob we first emit its 31-byte bodies, then
//! its spare-bit bytes, then move to the next blob. Doing all bodies first and all
//! spares second would corrupt any multi-blob batch.

use alloy_rlp::Header;

/// Field elements per EIP-4844 blob.
pub const FIELD_ELEMENTS_PER_BLOB: usize = 4096;
const BYTES_PER_FIELD_ELEMENT: usize = 32;
/// Raw byte size of one EIP-4844 blob (4096 × 32).
pub const BYTES_PER_BLOB: usize = FIELD_ELEMENTS_PER_BLOB * BYTES_PER_FIELD_ELEMENT;
/// Spare low bits packed into byte 0 of each field element.
const SPARE_BLOB_BITS: u32 = 6;
/// Usable payload bytes per blob = 254 * 4096 / 8.
pub const BLOB_ENCODABLE_DATA: usize = 254 * FIELD_ELEMENTS_PER_BLOB / 8;
/// Bytes held in the 31-byte bodies of one blob (31 × 4096). Used by the
/// round-trip test encoder.
#[allow(dead_code)]
const BODY_BYTES_PER_BLOB: usize = (BYTES_PER_FIELD_ELEMENT - 1) * FIELD_ELEMENTS_PER_BLOB;

/// One EIP-4844 blob (raw, as fetched from the beacon chain).
pub type Blob = [u8; BYTES_PER_BLOB];

/// Errors from [`decode_blobs`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobDecodeError {
    /// The spare-bit accumulator did not drain to zero — corrupt blob.
    SpareBits(u32),
    /// The reassembled stream was not a valid RLP byte string.
    Rlp(&'static str),
}

/// Decode Arbitrum-encoded EIP-4844 blobs into the raw batch payload bytes.
///
/// Mirrors `DecodeBlobs`: per blob, append the 31-byte bodies of every field
/// element, then append the bytes reassembled from the 6 spare bits in byte 0 of
/// every field element; finally RLP-decode the concatenation as a single byte
/// string (trailing zero padding is ignored).
pub fn decode_blobs(blobs: &[Blob]) -> Result<Vec<u8>, BlobDecodeError> {
    let mut rlp_data: Vec<u8> = Vec::with_capacity(blobs.len() * BLOB_ENCODABLE_DATA);
    for blob in blobs {
        // Pass 1: 31 body bytes (1..32) of every field element.
        for fe in 0..FIELD_ELEMENTS_PER_BLOB {
            let base = fe * BYTES_PER_FIELD_ELEMENT;
            rlp_data.extend_from_slice(&blob[base + 1..base + BYTES_PER_FIELD_ELEMENT]);
        }
        // Pass 2: reassemble bytes from the 6 spare bits in byte 0 of every field element.
        let mut acc: u16 = 0;
        let mut acc_bits: u32 = 0;
        for fe in 0..FIELD_ELEMENTS_PER_BLOB {
            acc |= (blob[fe * BYTES_PER_FIELD_ELEMENT] as u16) << acc_bits;
            acc_bits += SPARE_BLOB_BITS;
            if acc_bits >= 8 {
                rlp_data.push(acc as u8);
                acc >>= 8;
                acc_bits -= 8;
            }
        }
        if acc_bits != 0 {
            return Err(BlobDecodeError::SpareBits(acc_bits));
        }
    }

    // `rlp_data` is `RLP(byte string)`; take the string payload, ignore trailing padding.
    let mut slice = rlp_data.as_slice();
    let header = Header::decode(&mut slice).map_err(|_| BlobDecodeError::Rlp("bad header"))?;
    if header.list {
        return Err(BlobDecodeError::Rlp("expected string, got list"));
    }
    if header.payload_length > slice.len() {
        return Err(BlobDecodeError::Rlp("payload length exceeds data"));
    }
    Ok(slice[..header.payload_length].to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Inverse of [`decode_blobs`], for round-trip testing only. Mirrors Nitro's
    /// `EncodeBlobs` (`fillBlobBytes` then `fillBlobBits`): RLP-wrap the payload,
    /// split into `BLOB_ENCODABLE_DATA` chunks, and per chunk place the first
    /// `BODY_BYTES_PER_BLOB` into the field-element bodies and the remainder into
    /// the 6-bit spare slots.
    fn encode_blobs(data: &[u8]) -> Vec<Blob> {
        // RLP-encode `data` as a byte string (only alloy-rlp, no extra deps).
        let mut rlp = Vec::new();
        Header { list: false, payload_length: data.len() }.encode(&mut rlp);
        rlp.extend_from_slice(data);

        let n_blobs = rlp.len().div_ceil(BLOB_ENCODABLE_DATA).max(1);
        rlp.resize(n_blobs * BLOB_ENCODABLE_DATA, 0);

        let mut blobs = Vec::with_capacity(n_blobs);
        for chunk in rlp.chunks_exact(BLOB_ENCODABLE_DATA) {
            let mut blob = [0u8; BYTES_PER_BLOB];
            let (bodies, spares) = chunk.split_at(BODY_BYTES_PER_BLOB);
            // bodies -> bytes 1..32 of each field element
            for fe in 0..FIELD_ELEMENTS_PER_BLOB {
                let base = fe * BYTES_PER_FIELD_ELEMENT;
                blob[base + 1..base + BYTES_PER_FIELD_ELEMENT]
                    .copy_from_slice(&bodies[fe * 31..fe * 31 + 31]);
            }
            // spares -> 6 low bits of byte 0 of each field element (inverse of pass 2)
            let mut acc: u16 = 0;
            let mut acc_bits: u32 = 0;
            let mut si = 0usize;
            for fe in 0..FIELD_ELEMENTS_PER_BLOB {
                if acc_bits < SPARE_BLOB_BITS && si < spares.len() {
                    acc |= (spares[si] as u16) << acc_bits;
                    acc_bits += 8;
                    si += 1;
                }
                blob[fe * BYTES_PER_FIELD_ELEMENT] = (acc & 0x3f) as u8;
                acc_bits = acc_bits.saturating_sub(SPARE_BLOB_BITS);
                acc >>= SPARE_BLOB_BITS;
            }
            blobs.push(blob);
        }
        blobs
    }

    fn pattern(n: usize) -> Vec<u8> {
        // Deterministic non-trivial bytes (no rng in workflow/test env).
        (0..n).map(|i| ((i * 31 + 7) % 251) as u8).collect()
    }

    #[test]
    fn roundtrip_small() {
        let data = b"hello arbitrum".to_vec();
        assert_eq!(decode_blobs(&encode_blobs(&data)).unwrap(), data);
    }

    #[test]
    fn roundtrip_spans_spare_bits() {
        // > BODY_BYTES_PER_BLOB so the tail lands in the 6-bit spare region.
        let data = pattern(BODY_BYTES_PER_BLOB + 2_000);
        let blobs = encode_blobs(&data);
        assert_eq!(blobs.len(), 1);
        assert_eq!(decode_blobs(&blobs).unwrap(), data);
    }

    #[test]
    fn roundtrip_two_blobs() {
        // Forces the per-blob interleave ordering to matter.
        let data = pattern(BLOB_ENCODABLE_DATA + 50_000);
        let blobs = encode_blobs(&data);
        assert_eq!(blobs.len(), 2);
        assert_eq!(decode_blobs(&blobs).unwrap(), data);
    }

    #[test]
    fn empty_byte0_is_valid() {
        // All spare bits zero must still decode (and drain the accumulator).
        let data = pattern(100);
        let mut blobs = encode_blobs(&data);
        for fe in 0..FIELD_ELEMENTS_PER_BLOB {
            blobs[0][fe * BYTES_PER_FIELD_ELEMENT] = 0;
        }
        // Small payload fits entirely in bodies, so zeroing spares is lossless.
        assert_eq!(decode_blobs(&blobs).unwrap(), data);
    }
}
