//! L2 message parsing: turn a derived message's `l2_msg` into its signed
//! transaction encodings. Mirrors `nitro/arbos/parse_l2.go::parseL2Message`.
//! (Also the foundation of Stage E message→block production.)

use alloy_primitives::{keccak256, B256};

/// L2 message sub-kinds (`nitro/arbos/parse_l2.go`).
pub mod l2_kind {
    pub const UNSIGNED_USER_TX: u8 = 0;
    pub const CONTRACT_TX: u8 = 1;
    pub const NONMUTATING_CALL: u8 = 2;
    pub const BATCH: u8 = 3;
    pub const SIGNED_TX: u8 = 4;
    pub const SIGNED_COMPRESSED_TX: u8 = 7;
}

const MAX_L2_MESSAGE_SIZE: u64 = 262_144;
const MAX_BATCH_DEPTH: usize = 16;

/// Errors from [`parse_l2_message`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum L2ParseError {
    /// Batch nesting exceeded Nitro's depth limit of 16.
    BatchTooDeep,
    /// A SignedCompressedTx failed to decompress.
    Brotli,
}

/// Signed-transaction encodings extracted from an L2 message, plus a count of
/// Arbitrum-constructed (unsigned/contract) sub-messages that don't carry a
/// standard signed-tx hash.
#[derive(Debug, Default, Clone)]
pub struct ParsedL2 {
    /// EIP-2718 transaction encodings (the canonical bytes; `keccak256` = tx hash).
    pub signed_txs: Vec<Vec<u8>>,
    /// Number of UnsignedUserTx / ContractTx sub-messages (Arbitrum-constructed).
    pub unsigned_count: usize,
}

impl ParsedL2 {
    /// Transaction hashes of the signed txs (`keccak256` of the canonical encoding).
    pub fn tx_hashes(&self) -> Vec<B256> {
        self.signed_txs.iter().map(|b| keccak256(b)).collect()
    }
}

/// Parse an L2 message into its signed-transaction encodings.
pub fn parse_l2_message(data: &[u8]) -> Result<ParsedL2, L2ParseError> {
    let mut out = ParsedL2::default();
    parse_into(data, 0, &mut out)?;
    Ok(out)
}

fn parse_into(data: &[u8], depth: usize, out: &mut ParsedL2) -> Result<(), L2ParseError> {
    let Some((&kind, body)) = data.split_first() else { return Ok(()) };
    match kind {
        l2_kind::SIGNED_TX => out.signed_txs.push(body.to_vec()),
        l2_kind::SIGNED_COMPRESSED_TX => {
            let tx = brotli::decompress(body, brotli::Dictionary::Empty)
                .map_err(|_| L2ParseError::Brotli)?;
            out.signed_txs.push(tx);
        }
        l2_kind::BATCH => {
            if depth >= MAX_BATCH_DEPTH {
                return Err(L2ParseError::BatchTooDeep);
            }
            // A batch is a sequence of `[u64 BE length][message]` sub-messages.
            // Nitro stops (gracefully) at the first read error / size-too-large.
            let mut rd = body;
            loop {
                if rd.len() < 8 {
                    break;
                }
                let len = u64::from_be_bytes(rd[..8].try_into().unwrap());
                rd = &rd[8..];
                if len > MAX_L2_MESSAGE_SIZE || len as usize > rd.len() {
                    break;
                }
                let (sub, rest) = rd.split_at(len as usize);
                rd = rest;
                parse_into(sub, depth + 1, out)?;
            }
        }
        // UnsignedUserTx / ContractTx: Arbitrum-constructed; no standard signed-tx hash.
        l2_kind::UNSIGNED_USER_TX | l2_kind::CONTRACT_TX => out.unsigned_count += 1,
        // NonmutatingCall (unimplemented), reserved, heartbeat: ignored.
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn framed(msg: &[u8]) -> Vec<u8> {
        let mut v = (msg.len() as u64).to_be_bytes().to_vec();
        v.extend_from_slice(msg);
        v
    }

    #[test]
    fn signed_tx_is_extracted_verbatim() {
        let mut m = vec![l2_kind::SIGNED_TX];
        m.extend_from_slice(&[0x02, 0xaa, 0xbb]); // pretend 2718 tx bytes
        let p = parse_l2_message(&m).unwrap();
        assert_eq!(p.signed_txs, vec![vec![0x02, 0xaa, 0xbb]]);
    }

    #[test]
    fn batch_unwraps_nested_signed_txs() {
        // Batch { SignedTx(t1), SignedTx(t2) }
        let s1 = [vec![l2_kind::SIGNED_TX], vec![0x01, 0x11]].concat();
        let s2 = [vec![l2_kind::SIGNED_TX], vec![0x02, 0x22, 0x33]].concat();
        let mut batch = vec![l2_kind::BATCH];
        batch.extend_from_slice(&framed(&s1));
        batch.extend_from_slice(&framed(&s2));
        let p = parse_l2_message(&batch).unwrap();
        assert_eq!(p.signed_txs, vec![vec![0x01, 0x11], vec![0x02, 0x22, 0x33]]);
    }

    #[test]
    fn unsigned_is_counted_not_hashed() {
        let p = parse_l2_message(&[l2_kind::UNSIGNED_USER_TX, 0x00]).unwrap();
        assert_eq!(p.signed_txs.len(), 0);
        assert_eq!(p.unsigned_count, 1);
    }
}
