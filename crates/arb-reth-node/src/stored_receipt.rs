//! Decoder for the receipts a Nitro node keeps in its chain freezer.
//!
//! `rawdb.ReadReceiptsRLP` hands back geth's *storage* form, not the consensus form, and two things
//! are missing from it: the logs bloom, which geth recomputes on read, and the transaction type,
//! which is never stored at all. The type comes from the block's transactions, so a block's receipts
//! can only be decoded alongside its body.
//!
//! Nitro's storage form (`storedReceiptRLP`, `core/types/receipt.go`) is
//!
//! ```text
//! [ status, cumulativeGasUsed, l1GasUsed, logs, contractAddress?, multiGasUsed? ]
//! ```
//!
//! The two trailing fields are optional and hold data reth's receipt model does not carry. They are
//! skipped rather than stored: the target is a receipt identical to the one a forward sync would
//! have written, not a copy of geth's row.

use alloy_consensus::{Eip658Value, Receipt, ReceiptWithBloom};
use alloy_primitives::{Log, logs_bloom};
use alloy_rlp::{Decodable, Header};
use arbitrum_alloy_consensus::receipt::{ArbReceipt, ArbReceiptEnvelope};

/// Nitro puts this single zero byte in the status slot to mark a classic-chain receipt, which uses
/// a wider layout. A Nitro-era status is empty (failure), `0x01` (success), or a 32-byte post-state
/// root, so the marker cannot collide with one.
const ARBITRUM_LEGACY_STATUS: &[u8] = &[0x00];

/// Decode one block's stored receipts, taking each receipt's type from its transaction.
///
/// `tx_types` must be the block's transaction types in order; the counts have to agree, which is
/// also the check that the body and the receipt list belong to the same block.
pub fn decode_stored_receipts(
    rlp: &[u8],
    tx_types: &[u8],
) -> eyre::Result<Vec<ArbReceiptEnvelope<Log>>> {
    let mut buf = rlp;
    let outer = Header::decode(&mut buf)?;
    if !outer.list {
        return Err(eyre::eyre!("stored receipts are not an RLP list"));
    }
    if buf.len() != outer.payload_length {
        return Err(eyre::eyre!(
            "stored receipts claim {} bytes, {} follow",
            outer.payload_length,
            buf.len()
        ));
    }

    let mut receipts = Vec::with_capacity(tx_types.len());
    while !buf.is_empty() {
        let index = receipts.len();
        let tx_type = *tx_types.get(index).ok_or_else(|| {
            eyre::eyre!(
                "block has {} transactions but at least {} receipts",
                tx_types.len(),
                index + 1
            )
        })?;
        receipts.push(decode_one(&mut buf, tx_type, index)?);
    }
    if receipts.len() != tx_types.len() {
        return Err(eyre::eyre!(
            "block has {} transactions but {} receipts",
            tx_types.len(),
            receipts.len()
        ));
    }
    Ok(receipts)
}

fn decode_one(buf: &mut &[u8], tx_type: u8, index: usize) -> eyre::Result<ArbReceiptEnvelope<Log>> {
    let header = Header::decode(buf)?;
    if !header.list {
        return Err(eyre::eyre!("receipt {index} is not an RLP list"));
    }
    if buf.len() < header.payload_length {
        return Err(eyre::eyre!(
            "receipt {index} claims {} bytes, only {} remain",
            header.payload_length,
            buf.len()
        ));
    }
    let (mut body, rest) = buf.split_at(header.payload_length);
    *buf = rest;

    let status = alloy_primitives::Bytes::decode(&mut body)
        .map_err(|error| eyre::eyre!("receipt {index}: status: {error}"))?;
    if status.as_ref() == ARBITRUM_LEGACY_STATUS {
        return Err(eyre::eyre!(
            "receipt {index} is a classic-chain receipt (pre-Nitro layout); converting a chain \
             whose history reaches below its Nitro genesis is not supported"
        ));
    }
    let status = match status.as_ref() {
        [] => Eip658Value::Eip658(false),
        [1] => Eip658Value::Eip658(true),
        other => {
            return Err(eyre::eyre!(
                "receipt {index} has an unsupported status field of {} bytes; Arbitrum is \
                 post-Byzantium, so a post-state root cannot appear here",
                other.len()
            ));
        }
    };

    let cumulative_gas_used = u64::decode(&mut body)
        .map_err(|error| eyre::eyre!("receipt {index}: cumulativeGasUsed: {error}"))?;
    let gas_used_for_l1 = u64::decode(&mut body)
        .map_err(|error| eyre::eyre!("receipt {index}: l1GasUsed: {error}"))?;
    let logs = Vec::<Log>::decode(&mut body)
        .map_err(|error| eyre::eyre!("receipt {index}: logs: {error}"))?;

    // Anything left is contractAddress and/or multiGasUsed. Both are node-local.

    let logs_bloom = logs_bloom(logs.iter());
    let receipt = ArbReceipt {
        inner: Receipt {
            status,
            cumulative_gas_used,
            logs,
        },
        gas_used_for_l1,
    };
    Ok(arb_reth_evm::block::receipt_envelope_for_type(
        tx_type,
        ReceiptWithBloom {
            receipt,
            logs_bloom,
        },
    ))
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, B256, Bytes, LogData, address, b256};
    use alloy_rlp::Encodable;

    use super::*;

    /// Mirrors Nitro's `ReceiptForStorage.EncodeRLP` for the Nitro-era layout.
    fn stored(
        success: bool,
        cumulative_gas_used: u64,
        l1_gas: u64,
        logs: &[Log],
        contract_address: Option<Address>,
    ) -> Vec<u8> {
        let mut fields = Vec::new();
        if success {
            Bytes::from_static(&[1]).encode(&mut fields);
        } else {
            Bytes::new().encode(&mut fields);
        }
        cumulative_gas_used.encode(&mut fields);
        l1_gas.encode(&mut fields);
        alloy_rlp::encode_list(logs, &mut fields);
        if let Some(addr) = contract_address {
            addr.encode(&mut fields);
        }
        let mut out = Vec::new();
        Header {
            list: true,
            payload_length: fields.len(),
        }
        .encode(&mut out);
        out.extend_from_slice(&fields);
        out
    }

    fn list(items: &[Vec<u8>]) -> Vec<u8> {
        let payload: Vec<u8> = items.concat();
        let mut out = Vec::new();
        Header {
            list: true,
            payload_length: payload.len(),
        }
        .encode(&mut out);
        out.extend_from_slice(&payload);
        out
    }

    fn log() -> Log {
        Log {
            address: address!("00000000000000000000000000000000000000aa"),
            data: LogData::new_unchecked(
                vec![b256!(
                    "1111111111111111111111111111111111111111111111111111111111111111"
                )],
                Bytes::from_static(&[0xde, 0xad]),
            ),
        }
    }

    #[test]
    fn decodes_a_block_of_receipts_and_types_them_from_the_transactions() {
        let blob = list(&[
            stored(true, 21_000, 0, &[], None),
            stored(false, 50_000, 1234, &[log()], None),
        ]);
        let receipts = decode_stored_receipts(&blob, &[0x02, 0x69]).unwrap();

        assert!(matches!(receipts[0], ArbReceiptEnvelope::Eip1559(_)));
        assert!(matches!(
            receipts[1],
            ArbReceiptEnvelope::SubmitRetryable(_)
        ));

        let second = match &receipts[1] {
            ArbReceiptEnvelope::SubmitRetryable(r) => r,
            other => panic!("unexpected {other:?}"),
        };
        assert_eq!(second.receipt.inner.status, Eip658Value::Eip658(false));
        assert_eq!(second.receipt.inner.cumulative_gas_used, 50_000);
        assert_eq!(second.receipt.gas_used_for_l1, 1234);
        assert_eq!(second.receipt.inner.logs, vec![log()]);
        // Recomputed on decode, not stored.
        assert_eq!(second.logs_bloom, logs_bloom([&log()]));
    }

    /// The optional trailing fields carry node-local data. Their presence must not shift the
    /// decode of the next receipt in the list.
    #[test]
    fn skips_the_optional_trailing_fields() {
        let with_address = list(&[
            stored(
                true,
                7,
                0,
                &[],
                Some(address!("00000000000000000000000000000000000000bb")),
            ),
            stored(true, 9, 0, &[], None),
        ]);
        let receipts = decode_stored_receipts(&with_address, &[0x64, 0x64]).unwrap();
        assert_eq!(receipts.len(), 2);
        let second = match &receipts[1] {
            ArbReceiptEnvelope::Deposit(r) => r,
            other => panic!("unexpected {other:?}"),
        };
        assert_eq!(second.receipt.inner.cumulative_gas_used, 9);
    }

    #[test]
    fn rejects_a_receipt_count_that_disagrees_with_the_body() {
        let blob = list(&[stored(true, 1, 0, &[], None), stored(true, 2, 0, &[], None)]);
        let err = decode_stored_receipts(&blob, &[0x00])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("1 transactions but at least 2 receipts"),
            "{err}"
        );

        let err = decode_stored_receipts(&blob, &[0x00, 0x00, 0x00])
            .unwrap_err()
            .to_string();
        assert!(err.contains("3 transactions but 2 receipts"), "{err}");
    }

    /// The classic layout puts a zero byte where the status goes. Decoding it as a Nitro receipt
    /// would silently read `cumulativeGasUsed` out of the wrong field, so it has to be refused.
    #[test]
    fn rejects_a_classic_chain_receipt() {
        let mut fields = Vec::new();
        Bytes::from_static(&[0]).encode(&mut fields);
        for value in [100u64, 50, 10, 1] {
            value.encode(&mut fields);
        }
        Address::ZERO.encode(&mut fields);
        Vec::<Log>::new().encode(&mut fields);
        let mut receipt = Vec::new();
        Header {
            list: true,
            payload_length: fields.len(),
        }
        .encode(&mut receipt);
        receipt.extend_from_slice(&fields);

        let err = decode_stored_receipts(&list(&[receipt]), &[0x00])
            .unwrap_err()
            .to_string();
        assert!(err.contains("classic-chain receipt"), "{err}");
    }

    #[test]
    fn rejects_trailing_bytes_after_the_list() {
        let mut blob = list(&[stored(true, 1, 0, &[], None)]);
        blob.push(0xff);
        let err = decode_stored_receipts(&blob, &[0x00])
            .unwrap_err()
            .to_string();
        assert!(err.contains("bytes"), "{err}");
    }

    #[test]
    fn empty_receipt_list_is_valid() {
        assert!(decode_stored_receipts(&list(&[]), &[]).unwrap().is_empty());
    }

    /// A post-state root in the status slot means a pre-Byzantium chain, which Arbitrum is not.
    /// Accepting it would produce a receipt whose consensus encoding differs from the header's
    /// commitment.
    #[test]
    fn rejects_a_post_state_root_status() {
        let mut fields = Vec::new();
        B256::repeat_byte(0x33).encode(&mut fields);
        21_000u64.encode(&mut fields);
        0u64.encode(&mut fields);
        Vec::<Log>::new().encode(&mut fields);
        let mut receipt = Vec::new();
        Header {
            list: true,
            payload_length: fields.len(),
        }
        .encode(&mut receipt);
        receipt.extend_from_slice(&fields);

        let err = decode_stored_receipts(&list(&[receipt]), &[0x00])
            .unwrap_err()
            .to_string();
        assert!(err.contains("post-state root"), "{err}");
    }
}
