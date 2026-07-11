//! [`DelayedInboxReader`]: reconstruct delayed-inbox messages from L1.
//!
//! Metadata comes from the `Bridge`'s `MessageDelivered` events (the authoritative,
//! per-chain index numbering). The message body comes from an inbox's
//! `InboxMessageDelivered` (inline) or `InboxMessageDeliveredFromOrigin` (body in tx
//! calldata). Other Arbitrum deployments share these event signatures and reuse the
//! index space, so bodies are paired to metadata by `(index, inbox-address)` where
//! the inbox address is the `MessageDelivered.inbox` field; `keccak256(body)` is then
//! checked against the event's `messageDataHash`.

use std::collections::BTreeMap;

use alloy_consensus::Transaction as _;
use alloy_primitives::{Address, B256, U256};
use alloy_provider::Provider;
use alloy_rpc_types_eth::{Filter, Log};
use alloy_sol_types::{SolCall, SolEvent};

use arb_reth_derive::delayed::{DelayedMap, DelayedMessage};

use crate::contracts::{
    from_origin, InboxMessageDelivered, InboxMessageDeliveredFromOrigin, MessageDelivered,
    BRIDGE_MAINNET,
};
use crate::L1Error;

pub use crate::contracts::BRIDGE_MAINNET as BRIDGE_MAINNET_ADDR;

/// Decoded `MessageDelivered` event (metadata) plus the L1 block it was emitted in.
/// The message body is supplied separately via [`DelayedEvent::into_message`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelayedEvent {
    /// Global delayed-message index (`messageIndex`).
    pub index: u64,
    /// Accumulator before this message (`beforeInboxAcc`).
    pub before_inbox_acc: B256,
    /// The inbox that delivered the body (`MessageDelivered.inbox`); the body's
    /// `InboxMessageDelivered` is emitted by this address.
    pub inbox: Address,
    pub kind: u8,
    pub sender: Address,
    /// `keccak256(body)` as recorded on chain.
    pub message_data_hash: B256,
    pub base_fee_l1: U256,
    pub timestamp: u64,
    /// L1 block the event was emitted in (the message header's block number).
    pub l1_block: u64,
}

impl DelayedEvent {
    /// Pair this metadata with its body, verifying `keccak256(body)` against the
    /// event's `messageDataHash`.
    pub fn into_message(self, body: Vec<u8>) -> Result<DelayedMessage, L1Error> {
        let msg = DelayedMessage {
            kind: self.kind,
            sender: self.sender,
            block_number: self.l1_block,
            timestamp: self.timestamp,
            inbox_seq_num: self.index,
            base_fee_l1: self.base_fee_l1,
            data: body,
            before_inbox_acc: self.before_inbox_acc,
        };
        if msg.message_data_hash() != self.message_data_hash {
            return Err(L1Error::Blob(format!("delayed body hash mismatch at index {}", self.index)));
        }
        Ok(msg)
    }
}

/// Reads delayed-inbox messages over a [`Provider`].
#[derive(Debug, Clone)]
pub struct DelayedInboxReader<P> {
    provider: P,
    bridge: Address,
}

impl<P: Provider> DelayedInboxReader<P> {
    /// Reader for an arbitrary `Bridge` address.
    pub fn new(provider: P, bridge: Address) -> Self {
        Self { provider, bridge }
    }

    /// Reader pinned to the Arbitrum One `Bridge`.
    pub fn mainnet(provider: P) -> Self {
        Self::new(provider, BRIDGE_MAINNET)
    }

    /// The configured `Bridge` address.
    pub fn bridge(&self) -> Address {
        self.bridge
    }

    async fn get_logs(&self, filter: Filter) -> Result<Vec<Log>, L1Error> {
        self.provider.get_logs(&filter).await.map_err(|e| L1Error::Rpc(e.to_string()))
    }

    /// Reconstruct every delayed message whose `MessageDelivered` falls in the
    /// inclusive L1 block range, verifying each body against its `messageDataHash`.
    /// Returned in ascending delayed-index order.
    pub async fn fetch_delayed(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<DelayedMessage>, L1Error> {
        // The meta scan and the body scan are independent getLogs; run them concurrently so a
        // delayed-consuming window's tail is one round-trip deep, not three (this scan sits on the
        // sequential consume path, so its latency directly gates sync throughput on a chain with
        // frequent delayed messages).
        let (metas, bodies) = futures_util::future::try_join(
            self.fetch_metas(from_block, to_block),
            self.fetch_bodies(from_block, to_block),
        )
        .await?;

        let mut out = Vec::with_capacity(metas.len());
        for event in metas.into_values() {
            let body = bodies
                .get(&(event.index, event.inbox))
                .ok_or(L1Error::Missing("delayed message body for (index, inbox)"))?
                .clone();
            out.push(event.into_message(body)?);
        }
        Ok(out)
    }

    /// As [`fetch_delayed`](Self::fetch_delayed) but as a [`DelayedMap`] for the
    /// multiplexer.
    pub async fn fetch_delayed_map(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<DelayedMap, L1Error> {
        Ok(DelayedMap::from_messages(self.fetch_delayed(from_block, to_block).await?))
    }

    async fn fetch_metas(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<BTreeMap<u64, DelayedEvent>, L1Error> {
        let filter = Filter::new()
            .address(self.bridge)
            .event_signature(MessageDelivered::SIGNATURE_HASH)
            .from_block(from_block)
            .to_block(to_block);
        let mut metas = BTreeMap::new();
        for log in self.get_logs(filter).await? {
            let l1_block = log.block_number.ok_or(L1Error::Missing("MessageDelivered block_number"))?;
            let event = parse_message_delivered(log.inner.data.topics(), &log.inner.data.data, l1_block)?;
            metas.insert(event.index, event);
        }
        Ok(metas)
    }

    /// Bodies keyed by `(index, emitter)` so cross-chain index collisions cannot mix.
    async fn fetch_bodies(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<BTreeMap<(u64, Address), Vec<u8>>, L1Error> {
        let mut bodies = BTreeMap::new();

        // The inline-body and from-origin-body log scans are independent: run them concurrently.
        let inline = Filter::new()
            .event_signature(InboxMessageDelivered::SIGNATURE_HASH)
            .from_block(from_block)
            .to_block(to_block);
        let from_origin_filter = Filter::new()
            .event_signature(InboxMessageDeliveredFromOrigin::SIGNATURE_HASH)
            .from_block(from_block)
            .to_block(to_block);
        let (inline_logs, from_origin_logs) =
            futures_util::future::try_join(self.get_logs(inline), self.get_logs(from_origin_filter))
                .await?;

        for log in inline_logs {
            let index = topic_u64(&log, "InboxMessageDelivered messageNum")?;
            let data = parse_inbox_message_data(&log.inner.data.data)?;
            bodies.insert((index, log.inner.address), data);
        }

        // Each from-origin body is a separate tx fetch; resolve them concurrently (order-independent,
        // keyed by index+emitter).
        use futures_util::stream::{StreamExt, TryStreamExt};
        // Collect the per-log futures eagerly (not a lazy map in the stream) so the resulting future
        // stays `Send` for the spawned sync task.
        let body_futs: Vec<_> =
            from_origin_logs.iter().map(|log| self.from_origin_body_entry(log)).collect();
        let fetched: Vec<((u64, Address), Vec<u8>)> =
            futures_util::stream::iter(body_futs).buffer_unordered(8).try_collect().await?;
        bodies.extend(fetched);

        Ok(bodies)
    }

    /// `((index, emitter), body)` for one `InboxMessageDeliveredFromOrigin` log — the tx-calldata
    /// body fetch, packaged for concurrent resolution in [`Self::fetch_bodies`].
    async fn from_origin_body_entry(
        &self,
        log: &Log,
    ) -> Result<((u64, Address), Vec<u8>), L1Error> {
        let index = topic_u64(log, "InboxMessageDeliveredFromOrigin messageNum")?;
        let emitter = log.inner.address;
        let data = self.fetch_from_origin_body(log).await?;
        Ok(((index, emitter), data))
    }

    async fn fetch_from_origin_body(&self, log: &Log) -> Result<Vec<u8>, L1Error> {
        let tx_hash = log.transaction_hash.ok_or(L1Error::Missing("log transaction_hash"))?;
        let tx = self
            .provider
            .get_transaction_by_hash(tx_hash)
            .await
            .map_err(|e| L1Error::Rpc(e.to_string()))?
            .ok_or(L1Error::Missing("from-origin transaction"))?;
        let call = from_origin::sendL2MessageFromOriginCall::abi_decode(tx.input().as_ref())?;
        Ok(call.messageData.to_vec())
    }
}

/// Check the on-chain accumulator chains across ascending consecutive messages:
/// `msgs[i].accumulator() == msgs[i+1].before_inbox_acc`. Returns `true` for a run
/// of fewer than two messages.
pub fn verify_accumulator_chain(msgs: &[DelayedMessage]) -> bool {
    msgs.windows(2).all(|w| {
        w[1].inbox_seq_num == w[0].inbox_seq_num + 1 && w[0].accumulator() == w[1].before_inbox_acc
    })
}

fn topic_u64(log: &Log, what: &'static str) -> Result<u64, L1Error> {
    let topics = log.inner.data.topics();
    let t = topics.get(1).ok_or(L1Error::Missing(what))?;
    Ok(u64::from_be_bytes(t.0[24..32].try_into().unwrap()))
}

/// Parse a `MessageDelivered` event from its topics, non-indexed data, and the L1
/// block it was emitted in. Non-indexed data is 6 static words
/// `[inbox, kind, sender, messageDataHash, baseFeeL1, timestamp]`.
pub fn parse_message_delivered(
    topics: &[B256],
    data: &[u8],
    l1_block: u64,
) -> Result<DelayedEvent, L1Error> {
    if topics.len() < 3 {
        return Err(L1Error::Missing("MessageDelivered indexed topics"));
    }
    let index = u64::from_be_bytes(topics[1].0[24..32].try_into().unwrap());
    let before_inbox_acc = topics[2];

    if data.len() != 6 * 32 {
        return Err(L1Error::Batch(arb_reth_derive::batch::BatchError::EventDataWrongLen(
            data.len(),
        )));
    }
    let word = |i: usize| &data[i * 32..(i + 1) * 32];
    Ok(DelayedEvent {
        index,
        before_inbox_acc,
        inbox: Address::from_slice(&word(0)[12..32]),
        kind: word(1)[31],
        sender: Address::from_slice(&word(2)[12..32]),
        message_data_hash: B256::from_slice(word(3)),
        base_fee_l1: U256::from_be_slice(word(4)),
        timestamp: u64::from_be_bytes(word(5)[24..32].try_into().unwrap()),
        l1_block,
    })
}

/// Decode the inline `bytes data` of an `InboxMessageDelivered` event:
/// `abi.encode(bytes)` = `[offset(32), length(32), body...]`.
pub fn parse_inbox_message_data(data: &[u8]) -> Result<Vec<u8>, L1Error> {
    if data.len() < 64 {
        return Err(L1Error::Missing("InboxMessageDelivered data head"));
    }
    let len: usize = U256::from_be_slice(&data[32..64])
        .try_into()
        .map_err(|_| L1Error::Missing("InboxMessageDelivered length overflow"))?;
    let body = data.get(64..64 + len).ok_or(L1Error::Missing("InboxMessageDelivered body"))?;
    Ok(body.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::keccak256;

    fn word_u64(v: u64) -> [u8; 32] {
        let mut w = [0u8; 32];
        w[24..32].copy_from_slice(&v.to_be_bytes());
        w
    }

    fn word_addr(a: Address) -> [u8; 32] {
        let mut w = [0u8; 32];
        w[12..32].copy_from_slice(a.as_slice());
        w
    }

    /// Encode a `MessageDelivered` event (topics + non-indexed data) for `m` given
    /// the delivering inbox.
    fn encode_message_delivered(m: &DelayedMessage, inbox: Address) -> (Vec<B256>, Vec<u8>) {
        let topics = vec![
            B256::ZERO, // sig (ignored by the parser)
            B256::from(word_u64(m.inbox_seq_num)),
            m.before_inbox_acc,
        ];
        let mut data = Vec::new();
        data.extend_from_slice(&word_addr(inbox));
        data.extend_from_slice(&word_u64(m.kind as u64));
        data.extend_from_slice(&word_addr(m.sender));
        data.extend_from_slice(m.message_data_hash().as_slice());
        data.extend_from_slice(&m.base_fee_l1.to_be_bytes::<32>());
        data.extend_from_slice(&word_u64(m.timestamp));
        (topics, data)
    }

    /// Encode an `InboxMessageDelivered` non-indexed `bytes data` payload.
    fn encode_inbox_data(body: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&word_u64(0x20));
        out.extend_from_slice(&word_u64(body.len() as u64));
        out.extend_from_slice(body);
        out.resize(out.len().div_ceil(32) * 32, 0);
        out
    }

    fn sample(idx: u64, before: B256, body: Vec<u8>) -> DelayedMessage {
        DelayedMessage {
            kind: 12,
            sender: Address::repeat_byte(0xab),
            block_number: 25_000_000,
            timestamp: 1_700_000_000,
            inbox_seq_num: idx,
            base_fee_l1: U256::from(7_000_000_000u64),
            data: body,
            before_inbox_acc: before,
        }
    }

    #[test]
    fn message_delivered_round_trips() {
        let inbox = Address::repeat_byte(0x4d);
        let m = sample(2_486_842, B256::repeat_byte(0x11), b"delayed-body".to_vec());
        let (topics, data) = encode_message_delivered(&m, inbox);

        let event = parse_message_delivered(&topics, &data, m.block_number).unwrap();
        assert_eq!(event.index, m.inbox_seq_num);
        assert_eq!(event.inbox, inbox);
        assert_eq!(event.kind, m.kind);
        assert_eq!(event.sender, m.sender);
        assert_eq!(event.message_data_hash, keccak256(&m.data));
        assert_eq!(event.base_fee_l1, m.base_fee_l1);
        assert_eq!(event.timestamp, m.timestamp);

        let rebuilt = event.into_message(m.data.clone()).unwrap();
        assert_eq!(rebuilt, m);
    }

    #[test]
    fn into_message_rejects_wrong_body() {
        let m = sample(1, B256::ZERO, b"correct".to_vec());
        let (topics, data) = encode_message_delivered(&m, Address::ZERO);
        let event = parse_message_delivered(&topics, &data, m.block_number).unwrap();
        assert!(matches!(event.into_message(b"tampered".to_vec()), Err(L1Error::Blob(_))));
    }

    #[test]
    fn inbox_data_round_trips() {
        let body = b"\x03\x04\x05 arbitrary delayed payload bytes".to_vec();
        assert_eq!(parse_inbox_message_data(&encode_inbox_data(&body)).unwrap(), body);
    }

    #[test]
    fn accumulator_chain_links_and_detects_break() {
        let m0 = sample(10, B256::ZERO, b"first".to_vec());
        let m1 = sample(11, m0.accumulator(), b"second".to_vec());
        assert!(verify_accumulator_chain(&[m0.clone(), m1.clone()]));

        let mut broken = m1;
        broken.before_inbox_acc = B256::repeat_byte(0xff);
        assert!(!verify_accumulator_chain(&[m0, broken]));
    }
}
