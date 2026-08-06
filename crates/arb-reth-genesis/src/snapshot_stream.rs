//! Reader for the `reth-export --mode full-snapshot` stream.
//!
//! The stream holds a manifest, then blocks, then state history, then the state trie, all ending at
//! the same block: the one whose state is in the persisted trie. See
//! `arb-kb/decisions/ADR-004-snapshot-conversion-invariants.md`.
//!
//! Records arrive in one pass and are yielded one at a time, because the stream is far larger than
//! memory. Structural invariants that need no database are enforced here as records go past, so a
//! malformed stream fails before anything is written.

use std::io::Read;

use alloy_primitives::{B256, U256, keccak256};
use eyre::{Context as _, eyre};

// Bumped when the manifest gained the resume point: an older stream would parse its first section
// tag as the resume-present flag and desynchronise silently rather than fail.
const MAGIC: &[u8; 8] = b"ARBSNAP2";

// Section tags live above the record tags on purpose. They shared a range in the first draft, which
// made a receipts record inside the blocks section indistinguishable from the start of the history
// section, since both were 0x03.
const SECTION_MANIFEST: u8 = 0xf1;
const SECTION_BLOCKS: u8 = 0xf2;
const SECTION_HISTORY: u8 = 0xf3;
const SECTION_STATE: u8 = 0xf4;
const SECTION_END: u8 = 0xff;

const REC_END: u8 = 0x00;
const REC_HEADER: u8 = 0x01;
const REC_BODY: u8 = 0x02;
const REC_RECEIPTS: u8 = 0x03;
const REC_ACCOUNT: u8 = 0x04;
const REC_STORAGE: u8 = 0x05;
const REC_CODE: u8 = 0x06;

/// History objects identify storage slots by raw key from this version on.
const HISTORY_VERSION_RAW_SLOTS: u8 = 1;

/// Where L1 derivation should restart, so a converted datadir does not re-derive from batch 0.
///
/// Mirrors arb-reth's `L1ResumeCheckpoint`: after consuming every batch up to and including L1 block
/// `l1_block - 1`, the delayed cursor is `delayed_count` and the chain has reached `l2_block`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResumePoint {
    pub l1_block: u64,
    pub delayed_count: u64,
    pub l2_block: u64,
}

/// Where the conversion stops: state, blocks and history all end here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    /// `P`, the block of the persisted state trie.
    pub block: u64,
    /// `R_P`, the state root at `P`.
    pub root: B256,
    /// The pathdb persistent state id, which is `P`'s history object.
    pub state_id: u64,
    /// Canonical hash of block `P`.
    pub hash: B256,
    /// Present when the exporter was given the snapshot's `arbitrumdata`. Absent means the importer
    /// cannot write a derivation cursor and the node will re-derive from batch 0.
    pub resume: Option<ResumePoint>,
}

/// One account's pre-block state, and the slots the block overwrote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryAccount {
    pub address: alloy_primitives::Address,
    /// `None` when the account did not exist before the block.
    pub previous: Option<(u64, U256, Option<B256>)>,
    /// Raw slot key and its pre-block value.
    pub storage: Vec<(B256, U256)>,
}

/// One state history object: the state before `block`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryObject {
    pub state_id: u64,
    pub block: u64,
    pub parent_root: B256,
    pub post_root: B256,
    pub accounts: Vec<HistoryAccount>,
}

/// A record from the stream, in the order the exporter wrote it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record {
    Header {
        block: u64,
        hash: B256,
        rlp: Vec<u8>,
    },
    Body {
        block: u64,
        rlp: Vec<u8>,
    },
    Receipts {
        block: u64,
        rlp: Vec<u8>,
    },
    History(HistoryObject),
    /// An account at the exported state root. The address is hashed: a pruned snapshot carries no
    /// preimage for it.
    Account {
        hashed_address: B256,
        nonce: u64,
        balance: U256,
        code_hash: Option<B256>,
    },
    /// A storage slot belonging to the most recent [`Record::Account`]. The key is hashed.
    Storage {
        hashed_slot: B256,
        value: U256,
    },
    Code {
        hash: B256,
        code: Vec<u8>,
    },
}

/// Streaming reader over a full-snapshot export.
#[derive(Debug)]
pub struct SnapshotStream<R> {
    inner: R,
    manifest: Manifest,
    section: u8,
    /// Last block seen in the blocks section, for contiguity and parent linkage.
    last_block: Option<(u64, B256)>,
    /// Last history object seen, for root chaining.
    last_history: Option<(u64, B256)>,
    /// Set by [`SnapshotStream::unread`]; returned before anything is read from `inner`.
    held: Option<Record>,
    finished: bool,
}

impl<R: Read> SnapshotStream<R> {
    /// Read the magic and manifest, leaving the reader positioned at the first section.
    pub fn open(mut inner: R) -> eyre::Result<Self> {
        let mut magic = [0u8; 8];
        inner.read_exact(&mut magic).wrap_err("read stream magic")?;
        if &magic != MAGIC {
            return Err(eyre!(
                "not a full-snapshot stream: magic {:?}",
                String::from_utf8_lossy(&magic)
            ));
        }
        let tag = read_u8(&mut inner)?;
        if tag != SECTION_MANIFEST {
            return Err(eyre!("expected the manifest section, found tag {tag:#04x}"));
        }
        let mut manifest = Manifest {
            block: read_uvarint(&mut inner)?,
            root: read_b256(&mut inner)?,
            state_id: read_uvarint(&mut inner)?,
            hash: read_b256(&mut inner)?,
            resume: None,
        };
        if read_u8(&mut inner)? == 1 {
            manifest.resume = Some(ResumePoint {
                l1_block: read_uvarint(&mut inner)?,
                delayed_count: read_uvarint(&mut inner)?,
                l2_block: read_uvarint(&mut inner)?,
            });
        }
        Ok(Self {
            inner,
            manifest,
            section: SECTION_MANIFEST,
            last_block: None,
            last_history: None,
            held: None,
            finished: false,
        })
    }

    /// The convert point this stream was produced at.
    pub const fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Hand a record back, so the next [`Self::next_record`] returns it again.
    ///
    /// Sections are only delimited by the record that follows them, so a per-section reader has to
    /// read one record too far to learn it is done. This gives that record to the next reader.
    pub fn unread(&mut self, record: Record) {
        debug_assert!(self.held.is_none(), "only one record can be held back");
        self.held = Some(record);
    }

    /// Next record, or `None` at the end of the stream.
    pub fn next_record(&mut self) -> eyre::Result<Option<Record>> {
        if let Some(record) = self.held.take() {
            return Ok(Some(record));
        }
        if self.finished {
            return Ok(None);
        }
        loop {
            let tag = read_u8(&mut self.inner)?;
            match (self.section, tag) {
                // Section transitions. Order is fixed so the importer can write append-only.
                (SECTION_MANIFEST, SECTION_BLOCKS)
                | (SECTION_BLOCKS, SECTION_HISTORY)
                | (SECTION_HISTORY, SECTION_STATE) => {
                    self.section = tag;
                }
                (SECTION_STATE, SECTION_END) => {
                    self.finished = true;
                    return Ok(None);
                }
                (SECTION_MANIFEST, _) => {
                    return Err(eyre!("expected the blocks section, found tag {tag:#04x}"));
                }
                (_, REC_END) => {
                    // End of the current section; the next byte names the next one.
                    continue;
                }
                (SECTION_BLOCKS, REC_HEADER) => {
                    let block = read_uvarint(&mut self.inner)?;
                    let hash = read_b256(&mut self.inner)?;
                    let rlp = read_blob(&mut self.inner)?;
                    self.check_header(block, hash, &rlp)?;
                    return Ok(Some(Record::Header { block, hash, rlp }));
                }
                (SECTION_BLOCKS, REC_BODY) => {
                    let block = read_uvarint(&mut self.inner)?;
                    let rlp = read_blob(&mut self.inner)?;
                    return Ok(Some(Record::Body { block, rlp }));
                }
                (SECTION_BLOCKS, REC_RECEIPTS) => {
                    let block = read_uvarint(&mut self.inner)?;
                    let rlp = read_blob(&mut self.inner)?;
                    return Ok(Some(Record::Receipts { block, rlp }));
                }
                (SECTION_HISTORY, REC_HEADER) => {
                    let object = self.read_history()?;
                    return Ok(Some(Record::History(object)));
                }
                (SECTION_STATE, REC_ACCOUNT) => {
                    let hashed_address = read_b256(&mut self.inner)?;
                    let nonce = read_uvarint(&mut self.inner)?;
                    let balance = read_short_u256(&mut self.inner)?;
                    let code_hash = read_short_bytes(&mut self.inner)?;
                    return Ok(Some(Record::Account {
                        hashed_address,
                        nonce,
                        balance,
                        code_hash: (!code_hash.is_empty()).then(|| B256::from_slice(&code_hash)),
                    }));
                }
                (SECTION_STATE, REC_STORAGE) => {
                    let hashed_slot = read_b256(&mut self.inner)?;
                    let value = read_short_u256(&mut self.inner)?;
                    return Ok(Some(Record::Storage { hashed_slot, value }));
                }
                (SECTION_STATE, REC_CODE) => {
                    let hash = read_b256(&mut self.inner)?;
                    let code = read_blob(&mut self.inner)?;
                    if keccak256(&code) != hash {
                        return Err(eyre!("code blob does not hash to {hash:#x}"));
                    }
                    return Ok(Some(Record::Code { hash, code }));
                }
                (section, tag) => {
                    return Err(eyre!(
                        "record tag {tag:#04x} is not valid in section {section:#04x}"
                    ));
                }
            }
        }
    }

    /// ADR-004 B1, B2 (header hash) and B3.
    fn check_header(&mut self, block: u64, hash: B256, rlp: &[u8]) -> eyre::Result<()> {
        if keccak256(rlp) != hash {
            return Err(eyre!("block {block}: header does not hash to {hash:#x}"));
        }
        if block > self.manifest.block {
            return Err(eyre!(
                "block {block} is above the convert point {}",
                self.manifest.block
            ));
        }
        let parent =
            header_field(rlp, 0).wrap_err_with(|| format!("block {block}: parent hash"))?;
        if let Some((last_number, last_hash)) = self.last_block {
            if block != last_number + 1 {
                return Err(eyre!("blocks jump from {last_number} to {block}"));
            }
            if parent != last_hash.as_slice() {
                return Err(eyre!(
                    "block {block} does not build on {last_number}: parent is {}",
                    B256::from_slice(parent),
                ));
            }
        }
        self.last_block = Some((block, hash));
        Ok(())
    }

    /// ADR-004 S1, S2, S4 and S5.
    fn read_history(&mut self) -> eyre::Result<HistoryObject> {
        let state_id = read_uvarint(&mut self.inner)?;
        let block = read_uvarint(&mut self.inner)?;
        let version = read_u8(&mut self.inner)?;
        let parent_root = read_b256(&mut self.inner)?;
        let post_root = read_b256(&mut self.inner)?;

        if version != HISTORY_VERSION_RAW_SLOTS {
            return Err(eyre!(
                "history {state_id} has version {version}; only raw slot keys are supported"
            ));
        }
        if block > self.manifest.block {
            return Err(eyre!(
                "history {state_id} names block {block}, above the convert point {}",
                self.manifest.block
            ));
        }
        if let Some((last_block, last_post)) = self.last_history {
            if block <= last_block {
                return Err(eyre!(
                    "history {state_id} block {block} does not advance past {last_block}"
                ));
            }
            // The strong structural check: a gap, a reordering or a truncation all break the chain.
            if parent_root != last_post {
                return Err(eyre!(
                    "history {state_id} at block {block} starts from {parent_root:#x}, but the \
                     previous object ended at {last_post:#x}"
                ));
            }
        }
        self.last_history = Some((block, post_root));

        let count = read_uvarint(&mut self.inner)?;
        let mut accounts = Vec::with_capacity(count.min(4096) as usize);
        for _ in 0..count {
            let mut address = [0u8; 20];
            self.inner.read_exact(&mut address)?;
            let present = read_u8(&mut self.inner)?;
            let previous = if present == 0 {
                None
            } else {
                let nonce = read_uvarint(&mut self.inner)?;
                let balance = read_short_u256(&mut self.inner)?;
                let code_hash = read_short_bytes(&mut self.inner)?;
                Some((
                    nonce,
                    balance,
                    (!code_hash.is_empty()).then(|| B256::from_slice(&code_hash)),
                ))
            };
            let slots = read_uvarint(&mut self.inner)?;
            let mut storage = Vec::with_capacity(slots.min(4096) as usize);
            for _ in 0..slots {
                let key = read_b256(&mut self.inner)?;
                storage.push((key, read_short_u256(&mut self.inner)?));
            }
            accounts.push(HistoryAccount {
                address: alloy_primitives::Address::from(address),
                previous,
                storage,
            });
        }
        Ok(HistoryObject {
            state_id,
            block,
            parent_root,
            post_root,
            accounts,
        })
    }

    /// The last history object must end at the exported state root (ADR-004 S3).
    pub fn check_history_meets_state(&self) -> eyre::Result<()> {
        match self.last_history {
            None => Err(eyre!("stream carried no state history")),
            Some((block, post_root)) => {
                if block != self.manifest.block {
                    return Err(eyre!(
                        "history ends at block {block}, but the convert point is {}",
                        self.manifest.block
                    ));
                }
                if post_root != self.manifest.root {
                    return Err(eyre!(
                        "history ends at {post_root:#x}, but the exported state root is {:#x}",
                        self.manifest.root
                    ));
                }
                Ok(())
            }
        }
    }

    /// The highest block seen in the blocks section, if any.
    pub const fn last_block(&self) -> Option<u64> {
        match self.last_block {
            Some((n, _)) => Some(n),
            None => None,
        }
    }
}

/// Transactions trie root over a block body, for checking against `header.transactionsRoot`.
///
/// The body is `[transactions, uncles, ...]`. A leaf's value is the transaction's encoded form: the
/// full RLP list for a legacy transaction, and the bare `type || payload` for a typed one, which the
/// body carries wrapped in an RLP byte string. So a list item contributes its whole encoding and a
/// string item contributes only its payload.
pub fn transactions_root(body_rlp: &[u8]) -> eyre::Result<B256> {
    let (body, rest) = rlp_split(body_rlp, true)?;
    if !rest.is_empty() {
        return Err(eyre!("trailing bytes after the body"));
    }
    let (txs, _) = rlp_split(body, true).wrap_err("body has no transactions list")?;

    let mut encoded: Vec<&[u8]> = Vec::new();
    let mut cursor = txs;
    while !cursor.is_empty() {
        let is_list = cursor[0] >= 0xc0;
        let consumed_before = cursor.len();
        let (payload, next) = rlp_split(cursor, is_list)?;
        if is_list {
            // Legacy: the leaf is the entire RLP list, header included.
            encoded.push(&cursor[..consumed_before - next.len()]);
        } else {
            // Typed: the leaf is the envelope the string wraps, without the string header.
            encoded.push(payload);
        }
        cursor = next;
    }
    Ok(alloy_trie::root::ordered_trie_root_with_encoder(
        &encoded,
        |item, out| out.extend_from_slice(item),
    ))
}

/// Payload of the `i`th top-level field of an RLP-encoded header.
///
/// Avoids decoding the whole header, which lets this work regardless of any chain-specific trailing
/// fields. Field order is fixed by the Ethereum header encoding: 0 parentHash, 3 stateRoot,
/// 4 transactionsRoot, 5 receiptsRoot.
pub fn header_field(rlp: &[u8], index: usize) -> eyre::Result<&[u8]> {
    let (mut cursor, rest) = rlp_split(rlp, true)?;
    if !rest.is_empty() {
        return Err(eyre!("trailing bytes after the header"));
    }
    for _ in 0..index {
        let (_, next) = rlp_split(cursor, false)?;
        cursor = next;
    }
    Ok(rlp_split(cursor, false)?.0)
}

fn rlp_split(blob: &[u8], want_list: bool) -> eyre::Result<(&[u8], &[u8])> {
    let &prefix = blob.first().ok_or_else(|| eyre!("empty RLP input"))?;
    let (header, len) = match prefix {
        0x00..=0x7f => (0, 1),
        0x80..=0xb7 => (1, (prefix - 0x80) as usize),
        0xb8..=0xbf => {
            let n = (prefix - 0xb7) as usize;
            (
                1 + n,
                be_usize(blob.get(1..1 + n).ok_or_else(|| eyre!("short RLP"))?),
            )
        }
        0xc0..=0xf7 => (1, (prefix - 0xc0) as usize),
        0xf8..=0xff => {
            let n = (prefix - 0xf7) as usize;
            (
                1 + n,
                be_usize(blob.get(1..1 + n).ok_or_else(|| eyre!("short RLP"))?),
            )
        }
    };
    if (prefix >= 0xc0) != want_list {
        return Err(eyre!("RLP item is not the expected kind"));
    }
    let end = header + len;
    let payload = blob
        .get(header..end)
        .ok_or_else(|| eyre!("RLP item claims {len} bytes, only {} available", blob.len()))?;
    Ok((payload, &blob[end..]))
}

fn be_usize(bytes: &[u8]) -> usize {
    bytes.iter().fold(0usize, |n, &b| (n << 8) | b as usize)
}

fn read_u8(r: &mut impl Read) -> eyre::Result<u8> {
    let mut b = [0u8; 1];
    r.read_exact(&mut b)?;
    Ok(b[0])
}

fn read_b256(r: &mut impl Read) -> eyre::Result<B256> {
    let mut b = [0u8; 32];
    r.read_exact(&mut b)?;
    Ok(B256::from(b))
}

fn read_uvarint(r: &mut impl Read) -> eyre::Result<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = read_u8(r)?;
        value |= u64::from(byte & 0x7f)
            .checked_shl(shift)
            .ok_or_else(|| eyre!("varint overflows u64"))?;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            return Err(eyre!("varint overflows u64"));
        }
    }
}

/// A blob whose length is at most 255 bytes: balance, code hash, slot value.
fn read_short_bytes(r: &mut impl Read) -> eyre::Result<Vec<u8>> {
    let len = read_u8(r)? as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

fn read_short_u256(r: &mut impl Read) -> eyre::Result<U256> {
    let bytes = read_short_bytes(r)?;
    if bytes.len() > 32 {
        return Err(eyre!("integer is {} bytes, too wide for U256", bytes.len()));
    }
    Ok(U256::from_be_slice(&bytes))
}

fn read_blob(r: &mut impl Read) -> eyre::Result<Vec<u8>> {
    let len = read_uvarint(r)? as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

/// Builds a full-snapshot stream in memory, mirroring the exporter's framing.
///
/// This exists so tests can produce streams without a second copy of the format. It is not how real
/// snapshots are made: those come from `reth-export --mode full-snapshot`, which reads a Nitro
/// database. Nothing here enforces the invariants the reader checks, on purpose, so a test can build
/// a deliberately broken stream.
#[derive(Debug, Default)]
pub struct StreamBuilder {
    out: Vec<u8>,
}

impl StreamBuilder {
    /// Start a stream with its magic and manifest.
    pub fn new(manifest: &Manifest) -> Self {
        let mut out = MAGIC.to_vec();
        out.push(SECTION_MANIFEST);
        put_uvarint(&mut out, manifest.block);
        out.extend_from_slice(manifest.root.as_slice());
        put_uvarint(&mut out, manifest.state_id);
        out.extend_from_slice(manifest.hash.as_slice());
        match manifest.resume {
            None => out.push(0),
            Some(r) => {
                out.push(1);
                put_uvarint(&mut out, r.l1_block);
                put_uvarint(&mut out, r.delayed_count);
                put_uvarint(&mut out, r.l2_block);
            }
        }
        Self { out }
    }

    /// Open the blocks section.
    pub fn blocks(self) -> Self {
        self.section(SECTION_BLOCKS)
    }

    /// Open the state-history section.
    pub fn history_section(self) -> Self {
        self.section(SECTION_HISTORY)
    }

    /// Open the state section.
    pub fn state(self) -> Self {
        self.section(SECTION_STATE)
    }

    fn section(mut self, tag: u8) -> Self {
        self.out.push(tag);
        self
    }

    /// Close the current section.
    pub fn end_section(mut self) -> Self {
        self.out.push(REC_END);
        self
    }

    /// A block header. Its hash is derived from the bytes, so headers are self-consistent.
    pub fn header(mut self, block: u64, rlp: &[u8]) -> Self {
        self.out.push(REC_HEADER);
        put_uvarint(&mut self.out, block);
        self.out.extend_from_slice(keccak256(rlp).as_slice());
        put_uvarint(&mut self.out, rlp.len() as u64);
        self.out.extend_from_slice(rlp);
        self
    }

    /// A block body, as geth's `ReadBodyRLP` returns it.
    pub fn body(mut self, block: u64, rlp: &[u8]) -> Self {
        self.out.push(REC_BODY);
        put_uvarint(&mut self.out, block);
        put_uvarint(&mut self.out, rlp.len() as u64);
        self.out.extend_from_slice(rlp);
        self
    }

    /// A block's receipts, as geth's `ReadReceiptsRLP` returns them: the storage form.
    pub fn receipts(mut self, block: u64, rlp: &[u8]) -> Self {
        self.out.push(REC_RECEIPTS);
        put_uvarint(&mut self.out, block);
        put_uvarint(&mut self.out, rlp.len() as u64);
        self.out.extend_from_slice(rlp);
        self
    }

    /// One state-history object.
    pub fn history(mut self, o: &HistoryObject) -> Self {
        self.out.push(REC_HEADER);
        put_uvarint(&mut self.out, o.state_id);
        put_uvarint(&mut self.out, o.block);
        self.out.push(HISTORY_VERSION_RAW_SLOTS);
        self.out.extend_from_slice(o.parent_root.as_slice());
        self.out.extend_from_slice(o.post_root.as_slice());
        put_uvarint(&mut self.out, o.accounts.len() as u64);
        for a in &o.accounts {
            self.out.extend_from_slice(a.address.as_slice());
            match &a.previous {
                None => self.out.push(0),
                Some((nonce, balance, code)) => {
                    self.out.push(1);
                    put_uvarint(&mut self.out, *nonce);
                    put_short(&mut self.out, &trim(balance.to_be_bytes::<32>()));
                    put_short(
                        &mut self.out,
                        code.map(|c| c.to_vec()).unwrap_or_default().as_slice(),
                    );
                }
            }
            put_uvarint(&mut self.out, a.storage.len() as u64);
            for (k, v) in &a.storage {
                self.out.extend_from_slice(k.as_slice());
                put_short(&mut self.out, &trim(v.to_be_bytes::<32>()));
            }
        }
        self
    }

    /// An account at the exported state root, keyed by the hash of its address.
    pub fn account(
        mut self,
        hashed_address: B256,
        nonce: u64,
        balance: U256,
        code_hash: Option<B256>,
    ) -> Self {
        self.out.push(REC_ACCOUNT);
        self.out.extend_from_slice(hashed_address.as_slice());
        put_uvarint(&mut self.out, nonce);
        put_short(&mut self.out, &trim(balance.to_be_bytes::<32>()));
        put_short(
            &mut self.out,
            code_hash.map(|c| c.to_vec()).unwrap_or_default().as_slice(),
        );
        self
    }

    /// A storage slot belonging to the preceding account.
    pub fn storage(mut self, hashed_slot: B256, value: U256) -> Self {
        self.out.push(REC_STORAGE);
        self.out.extend_from_slice(hashed_slot.as_slice());
        put_short(&mut self.out, &trim(value.to_be_bytes::<32>()));
        self
    }

    /// Contract code, keyed by its hash.
    pub fn code(mut self, code: &[u8]) -> Self {
        self.out.push(REC_CODE);
        self.out.extend_from_slice(keccak256(code).as_slice());
        put_uvarint(&mut self.out, code.len() as u64);
        self.out.extend_from_slice(code);
        self
    }

    /// Close the stream and return its bytes.
    pub fn finish(mut self) -> Vec<u8> {
        self.out.push(SECTION_END);
        self.out
    }
}

fn trim(bytes: [u8; 32]) -> Vec<u8> {
    let first = bytes.iter().position(|b| *b != 0).unwrap_or(32);
    bytes[first..].to_vec()
}

fn put_uvarint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn put_short(out: &mut Vec<u8>, b: &[u8]) {
    out.push(b.len() as u8);
    out.extend_from_slice(b);
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, address};

    use super::*;

    /// A header RLP whose first field is `parent` and whose remaining fields are placeholders, so
    /// `header_field` and the linkage check have something real to walk.
    fn header_rlp(parent: B256) -> Vec<u8> {
        let mut fields = Vec::new();
        fields.push(0xa0);
        fields.extend_from_slice(parent.as_slice());
        for _ in 0..5 {
            fields.push(0xa0);
            fields.extend_from_slice(&[0x11u8; 32]);
        }
        let mut out = vec![0xf8, fields.len() as u8];
        out.extend_from_slice(&fields);
        out
    }

    fn manifest() -> Manifest {
        Manifest {
            block: 10,
            root: B256::repeat_byte(0xee),
            state_id: 12,
            hash: B256::repeat_byte(0xaa),
            resume: None,
        }
    }

    #[test]
    fn reads_manifest_blocks_and_history() {
        let m = manifest();
        let h0 = header_rlp(B256::ZERO);
        let h1 = header_rlp(keccak256(&h0));
        let obj = HistoryObject {
            state_id: 12,
            block: 10,
            parent_root: B256::repeat_byte(0xcc),
            post_root: m.root,
            accounts: vec![
                HistoryAccount {
                    address: address!("00000000000000000000000000000000000000aa"),
                    previous: Some((7, U256::from(1234u64), Some(B256::repeat_byte(0x9)))),
                    storage: vec![
                        (B256::repeat_byte(1), U256::from(5u64)),
                        (B256::repeat_byte(2), U256::ZERO),
                    ],
                },
                HistoryAccount {
                    address: Address::ZERO,
                    previous: None,
                    storage: vec![],
                },
            ],
        };
        let bytes = StreamBuilder::new(&m)
            .blocks()
            .header(0, &h0)
            .header(1, &h1)
            .end_section()
            .history_section()
            .history(&obj)
            .end_section()
            .state()
            .end_section()
            .finish();

        let mut s = SnapshotStream::open(bytes.as_slice()).unwrap();
        assert_eq!(s.manifest(), &m);

        let mut headers = 0;
        let mut got = None;
        while let Some(rec) = s.next_record().unwrap() {
            match rec {
                Record::Header { .. } => headers += 1,
                Record::History(o) => got = Some(o),
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(headers, 2);
        assert_eq!(got.as_ref(), Some(&obj));
        assert_eq!(s.last_block(), Some(1));
        s.check_history_meets_state().unwrap();
    }

    #[test]
    fn rejects_a_gap_in_blocks() {
        let m = manifest();
        let h0 = header_rlp(B256::ZERO);
        let h2 = header_rlp(keccak256(&h0));
        let bytes = StreamBuilder::new(&m)
            .blocks()
            .header(0, &h0)
            .header(2, &h2)
            .end_section()
            .finish();

        let mut s = SnapshotStream::open(bytes.as_slice()).unwrap();
        s.next_record().unwrap();
        let err = s.next_record().unwrap_err().to_string();
        assert!(err.contains("blocks jump from 0 to 2"), "{err}");
    }

    #[test]
    fn rejects_a_header_that_does_not_build_on_its_predecessor() {
        let m = manifest();
        let h0 = header_rlp(B256::ZERO);
        let h1 = header_rlp(B256::repeat_byte(0x77)); // wrong parent
        let bytes = StreamBuilder::new(&m)
            .blocks()
            .header(0, &h0)
            .header(1, &h1)
            .end_section()
            .finish();

        let mut s = SnapshotStream::open(bytes.as_slice()).unwrap();
        s.next_record().unwrap();
        let err = s.next_record().unwrap_err().to_string();
        assert!(err.contains("does not build on 0"), "{err}");
    }

    /// The check that catches a gap, a reordering, or a truncated history section.
    #[test]
    fn rejects_a_break_in_the_history_root_chain() {
        let m = manifest();
        let first = HistoryObject {
            state_id: 1,
            block: 1,
            parent_root: B256::repeat_byte(1),
            post_root: B256::repeat_byte(2),
            accounts: vec![],
        };
        let second = HistoryObject {
            state_id: 2,
            block: 2,
            parent_root: B256::repeat_byte(0x99), // should be first.post_root
            post_root: m.root,
            accounts: vec![],
        };
        let bytes = StreamBuilder::new(&m)
            .blocks()
            .end_section()
            .history_section()
            .history(&first)
            .history(&second)
            .end_section()
            .finish();

        let mut s = SnapshotStream::open(bytes.as_slice()).unwrap();
        s.next_record().unwrap();
        let err = s.next_record().unwrap_err().to_string();
        assert!(err.contains("previous object ended at"), "{err}");
    }

    #[test]
    fn rejects_history_that_does_not_reach_the_exported_state() {
        let m = manifest();
        let short = HistoryObject {
            state_id: 1,
            block: 9, // one below the convert point
            parent_root: B256::repeat_byte(1),
            post_root: B256::repeat_byte(2),
            accounts: vec![],
        };
        let bytes = StreamBuilder::new(&m)
            .blocks()
            .end_section()
            .history_section()
            .history(&short)
            .end_section()
            .state()
            .end_section()
            .finish();

        let mut s = SnapshotStream::open(bytes.as_slice()).unwrap();
        while s.next_record().unwrap().is_some() {}
        let err = s.check_history_meets_state().unwrap_err().to_string();
        assert!(err.contains("history ends at block 9"), "{err}");
    }

    #[test]
    fn rejects_a_bad_magic_and_a_corrupt_header_hash() {
        assert!(SnapshotStream::open(b"NOPE0000".as_slice()).is_err());

        let m = manifest();
        let mut bytes = StreamBuilder::new(&m)
            .blocks()
            .header(0, &header_rlp(B256::ZERO))
            .end_section()
            .finish();
        // Corrupt one byte of the header payload, leaving its recorded hash stale.
        let last = bytes.len() - 3;
        bytes[last] ^= 0xff;

        let mut s = SnapshotStream::open(bytes.as_slice()).unwrap();
        let err = s.next_record().unwrap_err().to_string();
        assert!(err.contains("does not hash to"), "{err}");
    }

    /// An empty body must produce the empty trie root, which is what a header with no transactions
    /// commits to.
    #[test]
    fn empty_body_has_the_empty_transactions_root() {
        // [[], []] : no transactions, no uncles.
        let body = [0xc2u8, 0xc0, 0xc0];
        assert_eq!(
            transactions_root(&body).unwrap(),
            alloy_trie::EMPTY_ROOT_HASH,
        );
    }

    /// A legacy transaction contributes its whole RLP list; a typed one contributes only the
    /// envelope the body wraps in a string. Getting that backwards yields a wrong but plausible
    /// root, so the two shapes must produce different results.
    #[test]
    fn legacy_and_typed_transactions_encode_differently() {
        let legacy: &[u8] = &[0xc3, 0x01, 0x02, 0x03]; // list [1,2,3]
        let typed_payload: &[u8] = &[0x02, 0x01, 0x02, 0x03]; // type 2 envelope
        let mut typed = vec![0x80 + typed_payload.len() as u8];
        typed.extend_from_slice(typed_payload);

        let mut body_a = vec![];
        body_a.extend_from_slice(legacy);
        let mut body_b = vec![];
        body_b.extend_from_slice(&typed);

        let wrap = |txs: &[u8]| {
            let mut inner = vec![0xc0 + txs.len() as u8];
            inner.extend_from_slice(txs);
            inner.push(0xc0); // empty uncles
            let mut out = vec![0xc0 + inner.len() as u8];
            out.extend_from_slice(&inner);
            out
        };

        let root_a = transactions_root(&wrap(&body_a)).unwrap();
        let root_b = transactions_root(&wrap(&body_b)).unwrap();
        assert_ne!(root_a, root_b);
        assert_ne!(root_a, alloy_trie::EMPTY_ROOT_HASH);
        assert_ne!(root_b, alloy_trie::EMPTY_ROOT_HASH);
    }

    #[test]
    fn extracts_header_fields_without_decoding_the_whole_header() {
        let parent = B256::repeat_byte(0x42);
        let rlp = header_rlp(parent);
        assert_eq!(header_field(&rlp, 0).unwrap(), parent.as_slice());
        assert_eq!(header_field(&rlp, 3).unwrap(), [0x11u8; 32]);
    }
}
