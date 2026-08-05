// Copyright 2026, Offchain Labs, Inc.
// reth-export: read a nitro l2chaindata geth DB (pebble + ancient freezer + path/hash state
// scheme) at its head block and stream the full state (and, optionally, blocks) for conversion
// into a reth MDBX database. Snapshot-height-agnostic.
//
// Usage:
//
//	reth-export <l2chaindata-dir> [--ancient DIR] [--mode diag|accounts|blocks|all] [--max N]
//
// diag (default): print head block / state scheme / preimage availability + a small account sample.
// accounts: stream every account as one JSON object per line (see DumpAccount).
package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/pathdb"
)

func fatal(msg string, err error) {
	fmt.Fprintf(os.Stderr, "reth-export: %s: %v\n", msg, err)
	os.Exit(1)
}

// partitionBoundary returns the 32-byte big-endian key floor(2^256 * i / n), the lower bound of
// the i-th of n equal account-key-space partitions.
func partitionBoundary(i, n int) []byte {
	span := new(big.Int).Lsh(big.NewInt(1), 256)
	b := new(big.Int).Mul(span, big.NewInt(int64(i)))
	b.Div(b, big.NewInt(int64(n)))
	buf := make([]byte, 32)
	b.FillBytes(buf)
	return buf
}

// walkRange streams state records for every account whose hashed key falls in [start, end) to w,
// following each account into its storage trie. start==nil means "from the first key"; end==nil
// means "to the last key". maxAcc>0 caps the number of accounts emitted (0 = unlimited). It opens
// its own account/storage tries, so it is safe to run concurrently over disjoint ranges (the
// backing triedb is read-concurrent). nAcc/nStor are incremented atomically for cross-goroutine
// progress. Records are identical to a serial walk, so concatenating disjoint ranges in key order
// reproduces the whole-state stream.
func walkRange(w *bufio.Writer, sdb state.Database, tdb *triedb.Database, root common.Hash, db ethdb.Database,
	start, end []byte, maxAcc uint64, nAcc, nStor *uint64) error {
	accTrie, err := sdb.OpenTrie(root)
	if err != nil {
		return fmt.Errorf("open account trie: %w", err)
	}
	accNodeIt, err := accTrie.NodeIterator(start)
	if err != nil {
		return fmt.Errorf("account node iterator: %w", err)
	}
	accIt := trie.NewIterator(accNodeIt)
	seenCode := make(map[common.Hash]bool)
	var local uint64
	for accIt.Next() {
		if end != nil && bytes.Compare(accIt.Key, end) >= 0 {
			break
		}
		var acc types.StateAccount
		if err := rlp.DecodeBytes(accIt.Value, &acc); err != nil {
			return fmt.Errorf("decode account %x: %w", accIt.Key, err)
		}
		codeHash := common.BytesToHash(acc.CodeHash)
		bal := "0"
		if b := acc.Balance.Bytes(); len(b) > 0 {
			bal = fmt.Sprintf("%x", b)
		}
		fmt.Fprintf(w, "A %x %d %s %x %x\n", accIt.Key, acc.Nonce, bal, codeHash, acc.Root)
		atomic.AddUint64(nAcc, 1)
		local++
		if codeHash != types.EmptyCodeHash && !seenCode[codeHash] {
			// Emit each distinct code once per range. The same codehash may still recur across
			// ranges (bounded by the walker count); the import keys code by hash, so a duplicate
			// C record is idempotent.
			seenCode[codeHash] = true
			fmt.Fprintf(w, "C %x %x\n", codeHash, rawdb.ReadCode(db, codeHash))
		}
		if acc.Root != types.EmptyRootHash {
			// Open the storage trie with the account hash yielded by the trie iterator. Calling
			// state.Database.OpenStorageTrie would hash an address preimage, but pruned Nitro
			// snapshots commonly omit preimages. StorageTrieID accepts the hashed owner directly
			// and therefore works for both path- and hash-scheme databases.
			owner := common.BytesToHash(accIt.Key)
			storageTr, err := trie.NewStateTrie(trie.StorageTrieID(root, owner, acc.Root), tdb)
			if err != nil {
				return fmt.Errorf("open storage trie: %w", err)
			}
			stNodeIt, err := storageTr.NodeIterator(nil)
			if err != nil {
				return fmt.Errorf("storage node iterator: %w", err)
			}
			stIt := trie.NewIterator(stNodeIt)
			for stIt.Next() {
				// Storage trie leaves are RLP(value); decode to the raw big-endian value bytes.
				var val []byte
				if err := rlp.DecodeBytes(stIt.Value, &val); err != nil {
					return fmt.Errorf("rlp-decode storage value: %w", err)
				}
				fmt.Fprintf(w, "S %x %x\n", common.BytesToHash(stIt.Key), val)
				atomic.AddUint64(nStor, 1)
			}
			if err := stIt.Err; err != nil {
				return fmt.Errorf("storage iter: %w", err)
			}
		}
		if maxAcc != 0 && local >= maxAcc {
			break
		}
	}
	return accIt.Err
}

func main() {
	ancient := flag.String("ancient", "", "ancients/freezer directory (default <dir>/ancient)")
	mode := flag.String("mode", "diag", "diag|state|blocks|history|full-snapshot|diskroot|accounts|addr")
	max := flag.Uint64("max", 0, "max accounts to dump (0 = all)")
	addr := flag.String("addr", "", "for --mode addr: a 0x address to dump (storage key form check)")
	from := flag.Int64("from", -1, "for --mode blocks: first block; for --mode history: first state id (default = earliest)")
	to := flag.Int64("to", -1, "for --mode blocks: last block; for --mode history: last state id (default = latest)")
	// A large pebble block cache is critical for `--mode state`: the trie walk is random-read
	// bound, and the default 16 MiB cache makes almost every node a cold disk read. Sizing this
	// to hold the hot upper-trie nodes turns most reads into RAM hits (orders of magnitude faster).
	cacheMB := flag.Int("cache", 8192, "pebble block cache size in MB")
	handles := flag.Int("handles", 4096, "max open DB file handles")
	// The serial trie walk is iodepth-1 random-read bound; flash storage has ample parallel
	// headroom. `--parallel N` splits the account-key space into N disjoint ranges walked
	// concurrently, each writing `<outbase>.partNNN`; concatenating the parts in index order yields
	// the same stream a serial walk would. `--outbase` is required when N > 1.
	parallel := flag.Int("parallel", 1, "for --mode state and --mode full-snapshot: number of concurrent key-range walkers")
	outbase := flag.String("outbase", "", "for --mode state --parallel N: output path prefix for part files")
	// Never defaults to the source directory: that is a live pebble database, and scattering part
	// files through it risks confusing a later open.
	tmpdir := flag.String("tmpdir", os.TempDir(), "for --mode full-snapshot --parallel N: scratch directory for state part files")
	flag.Parse()
	if flag.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "usage: reth-export <l2chaindata-dir> [--ancient DIR] [--mode diag|accounts] [--max N]")
		os.Exit(1)
	}
	dir := flag.Arg(0)
	anc := *ancient
	if anc == "" {
		anc = dir + "/ancient"
	}

	db, err := node.OpenDatabase(node.InternalOpenOptions{
		DbEngine:  "pebble",
		Directory: dir,
		DatabaseOptions: node.DatabaseOptions{
			AncientsDirectory: anc,
			MetricsNamespace:  "rethexport/",
			ReadOnly:          true,
			Cache:             *cacheMB,
			Handles:           *handles,
		},
	})
	if err != nil {
		fatal("open l2chaindata", err)
	}
	defer db.Close()

	scheme := rawdb.ReadStateScheme(db)
	headHash := rawdb.ReadHeadBlockHash(db)
	num, ok := rawdb.ReadHeaderNumber(db, headHash)
	if !ok {
		fatal("read head header number", fmt.Errorf("head hash %s not found", headHash))
	}
	header := rawdb.ReadHeader(db, headHash, num)
	if header == nil {
		fatal("read head header", fmt.Errorf("nil header at %d", num))
	}
	fmt.Fprintf(os.Stderr, "head: block=%d hash=%s stateRoot=%s scheme=%q\n", num, headHash.Hex(), header.Root.Hex(), scheme)

	// Build a read-only trie/state database matching the on-disk scheme.
	var tdb *triedb.Database
	if scheme == rawdb.PathScheme {
		tdb = triedb.NewDatabase(db, &triedb.Config{PathDB: pathdb.ReadOnly})
	} else {
		tdb = triedb.NewDatabase(db, triedb.HashDefaults)
	}
	defer tdb.Close()

	sdb := state.NewDatabase(tdb, nil)
	st, err := state.New(header.Root, sdb)
	if err != nil {
		fatal("open state at head root", err)
	}

	switch *mode {
	case "diag":
		// Dump a few accounts and report whether plaintext addresses (preimages) are present.
		d := st.RawDump(&state.DumpConfig{Max: 5, SkipStorage: true, SkipCode: true})
		withAddr, total := 0, 0
		for _, acc := range d.Accounts {
			total++
			if acc.Address != nil {
				withAddr++
			}
		}
		fmt.Fprintf(os.Stderr, "sampled %d accounts; %d have plaintext addresses (preimages %s)\n",
			total, withAddr, map[bool]string{true: "PRESENT", false: "ABSENT"}[withAddr > 0])
		enc := json.NewEncoder(os.Stdout)
		for _, acc := range d.Accounts {
			_ = enc.Encode(acc)
		}
	case "accounts":
		enc := json.NewEncoder(os.Stdout)
		st.IterativeDump(&state.DumpConfig{Max: *max}, enc)
	case "state":
		// Stream the full state as line-oriented records (scales to multi-million-slot accounts
		// without buffering any single account). All keys are hashed (no preimages needed):
		//   A <accountHash> <nonce> <balanceHex> <codeHashHex> <storageRootHex>
		//   C <codeHashHex> <codeHex>            (once per distinct non-empty code)
		//   S <slotHash> <slotValueHex>          (belongs to the most recent A)
		//
		// Walk the account/storage TRIES directly (OpenTrie/OpenStorageTrie), not geth's flat
		// snapshot layer. A pruned snapshot ships the head-state trie nodes but not the flat
		// snapshot, so the snapshot-backed AccountIterator returns "account iterator: not
		// supported". The trie walk works on any hash-scheme DB that retains the head state. The
		// iterator keys are already the keccak-hashed account/slot keys, so no preimages are
		// needed (and the produced state root is verified by `arb-reth snapshot import --expect`).
		var nAcc, nStor uint64
		if *parallel <= 1 {
			w := bufio.NewWriterSize(os.Stdout, 1<<20)
			if err := walkRange(w, sdb, tdb, header.Root, db, nil, nil, *max, &nAcc, &nStor); err != nil {
				fatal("walk state", err)
			}
			if err := w.Flush(); err != nil {
				fatal("flush", err)
			}
		} else {
			if *outbase == "" {
				fatal("parallel", fmt.Errorf("--outbase is required when --parallel > 1"))
			}
			n := *parallel
			// n+1 boundaries partition the 32-byte key space into n disjoint [lo, hi) ranges;
			// boundary 0 and n are nil (= from the very start / to the very end).
			bounds := make([][]byte, n+1)
			for i := 1; i < n; i++ {
				bounds[i] = partitionBoundary(i, n)
			}
			// Periodic progress: the walk is long, so surface live account/slot counts.
			stop := make(chan struct{})
			go func() {
				t := time.NewTicker(30 * time.Second)
				defer t.Stop()
				for {
					select {
					case <-stop:
						return
					case <-t.C:
						fmt.Fprintf(os.Stderr, "progress: %d accounts, %d storage slots\n",
							atomic.LoadUint64(&nAcc), atomic.LoadUint64(&nStor))
					}
				}
			}()
			var wg sync.WaitGroup
			errs := make([]error, n)
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					f, err := os.Create(fmt.Sprintf("%s.part%03d", *outbase, i))
					if err != nil {
						errs[i] = err
						return
					}
					defer f.Close()
					w := bufio.NewWriterSize(f, 1<<20)
					if err := walkRange(w, sdb, tdb, header.Root, db, bounds[i], bounds[i+1], 0, &nAcc, &nStor); err != nil {
						errs[i] = err
						return
					}
					errs[i] = w.Flush()
				}(i)
			}
			wg.Wait()
			close(stop)
			for i, e := range errs {
				if e != nil {
					fatal(fmt.Sprintf("partition %d", i), e)
				}
			}
			fmt.Fprintf(os.Stderr, "wrote %d part files: %s.part000 .. %s.part%03d (concatenate in order)\n",
				n, *outbase, *outbase, n-1)
		}
		fmt.Fprintf(os.Stderr, "exported %d accounts, %d storage slots\n", nAcc, nStor)
	case "blocks":
		// Stream blocks as raw-RLP records (header/body/receipts), default = head only:
		//   H <number> <hashHex> <headerRLPhex>
		//   B <number> <bodyRLPhex>       (omitted if empty)
		//   R <number> <receiptsRLPhex>   (omitted if empty)
		lo, hi := uint64(num), uint64(num)
		if *from >= 0 {
			lo = uint64(*from)
		}
		if *to >= 0 {
			hi = uint64(*to)
		}
		w := bufio.NewWriterSize(os.Stdout, 1<<20)
		defer w.Flush()
		var nBlk uint64
		for n := lo; n <= hi; n++ {
			hash := rawdb.ReadCanonicalHash(db, n)
			if hash == (common.Hash{}) {
				continue
			}
			hdr := rawdb.ReadHeaderRLP(db, hash, n)
			if len(hdr) == 0 {
				continue
			}
			fmt.Fprintf(w, "H %d %x %x\n", n, hash, hdr)
			if body := rawdb.ReadBodyRLP(db, hash, n); len(body) > 0 {
				fmt.Fprintf(w, "B %d %x\n", n, body)
			}
			if rcpts := rawdb.ReadReceiptsRLP(db, hash, n); len(rcpts) > 0 {
				fmt.Fprintf(w, "R %d %x\n", n, rcpts)
			}
			nBlk++
		}
		fmt.Fprintf(os.Stderr, "exported %d blocks [%d..%d]\n", nBlk, lo, hi)
	case "full-snapshot":
		fullSnapshot(db, anc, sdb, tdb, *parallel, *tmpdir)
	case "diskroot":
		// The path-scheme disk layer: the persisted trie, before any journalled diff layers.
		// pathdb derives it the same way in loadLayers(). Its root is the post root of the most
		// recent state history object, which is what makes it the point where state and history
		// agree.
		node := rawdb.ReadAccountTrieNode(db, nil)
		if len(node) == 0 {
			fatal("read disk-layer root node", fmt.Errorf("no account trie node at the empty path"))
		}
		fmt.Printf("diskRoot=%s persistentStateID=%d\n",
			crypto.Keccak256Hash(node).Hex(), rawdb.ReadPersistentStateID(db))
	case "history":
		exportHistory(anc, uint64Flag(*from), uint64Flag(*to))
	case "addr":
		a := common.HexToAddress(*addr)
		h := crypto.Keccak256Hash(a.Bytes())
		fmt.Fprintf(os.Stderr, "addr=%s keccak(addr)=%s\n", a.Hex(), h.Hex())
		d := st.RawDump(&state.DumpConfig{Start: h.Bytes(), Max: 1})
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		for _, acc := range d.Accounts {
			_ = enc.Encode(acc)
		}
	default:
		fatal("mode", fmt.Errorf("unknown mode %q", *mode))
	}
}

// ---------------------------------------------------------------------------
// --mode history: geth pathdb state history -> neutral change records
// ---------------------------------------------------------------------------

// Encoded sizes of the fixed-width records inside a state history object.
// Mirrors triedb/pathdb/history_state.go, which keeps these unexported.
const (
	histMetaSize      = 73 // version(1) parentRoot(32) postRoot(32) block(8)
	histAccIndexSize  = 33 // address(20) len(1) offset(4) storageOffset(4) storageSlots(4)
	histSlotIndexSize = 37 // slotKey(32) len(1) offset(4)

	histVersionHashedSlots = 0 // storage slot keys are hashed
	histVersionRawSlots    = 1 // storage slot keys are raw

	// Stream framing. The importer rejects anything else.
	histStreamMagic  = "ARBHIST1"
	histTagObject    = 0x01
	histTagStreamEnd = 0x00
)

func uint64Flag(v int64) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}

// exportHistory streams pathdb state history as neutral pre-state records.
//
// Each history object holds the values a block overwrote, which is what reth changesets store, so
// the conversion needs no re-execution. Output is binary rather than the hex-per-line form the
// state and block modes use: this chain has 27M objects and roughly 1.6B slot entries, where hex
// would more than double an already 100 GB stream.
//
// `from` and `to` are state ids, not block numbers, because that is how the freezer is addressed.
// Zero means "the whole available range". Each record carries its block number, so the importer
// never has to infer the mapping (ids 0 and 1 are both block 0 on a chain built from genesis).
func exportHistory(ancientDir string, from, to uint64) {
	store, err := rawdb.NewStateFreezer(ancientDir, false, true)
	if err != nil {
		fatal("open state freezer", err)
	}
	defer store.Close()

	// Ancients() counts items; state history ids are 1-based, so id i lives at position i-1.
	count, err := store.Ancients()
	if err != nil {
		fatal("read state history count", err)
	}
	if count == 0 {
		fatal("state freezer holds no history", fmt.Errorf("nothing to export from %s", ancientDir))
	}
	first, last := uint64(1), count
	if from != 0 {
		first = from
	}
	if to != 0 && to < last {
		last = to
	}
	if first > last {
		fatal("empty history range", fmt.Errorf("from %d > to %d", first, last))
	}

	w := bufio.NewWriterSize(os.Stdout, 1<<22)
	defer w.Flush()
	w.WriteString(histStreamMagic)

	started := time.Now()
	emitted, skipped, accounts, slots := streamHistoryRange(w, store, first, last, "history id")
	w.WriteByte(histTagStreamEnd)
	fmt.Fprintf(os.Stderr, "exported history ids [%d..%d]: %d objects (%d genesis v0 skipped), %d accounts, %d slots in %s\n",
		first, last, emitted, skipped, accounts, slots, time.Since(started).Truncate(time.Second))
}

// writeHistoryObject decodes one history object and writes its neutral form.
//
// Record layout, all integers unsigned varint unless stated:
//
//	tag(1)=0x01 stateID block version(1) parentRoot(32) postRoot(32) accountCount
//	  per account: address(20) present(1)
//	               if present: nonce, balanceLen(1) balance, codeHashLen(1) codeHash
//	               slotCount
//	    per slot:  key(32) valueLen(1) value
//
// A zero-length value means the slot was unset, and present=0 means the account did not exist
// before the block. Balances and slot values are minimal big-endian, so they cost a byte each when
// small, which most are.
func writeHistoryObject(w *bufio.Writer, id uint64, meta, accIndex, slotIndex, accData, slotData []byte) (uint64, uint64, error) {
	if len(meta) != histMetaSize {
		return 0, 0, fmt.Errorf("meta is %d bytes, want %d", len(meta), histMetaSize)
	}
	version := meta[0]
	if version > histVersionRawSlots {
		return 0, 0, fmt.Errorf("unknown state history version %d", version)
	}
	// A v0 object identifies storage slots by hash. Converting those to reth changesets needs a
	// preimage source the importer does not have, so refuse rather than emit unusable keys.
	// Chains built under a v1 geth carry at most a couple of v0 objects at genesis.
	if version == histVersionHashedSlots {
		return 0, 0, fmt.Errorf("state history %d uses hashed slot keys (v0); re-export from a range that excludes it", id)
	}
	if len(accIndex)%histAccIndexSize != 0 {
		return 0, 0, fmt.Errorf("account index is %d bytes, not a multiple of %d", len(accIndex), histAccIndexSize)
	}
	if len(slotIndex)%histSlotIndexSize != 0 {
		return 0, 0, fmt.Errorf("storage index is %d bytes, not a multiple of %d", len(slotIndex), histSlotIndexSize)
	}

	block := binary.BigEndian.Uint64(meta[65:histMetaSize])
	nAccounts := uint64(len(accIndex) / histAccIndexSize)

	w.WriteByte(histTagObject)
	writeUvarint(w, id)
	writeUvarint(w, block)
	w.WriteByte(version)
	w.Write(meta[1:33])  // parent root
	w.Write(meta[33:65]) // post root
	writeUvarint(w, nAccounts)

	var slotTotal uint64
	for i := uint64(0); i < nAccounts; i++ {
		rec := accIndex[i*histAccIndexSize : (i+1)*histAccIndexSize]
		addr := rec[0:20]
		length := int(rec[20])
		offset := int(binary.BigEndian.Uint32(rec[21:25]))
		storageOffset := int(binary.BigEndian.Uint32(rec[25:29]))
		storageSlots := int(binary.BigEndian.Uint32(rec[29:33]))

		if offset+length > len(accData) {
			return 0, 0, fmt.Errorf("account data range [%d,%d) exceeds %d bytes", offset, offset+length, len(accData))
		}
		w.Write(addr)

		blob := accData[offset : offset+length]
		if len(blob) == 0 {
			w.WriteByte(0) // did not exist before this block
		} else {
			acct, err := types.FullAccount(blob)
			if err != nil {
				return 0, 0, fmt.Errorf("decode previous account %x: %w", addr, err)
			}
			w.WriteByte(1)
			writeUvarint(w, acct.Nonce)
			writeShortBytes(w, acct.Balance.Bytes())
			// The storage root is a trie artefact reth recomputes, so it is not emitted. An empty
			// code hash is emitted as zero bytes rather than the empty-hash constant.
			if bytes.Equal(acct.CodeHash, types.EmptyCodeHash[:]) {
				w.WriteByte(0)
			} else {
				writeShortBytes(w, acct.CodeHash)
			}
		}

		writeUvarint(w, uint64(storageSlots))
		for s := 0; s < storageSlots; s++ {
			at := (storageOffset + s) * histSlotIndexSize
			if at+histSlotIndexSize > len(slotIndex) {
				return 0, 0, fmt.Errorf("storage index entry %d for %x out of range", storageOffset+s, addr)
			}
			entry := slotIndex[at : at+histSlotIndexSize]
			vlen := int(entry[32])
			voff := int(binary.BigEndian.Uint32(entry[33:37]))
			if voff+vlen > len(slotData) {
				return 0, 0, fmt.Errorf("storage data range [%d,%d) exceeds %d bytes", voff, voff+vlen, len(slotData))
			}
			w.Write(entry[0:32]) // raw slot key, guaranteed by the v1 check above
			// Stored values are RLP byte strings; strip the header so the importer gets the
			// integer bytes directly.
			val, err := rlpStringPayload(slotData[voff : voff+vlen])
			if err != nil {
				return 0, 0, fmt.Errorf("decode slot value for %x: %w", addr, err)
			}
			writeShortBytes(w, val)
		}
		slotTotal += uint64(storageSlots)
	}
	return nAccounts, slotTotal, nil
}

// rlpStringPayload strips the RLP header from a storage value. An empty input is a zero slot.
func rlpStringPayload(b []byte) ([]byte, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var out []byte
	if err := rlp.DecodeBytes(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func writeUvarint(w *bufio.Writer, v uint64) {
	var buf [binary.MaxVarintLen64]byte
	w.Write(buf[:binary.PutUvarint(buf[:], v)])
}

// writeShortBytes writes a length-prefixed blob of at most 255 bytes. Every field using it
// (balance, code hash, slot value) is bounded at 32.
func writeShortBytes(w *bufio.Writer, b []byte) {
	w.WriteByte(byte(len(b)))
	w.Write(b)
}

// ---------------------------------------------------------------------------
// --mode full-snapshot: one stream holding state, blocks and history up to P
// ---------------------------------------------------------------------------

// Stream framing. Sections appear in this order and each is self-terminating.
const (
	snapStreamMagic = "ARBSNAP1"

	// Section tags live above the record tags on purpose. They shared a range in the first draft,
	// which made a receipts record inside the blocks section indistinguishable from the start of
	// the history section.
	snapSectionManifest = 0xf1
	snapSectionBlocks   = 0xf2
	snapSectionHistory  = 0xf3
	snapSectionState    = 0xf4
	snapSectionEnd      = 0xff

	snapRecEnd     = 0x00
	snapRecHeader  = 0x01 // block header
	snapRecBody    = 0x02 // block body
	snapRecReceipt = 0x03 // block receipts
	snapRecAccount = 0x04 // account, at the exported state root
	snapRecStorage = 0x05 // storage slot, belongs to the preceding account
	snapRecCode    = 0x06 // contract code, keyed by hash
)

// convertPoint is where state, blocks and history all agree: the deepest state the trie database can
// still serve, which is pathdb's disk layer.
//
// It is not the persisted state id. That tracks how far trie node writes have been flushed, while
// the disk layer aggregates further transitions in its journal buffer, so the persisted root is not
// a root the layer tree can serve. Asking for it fails with "state is not available".
//
// The disk layer's root is the post root of the most recently flattened history object, because
// history is written at flatten time. Diff layers above it have no history yet, which is why the
// conversion stops here and lets the node re-derive the rest.
type convertPoint struct {
	block   uint64      // P
	root    common.Hash // R_P
	stateID uint64      // P's history object
}

// resolveConvertPoint finds the deepest servable state that also has history.
//
// Rather than deriving the disk layer's identity from pathdb internals, it walks history objects
// back from the newest and takes the first whose post root the trie database can actually open.
// That is the property the exporter needs, checked directly, and it self-corrects if the disk layer
// sits a few transitions below the newest history.
func resolveConvertPoint(db ethdb.Database, ancientDir string, sdb state.Database) convertPoint {
	store, err := rawdb.NewStateFreezer(ancientDir, false, true)
	if err != nil {
		fatal("open state freezer", err)
	}
	defer store.Close()

	count, err := store.Ancients()
	if err != nil {
		fatal("read state history count", err)
	}
	if count == 0 {
		fatal("resolve convert point", fmt.Errorf("state freezer holds no history"))
	}

	// Bounded: the disk layer is at most maxDiffLayers behind the newest history in a healthy
	// database. Scanning far past that would mean something is wrong, and silently converting an
	// ancient state is worse than stopping.
	const maxProbe = 512
	for probe := uint64(0); probe < maxProbe && probe < count; probe++ {
		id := count - probe
		meta, _, _, _, _, err := rawdb.ReadStateHistory(store, id)
		if err != nil {
			fatal(fmt.Sprintf("read state history %d", id), err)
		}
		if len(meta) != histMetaSize {
			fatal("decode state history meta", fmt.Errorf("meta is %d bytes, want %d", len(meta), histMetaSize))
		}
		root := common.BytesToHash(meta[33:65])
		if _, err := sdb.OpenTrie(root); err != nil {
			continue
		}
		block := binary.BigEndian.Uint64(meta[65:histMetaSize])
		if probe > 0 {
			fmt.Fprintf(os.Stderr,
				"note: newest history is %d ids above the servable state; converting at id %d\n", probe, id)
		}
		return convertPoint{block: block, root: root, stateID: id}
	}
	fatal("resolve convert point", fmt.Errorf(
		"no state root among the newest %d history objects can be opened; the snapshot's trie and its history do not meet", maxProbe))
	panic("unreachable")
}

// fullSnapshot writes one stream that an importer can turn into a complete reth datadir.
//
// Sections are ordered so the importer can write append-only: blocks ascending, then history
// ascending, then the state bulk load. Everything stops at P, so the three agree.
func fullSnapshot(db ethdb.Database, ancientDir string, sdb state.Database, tdb *triedb.Database, parallel int, tmpDir string) {
	point := resolveConvertPoint(db, ancientDir, sdb)
	fmt.Fprintf(os.Stderr, "convert point: block=%d root=%s stateID=%d\n",
		point.block, point.root.Hex(), point.stateID)

	w := bufio.NewWriterSize(os.Stdout, 1<<22)
	defer w.Flush()
	w.WriteString(snapStreamMagic)

	writeManifestSection(w, db, point)
	writeBlocksSection(w, db, point)
	writeHistorySection(w, ancientDir, point)
	writeStateSection(w, sdb, tdb, db, point, parallel, tmpDir)
	w.WriteByte(snapSectionEnd)
}

func writeManifestSection(w *bufio.Writer, db ethdb.Database, point convertPoint) {
	w.WriteByte(snapSectionManifest)
	writeUvarint(w, point.block)
	w.Write(point.root.Bytes())
	writeUvarint(w, point.stateID)
	hash := rawdb.ReadCanonicalHash(db, point.block)
	if hash == (common.Hash{}) {
		fatal("read convert-point hash", fmt.Errorf("no canonical hash at block %d", point.block))
	}
	w.Write(hash.Bytes())
}

// writeBlocksSection streams headers, bodies and receipts for [0, P] as raw RLP.
//
// The importer recomputes transactionsRoot and receiptsRoot from these and compares them against the
// header, so a truncated or misaligned body is caught per block rather than at the end (ADR-004 B2).
func writeBlocksSection(w *bufio.Writer, db ethdb.Database, point convertPoint) {
	w.WriteByte(snapSectionBlocks)
	var blocks, bodies, receipts uint64
	started := time.Now()
	for n := uint64(0); n <= point.block; n++ {
		hash := rawdb.ReadCanonicalHash(db, n)
		if hash == (common.Hash{}) {
			fatal("read canonical hash", fmt.Errorf("gap at block %d", n))
		}
		header := rawdb.ReadHeaderRLP(db, hash, n)
		if len(header) == 0 {
			fatal("read header", fmt.Errorf("missing header at block %d", n))
		}
		w.WriteByte(snapRecHeader)
		writeUvarint(w, n)
		w.Write(hash.Bytes())
		writeBlob(w, header)
		blocks++

		if body := rawdb.ReadBodyRLP(db, hash, n); len(body) > 0 {
			w.WriteByte(snapRecBody)
			writeUvarint(w, n)
			writeBlob(w, body)
			bodies++
		}
		if r := rawdb.ReadReceiptsRLP(db, hash, n); len(r) > 0 {
			w.WriteByte(snapRecReceipt)
			writeUvarint(w, n)
			writeBlob(w, r)
			receipts++
		}
		if n%1000000 == 0 {
			fmt.Fprintf(os.Stderr, "blocks %d/%d (%s)\n", n, point.block, time.Since(started).Truncate(time.Second))
		}
	}
	w.WriteByte(snapRecEnd)
	fmt.Fprintf(os.Stderr, "blocks section: %d headers, %d bodies, %d receipt sets in %s\n",
		blocks, bodies, receipts, time.Since(started).Truncate(time.Second))
}

// writeHistorySection streams every state history object up to and including P's.
func writeHistorySection(w *bufio.Writer, ancientDir string, point convertPoint) {
	w.WriteByte(snapSectionHistory)
	store, err := rawdb.NewStateFreezer(ancientDir, false, true)
	if err != nil {
		fatal("open state freezer", err)
	}
	defer store.Close()

	started := time.Now()
	emitted, skipped, accounts, slots := streamHistoryRange(w, store, 1, point.stateID, "history")
	w.WriteByte(snapRecEnd)
	fmt.Fprintf(os.Stderr, "history section: %d objects (%d genesis v0 skipped), %d accounts, %d slots in %s\n",
		emitted, skipped, accounts, slots, time.Since(started).Truncate(time.Second))
}

// writeStateSection walks the account and storage tries at R_P.
//
// The serial walk is random-read bound at iodepth 1 and measured at a few MB per minute on a real
// snapshot, which is unusable. `--parallel N` splits the account key space into N disjoint ranges
// walked concurrently, each into a temporary file, then appends them in index order. Records land in
// key order either way, so the result is byte-identical to a serial walk.
func writeStateSection(w *bufio.Writer, sdb state.Database, tdb *triedb.Database, db ethdb.Database, point convertPoint, parallel int, tmpDir string) {
	w.WriteByte(snapSectionState)
	var accounts, slots, codes uint64
	started := time.Now()
	// Code is shared across partitions: proxies mean one bytecode backs many accounts. Deduplicating
	// globally keeps a partitioned run from emitting the same blob once per partition.
	var seenCode sync.Map

	if parallel <= 1 {
		if err := walkStateRange(w, sdb, tdb, db, point.root, nil, nil, &seenCode, &accounts, &slots, &codes); err != nil {
			fatal("walk state", err)
		}
	} else {
		parts := make([]string, parallel)
		errs := make([]error, parallel)
		var wg sync.WaitGroup
		for i := 0; i < parallel; i++ {
			parts[i] = fmt.Sprintf("%s/state-part-%03d.bin", tmpDir, i)
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				f, err := os.Create(parts[i])
				if err != nil {
					errs[i] = err
					return
				}
				defer f.Close()
				pw := bufio.NewWriterSize(f, 1<<22)
				var lo, hi []byte
				if i > 0 {
					lo = partitionBoundary(i, parallel)
				}
				if i < parallel-1 {
					hi = partitionBoundary(i+1, parallel)
				}
				if err := walkStateRange(pw, sdb, tdb, db, point.root, lo, hi, &seenCode, &accounts, &slots, &codes); err != nil {
					errs[i] = err
					return
				}
				errs[i] = pw.Flush()
			}(i)
		}

		done := make(chan struct{})
		go func() {
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-done:
					return
				case <-ticker.C:
					fmt.Fprintf(os.Stderr, "state %d accounts, %d slots, %d code (%s)\n",
						atomic.LoadUint64(&accounts), atomic.LoadUint64(&slots),
						atomic.LoadUint64(&codes), time.Since(started).Truncate(time.Second))
				}
			}
		}()
		wg.Wait()
		close(done)
		for i, err := range errs {
			if err != nil {
				fatal(fmt.Sprintf("state partition %d", i), err)
			}
		}

		// Append in index order. Partition boundaries are ascending key ranges, so this is the same
		// sequence a serial walk would have produced.
		for i, name := range parts {
			f, err := os.Open(name)
			if err != nil {
				fatal(fmt.Sprintf("open state part %d", i), err)
			}
			if _, err := io.Copy(w, bufio.NewReaderSize(f, 1<<22)); err != nil {
				fatal(fmt.Sprintf("append state part %d", i), err)
			}
			f.Close()
			os.Remove(name)
		}
	}

	w.WriteByte(snapRecEnd)
	fmt.Fprintf(os.Stderr, "state section: %d accounts, %d slots, %d code blobs in %s\n",
		accounts, slots, codes, time.Since(started).Truncate(time.Second))
}

// walkStateRange emits accounts in [lo, hi) with their storage and code.
//
// Storage tries open by hashed owner: a pruned snapshot carries no address preimages, so the owner
// must come from the account iterator's key rather than from hashing an address.
func walkStateRange(w *bufio.Writer, sdb state.Database, tdb *triedb.Database, db ethdb.Database,
	root common.Hash, lo, hi []byte, seenCode *sync.Map, accounts, slots, codes *uint64) error {
	accTrie, err := sdb.OpenTrie(root)
	if err != nil {
		return fmt.Errorf("open account trie: %w", err)
	}
	nodeIt, err := accTrie.NodeIterator(lo)
	if err != nil {
		return fmt.Errorf("account node iterator: %w", err)
	}
	accIt := trie.NewIterator(nodeIt)

	for accIt.Next() {
		if hi != nil && bytes.Compare(accIt.Key, hi) >= 0 {
			break
		}
		var acc types.StateAccount
		if err := rlp.DecodeBytes(accIt.Value, &acc); err != nil {
			return fmt.Errorf("decode account %x: %w", accIt.Key, err)
		}
		owner := common.BytesToHash(accIt.Key)
		w.WriteByte(snapRecAccount)
		w.Write(owner.Bytes())
		writeUvarint(w, acc.Nonce)
		writeShortBytes(w, acc.Balance.Bytes())
		hasCode := !bytes.Equal(acc.CodeHash, types.EmptyCodeHash[:])
		if hasCode {
			writeShortBytes(w, acc.CodeHash)
		} else {
			w.WriteByte(0)
		}
		atomic.AddUint64(accounts, 1)

		if hasCode {
			codeHash := common.BytesToHash(acc.CodeHash)
			if _, loaded := seenCode.LoadOrStore(codeHash, struct{}{}); !loaded {
				code := rawdb.ReadCode(db, codeHash)
				if len(code) == 0 {
					return fmt.Errorf("no code for hash %s", codeHash.Hex())
				}
				w.WriteByte(snapRecCode)
				w.Write(codeHash.Bytes())
				writeBlob(w, code)
				atomic.AddUint64(codes, 1)
			}
		}

		if acc.Root == types.EmptyRootHash {
			continue
		}
		stTrie, err := trie.NewStateTrie(trie.StorageTrieID(root, owner, acc.Root), tdb)
		if err != nil {
			return fmt.Errorf("open storage trie for %x: %w", owner, err)
		}
		stNodeIt, err := stTrie.NodeIterator(nil)
		if err != nil {
			return fmt.Errorf("storage node iterator for %x: %w", owner, err)
		}
		stIt := trie.NewIterator(stNodeIt)
		for stIt.Next() {
			var val []byte
			if err := rlp.DecodeBytes(stIt.Value, &val); err != nil {
				return fmt.Errorf("decode storage value for %x: %w", owner, err)
			}
			w.WriteByte(snapRecStorage)
			w.Write(common.BytesToHash(stIt.Key).Bytes())
			writeShortBytes(w, val)
			atomic.AddUint64(slots, 1)
		}
		if err := stIt.Err; err != nil {
			return fmt.Errorf("storage iterator for %x: %w", owner, err)
		}
	}
	return accIt.Err
}

// writeBlob writes a varint-prefixed byte slice, for payloads that exceed 255 bytes.
func writeBlob(w *bufio.Writer, b []byte) {
	writeUvarint(w, uint64(len(b)))
	w.Write(b)
}

// streamHistoryRange writes history objects [first, last] and reports what it emitted.
//
// A leading run of v0 objects is the genesis state materialisation, which identifies slots by hash.
// It is skipped rather than refused: block 0 needs no changeset, since there is nothing below it to
// unwind into, and the first v1 object still chains to the genesis state root. A v0 object after
// real history has started is a different thing and is fatal.
func streamHistoryRange(w *bufio.Writer, store ethdb.AncientStore, first, last uint64, label string) (emitted, skipped, accounts, slots uint64) {
	started := time.Now()
	for id := first; id <= last; id++ {
		meta, accIndex, slotIndex, accData, slotData, err := rawdb.ReadStateHistory(store, id)
		if err != nil {
			fatal(fmt.Sprintf("read state history %d", id), err)
		}
		if len(meta) == histMetaSize && meta[0] == histVersionHashedSlots && emitted == 0 {
			block := binary.BigEndian.Uint64(meta[65:histMetaSize])
			if block != 0 {
				fatal("export history", fmt.Errorf(
					"state history %d uses hashed slot keys at block %d; only a genesis-block v0 object can be skipped", id, block))
			}
			skipped++
			continue
		}
		na, ns, err := writeHistoryObject(w, id, meta, accIndex, slotIndex, accData, slotData)
		if err != nil {
			fatal(fmt.Sprintf("encode state history %d", id), err)
		}
		emitted++
		accounts += na
		slots += ns
		if emitted%1000000 == 0 {
			fmt.Fprintf(os.Stderr, "%s %d/%d (%d accounts, %d slots, %s)\n",
				label, id, last, accounts, slots, time.Since(started).Truncate(time.Second))
		}
	}
	return emitted, skipped, accounts, slots
}
