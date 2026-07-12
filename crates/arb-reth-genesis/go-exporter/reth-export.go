// Copyright 2026, Offchain Labs, Inc.
// reth-export: read a nitro l2chaindata geth DB (pebble + ancient freezer + path/hash state
// scheme) at its head block and stream the full state (and, optionally, blocks) for conversion
// into a reth MDBX database. Snapshot-height-agnostic.
//
// Usage:
//   reth-export <l2chaindata-dir> [--ancient DIR] [--mode diag|accounts|blocks|all] [--max N]
//
// diag (default): print head block / state scheme / preimage availability + a small account sample.
// accounts: stream every account as one JSON object per line (see DumpAccount).
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
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

func main() {
	ancient := flag.String("ancient", "", "ancients/freezer directory (default <dir>/ancient)")
	mode := flag.String("mode", "diag", "diag|state|blocks|accounts|addr")
	max := flag.Uint64("max", 0, "max accounts to dump (0 = all)")
	addr := flag.String("addr", "", "for --mode addr: a 0x address to dump (storage key form check)")
	from := flag.Int64("from", -1, "for --mode blocks: first block (default = head)")
	to := flag.Int64("to", -1, "for --mode blocks: last block (default = head)")
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
		w := bufio.NewWriterSize(os.Stdout, 1<<20)
		defer w.Flush()
		seenCode := make(map[common.Hash]bool)
		accTrie, err := sdb.OpenTrie(header.Root)
		if err != nil {
			fatal("open account trie", err)
		}
		accNodeIt, err := accTrie.NodeIterator(nil)
		if err != nil {
			fatal("account node iterator", err)
		}
		accIt := trie.NewIterator(accNodeIt)
		var nAcc, nStor uint64
		for accIt.Next() {
			var acc types.StateAccount
			if err := rlp.DecodeBytes(accIt.Value, &acc); err != nil {
				fatal("decode account", err)
			}
			h := common.BytesToHash(accIt.Key)
			codeHash := common.BytesToHash(acc.CodeHash)
			bal := "0"
			if b := acc.Balance.Bytes(); len(b) > 0 {
				bal = fmt.Sprintf("%x", b)
			}
			fmt.Fprintf(w, "A %x %d %s %x %x\n", h, acc.Nonce, bal, codeHash, acc.Root)
			nAcc++
			if codeHash != types.EmptyCodeHash && !seenCode[codeHash] {
				seenCode[codeHash] = true
				fmt.Fprintf(w, "C %x %x\n", codeHash, rawdb.ReadCode(db, codeHash))
			}
			if acc.Root != types.EmptyRootHash {
				// `addr` only derives the storage-trie owner for the PATH scheme; on a hash-scheme
				// DB the nodes are keyed purely by hash, so the zero address is correct here (we
				// have no preimage to recover the real address, and don't need one).
				storageTr, err := sdb.OpenStorageTrie(header.Root, common.Address{}, acc.Root, accTrie)
				if err != nil {
					fatal("open storage trie", err)
				}
				stNodeIt, err := storageTr.NodeIterator(nil)
				if err != nil {
					fatal("storage node iterator", err)
				}
				stIt := trie.NewIterator(stNodeIt)
				for stIt.Next() {
					// Storage trie leaves are RLP(value); decode to the raw big-endian value bytes.
					var val []byte
					if err := rlp.DecodeBytes(stIt.Value, &val); err != nil {
						fatal("rlp-decode storage value", err)
					}
					fmt.Fprintf(w, "S %x %x\n", common.BytesToHash(stIt.Key), val)
					nStor++
				}
				if err := stIt.Err; err != nil {
					fatal("storage iter", err)
				}
			}
			if *max != 0 && nAcc >= *max {
				break
			}
		}
		if err := accIt.Err; err != nil {
			fatal("account iter", err)
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
