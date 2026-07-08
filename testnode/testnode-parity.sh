#!/usr/bin/env bash
# Local testnode parity harness.
#
# Validates arb-reth against a running nitro-testnode by deriving the testnode's chain from
# its L1 and checking every produced block's state root against the testnode's own Nitro node.
# Unlike the mainnet sync this needs no RPC budget, no disk for history, and no tip: the whole
# chain is short and lives on localhost, so it is the cheap way to get correctness coverage
# across ArbOS versions and exotic transaction types.
#
# The testnode exposes:
#   L1 geth   http://127.0.0.1:8545   (the parent chain arb-reth derives from)
#   L2 Nitro  http://127.0.0.1:8547   (the ground-truth chain to check against)
#   config volume  config:/config     (l2_chain_config.json, l2_chain_info.json)
#
# arb-reth boots genesis from the extracted chain config, then follows the testnode L1 with the
# testnode's SequencerInbox/Bridge addresses. The genesis anchor is L2 block 0 (a fresh chain),
# not the Arbitrum One Nitro genesis.
#
# Subcommands:
#   capture   pull the chain config + contract addresses out of the running testnode
#   run       boot arb-reth against the testnode L1 (derivation path) and leave it running
#   compare   diff arb-reth block roots against the testnode L2, bisecting to the first divergence
#   all       capture, run, wait for catch-up, compare, then stop arb-reth
#
# Env overrides: L1_RPC, L2_RPC, ARB_HTTP_PORT, WORKDIR, TESTNODE_DIR, ARB_RETH_BIN.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARB_RETH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

L1_RPC="${L1_RPC:-http://127.0.0.1:8545}"
L2_RPC="${L2_RPC:-http://127.0.0.1:8547}"
# Testnode Prysm beacon REST endpoint. The batch-poster posts blob batches to the local L1, so
# derivation needs this to fetch sidecars; it is only consulted for blob batches (harmless on a
# calldata-only chain). Set L1_BEACON= empty to force the calldata-only path.
L1_BEACON="${L1_BEACON-http://127.0.0.1:3500}"
# arb-reth's own RPC. 8545/8547 are taken by the testnode L1/L2, so default elsewhere.
ARB_HTTP_PORT="${ARB_HTTP_PORT:-8560}"
ARB_RPC="http://127.0.0.1:$ARB_HTTP_PORT"
WORKDIR="${WORKDIR:-/tmp/arb-testnode-parity}"
TESTNODE_DIR="${TESTNODE_DIR:-$ARB_RETH_DIR/../nitro-testnode}"
ARB_RETH_BIN="${ARB_RETH_BIN:-$ARB_RETH_DIR/target/release/arb-reth}"

CHAIN_CONFIG="$WORKDIR/l2_chain_config.json"
CHAIN_INFO="$WORKDIR/l2_chain_info.json"
CONTRACTS="$WORKDIR/contracts.env"
DATADIR="$WORKDIR/datadir"
ARB_LOG="$WORKDIR/arb-reth.log"
ARB_PIDFILE="$WORKDIR/arb-reth.pid"

log() { echo "[testnode-parity] $*" >&2; }
die() { log "error: $*"; exit 1; }

rpc() {
  # rpc <url> <method> <json-params>
  curl -s -m 30 -X POST "$1" -H 'content-type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$2\",\"params\":$3}"
}

# Read a file out of the testnode config volume via the `scripts` service (it mounts config:/config).
testnode_cat() {
  ( cd "$TESTNODE_DIR" && docker compose run --rm --no-deps --entrypoint sh scripts -c "cat $1" )
}

cmd_capture() {
  mkdir -p "$WORKDIR"
  command -v docker >/dev/null || die "docker not found"
  [ -d "$TESTNODE_DIR" ] || die "testnode dir not found: $TESTNODE_DIR"

  log "extracting chain config from the testnode config volume"
  testnode_cat /config/l2_chain_config.json > "$CHAIN_CONFIG" \
    || die "could not read l2_chain_config.json (is the testnode up and deployed?)"
  testnode_cat /config/l2_chain_info.json > "$CHAIN_INFO" \
    || die "could not read l2_chain_info.json"

  # l2_chain_info.json is an array; entry 0 carries the rollup contract addresses.
  local inbox bridge chain_id arbos
  inbox=$(python3 -c "import json,sys;print(json.load(open('$CHAIN_INFO'))[0]['rollup']['sequencer-inbox'])")
  bridge=$(python3 -c "import json,sys;print(json.load(open('$CHAIN_INFO'))[0]['rollup']['bridge'])")
  chain_id=$(python3 -c "import json;print(json.load(open('$CHAIN_CONFIG'))['chainId'])")
  arbos=$(python3 -c "import json;print(json.load(open('$CHAIN_CONFIG'))['arbitrum']['InitialArbOSVersion'])")

  {
    echo "SEQUENCER_INBOX=$inbox"
    echo "BRIDGE=$bridge"
    echo "CHAIN_ID=$chain_id"
    echo "ARBOS_VERSION=$arbos"
  } > "$CONTRACTS"

  log "captured chain $chain_id (ArbOS $arbos)"
  log "  SequencerInbox $inbox"
  log "  Bridge         $bridge"
  log "  -> $WORKDIR"
}

cmd_run() {
  [ -f "$CONTRACTS" ] || die "run 'capture' first (no $CONTRACTS)"
  [ -x "$ARB_RETH_BIN" ] || die "arb-reth binary not built: $ARB_RETH_BIN (cargo build --release -p arb-reth-node --bin arb-reth)"
  # shellcheck disable=SC1090
  source "$CONTRACTS"

  rm -rf "$DATADIR"
  mkdir -p "$DATADIR"

  # Derivation path. Genesis (chain id, chain config, initial L1 base fee) is bootstrapped from
  # the chain's Initialize message on L1, the way Nitro does it, so no chain-config file or base
  # fee flag is needed: passing --l1-rpc without --chain triggers that. The inbox/bridge pair
  # marks this as a custom deployment, so the deploy-block and L2-genesis anchors default to 0
  # (a fresh chain) with no extra flags. --l1-start-block 0 walks the testnode L1 from genesis.
  log "starting arb-reth (RPC $ARB_RPC, datadir $DATADIR)"
  "$ARB_RETH_BIN" node \
    --datadir "$DATADIR" \
    --http --http.port "$ARB_HTTP_PORT" \
    --l1-rpc "$L1_RPC" \
    ${L1_BEACON:+--l1-beacon "$L1_BEACON"} \
    --l1-sequencer-inbox "$SEQUENCER_INBOX" \
    --l1-bridge "$BRIDGE" \
    --l1-start-block 0 \
    --l1-start-delayed 0 \
    > "$ARB_LOG" 2>&1 &
  echo $! > "$ARB_PIDFILE"
  log "arb-reth pid $(cat "$ARB_PIDFILE"); logs at $ARB_LOG"
}

cmd_stop() {
  [ -f "$ARB_PIDFILE" ] || return 0
  local pid; pid=$(cat "$ARB_PIDFILE")
  if kill -0 "$pid" 2>/dev/null; then
    log "stopping arb-reth (pid $pid)"
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 10); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
    kill -9 "$pid" 2>/dev/null || true
  fi
  rm -f "$ARB_PIDFILE"
}

# Compare arb-reth block roots against the testnode L2, bisecting to the first divergence.
# Mirrors arb-parity-monitor.sh's monotone-forward algorithm: a divergence poisons every
# descendant, so checking the newest block is enough, and the first mismatch is bisected exactly.
cmd_compare() {
  python3 - "$ARB_RPC" "$L2_RPC" <<'PY'
import sys, json, time, urllib.request

local, remote = sys.argv[1], sys.argv[2]

def rpc(url, method, params, tries=5):
    body = json.dumps({"jsonrpc":"2.0","id":1,"method":method,"params":params}).encode()
    last = None
    for _ in range(tries):
        try:
            req = urllib.request.Request(url, data=body, headers={"content-type":"application/json"})
            with urllib.request.urlopen(req, timeout=30) as r:
                d = json.load(r)
            if d.get("error"): raise RuntimeError(d["error"])
            return d["result"]
        except Exception as e:
            last = e; time.sleep(1.0)
    raise last

def head(url): return int(rpc(url, "eth_blockNumber", []), 16)
def root(url, bn):
    b = rpc(url, "eth_getBlockByNumber", [hex(bn), False])
    return b["stateRoot"] if b else None
def matches(bn): return root(local, bn) == root(remote, bn)

def first_divergent(lo, hi):
    while hi - lo > 1:
        mid = (lo + hi) // 2
        if matches(mid): lo = mid
        else: hi = mid
    return hi

# The testnode L2 is the ground truth; only check up to what arb-reth has caught up to.
target = min(head(local), head(remote))
print(f"comparing blocks 0..{target} (arb-reth vs testnode L2)")
if not matches(0):
    print(f"DIVERGE 0 (genesis) local={root(local,0)} testnode={root(remote,0)}")
    print("genesis root mismatch: the L1 Initialize message did not reproduce the testnode genesis")
    sys.exit(1)
if matches(target):
    print(f"OK: all {target+1} blocks match the testnode")
    sys.exit(0)
bad = first_divergent(0, target)
print(f"DIVERGE {bad} local={root(local,bad)} testnode={root(remote,bad)}")
sys.exit(1)
PY
}

cmd_all() {
  cmd_capture
  cmd_run
  trap cmd_stop EXIT
  local target; target=$(rpc "$L2_RPC" eth_blockNumber '[]' | python3 -c "import json,sys;print(int(json.load(sys.stdin)['result'],16))")
  log "waiting for arb-reth to catch up to testnode L2 tip ($target)"
  for _ in $(seq 1 120); do
    local best; best=$(rpc "$ARB_RPC" eth_blockNumber '[]' 2>/dev/null | python3 -c "import json,sys;print(int(json.load(sys.stdin)['result'],16))" 2>/dev/null || echo 0)
    log "  arb-reth at $best / $target"
    [ "$best" -ge "$target" ] && break
    sleep 2
  done
  cmd_compare
}

case "${1:-all}" in
  capture) cmd_capture ;;
  run)     cmd_run ;;
  stop)    cmd_stop ;;
  compare) cmd_compare ;;
  all)     cmd_all ;;
  *) die "unknown subcommand: $1 (capture|run|stop|compare|all)" ;;
esac
