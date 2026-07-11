#!/usr/bin/env bash
# Run the standalone arb-reth node for an Orbit chain (defaults to the Robinhood fixtures).
#
# Config comes from the environment (or scripts/run.env, which is gitignored so your L1 keys
# never get committed). Copy scripts/run.env.example to scripts/run.env and fill it in, or export
# the vars yourself:
#
#   L1_RPC            (required) Ethereum L1 execution RPC (the parent chain), e.g. an Alchemy URL
#   L1_BEACON         (required) Ethereum L1 beacon/blob endpoint (for blob batches)
#   FEED_URL          (optional) sequencer feed relay, ws:// or wss://; rides the tip once caught up
#   CHAIN_INFO        (optional) path to chaininfo.json    [default: Robinhood fixture]
#   GENESIS           (optional) path to genesis.json      [default: Robinhood fixture]
#   DATADIR           (optional) node data directory       [default: ~/.arb-reth/<chain>]
#   HTTP_PORT         (optional) JSON-RPC HTTP port         [default: 8545]
#   L1_END_BLOCK      (optional) stop L1 derivation at this L1 block (omit = follow the tip forever)
#   L1_GETLOGS_RANGE  (optional) getLogs span cap for the L1 provider [default: 500]
#
# Anything after `--` (or any extra args) is forwarded verbatim to `arb-reth node`.
#
# Examples:
#   scripts/run-node.sh                       # derive Robinhood from L1 into ~/.arb-reth/robinhood
#   HTTP_PORT=8600 scripts/run-node.sh        # serve RPC on a different port
#   scripts/run-node.sh --build               # (re)build the release binary first
#   scripts/run-node.sh -- --no-fsync         # forward extra flags to the node
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FIXTURES="$REPO_ROOT/crates/arb-reth-node/tests/fixtures"

# Load local secrets/config if present (gitignored).
if [[ -f "$SCRIPT_DIR/run.env" ]]; then
  # shellcheck disable=SC1091
  source "$SCRIPT_DIR/run.env"
fi

# Optional first-arg --build to compile the release binary before running.
DO_BUILD=0
if [[ "${1:-}" == "--build" ]]; then
  DO_BUILD=1
  shift
fi
# Drop a leading `--` separator if the caller used one.
[[ "${1:-}" == "--" ]] && shift || true

CHAIN_INFO="${CHAIN_INFO:-$FIXTURES/robinhood-chain-info.json}"
GENESIS="${GENESIS:-$FIXTURES/robinhood-genesis.json}"
HTTP_PORT="${HTTP_PORT:-8545}"
L1_GETLOGS_RANGE="${L1_GETLOGS_RANGE:-500}"

# Derive a default datadir name from the chain-info filename (…-chain-info.json -> …).
CHAIN_TAG="$(basename "$CHAIN_INFO" | sed -E 's/-?chain-?info\.json$//; s/\.json$//; s/-$//')"
DATADIR="${DATADIR:-$HOME/.arb-reth/${CHAIN_TAG:-orbit}}"

die() { echo "error: $*" >&2; exit 1; }
[[ -n "${L1_RPC:-}" ]]    || die "L1_RPC is not set (Ethereum L1 execution RPC). See scripts/run.env.example."
[[ -n "${L1_BEACON:-}" ]] || die "L1_BEACON is not set (Ethereum L1 beacon endpoint). See scripts/run.env.example."
[[ -f "$CHAIN_INFO" ]]    || die "CHAIN_INFO not found: $CHAIN_INFO"
[[ -f "$GENESIS" ]]       || die "GENESIS not found: $GENESIS"

BIN="$REPO_ROOT/target/release/arb-reth"
if [[ "$DO_BUILD" == "1" || ! -x "$BIN" ]]; then
  echo ">> building arb-reth (release)…" >&2
  ( cd "$REPO_ROOT" && cargo build --release -p arb-reth-node --bin arb-reth )
fi

mkdir -p "$DATADIR"

args=(
  node
  --datadir "$DATADIR"
  --chain-info "$CHAIN_INFO"
  --genesis "$GENESIS"
  --l1-rpc "$L1_RPC"
  --l1-beacon "$L1_BEACON"
  --l1-getlogs-range "$L1_GETLOGS_RANGE"
  --http --http.port "$HTTP_PORT"
)
[[ -n "${FEED_URL:-}" ]]     && args+=( --feed-url "$FEED_URL" )
[[ -n "${L1_END_BLOCK:-}" ]] && args+=( --l1-end-block "$L1_END_BLOCK" )

echo ">> chain=$CHAIN_TAG datadir=$DATADIR rpc=http://127.0.0.1:$HTTP_PORT${FEED_URL:+ feed=$FEED_URL}" >&2
exec "$BIN" "${args[@]}" "$@"
