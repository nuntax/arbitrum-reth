#!/usr/bin/env bash
# Walk ArbOS versions IN PLACE on a running nitro-testnode.
#
# Rather than redeploying a fresh chain per version (which only tests genesis-at-version), this
# deploys once at the lowest version and then schedules real ArbOS upgrades on the live chain via
# ArbOwner.scheduleArbOSUpgrade. That exercises the ArbOS upgrade-migration path (the same code
# that runs when Arbitrum One bumps ArbOS), and checks arb-reth reproduces each crossing purely
# from L1 derivation. For each milestone: schedule the upgrade, drive L2 traffic so blocks cross
# the upgrade and get batched to L1, wait for arb-reth to derive past the crossing, and compare
# every block root against the testnode.
#
# A divergence poisons every later block, so the walk stops at the first FAIL and pins the version.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARB_RETH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TESTNODE_DIR="${TESTNODE_DIR:-$ARB_RETH_DIR/../nitro-testnode}"
HARNESS="$SCRIPT_DIR/testnode-parity.sh"
CONFIG_TS="$TESTNODE_DIR/scripts/config.ts"

START="${START:-2}"                                  # genesis ArbOS version
STEPS="${STEPS:-6 11 20 30 31 32 40 41 50 51}"       # upgrade targets, low to high (<= build max 51)

WORKDIR="${WORKDIR:-/tmp/arb-testnode-parity}"
RESULTS="$WORKDIR/walk-results.txt"
L2="http://127.0.0.1:8547"; ARB="http://127.0.0.1:8560"
ARB_OWNER="0x0000000000000000000000000000000000000070"
ARBSYS="0x0000000000000000000000000000000000000064"
AOV_SEL="0x051038f2"          # arbOSVersion()  -> returns 55 + arbos version
SCHED_SEL="e388b381"          # scheduleArbOSUpgrade(uint64 newVersion, uint64 timestamp)

mkdir -p "$WORKDIR"; : > "$RESULTS"
log(){ echo "[walk] $*"; }
record(){ echo "$1" >> "$RESULTS"; log "RESULT: $1"; }

run_deadline(){ local cmd="$1" secs="$2" lf="$3"; ( eval "$cmd" ) >"$lf" 2>&1 & local p=$!;
  for _ in $(seq 1 "$secs"); do kill -0 "$p" 2>/dev/null || { wait "$p"; return $?; }; sleep 1; done
  kill -9 "$p" 2>/dev/null; return 124; }

jnum(){ python3 -c "import json,sys;print(int(json.load(sys.stdin)['result'],16))" 2>/dev/null || echo 0; }
tip(){ curl -s -m 10 -X POST "$1" -H 'content-type: application/json' -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' | jnum; }
# ArbOS version (subtract the 55 offset ArbSys.arbOSVersion applies); 0 if unreachable.
arbos(){ local r; r=$(curl -s -m 10 -X POST "$1" -H 'content-type: application/json' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_call\",\"params\":[{\"to\":\"$ARBSYS\",\"data\":\"$AOV_SEL\"},\"latest\"]}" | jnum); echo $(( r > 55 ? r - 55 : 0 )); }
tn(){ ( cd "$TESTNODE_DIR" && ./test-node.bash script "$@" ); }
sched_calldata(){ python3 -c "print('0x$SCHED_SEL'+format($1,'064x')+format(0,'064x'))"; }
# Drive $1 L2 transfers (funded funnel -> throwaway users) to produce blocks and trigger batching.
drive(){ local n="$1" i; for i in $(seq 1 "$n"); do tn send-l2 --from funnel --to "user_$RANDOM" --ethamount 1 --wait >/dev/null 2>&1; done; }

cleanup(){ "$HARNESS" stop 2>/dev/null; ( cd "$TESTNODE_DIR" && git checkout scripts/config.ts 2>/dev/null ); }
trap cleanup EXIT

# ---- deploy once at START ----
log "deploying testnode at ArbOS $START"
"$HARNESS" stop 2>/dev/null
( cd "$TESTNODE_DIR" && git checkout scripts/config.ts 2>/dev/null )
perl -0pi -e "s/(txfiltering \\? 60 : )\\d+/\${1}$START/" "$CONFIG_TS"
# Force a clean slate: a stray `docker compose run` container can otherwise block the network
# teardown and leave init-force half-done (no L2).
( cd "$TESTNODE_DIR" && docker compose down --remove-orphans -t 5 2>/dev/null
  docker ps -aq --filter label=com.docker.compose.project=nitro-testnode | xargs -r docker rm -f 2>/dev/null
  docker network rm nitro-testnode_default 2>/dev/null ) >/dev/null 2>&1
run_deadline "cd '$TESTNODE_DIR' && docker compose build scripts" 240 "$WORKDIR/walk-build.log" || { record "deploy: BUILD-FAIL"; exit 1; }
run_deadline "cd '$TESTNODE_DIR' && ./test-node.bash --init-force --detach" 480 "$WORKDIR/walk-init.log"
sleep 3
for _ in $(seq 1 30); do [ "$(tip "$L2")" -gt 0 ] && break; sleep 2; done
[ "$(tip "$L2")" -gt 0 ] || { record "deploy: L2-DOWN"; exit 1; }
log "testnode up at ArbOS $(arbos "$L2")"

# Fund L2 so we can drive traffic, then start arb-reth deriving from L1.
tn bridge-funds --ethamount 50 --wait >/dev/null 2>&1
"$HARNESS" capture >"$WORKDIR/walk-capture.log" 2>&1 || { record "deploy: CAPTURE-FAIL"; exit 1; }
"$HARNESS" run >"$WORKDIR/walk-run.log" 2>&1

# Baseline: let arb-reth catch up to the freshly-deployed chain and confirm parity at START.
drive 4
for _ in $(seq 1 30); do a=$(tip "$ARB"); [ "$a" -gt 0 ] && [ "$a" -ge "$(( $(tip "$L2") - 2 ))" ] && break; sleep 4; done
if "$HARNESS" compare 2>&1 | grep -q "^OK"; then record "genesis ArbOS $START: PASS ($(tip "$ARB")+ blocks)"
else record "genesis ArbOS $START: FAIL ($("$HARNESS" compare 2>&1 | tail -1))"; exit 1; fi

# ---- walk upgrades in place ----
current="$START"
for target in $STEPS; do
  log "==================== upgrade $current -> $target ===================="
  # Owner schedules the upgrade for immediate effect (timestamp 0 => next block applies it).
  tn send-l2 --from l2owner --to "address_$ARB_OWNER" --data "$(sched_calldata "$target")" --ethamount 0 --wait \
    >"$WORKDIR/walk-sched-$target.log" 2>&1
  # Drive traffic until arb-reth derives past the crossing (ArbOS == target) or we give up.
  crossed=0
  for _ in $(seq 1 25); do
    drive 3
    sleep 5
    [ "$(arbos "$ARB")" -ge "$target" ] && { crossed=1; break; }
  done
  out="$("$HARNESS" compare 2>&1)"; last="$(echo "$out" | tail -1)"
  tv="$(arbos "$L2")"; av="$(arbos "$ARB")"
  if [ "$crossed" = 1 ] && echo "$out" | grep -q "^OK"; then
    record "upgrade $current -> $target: PASS (arb-reth ArbOS $av, ${last#OK: })"
  else
    record "upgrade $current -> $target: FAIL (arb-reth ArbOS $av, testnode $tv; $last)"
    log "stopping walk: divergence or missed crossing at $target (see $WORKDIR/arb-reth.log)"
    break
  fi
  current="$target"
done

echo; echo "==================== UPGRADE WALK MATRIX ===================="
cat "$RESULTS"
