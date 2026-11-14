#!/usr/bin/env bash
# Runs the checked-in workload set (bench/workloads.json) against a fresh
# single-node on-disk store and a fresh 3-node local cluster, writing one
# JSON record per workload under $OUT/{single,cluster}/. Compare two runs
# with `datax bench compare OUT_A/single OUT_B/single`.
#
# Both targets always run. The script then exits non-zero if either run
# had a workload that did not complete or that counted more errors than
# its tolerance (`datax bench run --max-errors`, default 0), and repeats
# the failing workloads as its last lines.
#
#   make bench                      # full set, ~7 minutes
#   DURATION_SCALE=0.1 make bench   # a smoke run
#   OUT=/tmp/before make bench      # keep the records elsewhere
#   MAX_ERRORS=-1 make bench        # record, whatever the error counts
set -euo pipefail
cd "$(dirname "$0")/.."

OUT=${OUT:-bench-results/$(date -u +%Y%m%dT%H%M%SZ)}
DURATION_SCALE=${DURATION_SCALE:-1}
SET=${SET:-bench/workloads.json}
MAX_ERRORS=${MAX_ERRORS:-0}
DATAX=${DATAX:-bin/datax}
PG=${PG:-27433}
RPC=${RPC:-27257}
HTTP=${HTTP:-28080}
WORK=$(mktemp -d)
pids=()
cleanup() {
  for p in "${pids[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

[ -x "$DATAX" ] || go build -o "$DATAX" ./cmd/datax
mkdir -p "$OUT"

# run_set <target>: the workload set against the node listening on $PG.
# A failed run is remembered rather than fatal, so the other target's
# records are still written; its verdict lines are kept for the end.
failed=()
run_set() {
  if ! "$DATAX" bench run --set "$SET" --url "postgres://root@127.0.0.1:$PG/datax?sslmode=disable" \
    --server-url "http://127.0.0.1:$HTTP" --out "$OUT/$1" --duration-scale "$DURATION_SCALE" \
    --max-errors "$MAX_ERRORS" 2>&1 | tee "$WORK/$1.out"; then
    failed+=("$1")
  fi
}

wait_ready() { # url
  for _ in $(seq 1 100); do
    curl -fsS "$1/status" >/dev/null 2>&1 && return 0
    sleep 0.2
  done
  echo "node at $1 never became ready" >&2
  return 1
}

echo "== single node (on-disk store at $WORK/single)"
"$DATAX" init --dir "$WORK/single" --listen "127.0.0.1:$RPC" --pg-listen "127.0.0.1:$PG" --http-listen "127.0.0.1:$HTTP" >"$WORK/single.log" 2>&1 &
pids+=($!)
wait_ready "http://127.0.0.1:$HTTP"
run_set single
kill "${pids[0]}"; wait "${pids[0]}" 2>/dev/null || true; pids=()

echo
echo "== 3-node local cluster (in-memory)"
"$DATAX" demo --nodes 3 --pg-port "$PG" --rpc-port "$RPC" --http-port "$HTTP" >"$WORK/cluster.log" 2>&1 &
pids+=($!)
wait_ready "http://127.0.0.1:$HTTP"
run_set cluster

echo
echo "records under $OUT"
if [ "${#failed[@]}" -gt 0 ]; then
  # Last, so that whoever reads only the end of the output (the nightly's
  # standing issue quotes it) sees every failing workload of both targets.
  for target in "${failed[@]}"; do
    echo "== $target: workloads failed"
    grep -E '^(FAIL |error: )' "$WORK/$target.out" || true
  done
  exit 1
fi
