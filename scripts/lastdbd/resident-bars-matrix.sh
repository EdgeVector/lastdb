#!/usr/bin/env bash
# Run both resident bars for off, read and write, N times each, on fresh
# synthetic homes (no copy of ~/.lastdb), and print the receipts and medians.
#
# Usage: resident-bars-matrix.sh --bin PATH --out DIR [--runs 3] [--reads N]
#                                [--writes N] [--reps N]
#
# Writes DIR/lat-<mode>-<n>.{json,out} and DIR/raa-<mode>-<n>.{json,out}, and
# prints a table of median point/write latency and the read-after-ack matrix.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
BIN=""
OUT=""
RUNS=3
READS=200
WRITES=100
REPS=20

while [ "$#" -gt 0 ]; do
  case "$1" in
    --bin) BIN="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --runs) RUNS="$2"; shift 2 ;;
    --reads) READS="$2"; shift 2 ;;
    --writes) WRITES="$2"; shift 2 ;;
    --reps) REPS="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 64 ;;
  esac
done
[ -n "$BIN" ] && [ -x "$BIN" ] && [ -n "$OUT" ] || { echo "usage: --bin PATH --out DIR" >&2; exit 64; }
mkdir -p "$OUT"

for mode in off read write; do
  for n in $(seq 1 "$RUNS"); do
    "$HERE/resident-latency-bar.sh" --bin "$BIN" --mode "$mode" --label "$mode-$n" \
      --reads "$READS" --writes "$WRITES" \
      --json-out "$OUT/lat-$mode-$n.json" >"$OUT/lat-$mode-$n.out" 2>&1 || true
    "$HERE/read-after-ack-bar.sh" --bin "$BIN" --mode "$mode" --label "$mode-$n" \
      --reps "$REPS" --json-out "$OUT/raa-$mode-$n.json" --status-dir "$OUT" \
      >"$OUT/raa-$mode-$n.out" 2>&1 || true
  done
done

median() {
  sort -n | awk '{ v[NR] = $1 } END { if (NR == 0) print "-"; else print v[int((NR + 1) / 2)] }'
}

printf '%-6s %10s %10s %10s %10s\n' mode point_p50 point_p95 write_p50 write_p95
for mode in off read write; do
  row=()
  for field in point_p50_ms point_p95_ms write_p50_ms write_p95_ms; do
    row+=("$(for n in $(seq 1 "$RUNS"); do jq -r ".$field" "$OUT/lat-$mode-$n.json" 2>/dev/null; done | median)")
  done
  printf '%-6s %10s %10s %10s %10s\n' "$mode" "${row[@]}"
done

echo
echo "read-after-ack stale counts per cell (sum over runs; reps=$REPS x runs=$RUNS per cell)"
for mode in off read write; do
  echo "mode=$mode"
  for n in $(seq 1 "$RUNS"); do
    jq -r --arg n "$n" '.cells | to_entries | map("\(.key)=\(.value.stale)") | "  run " + $n + ": " + join(" ")' \
      "$OUT/raa-$mode-$n.json" 2>/dev/null || echo "  run $n: no result"
  done
  persist="$(for n in $(seq 1 "$RUNS"); do jq -r '.persist_enqueued_after - .persist_enqueued_before' "$OUT/raa-$mode-$n.json" 2>/dev/null; done | tr '\n' ' ')"
  failed="$(for n in $(seq 1 "$RUNS"); do jq -r '.deferred_persist_failed_after' "$OUT/raa-$mode-$n.json" 2>/dev/null; done | tr '\n' ' ')"
  echo "  persist_enqueued_delta: $persist"
  echo "  deferred_persist_failed: $failed"
done
