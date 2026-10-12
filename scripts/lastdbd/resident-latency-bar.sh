#!/usr/bin/env bash
# Resident latency bar on a FRESH EMPTY home with synthetic data.
# It never copies or touches ~/.lastdb (Tom, 2026-10-12: disk).
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"

usage() {
  cat <<'EOF'
Usage: resident-latency-bar.sh --bin PATH [--mode off|read|write]
                               [--reads N] [--writes N] [--label TEXT]
                               [--json-out PATH] [--keep] [--print-probe-config]
                               [-- ENV=VAL ...]

Boots lastdbd on a fresh empty home (mktemp under /private/tmp), mirrors the
live LaunchAgent's LASTDB_* tuning, sets LASTDB_RESIDENT_MODE=--mode, declares
the synthetic schema validation/MiniFreshInstall (HashRange: bucket, id,
title, body), seeds one record, then measures:

  - point read:  POST /api/query HashRangeKey on the seeded record
                 (20 uncounted warmups, then --reads counted samples)
  - write ack:   POST /api/mutation update of the seeded record with a
                 unique title (--writes counted samples)
  - floor:       GET /api/status (20 samples), the curl+socket overhead

Reports p50/p95 per op, the cold_shard_loads delta across the read phase, and
a PASS/RED verdict against the bars (env-overridable):
  POINT_P50_BAR_MS (default 50), WRITE_P50_BAR_MS (default 10).

Extra ENV=VAL words after `--` go to the node (one-env-at-a-time experiments).
Synthetic data is tiny. Compare runs of the same bar on the same machine, not
numbers measured on a copy of real data.

Exit: 0 = PASS, 2 = RED, 1 = could not measure.
EOF
}

BIN=""
MODE="read"
READS=200
WRITES=100
LABEL=""
JSON_OUT=""
KEEP=0
PRINT_PROBE_CONFIG=0
NODE_ENV=()
POINT_P50_BAR_MS="${POINT_P50_BAR_MS:-50}"
WRITE_P50_BAR_MS="${WRITE_P50_BAR_MS:-10}"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --bin) BIN="$2"; shift 2 ;;
    --mode) MODE="$2"; shift 2 ;;
    --reads) READS="$2"; shift 2 ;;
    --writes) WRITES="$2"; shift 2 ;;
    --label) LABEL="$2"; shift 2 ;;
    --json-out) JSON_OUT="$2"; shift 2 ;;
    --keep) KEEP=1; shift ;;
    --print-probe-config) PRINT_PROBE_CONFIG=1; shift ;;
    --source) echo "--source is gone: the bar never copies a home" >&2; exit 64 ;;
    -h|--help) usage; exit 0 ;;
    --) shift; NODE_ENV=("$@"); break ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 64 ;;
  esac
done

case "$MODE" in off|read|write) ;; *) echo "--mode must be off|read|write" >&2; exit 64 ;; esac
LABEL="${LABEL:-mode-$MODE}"

log() { printf 'resident-bar: %s\n' "$*" >&2; }

# shellcheck source=lib/synthetic-node.sh
. "$HERE/lib/synthetic-node.sh"

if [ "$PRINT_PROBE_CONFIG" = "1" ]; then
  syn_load_env_pairs
  echo "launchd_plist=$SYN_LAUNCHD_PLIST"
  echo "bin=${BIN:-}"
  echo "resident_mode=$MODE"
  echo "home=fresh empty home under /private/tmp (no copy)"
  echo "launchd_nofile_limit=${SYN_NOFILE_LIMIT:-}"
  if [ "${#SYN_ENV_PAIRS[@]}" -eq 0 ]; then
    echo "launchd_env=none"
  else
    printf 'launchd_env=%s\n' "${SYN_ENV_PAIRS[@]}"
  fi
  [ "${#NODE_ENV[@]}" -eq 0 ] || printf 'extra_env=%s\n' "${NODE_ENV[@]}"
  exit 0
fi

[ -n "$BIN" ] && [ -x "$BIN" ] || { echo "no runnable lastdbd (--bin?)" >&2; exit 1; }

now_ms() { perl -MTime::HiRes=time -e 'printf "%d\n", time()*1000'; }

SAMPLES_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "$SAMPLES_DIR" 2>/dev/null || true
  syn_cleanup
}
trap cleanup EXIT INT TERM

syn_boot "$BIN" "$MODE" ${NODE_ENV[@]+"${NODE_ENV[@]}"}
syn_declare
syn_mutate create lat seed seed || { echo "seed record failed: $SYN_LAST_BODY" >&2; exit 1; }

status_cold_loads() {
  syn_status | jq -r '.status.read_cost.cold_shard_loads // -1' 2>/dev/null || echo -1
}

op_point() {
  syn_query_point lat seed | jq -e '.ok == true and ((.results // []) | length) == 1' >/dev/null 2>&1
}

op_write() {
  # $1 = unique token: distinct content, so every write makes a new atom and
  # moves the tip, the shape an ordinary app mutation pays.
  syn_mutate update lat seed "resident-bar $1"
}

op_floor() {
  syn_status >/dev/null 2>&1
}

# run_samples <fn> <count> <outfile> <label> [tokened]: wall ms per op, one per line.
run_samples() {
  local fn="$1" count="$2" outf="$3" label="$4" tokened="${5:-0}"
  local t0 t1 rc k fails=0
  : > "$outf"
  for k in $(seq 1 "$count"); do
    t0="$(now_ms)"
    rc=0
    if [ "$tokened" = "1" ]; then "$fn" "$$-${k}" || rc=$?; else "$fn" || rc=$?; fi
    t1="$(now_ms)"
    if [ "$rc" -eq 0 ]; then
      echo $((t1 - t0)) >> "$outf"
    else
      fails=$((fails + 1))
    fi
  done
  if [ "$fails" -gt 0 ]; then log "$label: $fails/$count samples failed"; fi
}

pct() {
  # $1 = samples file, $2 = percentile. Empty file gives -1.
  sort -n "$1" 2>/dev/null | awk -v p="$2" '
    { v[NR] = $1 }
    END {
      if (NR == 0) { print -1; exit }
      idx = int((p / 100) * NR + 0.999999); if (idx < 1) idx = 1; if (idx > NR) idx = NR
      print v[idx]
    }'
}

log "floor: 20 /api/status samples"
run_samples op_floor 20 "$SAMPLES_DIR/floor" "floor"
log "warmup: 20 uncounted point reads"
run_samples op_point 20 "$SAMPLES_DIR/warmup" "warmup"

COLD_BEFORE="$(status_cold_loads)"
log "point reads: $READS counted samples"
run_samples op_point "$READS" "$SAMPLES_DIR/point" "point"
COLD_AFTER="$(status_cold_loads)"

log "writes: $WRITES counted samples"
run_samples op_write "$WRITES" "$SAMPLES_DIR/write" "write" 1

FLOOR_P50="$(pct "$SAMPLES_DIR/floor" 50)"
POINT_P50="$(pct "$SAMPLES_DIR/point" 50)"
POINT_P95="$(pct "$SAMPLES_DIR/point" 95)"
WRITE_P50="$(pct "$SAMPLES_DIR/write" 50)"
WRITE_P95="$(pct "$SAMPLES_DIR/write" 95)"
COLD_DELTA=-1
if [ "$COLD_BEFORE" != "-1" ] && [ "$COLD_AFTER" != "-1" ]; then
  COLD_DELTA=$((COLD_AFTER - COLD_BEFORE))
fi

VERDICT="PASS"
if [ "$POINT_P50" = "-1" ] || [ "$WRITE_P50" = "-1" ]; then
  VERDICT="RED"
  log "no counted samples for point or write"
fi
if [ "$VERDICT" = "PASS" ] && [ "$POINT_P50" -gt "$POINT_P50_BAR_MS" ]; then VERDICT="RED"; fi
if [ "$VERDICT" = "PASS" ] && [ "$WRITE_P50" -gt "$WRITE_P50_BAR_MS" ]; then VERDICT="RED"; fi

echo "label=$LABEL"
echo "mode=$MODE"
echo "bin=$BIN"
echo "reads=$READS writes=$WRITES"
echo "floor_p50_ms=$FLOOR_P50"
echo "point_p50_ms=$POINT_P50"
echo "point_p95_ms=$POINT_P95"
echo "write_p50_ms=$WRITE_P50"
echo "write_p95_ms=$WRITE_P95"
echo "cold_shard_loads_delta_read_phase=$COLD_DELTA"
echo "point_bar_ms=$POINT_P50_BAR_MS write_bar_ms=$WRITE_P50_BAR_MS"
echo "verdict=$VERDICT"

if [ -n "$JSON_OUT" ]; then
  jq -n \
    --arg label "$LABEL" --arg mode "$MODE" --arg verdict "$VERDICT" \
    --argjson reads "$READS" --argjson writes "$WRITES" \
    --argjson floor_p50 "$FLOOR_P50" \
    --argjson point_p50 "$POINT_P50" --argjson point_p95 "$POINT_P95" \
    --argjson write_p50 "$WRITE_P50" --argjson write_p95 "$WRITE_P95" \
    --argjson cold_delta "$COLD_DELTA" \
    --argjson point_bar "$POINT_P50_BAR_MS" --argjson write_bar "$WRITE_P50_BAR_MS" \
    '{label: $label, mode: $mode, verdict: $verdict, reads: $reads, writes: $writes,
      floor_p50_ms: $floor_p50, point_p50_ms: $point_p50, point_p95_ms: $point_p95,
      write_p50_ms: $write_p50, write_p95_ms: $write_p95,
      cold_shard_loads_delta_read_phase: $cold_delta,
      point_bar_ms: $point_bar, write_bar_ms: $write_bar}' >"$JSON_OUT"
  log "json -> $JSON_OUT"
fi

[ "$VERDICT" = "PASS" ] && exit 0
exit 2
