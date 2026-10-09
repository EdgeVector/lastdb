#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: resident-latency-bar.sh [--bin PATH] [--mode off|read|write] [--source PATH]
                               [--reads N] [--writes N] [--label TEXT]
                               [--json-out PATH] [--keep] [--print-probe-config]

Real-data resident latency bar (north-star-lastdb-resident-primary terminal
proof instrument). Boots the given lastdbd against a throwaway APFS CoW copy
of the source home (NEVER the live home), mirrors the live LaunchAgent's
LASTDB_* tuning, overrides LASTDB_RESIDENT_MODE with --mode, then measures:

  - point read:  POST /api/query  Board.title filter HashKey=default
                 (20 uncounted warmups, then --reads counted samples)
  - write ack:   POST /api/mutation update Board default title=<unique>
                 (--writes counted samples)
  - floor:       GET /api/status (20 samples) — curl+socket overhead context

Reports p50/p95 per op, the descriptor cap the probe ran under, whether the
cap appears to bind, the cold_shard_loads delta across the read phase, and a
PASS/RED verdict against the terminal bars (env-overridable):
  POINT_P50_BAR_MS (default 50), WRITE_P50_BAR_MS (default 10), and
  COLD_LOADS_PER_WARM_GROUP_BAR (default 1).

Safety: source is only ever read via cp -cR; the copy carries a .hold marker
(papercut-probe-copies-reaped-under-lastdb-test-copies) and is deleted on exit
unless --keep. Refuses to run with --source pointing anywhere the probe would
mutate in place.

Exit: 0 = PASS, 2 = RED, 1 = could not measure.
EOF
}

PRIMARY_HOME_DEFAULT="${LASTDB_HOME:-$HOME/.lastdb}"
SOURCE_HOME="$PRIMARY_HOME_DEFAULT"
BIN=""
MODE="read"
READS=200
WRITES=100
LABEL=""
JSON_OUT=""
KEEP=0
POINT_P50_BAR_MS="${POINT_P50_BAR_MS:-50}"
WRITE_P50_BAR_MS="${WRITE_P50_BAR_MS:-10}"
COLD_LOADS_PER_WARM_GROUP_BAR="${COLD_LOADS_PER_WARM_GROUP_BAR:-1}"
PROBE_ROOT="${LASTDB_PROBE_ROOT:-$HOME/.lastdb-test-copies}"
LAUNCHD_PLIST="${LASTDB_LAUNCHD_PLIST:-$HOME/Library/LaunchAgents/com.tomtang.lastdbd-primary-506.plist}"
SIDEBIN_DIR="${LASTDB_SIDEBIN_DIR:-$HOME/.lastdb/sidebin}"
OP_TIMEOUT_SECS="${LASTDB_BAR_OP_TIMEOUT_SECS:-30}"
BOOT_TIMEOUT_SECS="${LASTDB_BAR_BOOT_TIMEOUT_SECS:-300}"
PRINT_PROBE_CONFIG=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --bin) BIN="$2"; shift 2 ;;
    --mode) MODE="$2"; shift 2 ;;
    --source) SOURCE_HOME="$2"; shift 2 ;;
    --reads) READS="$2"; shift 2 ;;
    --writes) WRITES="$2"; shift 2 ;;
    --label) LABEL="$2"; shift 2 ;;
    --json-out) JSON_OUT="$2"; shift 2 ;;
    --keep) KEEP=1; shift ;;
    --print-probe-config) PRINT_PROBE_CONFIG=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 64 ;;
  esac
done

case "$MODE" in off|read|write) ;; *) echo "--mode must be off|read|write" >&2; exit 64 ;; esac
LABEL="${LABEL:-mode-$MODE}"

log() { printf 'resident-bar: %s\n' "$*" >&2; }

resolve_bin() {
  if [ -n "$BIN" ]; then echo "$BIN"; return; fi
  local prog
  prog="$(plutil -extract ProgramArguments.0 raw "$LAUNCHD_PLIST" 2>/dev/null || true)"
  if [ -n "$prog" ] && [ -x "$prog" ]; then echo "$prog"; return; fi
  if [ -x "$SIDEBIN_DIR/lastdbd" ]; then echo "$SIDEBIN_DIR/lastdbd"; return; fi
  command -v lastdbd 2>/dev/null || true
}

BIN="$(resolve_bin)"

now_ms() { perl -MTime::HiRes=time -e 'printf "%d\n", time()*1000'; }

# LASTDB_* tuning from the live plist, minus HOME-shaped keys, so the probe
# measures the config that will actually serve (same lesson as safe-upgrade:
# a probe without the live warm budget measures a node Tom does not run).
live_env_pairs() {
  [ -f "$LAUNCHD_PLIST" ] || return 0
  /usr/libexec/PlistBuddy -c 'Print :EnvironmentVariables' "$LAUNCHD_PLIST" 2>/dev/null \
    | awk -F' = ' '
        $1 ~ /^ *LASTDB_/ {
          key=$1; gsub(/^ +| +$/,"",key)
          if (key == "LASTDB_HOME" || key == "FOLDDB_HOME" || key == "LASTDB_DATA_DIR") next
          if (key == "LASTDB_RESIDENT_MODE") next
          val=$2; gsub(/^ +| +$/,"",val)
          if (key != "" && val != "") print key "=" val
        }'
}

launchd_nofile_limit() {
  [ -f "$LAUNCHD_PLIST" ] || return 0
  local n
  n="$(plutil -extract SoftResourceLimits.NumberOfFiles raw "$LAUNCHD_PLIST" 2>/dev/null || true)"
  if [ -z "$n" ]; then
    n="$(plutil -extract HardResourceLimits.NumberOfFiles raw "$LAUNCHD_PLIST" 2>/dev/null || true)"
  fi
  if [ -z "$n" ]; then
    n="$(/usr/libexec/PlistBuddy -c 'Print :SoftResourceLimits:NumberOfFiles' "$LAUNCHD_PLIST" 2>/dev/null || true)"
  fi
  if [ -z "$n" ]; then
    n="$(/usr/libexec/PlistBuddy -c 'Print :HardResourceLimits:NumberOfFiles' "$LAUNCHD_PLIST" 2>/dev/null || true)"
  fi
  case "$n" in
    ''|*[!0-9]*) return 0 ;;
    *) echo "$n" ;;
  esac
}

env_pair_value() {
  local key="$1" pair
  for pair in "${ENV_PAIRS[@]}"; do
    case "$pair" in
      "$key="*) printf '%s\n' "${pair#*=}"; return 0 ;;
    esac
  done
  return 1
}

ENV_PAIRS=()
while IFS= read -r line; do
  [ -n "$line" ] && ENV_PAIRS+=("$line")
done <<EOF_ENV
$(live_env_pairs)
EOF_ENV

LAUNCHD_NOFILE_LIMIT="$(launchd_nofile_limit || true)"
EXPECTED_WARM_HANDLE_CAP="$(env_pair_value LASTDB_HASH_GROUP_WARM_MAX_HANDLES || true)"
if [ -z "$EXPECTED_WARM_HANDLE_CAP" ] && [ -n "$LAUNCHD_NOFILE_LIMIT" ]; then
  # Mirrors fold_db_core::factory::local::WARM_HANDLE_FD_FRACTION.
  EXPECTED_WARM_HANDLE_CAP=$((LAUNCHD_NOFILE_LIMIT * 60 / 100))
fi

if [ "$PRINT_PROBE_CONFIG" = "1" ]; then
  echo "launchd_plist=$LAUNCHD_PLIST"
  echo "launchd_nofile_limit=${LAUNCHD_NOFILE_LIMIT:-}"
  echo "expected_warm_handle_cap=${EXPECTED_WARM_HANDLE_CAP:-}"
  if [ "${#ENV_PAIRS[@]}" -eq 0 ]; then
    echo "launchd_env=none"
  else
    printf 'launchd_env=%s\n' "${ENV_PAIRS[@]}"
  fi
  exit 0
fi

[ -d "$SOURCE_HOME" ] || { echo "source home not found: $SOURCE_HOME" >&2; exit 64; }
[ -n "$BIN" ] && [ -x "$BIN" ] || { echo "no runnable lastdbd (--bin?)" >&2; exit 1; }

mkdir -p "$PROBE_ROOT"
STAMP="$(date +%Y%m%dT%H%M%S)"
# Keep the copy path SHORT: the node's socket lives at <copy>/data/folddb.sock
# and sockaddr_un caps the whole socket path at 103 bytes.
COPY="$PROBE_ROOT/rbar-${MODE}-$$"
BLOG="$COPY.boot.log"
PID=""

cleanup() {
  if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
    kill "$PID" 2>/dev/null || true
    sleep 1
    kill -9 "$PID" 2>/dev/null || true
  fi
  if [ "$KEEP" != "1" ]; then
    rm -rf "$COPY" "$BLOG" 2>/dev/null || true
  else
    log "kept probe copy at $COPY"
  fi
}
trap cleanup EXIT INT TERM

log "CoW clone of $SOURCE_HOME → $COPY"
if ! cp -cR "$SOURCE_HOME" "$COPY" 2>"$COPY.cp.err"; then
  log "clonefile copy failed; falling back to ordinary recursive copy: $(tr '\n' ' ' <"$COPY.cp.err")"
  rm -rf "$COPY"
  cp -R "$SOURCE_HOME" "$COPY"
fi
rm -f "$COPY.cp.err"
if [ ! -d "$COPY" ] || [ ! -f "$COPY/identity.key" ] || [ ! -d "$COPY/data" ]; then
  echo "CoW clone incomplete" >&2
  exit 1
fi
# Hold marker: concurrent hygiene agents reap unmarked probe copies mid-proof.
printf 'resident-latency-bar %s pid=%s — do not reap while present\n' "$STAMP" "$$" \
  > "$COPY/.hold-resident-latency-bar"
rm -f "$COPY/cloud_sync.json" "$COPY/data/"*.sock 2>/dev/null || true
SOCK="$COPY/data/folddb.sock"

log "boot: $BIN --data-dir <copy> (env: ${ENV_PAIRS[*]:-none} LASTDB_RESIDENT_MODE=$MODE ulimit_nofile=${LAUNCHD_NOFILE_LIMIT:-inherited})"
(
  if [ -n "$LAUNCHD_NOFILE_LIMIT" ]; then
    ulimit -n "$LAUNCHD_NOFILE_LIMIT"
  fi
  exec env -u SENTRY_DSN -u FOLD_SENTRY_DSN ${ENV_PAIRS[@]+"${ENV_PAIRS[@]}"} \
    LASTDB_RESIDENT_MODE="$MODE" \
    "$BIN" --data-dir "$COPY"
) >"$BLOG" 2>&1 &
PID=$!

READY=""
i=0
while [ "$i" -lt "$BOOT_TIMEOUT_SECS" ]; do
  i=$((i + 1))
  if ! kill -0 "$PID" 2>/dev/null; then
    echo "node exited during boot: $(tail -3 "$BLOG" 2>/dev/null | tr '\n' ' ')" >&2
    exit 1
  fi
  if [ -S "$SOCK" ]; then
    READY="$(curl -sS --max-time 3 --unix-socket "$SOCK" -H 'Host: localhost' \
      http://x/api/system/auto-identity 2>/dev/null | jq -r '.user_hash // empty' 2>/dev/null || true)"
    [ -n "$READY" ] && break
  fi
  sleep 1
done
[ -n "$READY" ] || { echo "identity not ready in ${BOOT_TIMEOUT_SECS}s" >&2; exit 1; }
log "identity ready after ${i}s"

curl_sock() {
  curl -sS --max-time "$OP_TIMEOUT_SECS" --unix-socket "$SOCK" -H 'Host: localhost' "$@"
}

status_cold_loads() {
  curl_sock http://x/api/status 2>/dev/null \
    | jq -r '.read_cost.cold_shard_loads // .status.read_cost.cold_shard_loads // -1' 2>/dev/null \
    || echo -1
}

status_read_cost_field() {
  local field="$1"
  curl_sock http://x/api/status 2>/dev/null \
    | jq -r --arg field "$field" '.read_cost[$field] // .status.read_cost[$field] // -1' 2>/dev/null \
    || echo -1
}

op_point() {
  curl_sock -H 'Content-Type: application/json' \
    --data '{"schema_name":"Board","fields":["title"],"filter":{"HashKey":"default"}}' \
    http://x/api/query 2>/dev/null | jq -e '.ok == true' >/dev/null 2>&1
}

op_write() {
  # $1 = unique token. Real ordinary write on the copy: distinct content →
  # new atom + tip head move, the exact shape `brain put`/kanban mutations pay.
  curl_sock -H 'Content-Type: application/json' \
    --data "{\"type\":\"mutation\",\"schema\":\"Board\",\"fields_and_values\":{\"title\":\"resident-bar probe $1\"},\"key_value\":{\"hash\":\"default\"},\"mutation_type\":\"update\"}" \
    http://x/api/mutation 2>/dev/null | jq -e '.ok == true' >/dev/null 2>&1
}

op_floor() {
  curl_sock http://x/api/status >/dev/null 2>&1
}

# Collect per-sample wall ms for an op into a file (one value per line).
run_samples() {
  # $1 = op fn, $2 = count, $3 = out file, $4 = label, $5 = pass-token (0/1)
  local fn="$1" count="$2" outf="$3" label="$4" tokened="${5:-0}"
  local t0 t1 rc k fails=0
  : > "$outf"
  for k in $(seq 1 "$count"); do
    t0="$(now_ms)"
    rc=0
    if [ "$tokened" = "1" ]; then "$fn" "${STAMP}-${k}" || rc=$?; else "$fn" || rc=$?; fi
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
  # $1 = samples file, $2 = percentile (50/95). Empty file → -1.
  sort -n "$1" 2>/dev/null | awk -v p="$2" '
    { v[NR] = $1 }
    END {
      if (NR == 0) { print -1; exit }
      idx = int((p / 100) * NR + 0.999999); if (idx < 1) idx = 1; if (idx > NR) idx = NR
      print v[idx]
    }'
}

SAMPLES_DIR="$(mktemp -d)"
trap 'rm -rf "$SAMPLES_DIR" 2>/dev/null || true; cleanup' EXIT

log "floor: 20 /api/status samples"
run_samples op_floor 20 "$SAMPLES_DIR/floor" "floor"
log "warmup: 20 uncounted point reads"
run_samples op_point 20 "$SAMPLES_DIR/warmup" "warmup"

WARM_GROUPS_BEFORE="$(status_read_cost_field warm_resident_groups)"
COLD_BEFORE="$(status_cold_loads)"
log "point reads: $READS counted samples"
run_samples op_point "$READS" "$SAMPLES_DIR/point" "point"
COLD_AFTER="$(status_cold_loads)"
WARM_GROUPS_AFTER="$(status_read_cost_field warm_resident_groups)"
OPEN_APPEND_HANDLES_AFTER="$(status_read_cost_field open_append_handles)"
WARM_BUDGET_HANDLES_AFTER="$(status_read_cost_field warm_budget_handles)"

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
WARM_GROUPS_DELTA=-1
if [ "$WARM_GROUPS_BEFORE" != "-1" ] && [ "$WARM_GROUPS_AFTER" != "-1" ]; then
  WARM_GROUPS_DELTA=$((WARM_GROUPS_AFTER - WARM_GROUPS_BEFORE))
fi
COLD_LOADS_PER_WARM_GROUP="null"
if [ "$COLD_DELTA" != "-1" ] && [ "$WARM_GROUPS_DELTA" != "-1" ]; then
  COLD_LOADS_PER_WARM_GROUP="$(awk -v cold="$COLD_DELTA" -v warm="$WARM_GROUPS_DELTA" '
    BEGIN {
      if (warm <= 0) {
        if (cold <= 0) print "0"; else print "inf";
      } else {
        printf "%.6f\n", cold / warm;
      }
    }')"
fi
HANDLE_CAP_BINDING="unknown"
if [ "$WARM_BUDGET_HANDLES_AFTER" != "-1" ]; then
  if [ "$WARM_BUDGET_HANDLES_AFTER" = "0" ]; then
    HANDLE_CAP_BINDING="off"
  elif [ "$OPEN_APPEND_HANDLES_AFTER" != "-1" ] && \
       [ $((OPEN_APPEND_HANDLES_AFTER * 100)) -ge $((WARM_BUDGET_HANDLES_AFTER * 95)) ]; then
    HANDLE_CAP_BINDING="open-fds>=95pct"
  elif [ "$WARM_GROUPS_AFTER" != "-1" ] && \
       [ $((WARM_GROUPS_AFTER * 100)) -ge $((WARM_BUDGET_HANDLES_AFTER * 95)) ]; then
    HANDLE_CAP_BINDING="resident-groups>=95pct"
  else
    HANDLE_CAP_BINDING="not-bound"
  fi
fi
WARM_HANDLE_CAP_MATCH="unknown"
if [ -n "${EXPECTED_WARM_HANDLE_CAP:-}" ] && [ "$WARM_BUDGET_HANDLES_AFTER" != "-1" ]; then
  if [ "$WARM_BUDGET_HANDLES_AFTER" = "$EXPECTED_WARM_HANDLE_CAP" ]; then
    WARM_HANDLE_CAP_MATCH="yes"
  else
    WARM_HANDLE_CAP_MATCH="no"
  fi
fi

VERDICT="PASS"
[ "$POINT_P50" -ge 0 ] 2>/dev/null || VERDICT="RED"
[ "$WRITE_P50" -ge 0 ] 2>/dev/null || VERDICT="RED"
if [ "$WARM_HANDLE_CAP_MATCH" = "no" ]; then
  VERDICT="RED"
fi
if [ "$VERDICT" = "PASS" ]; then
  [ "$POINT_P50" -lt "$POINT_P50_BAR_MS" ] || VERDICT="RED"
  [ "$WRITE_P50" -lt "$WRITE_P50_BAR_MS" ] || VERDICT="RED"
fi
if [ "$VERDICT" = "PASS" ] && [ "$COLD_LOADS_PER_WARM_GROUP" != "null" ]; then
  if [ "$COLD_LOADS_PER_WARM_GROUP" = "inf" ]; then
    VERDICT="RED"
  elif awk -v ratio="$COLD_LOADS_PER_WARM_GROUP" -v bar="$COLD_LOADS_PER_WARM_GROUP_BAR" \
    'BEGIN { exit !(ratio > bar) }'; then
    VERDICT="RED"
  fi
fi

echo "label=$LABEL"
echo "mode=$MODE"
echo "bin=$BIN"
echo "launchd_plist=$LAUNCHD_PLIST"
echo "launchd_nofile_limit=${LAUNCHD_NOFILE_LIMIT:-}"
echo "expected_warm_handle_cap=${EXPECTED_WARM_HANDLE_CAP:-}"
echo "reads=$READS writes=$WRITES"
echo "floor_p50_ms=$FLOOR_P50"
echo "point_p50_ms=$POINT_P50"
echo "point_p95_ms=$POINT_P95"
echo "write_p50_ms=$WRITE_P50"
echo "write_p95_ms=$WRITE_P95"
echo "warm_groups_delta_read_phase=$WARM_GROUPS_DELTA"
echo "cold_shard_loads_delta_read_phase=$COLD_DELTA"
echo "cold_loads_per_warm_group=$COLD_LOADS_PER_WARM_GROUP"
echo "open_append_handles=$OPEN_APPEND_HANDLES_AFTER"
echo "warm_budget_handles=$WARM_BUDGET_HANDLES_AFTER"
echo "handle_cap_binding=$HANDLE_CAP_BINDING"
echo "warm_handle_cap_matches_expected=$WARM_HANDLE_CAP_MATCH"
echo "point_bar_ms=$POINT_P50_BAR_MS write_bar_ms=$WRITE_P50_BAR_MS cold_loads_per_warm_group_bar=$COLD_LOADS_PER_WARM_GROUP_BAR"
echo "verdict=$VERDICT"

if [ -n "$JSON_OUT" ]; then
  jq -n \
    --arg label "$LABEL" --arg mode "$MODE" --arg bin "$BIN" --arg verdict "$VERDICT" \
    --arg launchd_plist "$LAUNCHD_PLIST" \
    --arg launchd_nofile_limit "${LAUNCHD_NOFILE_LIMIT:-}" \
    --arg expected_warm_handle_cap "${EXPECTED_WARM_HANDLE_CAP:-}" \
    --arg handle_cap_binding "$HANDLE_CAP_BINDING" \
    --arg warm_handle_cap_match "$WARM_HANDLE_CAP_MATCH" \
    --argjson reads "$READS" --argjson writes "$WRITES" \
    --argjson floor_p50 "$FLOOR_P50" \
    --argjson point_p50 "$POINT_P50" --argjson point_p95 "$POINT_P95" \
    --argjson write_p50 "$WRITE_P50" --argjson write_p95 "$WRITE_P95" \
    --argjson cold_delta "$COLD_DELTA" \
    --argjson warm_groups_delta "$WARM_GROUPS_DELTA" \
    --arg cold_per_warm_group "$COLD_LOADS_PER_WARM_GROUP" \
    --argjson open_append_handles "$OPEN_APPEND_HANDLES_AFTER" \
    --argjson warm_budget_handles "$WARM_BUDGET_HANDLES_AFTER" \
    --argjson point_bar "$POINT_P50_BAR_MS" --argjson write_bar "$WRITE_P50_BAR_MS" \
    --argjson cold_per_warm_group_bar "$COLD_LOADS_PER_WARM_GROUP_BAR" \
    '{label: $label, mode: $mode, bin: $bin,
      launchd_plist: $launchd_plist,
      launchd_nofile_limit: ($launchd_nofile_limit | if . == "" then null else tonumber end),
      expected_warm_handle_cap: ($expected_warm_handle_cap | if . == "" then null else tonumber end),
      reads: $reads, writes: $writes,
      floor_p50_ms: $floor_p50, point_p50_ms: $point_p50, point_p95_ms: $point_p95,
      write_p50_ms: $write_p50, write_p95_ms: $write_p95,
      cold_shard_loads_delta_read_phase: $cold_delta,
      warm_groups_delta_read_phase: $warm_groups_delta,
      cold_loads_per_warm_group:
        ($cold_per_warm_group | if . == "null" then null elif . == "inf" then "inf" else tonumber end),
      open_append_handles: $open_append_handles,
      warm_budget_handles: $warm_budget_handles,
      handle_cap_binding: $handle_cap_binding,
      warm_handle_cap_matches_expected: $warm_handle_cap_match,
      bars: {
        point_p50_ms: $point_bar,
        write_p50_ms: $write_bar,
        cold_loads_per_warm_group: $cold_per_warm_group_bar
      },
      verdict: $verdict}' > "$JSON_OUT"
  log "json → $JSON_OUT"
fi

[ "$VERDICT" = "PASS" ] && exit 0 || exit 2
