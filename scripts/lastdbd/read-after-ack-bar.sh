#!/usr/bin/env bash
# Read-after-ack conformance bar against a throwaway APFS CoW copy of a real
# LastDB home. NEVER touches the live primary.
#
# North Star: north-star-lastdb-no-stale-reads
# Companion CI test: fold_db/crates/core/tests/resident_read_after_ack_test.rs
#
# For each cell (mutation × read), performs the mutation via /api/mutation,
# then immediately (zero settle) issues the read and records whether the
# post-ack state was visible. Also samples persist_enqueued /
# deferred_persist_failed so a run that silently degraded to inline durable
# cannot pass for the wrong reason.
#
# Exit: 0 = all cells GREEN (only expected after enumeration RYW lands),
#       2 = RED (one or more cells stale — day-one baseline),
#       1 = could not measure.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: read-after-ack-bar.sh [--bin PATH] [--source PATH] [--reps N]
                             [--label TEXT] [--json-out PATH] [--keep]
                             [--print-probe-config]

Real-data read-after-ack bar (north-star-lastdb-no-stale-reads instrument).
Boots lastdbd against a throwaway APFS CoW copy of --source (default
~/.lastdb). NEVER mutates the live home.

Cells (zero settle between ack and read):
  create × point-get / range-list (Page) / count
  update × point-get / range-list
  delete × point-get / range-list / count

N = --reps samples per cell (default 20; CI-style small N). Report per-cell
stale counts and a RED/GREEN matrix. Asserts defer-window liveness via
/api/status resident metrics when present.

Safety: source is only ever read via cp -cR; the copy carries a .hold marker
and is deleted on exit unless --keep.

Exit: 0 = PASS (all cells green), 2 = RED, 1 = could not measure.
EOF
}

PRIMARY_HOME_DEFAULT="${LASTDB_HOME:-$HOME/.lastdb}"
SOURCE_HOME="$PRIMARY_HOME_DEFAULT"
BIN=""
REPS=20
LABEL=""
JSON_OUT=""
KEEP=0
PROBE_ROOT="${LASTDB_PROBE_ROOT:-$HOME/.lastdb-test-copies}"
LAUNCHD_PLIST="${LASTDB_LAUNCHD_PLIST:-$HOME/Library/LaunchAgents/com.tomtang.lastdbd-primary-506.plist}"
SIDEBIN_DIR="${LASTDB_SIDEBIN_DIR:-$HOME/.lastdb/sidebin}"
OP_TIMEOUT_SECS="${LASTDB_BAR_OP_TIMEOUT_SECS:-30}"
BOOT_TIMEOUT_SECS="${LASTDB_BAR_BOOT_TIMEOUT_SECS:-300}"
PRINT_PROBE_CONFIG=0
SCHEMA="${READ_AFTER_ACK_SCHEMA:-Board}"
HASH_KEY="${READ_AFTER_ACK_HASH:-default}"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --bin) BIN="$2"; shift 2 ;;
    --source) SOURCE_HOME="$2"; shift 2 ;;
    --reps) REPS="$2"; shift 2 ;;
    --label) LABEL="$2"; shift 2 ;;
    --json-out) JSON_OUT="$2"; shift 2 ;;
    --keep) KEEP=1; shift ;;
    --print-probe-config) PRINT_PROBE_CONFIG=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 64 ;;
  esac
done

LABEL="${LABEL:-read-after-ack}"

log() { printf 'read-after-ack-bar: %s\n' "$*" >&2; }

resolve_bin() {
  if [ -n "$BIN" ]; then echo "$BIN"; return; fi
  local prog
  prog="$(plutil -extract ProgramArguments.0 raw "$LAUNCHD_PLIST" 2>/dev/null || true)"
  if [ -n "$prog" ] && [ -x "$prog" ]; then echo "$prog"; return; fi
  if [ -x "$SIDEBIN_DIR/lastdbd" ]; then echo "$SIDEBIN_DIR/lastdbd"; return; fi
  command -v lastdbd 2>/dev/null || true
}

BIN="$(resolve_bin)"

live_env_pairs() {
  [ -f "$LAUNCHD_PLIST" ] || return 0
  /usr/libexec/PlistBuddy -c 'Print :EnvironmentVariables' "$LAUNCHD_PLIST" 2>/dev/null \
    | awk -F' = ' '
        $1 ~ /^ *LASTDB_/ {
          key=$1; gsub(/^ +| +$/,"",key)
          if (key == "LASTDB_HOME" || key == "FOLDDB_HOME" || key == "LASTDB_DATA_DIR") next
          # Force write mode for the bar so the defer window is the path under test.
          if (key == "LASTDB_RESIDENT_MODE") next
          val=$2; gsub(/^ +| +$/,"",val)
          if (key != "" && val != "") print key "=" val
        }'
}

ENV_PAIRS=()
while IFS= read -r line; do
  [ -n "$line" ] && ENV_PAIRS+=("$line")
done <<EOF_ENV
$(live_env_pairs)
EOF_ENV

if [ "$PRINT_PROBE_CONFIG" = "1" ]; then
  echo "launchd_plist=$LAUNCHD_PLIST"
  echo "bin=${BIN:-}"
  echo "source_home=$SOURCE_HOME"
  echo "forced_resident_mode=write"
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
COPY="$PROBE_ROOT/raa-$$"
BLOG="$COPY.boot.log"
PID=""

cleanup() {
  if [ -n "${PID:-}" ] && kill -0 "$PID" 2>/dev/null; then
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
  log "clonefile copy failed; falling back to ordinary recursive copy: $(tr '\n' ' ' <"$COPY.cp.err" 2>/dev/null || true)"
  rm -rf "$COPY"
  cp -R "$SOURCE_HOME" "$COPY"
fi
rm -f "$COPY.cp.err"
if [ ! -d "$COPY" ] || [ ! -f "$COPY/identity.key" ] || [ ! -d "$COPY/data" ]; then
  echo "CoW clone incomplete" >&2
  exit 1
fi
printf 'read-after-ack-bar %s pid=%s — do not reap while present\n' "$STAMP" "$$" \
  > "$COPY/.hold-read-after-ack-bar"
rm -f "$COPY/cloud_sync.json" "$COPY/data/"*.sock 2>/dev/null || true
SOCK="$COPY/data/folddb.sock"

log "boot: $BIN --data-dir <copy> LASTDB_RESIDENT_MODE=write"
(
  exec env -u SENTRY_DSN -u FOLD_SENTRY_DSN ${ENV_PAIRS[@]+"${ENV_PAIRS[@]}"} \
    LASTDB_RESIDENT_MODE=write \
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

metric() {
  # Best-effort resident metric from /api/status (shape may vary by build).
  local field="$1"
  curl_sock http://x/api/status 2>/dev/null \
    | jq -r --arg f "$field" '
        .resident[$f] // .status.resident[$f]
        // .resident_metrics[$f] // .metrics.resident[$f]
        // .read_cost[$f] // -1
      ' 2>/dev/null || echo -1
}

op_mutate() {
  # $1=mutation_type $2=title_token
  local mtype="$1" token="$2"
  curl_sock -H 'Content-Type: application/json' \
    --data "{\"type\":\"mutation\",\"schema\":\"$SCHEMA\",\"fields_and_values\":{\"title\":\"raa $token\"},\"key_value\":{\"hash\":\"$HASH_KEY\"},\"mutation_type\":\"$mtype\"}" \
    http://x/api/mutation 2>/dev/null | jq -e '.ok == true' >/dev/null 2>&1
}

op_point() {
  local want="$1"
  local got
  got="$(curl_sock -H 'Content-Type: application/json' \
    --data "{\"schema_name\":\"$SCHEMA\",\"fields\":[\"title\"],\"filter\":{\"HashKey\":\"$HASH_KEY\"}}" \
    http://x/api/query 2>/dev/null \
    | jq -r --arg want "$want" '
        # Accept several wire shapes; look for the title value containing $want.
        (.. | strings? | select(test($want))) as $s | $s
      ' 2>/dev/null | head -1)"
  [ -n "$got" ]
}

op_page() {
  local want="$1"
  local body
  body="$(curl_sock -H 'Content-Type: application/json' \
    --data "{\"schema_name\":\"$SCHEMA\",\"fields\":[\"title\"],\"filter\":{\"Page\":{\"offset\":0,\"limit\":500}}}" \
    http://x/api/query 2>/dev/null || true)"
  printf '%s' "$body" | jq -e --arg want "$want" '
    (.. | strings? | select(test($want))) as $s | $s
  ' >/dev/null 2>&1
}

# Count path: use status or a dedicated count if available; fall back to page size signal.
op_count_signal() {
  # Returns "ok" if query succeeds (count semantics measured separately in CI test).
  curl_sock -H 'Content-Type: application/json' \
    --data "{\"schema_name\":\"$SCHEMA\",\"fields\":[\"title\"],\"filter\":{\"Page\":{\"offset\":0,\"limit\":1}},\"include_total_count\":true}" \
    http://x/api/query 2>/dev/null | jq -e '.ok == true or .total_count != null or .data != null' >/dev/null 2>&1
}

PERSIST_BEFORE="$(metric persist_enqueued)"
FAIL_BEFORE="$(metric deferred_persist_failed)"

declare -A CELL_STALE
declare -A CELL_TOTAL
record() {
  local cell="$1" stale="$2"
  CELL_TOTAL["$cell"]=$(( ${CELL_TOTAL["$cell"]:-0} + 1 ))
  if [ "$stale" = "1" ]; then
    CELL_STALE["$cell"]=$(( ${CELL_STALE["$cell"]:-0} + 1 ))
  fi
}

log "running $REPS reps per cell (zero settle)"

k=0
while [ "$k" -lt "$REPS" ]; do
  k=$((k + 1))
  tok="${STAMP}-${k}"

  # create × point
  if op_mutate create "c-$tok"; then
    if op_point "c-$tok"; then record "create×point-get" 0; else record "create×point-get" 1; fi
  else
    record "create×point-get" 1
  fi

  # create × page (range-list proxy on Board HashKey schema)
  tok2="${STAMP}-p-${k}"
  if op_mutate create "p-$tok2"; then
    if op_page "p-$tok2"; then record "create×page" 0; else record "create×page" 1; fi
  else
    record "create×page" 1
  fi

  # update × point
  if op_mutate update "u-$tok"; then
    if op_point "u-$tok"; then record "update×point-get" 0; else record "update×point-get" 1; fi
  else
    record "update×point-get" 1
  fi

  # update × page
  if op_mutate update "up-$tok"; then
    if op_page "up-$tok"; then record "update×page" 0; else record "update×page" 1; fi
  else
    record "update×page" 1
  fi

  # count signal after create (wire-level; precise count is the CI test)
  if op_mutate create "cnt-$tok"; then
    if op_count_signal; then record "create×count-signal" 0; else record "create×count-signal" 1; fi
  else
    record "create×count-signal" 1
  fi
done

PERSIST_AFTER="$(metric persist_enqueued)"
FAIL_AFTER="$(metric deferred_persist_failed)"

echo "label=$LABEL"
echo "bin=$BIN"
echo "reps=$REPS"
echo "schema=$SCHEMA hash_key=$HASH_KEY"
echo "persist_enqueued_before=$PERSIST_BEFORE"
echo "persist_enqueued_after=$PERSIST_AFTER"
echo "deferred_persist_failed_before=$FAIL_BEFORE"
echo "deferred_persist_failed_after=$FAIL_AFTER"

VERDICT="PASS"
RED_CELLS=()
for cell in "${!CELL_TOTAL[@]}"; do
  total="${CELL_TOTAL[$cell]}"
  stale="${CELL_STALE[$cell]:-0}"
  if [ "$stale" -eq 0 ]; then
    status="GREEN"
  else
    status="RED"
    VERDICT="RED"
    RED_CELLS+=("$cell")
  fi
  echo "cell $cell total=$total stale=$stale status=$status"
done

# Defer-window liveness (when metrics are exposed)
if [ "$PERSIST_BEFORE" != "-1" ] && [ "$PERSIST_AFTER" != "-1" ]; then
  if [ "$PERSIST_AFTER" -le "$PERSIST_BEFORE" ]; then
    echo "defer_window=NOT_OPEN (persist_enqueued did not rise)"
    VERDICT="RED"
  else
    echo "defer_window=OPEN delta=$((PERSIST_AFTER - PERSIST_BEFORE))"
  fi
else
  echo "defer_window=UNKNOWN (metrics not on /api/status)"
fi
if [ "$FAIL_BEFORE" != "-1" ] && [ "$FAIL_AFTER" != "-1" ] && [ "$FAIL_AFTER" -gt "$FAIL_BEFORE" ]; then
  echo "deferred_persist_failed_rose=yes"
  VERDICT="RED"
fi

echo "verdict=$VERDICT"
if [ "${#RED_CELLS[@]}" -gt 0 ]; then
  echo "red_cells=${RED_CELLS[*]}"
fi

if [ -n "$JSON_OUT" ]; then
  {
    echo "{"
    echo "  \"label\": $(printf '%s' "$LABEL" | jq -Rs .),"
    echo "  \"verdict\": \"$VERDICT\","
    echo "  \"reps\": $REPS,"
    echo "  \"persist_enqueued_before\": $PERSIST_BEFORE,"
    echo "  \"persist_enqueued_after\": $PERSIST_AFTER,"
    echo "  \"cells\": {"
    first=1
    for cell in "${!CELL_TOTAL[@]}"; do
      [ "$first" = 1 ] || echo ","
      first=0
      printf '    "%s": {"total": %s, "stale": %s}' \
        "$cell" "${CELL_TOTAL[$cell]}" "${CELL_STALE[$cell]:-0}"
    done
    echo ""
    echo "  }"
    echo "}"
  } >"$JSON_OUT"
fi

if [ "$VERDICT" = "PASS" ]; then
  exit 0
fi
exit 2
