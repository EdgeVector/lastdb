#!/usr/bin/env bash
# Read-after-ack conformance bar on a FRESH EMPTY home with synthetic data.
# It never copies or touches ~/.lastdb.
#
# North Star: north-star-lastdb-no-stale-reads
#
# Each cell does a mutation over /api/mutation, then reads with zero settle and
# records whether the post-ack state was visible. Every rep uses its own fresh
# hash key, so no rep can pass because an earlier rep warmed the same key.
#
# Exit: 0 = all cells GREEN, 2 = at least one cell RED, 1 = could not measure.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"

usage() {
  cat <<'EOF'
Usage: read-after-ack-bar.sh --bin PATH [--mode off|read|write] [--reps N]
                             [--label TEXT] [--json-out PATH] [--status-dir DIR]
                             [--keep] [--print-probe-config]
                             [-- ENV=VAL ...]

Boots lastdbd on a fresh empty home (mktemp under /private/tmp), mirrors the
live LaunchAgent's LASTDB_* environment, sets LASTDB_RESIDENT_MODE=--mode
(default write), declares the synthetic schema validation/MiniFreshInstall
(HashRange: bucket, id, title, body), and runs these cells with zero settle:

  create x point-get / range-list / count
  update x point-get / range-list
  delete x point-get / range-list / count

Count compares returned_count of a HashKey range query before and after the
mutation. The response carries total_count as null for a keyed read, so
returned_count is the count signal.

Extra ENV=VAL words after `--` go to the node (one-env-at-a-time experiments).
--status-dir writes the full /api/status JSON before and after the run.
The first raw response of every stale cell is printed as sample_response.

Exit: 0 = PASS, 2 = RED, 1 = could not measure.
EOF
}

BIN=""
MODE="write"
REPS=20
LABEL=""
JSON_OUT=""
STATUS_DIR=""
KEEP=0
PRINT_PROBE_CONFIG=0
NODE_ENV=()

while [ "$#" -gt 0 ]; do
  case "$1" in
    --bin) BIN="$2"; shift 2 ;;
    --mode) MODE="$2"; shift 2 ;;
    --reps) REPS="$2"; shift 2 ;;
    --label) LABEL="$2"; shift 2 ;;
    --json-out) JSON_OUT="$2"; shift 2 ;;
    --status-dir) STATUS_DIR="$2"; shift 2 ;;
    --keep) KEEP=1; shift ;;
    --print-probe-config) PRINT_PROBE_CONFIG=1; shift ;;
    --source) echo "--source is gone: the bar never copies a home" >&2; exit 64 ;;
    -h|--help) usage; exit 0 ;;
    --) shift; NODE_ENV=("$@"); break ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 64 ;;
  esac
done

case "$MODE" in off|read|write) ;; *) echo "--mode must be off|read|write" >&2; exit 64 ;; esac
LABEL="${LABEL:-read-after-ack-$MODE}"

log() { printf 'read-after-ack-bar: %s\n' "$*" >&2; }

# shellcheck source=lib/synthetic-node.sh
. "$HERE/lib/synthetic-node.sh"

if [ "$PRINT_PROBE_CONFIG" = "1" ]; then
  syn_load_env_pairs
  echo "launchd_plist=$SYN_LAUNCHD_PLIST"
  echo "bin=${BIN:-}"
  echo "resident_mode=$MODE"
  echo "home=fresh empty home under /private/tmp (no copy)"
  echo "forced_env=LASTDB_SELF_METRICS_SAMPLE_SECS=1"
  if [ "${#SYN_ENV_PAIRS[@]}" -eq 0 ]; then
    echo "launchd_env=none"
  else
    printf 'launchd_env=%s\n' "${SYN_ENV_PAIRS[@]}"
  fi
  [ "${#NODE_ENV[@]}" -eq 0 ] || printf 'extra_env=%s\n' "${NODE_ENV[@]}"
  exit 0
fi

[ -n "$BIN" ] && [ -x "$BIN" ] || { echo "no runnable lastdbd (--bin?)" >&2; exit 1; }
[ -z "$STATUS_DIR" ] || mkdir -p "$STATUS_DIR"

trap syn_cleanup EXIT INT TERM
syn_boot "$BIN" "$MODE" ${NODE_ENV[@]+"${NODE_ENV[@]}"}
syn_declare

metric() {
  # $1 = field under .status.resident (or memory_budget); -1 when absent.
  syn_status | jq -r --arg f "$1" '.status.resident[$f] // .status.memory_budget[$f] // -1' 2>/dev/null || echo -1
}

capture_status() {
  [ -n "$STATUS_DIR" ] || return 0
  syn_status > "$STATUS_DIR/$LABEL.$1.json" 2>/dev/null || true
}

declare -A CELL_STALE
declare -A CELL_TOTAL
declare -A CELL_SAMPLE
LAST_BODY=""

record() {
  local cell="$1" stale="$2"
  CELL_TOTAL["$cell"]=$(( ${CELL_TOTAL["$cell"]:-0} + 1 ))
  if [ "$stale" = "1" ]; then
    CELL_STALE["$cell"]=$(( ${CELL_STALE["$cell"]:-0} + 1 ))
    if [ -z "${CELL_SAMPLE["$cell"]:-}" ]; then
      CELL_SAMPLE["$cell"]="$(printf '%s' "$LAST_BODY" | head -c 600)"
    fi
  fi
}

# point_has <bucket> <id> <title>: the title is visible at the key.
point_has() {
  LAST_BODY="$(syn_query_point "$1" "$2")"
  printf '%s' "$LAST_BODY" | jq -e --arg t "$3" '.ok == true and ((.results // []) | map(.fields.title) | index($t)) != null' >/dev/null 2>&1
}

# point_absent <bucket> <id>: the key answers ok with no record.
point_absent() {
  LAST_BODY="$(syn_query_point "$1" "$2")"
  printf '%s' "$LAST_BODY" | jq -e '.ok == true and ((.results // []) | length) == 0' >/dev/null 2>&1
}

# range_has <bucket> <id>: the id is in the hash's range list.
range_has() {
  LAST_BODY="$(syn_query_range "$1")"
  printf '%s' "$LAST_BODY" | jq -e --arg id "$2" '.ok == true and ((.results // []) | map(.key.range) | index($id)) != null' >/dev/null 2>&1
}

range_lacks() {
  LAST_BODY="$(syn_query_range "$1")"
  printf '%s' "$LAST_BODY" | jq -e --arg id "$2" '.ok == true and ((.results // []) | map(.key.range) | index($id)) == null' >/dev/null 2>&1
}

# range_title <bucket> <id> <title>: the range list shows the new title.
range_title() {
  LAST_BODY="$(syn_query_range "$1")"
  printf '%s' "$LAST_BODY" | jq -e --arg id "$2" --arg t "$3" '.ok == true and ((.results // []) | map(select(.key.range == $id) | .fields.title) | index($t)) != null' >/dev/null 2>&1
}

range_count() {
  syn_query_range "$1" | jq -r '.returned_count // -1' 2>/dev/null || echo -1
}

# check <cell> <fn> args...: run the read; a failed setup mutation is stale too.
check() {
  local cell="$1"
  shift
  if "$@"; then record "$cell" 0; else record "$cell" 1; fi
}

STAMP="$(date +%H%M%S)$$"

capture_status before
PERSIST_BEFORE="$(metric persist_enqueued)"
FAIL_BEFORE="$(metric deferred_persist_failed)"

log "running $REPS reps per cell on fresh hash keys (zero settle)"
k=0
while [ "$k" -lt "$REPS" ]; do
  k=$((k + 1))
  b="b$STAMP-$k"

  # create x point-get
  if syn_mutate create "$b-cp" r1 "c$k"; then check "create×point-get" point_has "$b-cp" r1 "c$k"; else LAST_BODY="$SYN_LAST_BODY"; record "create×point-get" 1; fi

  # create x range-list
  if syn_mutate create "$b-cr" r1 "c$k"; then check "create×range-list" range_has "$b-cr" r1; else LAST_BODY="$SYN_LAST_BODY"; record "create×range-list" 1; fi

  # create x count: one row exists, a second create must raise the count to 2
  if syn_mutate create "$b-cc" r1 "c$k" && syn_mutate create "$b-cc" r2 "c$k"; then
    n="$(range_count "$b-cc")"
    LAST_BODY="returned_count=$n expected=2"
    if [ "$n" = "2" ]; then record "create×count" 0; else record "create×count" 1; fi
  else
    LAST_BODY="$SYN_LAST_BODY"; record "create×count" 1
  fi

  # update x point-get
  if syn_mutate create "$b-up" r1 "old$k" && syn_mutate update "$b-up" r1 "new$k"; then check "update×point-get" point_has "$b-up" r1 "new$k"; else LAST_BODY="$SYN_LAST_BODY"; record "update×point-get" 1; fi

  # update x range-list
  if syn_mutate create "$b-ur" r1 "old$k" && syn_mutate update "$b-ur" r1 "new$k"; then check "update×range-list" range_title "$b-ur" r1 "new$k"; else LAST_BODY="$SYN_LAST_BODY"; record "update×range-list" 1; fi

  # delete x point-get
  if syn_mutate create "$b-dp" r1 "d$k" && syn_mutate delete "$b-dp" r1; then check "delete×point-get" point_absent "$b-dp" r1; else LAST_BODY="$SYN_LAST_BODY"; record "delete×point-get" 1; fi

  # delete x range-list
  if syn_mutate create "$b-dr" r1 "d$k" && syn_mutate delete "$b-dr" r1; then check "delete×range-list" range_lacks "$b-dr" r1; else LAST_BODY="$SYN_LAST_BODY"; record "delete×range-list" 1; fi

  # delete x count: two rows, delete one, the count must fall to 1
  if syn_mutate create "$b-dc" r1 "d$k" && syn_mutate create "$b-dc" r2 "d$k" && syn_mutate delete "$b-dc" r1; then
    n="$(range_count "$b-dc")"
    LAST_BODY="returned_count=$n expected=1"
    if [ "$n" = "1" ]; then record "delete×count" 0; else record "delete×count" 1; fi
  else
    LAST_BODY="$SYN_LAST_BODY"; record "delete×count" 1
  fi
done

# The status sampler publishes once a second. Wait for a sample taken after
# the last ack, or the counters read zero for a run shorter than the interval.
syn_status_sampled_wait
PERSIST_AFTER="$(metric persist_enqueued)"
FAIL_AFTER="$(metric deferred_persist_failed)"
capture_status after

echo "label=$LABEL"
echo "mode=$MODE"
echo "bin=$BIN"
echo "reps=$REPS"
echo "schema=$SYN_SCHEMA_NAMESPACE/$SYN_SCHEMA_NAME hash_key=fresh per rep"
echo "persist_enqueued_before=$PERSIST_BEFORE"
echo "persist_enqueued_after=$PERSIST_AFTER"
echo "deferred_persist_failed_before=$FAIL_BEFORE"
echo "deferred_persist_failed_after=$FAIL_AFTER"

VERDICT="PASS"
RED_CELLS=()
for cell in $(printf '%s\n' "${!CELL_TOTAL[@]}" | sort); do
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

for cell in $(printf '%s\n' "${!CELL_SAMPLE[@]}" | sort); do
  echo "sample_response cell=$cell body=${CELL_SAMPLE[$cell]}"
done

if [ "$PERSIST_BEFORE" != "-1" ] && [ "$PERSIST_AFTER" != "-1" ]; then
  echo "persist_enqueued_delta=$((PERSIST_AFTER - PERSIST_BEFORE))"
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
    echo "  \"mode\": \"$MODE\","
    echo "  \"verdict\": \"$VERDICT\","
    echo "  \"reps\": $REPS,"
    echo "  \"persist_enqueued_before\": $PERSIST_BEFORE,"
    echo "  \"persist_enqueued_after\": $PERSIST_AFTER,"
    echo "  \"deferred_persist_failed_after\": $FAIL_AFTER,"
    echo "  \"cells\": {"
    first=1
    for cell in $(printf '%s\n' "${!CELL_TOTAL[@]}" | sort); do
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

[ "$VERDICT" = "PASS" ] && exit 0
exit 2
