#!/usr/bin/env bash
# CoW audit before dropped-schema tip reclaim.
#
# Boots only a copy of real data. The copy is an explicit lastdb-dev home
# (USE_LASTDB_DEV=1 plus LASTDB_DEV_HOME) or an equivalent clone of
# PRIMARY_HOME. The live primary home and its socket are refused.
#
# Before the first reap batch the driver stops the copy and writes a
# directory rollback. Restore is `lastdbd --data-dir <rollback>` with the
# same binary. No node upgrade and no new backup format.
#
# For every active catalog schema the driver saves one exact point read and
# one bounded range under one hash. It then runs bounded
# `db reap-dropped-schema` batches. Each batch stores a
# DroppedSchemaReapReport. The same reads run again after every batch.
# The run stops on refuse_scan, truncated-without-progress, or a read
# mismatch. It does not reap live-schema order-log rows or superseded
# version history.
#
#   PRIMARY_HOME=$HOME/.lastdb WORK_ROOT=/private/tmp/dsr \
#     DROPPED_SCHEMAS_FILE=/path/dropped.txt \
#     scripts/lastdbd/dropped-schema-tips-reclaim-cow-audit.sh
#
# EXECUTE=1 runs delete batches. The default is dry-run (no --execute).
# USE_LASTDB_DEV=1 clones LASTDB_DEV_HOME into WORK_HOME and boots only
# WORK_HOME. It does not boot the shared dev home.
set -euo pipefail

fail() {
  echo "RED dropped-schema-tips-reclaim-cow-audit: $*" >&2
  if [[ -n "${REPORT_DIR:-}" && -d "${REPORT_DIR}" ]]; then
    jq -n --arg error "$*" '{ok:false, error:$error, stopped:"fail"}' \
      >"$REPORT_DIR/proof.json" || true
  fi
  exit 1
}

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUN_ID="${1:-$(date -u +%Y%m%dT%H%M%SZ)}"
PRIMARY="${PRIMARY_HOME:-$HOME/.lastdb}"
WORK_ROOT="${WORK_ROOT:-/private/tmp/dsr}"
WORK_HOME="${WORK_HOME:-$WORK_ROOT/$RUN_ID/home}"
ROLLBACK_HOME="${ROLLBACK_HOME:-$WORK_ROOT/$RUN_ID/rollback}"
PROOF_ROOT="${PROOF_ROOT:-$HOME/.local/state/last-stack/dropped-schema-tips-reclaim-cow-audit}"
RUN_DIR="${RUN_DIR:-$PROOF_ROOT/runs/$RUN_ID}"
REPORT_DIR="$RUN_DIR/report"
LOG="$REPORT_DIR/audit.log"
LASTDB="${LASTDB:-$ROOT/target/debug/lastdb}"
LASTDBD="${LASTDBD:-$ROOT/target/debug/lastdbd}"
EXECUTE="${EXECUTE:-0}"
MAX_OPS="${MAX_OPS:-256}"
RANGE_LIMIT="${RANGE_LIMIT:-32}"
MAX_PASSES_PER_SCHEMA="${MAX_PASSES_PER_SCHEMA:-8}"
USE_LASTDB_DEV="${USE_LASTDB_DEV:-0}"
DROPPED_SCHEMAS_FILE="${DROPPED_SCHEMAS_FILE:-}"
KEEP_WORK_HOME="${KEEP_WORK_HOME:-0}"
ALLOW_OFFLINE_SOURCE="${ALLOW_OFFLINE_SOURCE:-0}"
DAEMON_PID=""
STOP_REASON=""
DIVERGENCE=0
CLONE_METHOD=""
ROLLBACK_METHOD=""
SOURCE_KIND="clone"
SOURCE_HOME=""

mkdir -p "$REPORT_DIR" "$REPORT_DIR/receipts"
if [[ "${PROOF_TEE:-0}" == "1" ]]; then
  exec > >(tee -a "$LOG") 2>&1
else
  exec >>"$LOG" 2>&1
fi

abs_path() {
  python3 - "$1" <<'PY'
import pathlib, sys
print(pathlib.Path(sys.argv[1]).expanduser().resolve(strict=False))
PY
}

is_same_or_child() {
  python3 - "$1" "$2" <<'PY'
import pathlib, sys
path = pathlib.Path(sys.argv[1]).expanduser().resolve(strict=False)
parent = pathlib.Path(sys.argv[2]).expanduser().resolve(strict=False)
try:
    path.relative_to(parent)
except ValueError:
    raise SystemExit(1)
raise SystemExit(0)
PY
}

refuse_live_primary() {
  local work="$1"
  local primary="$2"
  local label="$3"
  [[ -n "$work" ]] || fail "$label resolved empty"
  [[ "$work" != "/" ]] || fail "$label resolved to /"
  if is_same_or_child "$work" "$primary"; then
    fail "refusing $label under the live primary: $work"
  fi
  local name canonical
  for name in .lastdb .folddb; do
    canonical="$HOME/$name"
    if [[ -e "$canonical" ]] && is_same_or_child "$work" "$canonical"; then
      fail "refusing live primary home as $label: $work"
    fi
  done
  local primary_sock work_sock
  primary_sock="$(abs_path "$primary/data/folddb.sock")"
  work_sock="$(abs_path "$work/data/folddb.sock")"
  if [[ "$primary_sock" == "$work_sock" ]]; then
    fail "refusing live primary socket as $label: $work_sock"
  fi
}

refuse_socket_depth() {
  local work="$1"
  local socket_tmp="$work/data/folddb-full.sock.tmp"
  if (( ${#socket_tmp} > 103 )); then
    fail "work home is too deep for a Unix socket (${#socket_tmp} bytes): $socket_tmp"
  fi
}

snapshot_primary_pid() {
  if [[ -n "${PRIMARY_DAEMON_PID:-}" ]]; then
    printf '%s\n' "$PRIMARY_DAEMON_PID"
    return 0
  fi
  local sock="$PRIMARY/data/folddb.sock"
  if [[ -S "$sock" ]] && command -v lsof >/dev/null 2>&1; then
    lsof -n -t -- "$sock" 2>/dev/null | sort -u | head -1 || true
    return 0
  fi
  printf '\n'
}

file_fingerprint() {
  local path="$1"
  if [[ -f "$path" ]]; then
    cksum "$path" | awk '{print $1 ":" $2}'
  else
    printf 'missing\n'
  fi
}

stop_daemon() {
  if [[ -n "${DAEMON_PID}" ]] && kill -0 "$DAEMON_PID" 2>/dev/null; then
    kill "$DAEMON_PID" 2>/dev/null || true
    wait "$DAEMON_PID" 2>/dev/null || true
  fi
  DAEMON_PID=""
}

cleanup() {
  stop_daemon
  if [[ "$KEEP_WORK_HOME" != "1" && -n "${WORK_HOME:-}" && -d "$WORK_HOME" ]]; then
    if is_same_or_child "$WORK_HOME" "$WORK_ROOT" \
      && ! is_same_or_child "$WORK_HOME" "$PRIMARY"; then
      rm -rf "$WORK_HOME"
    fi
  fi
}
trap cleanup EXIT

run_lastdb() {
  printf '+ %s --data-dir %s %s\n' "$LASTDB" "$WORK_HOME" "$*" \
    >>"$REPORT_DIR/commands.log"
  "$LASTDB" --data-dir "$WORK_HOME" "$@"
}

curl_work() {
  printf '+ curl --unix-socket %s %s\n' "$WORK_SOCKET" "$*" \
    >>"$REPORT_DIR/commands.log"
  "$CURL" --fail --silent --show-error --max-time "${HTTP_TIMEOUT_SECS:-120}" \
    --unix-socket "$WORK_SOCKET" -H 'Host: localhost' "$@"
}

wait_for_node() {
  local tries="${NODE_WAIT_TRIES:-600}"
  local attempt=1
  while (( attempt <= tries )); do
    if [[ -e "$WORK_SOCKET" ]] \
      && run_lastdb status --json >"$REPORT_DIR/status-ready.json" \
        2>"$REPORT_DIR/status-ready.err"; then
      return 0
    fi
    if ! kill -0 "$DAEMON_PID" 2>/dev/null; then
      tail -80 "$REPORT_DIR/lastdbd.err" >&2 || true
      fail "isolated lastdbd exited before its socket became ready"
    fi
    sleep 1
    attempt=$((attempt + 1))
  done
  tail -80 "$REPORT_DIR/lastdbd.err" >&2 || true
  fail "isolated lastdbd did not become ready at $WORK_SOCKET"
}

start_daemon() {
  stop_daemon
  rm -f "$WORK_HOME/data/folddb.sock" "$WORK_HOME/data/folddb-full.sock"
  (
    unset LASTDB_SOCKET FOLDDB_SOCKET LASTDB_SOCKET_PATH FOLDDB_SOCKET_PATH
    exec env HOME="$WORK_HOME" LASTDB_HOME="$WORK_HOME" FOLDDB_HOME="$WORK_HOME" \
      FOLDDB_DISABLE_KEYCHAIN=1 "$LASTDBD" --data-dir "$WORK_HOME"
  ) >>"$REPORT_DIR/lastdbd.out" 2>>"$REPORT_DIR/lastdbd.err" &
  DAEMON_PID=$!
  wait_for_node
}

strip_cloud_sync() {
  local home="$1"
  if [[ -f "$home/cloud_sync.json" ]]; then
    mv "$home/cloud_sync.json" \
      "$home/cloud_sync.json.paused-by-dropped-schema-tip-audit"
  fi
}

clone_tree() {
  local src="$1"
  local dest="$2"
  local err="$3"
  refuse_live_primary "$dest" "$PRIMARY" "clone destination"
  if [[ -d "$dest" ]]; then
    rm -rf "$dest"
  fi
  mkdir -p "$(dirname "$dest")"
  if cp -cR "$src" "$dest" 2>"$err"; then
    printf 'apfs-clone\n'
  else
    rm -rf "$dest"
    cp -a "$src" "$dest"
    printf 'full-copy\n'
  fi
  rm -f "$dest/data/folddb.sock" "$dest/data/folddb-full.sock"
  strip_cloud_sync "$dest"
}

canon_point() {
  jq -c '{
    results: (
      (.results // [])
      | map({key: (.key // null), fields: (.fields // {})})
      | sort_by((.key.hash // .key // ""), (.key.range // ""))
    )
  }' "$1"
}

canon_range() {
  jq -c '{
    schema: (.schema // null),
    hash_filter: (.hash_filter // null),
    has_more: (.has_more // false),
    keys: (
      (.keys // [])
      | map({hash: (.hash // ""), range: (.range // "")})
      | sort_by(.hash, .range)
    )
  }' "$1"
}

append_json() {
  local file="$1"
  local entry="$2"
  jq --argjson entry "$entry" '. + [$entry]' "$file" >"$file.tmp"
  mv "$file.tmp" "$file"
}

load_active_schemas() {
  if ! curl_work "http://localhost/api/schemas?include_system=true" \
    >"$REPORT_DIR/schemas.json"; then
    fail "active schema catalog read failed"
  fi
  jq -e '.ready == true or .data.ready == true or ((.schemas // .data.schemas) | type == "array")' \
    "$REPORT_DIR/schemas.json" >/dev/null \
    || fail "active schema catalog was not ready"
  jq -r '
    (.schemas // .data.schemas // [])[]
    | select((.state // "Available") != "Blocked")
    | .name
  ' "$REPORT_DIR/schemas.json" | sort >"$REPORT_DIR/active-schemas.txt"
  if [[ ! -s "$REPORT_DIR/active-schemas.txt" ]]; then
    fail "catalog returned no active schema"
  fi
}

load_dropped_schemas() {
  [[ -n "$DROPPED_SCHEMAS_FILE" && -f "$DROPPED_SCHEMAS_FILE" ]] \
    || fail "DROPPED_SCHEMAS_FILE is required"
  : >"$REPORT_DIR/dropped-schemas.txt"
  while IFS= read -r line || [[ -n "$line" ]]; do
    case "$line" in
      ''|\#*) continue ;;
    esac
    printf '%s\n' "$line" >>"$REPORT_DIR/dropped-schemas.txt"
  done <"$DROPPED_SCHEMAS_FILE"
}

refuse_active_targets() {
  sort "$REPORT_DIR/dropped-schemas.txt" >"$REPORT_DIR/dropped.sorted"
  local overlap
  overlap="$(comm -12 "$REPORT_DIR/active-schemas.txt" "$REPORT_DIR/dropped.sorted" || true)"
  if [[ -n "$overlap" ]]; then
    fail "refusing to reap an active schema: ${overlap//$'\n'/, }"
  fi
}

capture_before() {
  local out="$REPORT_DIR/reads-before.json"
  printf '[]\n' >"$out"
  while IFS= read -r schema || [[ -n "$schema" ]]; do
    [[ -n "$schema" ]] || continue
    local disc="$REPORT_DIR/raw-discover.json"
    run_lastdb get-keys "$schema" --limit 1 --json >"$disc"
    local hash range
    hash="$(jq -r '.keys[0].hash // empty' "$disc")"
    range="$(jq -r '.keys[0].range // empty' "$disc")"
    local point_raw="$REPORT_DIR/raw-point.json"
    local range_raw="$REPORT_DIR/raw-range.json"
    local point_canon range_canon empty_json
    if [[ -z "$hash" ]]; then
      empty_json=true
      point_canon='{"results":[]}'
      range_canon="$(jq -nc --arg schema "$schema" \
        '{schema:$schema, hash_filter:null, has_more:false, keys:[], empty:true}')"
    else
      empty_json=false
      if [[ -n "$range" ]]; then
        run_lastdb get "$schema" --key-hash "$hash" --key-range "$range" --json >"$point_raw"
      else
        run_lastdb get "$schema" --key-hash "$hash" --json >"$point_raw"
      fi
      run_lastdb get-keys "$schema" --key-hash "$hash" --limit "$RANGE_LIMIT" --json \
        >"$range_raw"
      point_canon="$(canon_point "$point_raw")"
      range_canon="$(canon_range "$range_raw")"
    fi
    local entry
    entry="$(jq -nc \
      --arg schema "$schema" \
      --arg key_hash "$hash" \
      --arg key_range "$range" \
      --argjson empty "$empty_json" \
      --argjson point "$point_canon" \
      --argjson range_doc "$range_canon" \
      '{
        schema: $schema,
        key_hash: (if $key_hash == "" then null else $key_hash end),
        key_range: (if $key_range == "" then null else $key_range end),
        empty: $empty,
        point: $point,
        range: $range_doc
      }')"
    append_json "$out" "$entry"
  done <"$REPORT_DIR/active-schemas.txt"
}

replay_reads() {
  local before="$REPORT_DIR/reads-before.json"
  local out="$REPORT_DIR/reads-after.json"
  local count i schema hash range empty
  count="$(jq 'length' "$before")"
  printf '[]\n' >"$out"
  i=0
  while (( i < count )); do
    schema="$(jq -r ".[$i].schema" "$before")"
    hash="$(jq -r ".[$i].key_hash // empty" "$before")"
    range="$(jq -r ".[$i].key_range // empty" "$before")"
    empty="$(jq -r ".[$i].empty" "$before")"
    local point_raw="$REPORT_DIR/raw-point.json"
    local range_raw="$REPORT_DIR/raw-range.json"
    local point_canon range_canon
    if [[ "$empty" == "true" ]]; then
      run_lastdb get-keys "$schema" --limit 1 --json >"$range_raw"
      local left
      left="$(jq -r '(.keys // []) | length' "$range_raw")"
      if [[ "$left" == "0" ]]; then
        point_canon='{"results":[]}'
        range_canon="$(jq -nc --arg schema "$schema" \
          '{schema:$schema, hash_filter:null, has_more:false, keys:[], empty:true}')"
      else
        point_canon='{"results":[{"key":"unexpected","fields":{}}]}'
        range_canon="$(canon_range "$range_raw")"
      fi
    else
      if [[ -n "$range" ]]; then
        run_lastdb get "$schema" --key-hash "$hash" --key-range "$range" --json >"$point_raw"
      else
        run_lastdb get "$schema" --key-hash "$hash" --json >"$point_raw"
      fi
      run_lastdb get-keys "$schema" --key-hash "$hash" --limit "$RANGE_LIMIT" --json \
        >"$range_raw"
      point_canon="$(canon_point "$point_raw")"
      range_canon="$(canon_range "$range_raw")"
    fi
    local entry
    entry="$(jq -nc \
      --arg schema "$schema" \
      --arg key_hash "$hash" \
      --arg key_range "$range" \
      --argjson empty "$([[ "$empty" == "true" ]] && printf 'true' || printf 'false')" \
      --argjson point "$point_canon" \
      --argjson range_doc "$range_canon" \
      '{
        schema: $schema,
        key_hash: (if $key_hash == "" then null else $key_hash end),
        key_range: (if $key_range == "" then null else $key_range end),
        empty: $empty,
        point: $point,
        range: $range_doc
      }')"
    append_json "$out" "$entry"
    i=$((i + 1))
  done
  jq -S 'map({schema, key_hash, key_range, empty: .empty, point, range})' "$before" \
    >"$REPORT_DIR/reads-before.canon.json"
  jq -S 'map({schema, key_hash, key_range, empty: .empty, point, range})' "$out" \
    >"$REPORT_DIR/reads-after.canon.json"
  local mismatches
  mismatches="$(jq -c -n \
    --slurpfile before "$REPORT_DIR/reads-before.canon.json" \
    --slurpfile after "$REPORT_DIR/reads-after.canon.json" '
      [range(0; ($before[0] | length)) as $i
        | select($before[0][$i] != $after[0][$i])
        | $before[0][$i].schema]
    ')"
  DIVERGENCE="$(jq 'length' <<<"$mismatches")"
  jq -n \
    --argjson divergence "$DIVERGENCE" \
    --argjson mismatches "$mismatches" \
    '{divergence_count: $divergence, mismatches: $mismatches}' \
    >"$REPORT_DIR/point-read-report.json"
}

write_rollback() {
  stop_daemon
  refuse_live_primary "$ROLLBACK_HOME" "$PRIMARY" "rollback home"
  if is_same_or_child "$ROLLBACK_HOME" "$WORK_HOME" \
    || is_same_or_child "$WORK_HOME" "$ROLLBACK_HOME"; then
    fail "rollback home must be a sibling of the work home, not the work home"
  fi
  ROLLBACK_METHOD="$(clone_tree "$WORK_HOME" "$ROLLBACK_HOME" "$REPORT_DIR/rollback-clone.err")"
  cat >"$ROLLBACK_HOME/ROLLBACK_POINT" <<'EOF'
This directory is a home copy taken before dropped-schema tip reclaim.
Restore: stop the work daemon, then boot the same lastdbd with --data-dir
set to this directory. Do not upgrade the node. This is not a new backup format.
EOF
  jq -n \
    --arg path "$ROLLBACK_HOME" \
    --arg method "$ROLLBACK_METHOD" \
    --arg run_id "$RUN_ID" \
    '{
      path: $path,
      method: $method,
      run_id: $run_id,
      restore_without_node_upgrade: true,
      restore: "Stop the work daemon. Boot the same lastdbd with --data-dir set to path. Do not upgrade the node."
    }' >"$REPORT_DIR/rollback-point.json"
  start_daemon
}

reap_one() {
  local schema="$1"
  local out="$2"
  if [[ "$EXECUTE" == "1" ]]; then
    run_lastdb db reap-dropped-schema --schema "$schema" --max-ops "$MAX_OPS" --json --execute \
      >"$out"
  else
    run_lastdb db reap-dropped-schema --schema "$schema" --max-ops "$MAX_OPS" --json >"$out"
  fi
}

batch_progress() {
  local file="$1"
  jq -e '
    (has("schema") and has("dry_run") and has("refused_scan") and has("truncated")
      and has("tips_deleted"))
    and (.refused_scan | type == "boolean")
    and (.truncated | type == "boolean")
    and (.dry_run | type == "boolean")
    and (.tips_deleted | type == "number")
  ' "$file" >/dev/null
}

write_receipt() {
  local dry=true
  if [[ "$EXECUTE" == "1" ]]; then
    dry=false
  fi
  local stopped="null"
  if [[ -n "$STOP_REASON" ]]; then
    stopped="$(jq -n --arg s "$STOP_REASON" '$s')"
  fi
  jq -n \
    --argjson dry "$dry" \
    --argjson stopped "$stopped" \
    --slurpfile batches "$REPORT_DIR/batches.json" \
    '{
      tool: "dropped-schema-tips-reclaim-cow-audit",
      dry_run: $dry,
      stopped: $stopped,
      batches: $batches[0],
      tips_deleted: ([$batches[0][].tips_deleted] | add // 0),
      index_keys_deleted: ([$batches[0][].index_keys_deleted // 0] | add // 0)
    }' >"$REPORT_DIR/receipt.json"
}

write_proof() {
  local ok=true
  if [[ -n "$STOP_REASON" || "$DIVERGENCE" != "0" ]]; then
    ok=false
  fi
  local primary_used=false
  local socket_used=false
  jq -n \
    --argjson ok "$ok" \
    --arg stopped "$STOP_REASON" \
    --argjson execute "$([[ "$EXECUTE" == "1" ]] && printf 'true' || printf 'false')" \
    --arg source_kind "$SOURCE_KIND" \
    --arg work_home "$WORK_HOME" \
    --arg primary_home "$PRIMARY" \
    --arg rollback "$ROLLBACK_HOME" \
    --arg rollback_method "$ROLLBACK_METHOD" \
    --arg receipt_path "$REPORT_DIR/receipt.json" \
    --arg point_report "$REPORT_DIR/point-read-report.json" \
    --argjson divergence "$DIVERGENCE" \
    --argjson primary_used "$primary_used" \
    --argjson socket_used "$socket_used" \
    --slurpfile receipt_doc "$REPORT_DIR/receipt.json" \
    '{
      ok: $ok,
      stopped: (if $stopped == "" then null else $stopped end),
      execute: $execute,
      source_kind: $source_kind,
      work_home: $work_home,
      primary_home: $primary_home,
      guards: {
        primary_home_used: $primary_used,
        live_primary_socket_used: $socket_used,
        cow_home_only: true,
        live_schema_history_reaped: false
      },
      rollback: {
        path: $rollback,
        method: $rollback_method,
        restore_without_node_upgrade: true
      },
      receipt: $receipt_path,
      point_read_report: $point_report,
      divergence_count: $divergence,
      tips_deleted: ($receipt_doc[0].tips_deleted // 0)
    }' >"$REPORT_DIR/proof.json"
}

positive_int() {
  local name="$1"
  local value="$2"
  case "$value" in
    ''|*[!0-9]*) fail "$name must be a positive integer" ;;
  esac
  if (( value < 1 )); then
    fail "$name must be >= 1"
  fi
}

CURL="${CURL:-curl}"
PRIMARY="$(abs_path "$PRIMARY")"
WORK_ROOT="$(abs_path "$WORK_ROOT")"
WORK_HOME="$(abs_path "$WORK_HOME")"
ROLLBACK_HOME="$(abs_path "$ROLLBACK_HOME")"
WORK_SOCKET="$WORK_HOME/data/folddb.sock"

[[ -x "$LASTDB" ]] || fail "LASTDB is not executable: $LASTDB"
[[ -x "$LASTDBD" ]] || fail "LASTDBD is not executable: $LASTDBD"
command -v jq >/dev/null 2>&1 || fail "jq is required"
command -v "$CURL" >/dev/null 2>&1 || [[ -x "$CURL" ]] || fail "curl is required: $CURL"
positive_int MAX_OPS "$MAX_OPS"
positive_int RANGE_LIMIT "$RANGE_LIMIT"
positive_int MAX_PASSES_PER_SCHEMA "$MAX_PASSES_PER_SCHEMA"
[[ "$EXECUTE" == "0" || "$EXECUTE" == "1" ]] || fail "EXECUTE must be 0 or 1"
refuse_live_primary "$WORK_HOME" "$PRIMARY" "work home"
refuse_live_primary "$ROLLBACK_HOME" "$PRIMARY" "rollback home"
is_same_or_child "$WORK_HOME" "$WORK_ROOT" \
  || fail "WORK_HOME must stay under WORK_ROOT"
refuse_socket_depth "$WORK_HOME"

if [[ "$USE_LASTDB_DEV" == "1" ]]; then
  SOURCE_HOME="${LASTDB_DEV_HOME:-}"
  [[ -n "$SOURCE_HOME" ]] || fail "USE_LASTDB_DEV=1 requires LASTDB_DEV_HOME"
  SOURCE_HOME="$(abs_path "$SOURCE_HOME")"
  SOURCE_KIND="lastdb-dev"
  refuse_live_primary "$SOURCE_HOME" "$PRIMARY" "lastdb-dev home"
else
  SOURCE_HOME="$PRIMARY"
  SOURCE_KIND="clone"
fi
[[ "$SOURCE_HOME" != "$WORK_HOME" ]] || fail "work home must not be the clone source"

[[ -d "$SOURCE_HOME" ]] || fail "clone source missing: $SOURCE_HOME"
[[ -f "$SOURCE_HOME/identity.key" ]] || fail "clone source has no identity.key: $SOURCE_HOME"
PRIMARY_PID="$(snapshot_primary_pid)"
if [[ -z "$PRIMARY_PID" && "$ALLOW_OFFLINE_SOURCE" != "1" && "$USE_LASTDB_DEV" != "1" ]]; then
  fail "source daemon pid not found; set ALLOW_OFFLINE_SOURCE=1 only for an offline fixture"
fi
IDENTITY_BEFORE="$(file_fingerprint "$PRIMARY/identity.key")"
SOURCE_IDENTITY_BEFORE="$(file_fingerprint "$SOURCE_HOME/identity.key")"

if [[ "${REUSE_WORK_HOME:-0}" == "1" && -d "$WORK_HOME/identity.key" ]]; then
  echo "Reusing existing clone: $WORK_HOME"
  CLONE_METHOD="reused"
  strip_cloud_sync "$WORK_HOME"
  rm -f "$WORK_HOME/data/folddb.sock" "$WORK_HOME/data/folddb-full.sock"
else
  CLONE_METHOD="$(clone_tree "$SOURCE_HOME" "$WORK_HOME" "$REPORT_DIR/clone.err")"
  echo "Clone method $CLONE_METHOD from $SOURCE_KIND $SOURCE_HOME"
fi
refuse_live_primary "$WORK_HOME" "$PRIMARY" "work home"

start_daemon
load_active_schemas
load_dropped_schemas
refuse_active_targets
capture_before
write_rollback

printf '[]\n' >"$REPORT_DIR/batches.json"
batch_n=0
while IFS= read -r schema || [[ -n "$schema" ]]; do
  [[ -n "$schema" ]] || continue
  pass=0
  while :; do
    pass=$((pass + 1))
    if (( pass > MAX_PASSES_PER_SCHEMA )); then
      STOP_REASON="batch_cap"
      break
    fi
    batch_n=$((batch_n + 1))
    batch_file="$REPORT_DIR/receipts/batch-$(printf '%03d' "$batch_n").json"
    reap_one "$schema" "$batch_file"
    batch_progress "$batch_file" || fail "reap report missing required fields: $schema"
    local_dry="$(jq -r '.dry_run' "$batch_file")"
    local_tips="$(jq -r '.tips_deleted' "$batch_file")"
    local_index="$(jq -r '.index_keys_deleted // 0' "$batch_file")"
    local_refused="$(jq -r '.refused_scan' "$batch_file")"
    local_truncated="$(jq -r '.truncated' "$batch_file")"
    if [[ "$EXECUTE" == "1" && "$local_dry" != "false" ]]; then
      fail "execute batch returned dry_run for $schema"
    fi
    if [[ "$EXECUTE" != "1" ]]; then
      if [[ "$local_dry" != "true" || "$local_tips" != "0" || "$local_index" != "0" ]]; then
        STOP_REASON="dry_run_deleted"
        jq --slurpfile row "$batch_file" '. + [$row[0]]' "$REPORT_DIR/batches.json" \
          >"$REPORT_DIR/batches.json.tmp"
        mv "$REPORT_DIR/batches.json.tmp" "$REPORT_DIR/batches.json"
        break
      fi
    fi
    jq --slurpfile row "$batch_file" '. + [$row[0]]' "$REPORT_DIR/batches.json" \
      >"$REPORT_DIR/batches.json.tmp"
    mv "$REPORT_DIR/batches.json.tmp" "$REPORT_DIR/batches.json"
    if [[ "$local_refused" == "true" ]]; then
      replay_reads
      STOP_REASON="refuse_scan"
      break
    fi
    if [[ "$local_truncated" == "true" && "$local_tips" == "0" && "$local_index" == "0" ]]; then
      replay_reads
      STOP_REASON="truncated_without_progress"
      break
    fi
    replay_reads
    if [[ "$DIVERGENCE" != "0" ]]; then
      STOP_REASON="read_mismatch"
      break
    fi
    if [[ "$local_truncated" != "true" ]]; then
      break
    fi
  done
  if [[ -n "$STOP_REASON" ]]; then
    break
  fi
done <"$REPORT_DIR/dropped-schemas.txt"

if [[ ! -f "$REPORT_DIR/reads-after.json" ]]; then
  replay_reads
fi
if [[ -z "$STOP_REASON" && "$DIVERGENCE" != "0" ]]; then
  STOP_REASON="read_mismatch"
fi

write_receipt
IDENTITY_AFTER="$(file_fingerprint "$PRIMARY/identity.key")"
SOURCE_IDENTITY_AFTER="$(file_fingerprint "$SOURCE_HOME/identity.key")"
[[ "$IDENTITY_BEFORE" == "$IDENTITY_AFTER" ]] || fail "primary identity changed"
[[ "$SOURCE_IDENTITY_BEFORE" == "$SOURCE_IDENTITY_AFTER" ]] \
  || fail "clone source identity changed"
if [[ -n "$PRIMARY_PID" ]]; then
  kill -0 "$PRIMARY_PID" 2>/dev/null || fail "source daemon pid exited"
fi

write_proof
stop_daemon
if [[ -n "$STOP_REASON" || "$DIVERGENCE" != "0" ]]; then
  echo "FAIL dropped-schema-tips-reclaim-cow-audit stopped=$STOP_REASON divergence=$DIVERGENCE"
  exit 1
fi
echo "PASS dropped-schema-tips-reclaim-cow-audit report=$REPORT_DIR/proof.json"
