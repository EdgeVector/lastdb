#!/usr/bin/env bash
# Isolated CoW harness for the order-log retention proof.
#
# The source home is read only. The harness clones it, removes inherited Cloud
# Sync configuration into the durable paused state, starts one daemon on the
# clone socket, pauses Cloud Sync in that daemon, and runs every mutation and
# admin command against the clone.
#
# A complete proof requires EXECUTE_COMPACTION=1. Without it, the harness runs
# the safe dry-run and writes a DRY_RUN report.
#
# Real-data example (the full job can take hours):
#   cargo build -p lastdb_node --bin lastdb --bin lastdbd
#   PRIMARY_HOME="$HOME/.lastdb" WORK_ROOT=/private/tmp/olr \
#     EXECUTE_COMPACTION=1 LASTDB=target/debug/lastdb \
#     LASTDBD=target/debug/lastdbd \
#     scripts/lastdbd/order-log-retention-cow-proof.sh
#
# Important environment:
#   PRIMARY_HOME          source home (default: $HOME/.lastdb)
#   WORK_ROOT             disposable root (default: /private/tmp/olr)
#   WORK_HOME             clone home (default: $WORK_ROOT/<run-id>/home)
#   PROOF_ROOT            durable reports outside the clone
#   DONE_WHEN_PROOF       validation-card proof file
#   EXECUTE_COMPACTION    1 runs logical and physical compaction (default: 0)
#   KEEP_WORK_HOME        1 preserves the clone for inspection (default: 0)
#   REUSE_WORK_HOME       1 reuses an existing clone (default: 0)
#   MAX_KEYS              compact-order-log page size (default: 256)
#   PROOF_SCHEMA_NAME     stable validation schema name
#   PRIMARY_DAEMON_PID    exact source daemon pid override
#   ALLOW_OFFLINE_SOURCE  1 permits a source with no daemon (fixture/dev only)
set -euo pipefail

fail() {
  echo "RED order-log-retention-cow-proof: $*" >&2
  exit 1
}

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUN_ID="${1:-$(date -u +%Y%m%dT%H%M%SZ)}"
PRIMARY="${PRIMARY_HOME:-$HOME/.lastdb}"
WORK_ROOT="${WORK_ROOT:-/private/tmp/olr}"
WORK_HOME="${WORK_HOME:-$WORK_ROOT/$RUN_ID/home}"
WORK_SOCKET="$WORK_HOME/data/folddb.sock"
PRIMARY_SOCKET="$PRIMARY/data/folddb.sock"
PROOF_ROOT="${PROOF_ROOT:-$HOME/.local/state/last-stack/order-log-retention-cow-proof}"
RUN_DIR="${RUN_DIR:-$PROOF_ROOT/runs/$RUN_ID}"
REPORT_DIR="$RUN_DIR/report"
PROOF_JSON="$REPORT_DIR/proof.json"
DONE_WHEN_PROOF="${DONE_WHEN_PROOF:-$HOME/.local/state/last-stack/proofs/lastdb-order-log-retention-cow-proof-20260826.md}"
LASTDB="${LASTDB:-$ROOT/target/debug/lastdb}"
LASTDBD="${LASTDBD:-$ROOT/target/debug/lastdbd}"
CURL="${CURL:-curl}"
MAX_KEYS="${MAX_KEYS:-256}"
EXECUTE_COMPACTION="${EXECUTE_COMPACTION:-0}"
KEEP_WORK_HOME="${KEEP_WORK_HOME:-0}"
ALLOW_OFFLINE_SOURCE="${ALLOW_OFFLINE_SOURCE:-0}"
DAEMON_PID=""

mkdir -p "$REPORT_DIR" "$(dirname "$DONE_WHEN_PROOF")"

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

refuse_unsafe_work_home() {
  local work="$1"
  local primary="$2"
  local root="$3"
  [[ -n "$work" ]] || fail "WORK_HOME resolved empty"
  [[ "$work" != "/" ]] || fail "WORK_HOME resolved to /"
  is_same_or_child "$work" "$root" \
    || fail "WORK_HOME must stay under WORK_ROOT: work=$(abs_path "$work") root=$(abs_path "$root")"
  if is_same_or_child "$work" "$primary"; then
    fail "refusing work home under PRIMARY_HOME: $(abs_path "$work")"
  fi
  for name in .lastdb .folddb; do
    local canonical="$HOME/$name"
    if [[ -e "$canonical" ]] && is_same_or_child "$work" "$canonical"; then
      fail "refusing primary or compatibility home as WORK_HOME: $(abs_path "$work")"
    fi
  done
  local socket_tmp="$work/data/folddb-full.sock.tmp"
  (( ${#socket_tmp} <= 103 )) \
    || fail "WORK_HOME is too deep for a Unix socket (${#socket_tmp} bytes): $socket_tmp"
}

snapshot_primary_pid() {
  local pid=""
  if [[ -n "${PRIMARY_DAEMON_PID:-}" ]]; then
    printf '%s\n' "$PRIMARY_DAEMON_PID"
    return 0
  fi
  if [[ -S "$PRIMARY_SOCKET" ]] && command -v lsof >/dev/null 2>&1; then
    pid="$(lsof -n -t -- "$PRIMARY_SOCKET" 2>/dev/null | sort -u | head -1 || true)"
  fi
  printf '%s\n' "$pid"
}

stop_daemon() {
  if [[ -n "$DAEMON_PID" ]] && kill -0 "$DAEMON_PID" 2>/dev/null; then
    kill "$DAEMON_PID" 2>/dev/null || true
    wait "$DAEMON_PID" 2>/dev/null || true
  fi
}

cleanup() {
  stop_daemon
  if [[ "$KEEP_WORK_HOME" != "1" && -n "$WORK_HOME" && -d "$WORK_HOME" ]]; then
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
  printf '+ %s --unix-socket %s %s\n' "$CURL" "$WORK_SOCKET" "$*" \
    >>"$REPORT_DIR/commands.log"
  "$CURL" --fail --silent --show-error --max-time "${HTTP_TIMEOUT_SECS:-120}" \
    --unix-socket "$WORK_SOCKET" -H 'Host: localhost' "$@"
}

wait_for_node() {
  local tries="${NODE_WAIT_TRIES:-120}"
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

file_fingerprint() {
  local path="$1"
  if [[ -f "$path" ]]; then
    cksum "$path" | awk '{print $1 ":" $2}'
  else
    printf 'missing\n'
  fi
}

[[ -d "$PRIMARY" ]] || fail "PRIMARY_HOME missing: $PRIMARY"
[[ -f "$PRIMARY/identity.key" ]] || fail "PRIMARY_HOME has no identity.key: $PRIMARY"
[[ -x "$LASTDB" ]] || fail "LASTDB is not executable: $LASTDB"
[[ -x "$LASTDBD" ]] || fail "LASTDBD is not executable: $LASTDBD"
command -v jq >/dev/null 2>&1 || fail "jq is required"
command -v "$CURL" >/dev/null 2>&1 || [[ -x "$CURL" ]] || fail "curl is required: $CURL"
refuse_unsafe_work_home "$WORK_HOME" "$PRIMARY" "$WORK_ROOT"
[[ "$(abs_path "$WORK_SOCKET")" != "$(abs_path "$PRIMARY_SOCKET")" ]] \
  || fail "isolated socket resolved to the primary socket"

PRIMARY_PID_BEFORE="$(snapshot_primary_pid)"
if [[ -z "$PRIMARY_PID_BEFORE" && "$ALLOW_OFFLINE_SOURCE" != "1" ]]; then
  fail "source daemon pid not found; set ALLOW_OFFLINE_SOURCE=1 only for an offline fixture"
fi
IDENTITY_BEFORE="$(file_fingerprint "$PRIMARY/identity.key")"
CLOUD_CONFIG_BEFORE="$(file_fingerprint "$PRIMARY/cloud_sync.json")"

if [[ "${REUSE_WORK_HOME:-0}" == "1" && -d "$WORK_HOME" ]]; then
  echo "Reusing existing clone: $WORK_HOME"
else
  rm -rf "$WORK_HOME"
  mkdir -p "$(dirname "$WORK_HOME")"
  if cp -cR "$PRIMARY" "$WORK_HOME" 2>/dev/null; then
    echo "CoW clone created: $WORK_HOME"
  else
    cp -a "$PRIMARY" "$WORK_HOME"
    echo "Full copy created: $WORK_HOME"
  fi
fi

refuse_unsafe_work_home "$WORK_HOME" "$PRIMARY" "$WORK_ROOT"
find "$WORK_HOME" -name '*.sock' -delete 2>/dev/null || true
if [[ -f "$WORK_HOME/cloud_sync.json" ]]; then
  mv "$WORK_HOME/cloud_sync.json" "$WORK_HOME/cloud_sync.json.paused"
elif [[ ! -f "$WORK_HOME/cloud_sync.json.paused" ]]; then
  printf '{"paused_by":"order-log-retention-cow-proof","run_id":"%s"}\n' \
    "$RUN_ID" >"$WORK_HOME/cloud_sync.json.paused"
fi

(
  unset LASTDB_SOCKET FOLDDB_SOCKET LASTDB_SOCKET_PATH FOLDDB_SOCKET_PATH
  exec env HOME="$WORK_HOME" LASTDB_HOME="$WORK_HOME" FOLDDB_HOME="$WORK_HOME" \
    FOLDDB_DISABLE_KEYCHAIN=1 "$LASTDBD" --data-dir "$WORK_HOME"
) >"$REPORT_DIR/lastdbd.out" 2>"$REPORT_DIR/lastdbd.err" &
DAEMON_PID=$!
wait_for_node

curl_work -X POST "http://localhost/api/sync/cloud-off" \
  >"$REPORT_DIR/cloud-off.json"
jq -e '.ok == true and .intent == "off" and .file_state == "off" and
  (.engine_paused == true or (.engine_note | contains("not running")))' \
  "$REPORT_DIR/cloud-off.json" >/dev/null \
  || fail "isolated daemon did not confirm Cloud Sync off"

SCHEMA_NAME="${PROOF_SCHEMA_NAME:-OrderLogCowProof}"
jq -n --arg name "$SCHEMA_NAME" '{
  namespace: "validation",
  schema: {
    name: $name,
    descriptive_name: "Order Log CoW Proof",
    schema_type: "HashRange",
    key: {hash_field: "bucket", range_field: "id"},
    fields: ["bucket", "id", "value"],
    field_descriptions: {
      bucket: "isolated proof partition",
      id: "ordered proof record id",
      value: "proof payload"
    }
  }
}' >"$REPORT_DIR/declare-request.json"
curl_work -H 'Content-Type: application/json' \
  --data-binary "@$REPORT_DIR/declare-request.json" \
  "http://localhost/api/schemas/declare" >"$REPORT_DIR/declare.json"
SCHEMA_REF="$(jq -r '.canonical // .identity_hash // empty' "$REPORT_DIR/declare.json")"
[[ -n "$SCHEMA_REF" ]] || fail "schema declaration did not return a canonical identity"

for id in recent-1 recent-2 recent-3; do
  jq -n --arg schema "$SCHEMA_REF" --arg id "$id" --arg run "$RUN_ID" '{
    type: "mutation",
    schema: $schema,
    fields_and_values: {bucket: "proof", id: $id, value: ($run + ":" + $id)},
    key_value: {hash: "proof", range: $id},
    mutation_type: "create"
  }' >"$REPORT_DIR/mutation-$id-request.json"
  curl_work -H 'Content-Type: application/json' \
    --data-binary "@$REPORT_DIR/mutation-$id-request.json" \
    "http://localhost/api/mutation" >"$REPORT_DIR/mutation-$id.json"
  jq -e '.ok == true and .success == true' "$REPORT_DIR/mutation-$id.json" >/dev/null \
    || fail "fresh proof mutation failed for $id"
done

jq -n --arg schema "$SCHEMA_REF" '{
  schema_name: $schema,
  fields: ["bucket", "id", "value"],
  filter: {HashRangePrefix: {hash: "proof", prefix: "recent-"}}
}' >"$REPORT_DIR/order-query-request.json"
jq -n --arg schema "$SCHEMA_REF" '{
  schema_name: $schema,
  fields: ["bucket", "id", "value"],
  filter: {HashRangeKey: {hash: "proof", range: "recent-3"}}
}' >"$REPORT_DIR/point-query-request.json"

curl_work -H 'Content-Type: application/json' \
  --data-binary "@$REPORT_DIR/order-query-request.json" \
  "http://localhost/api/query" >"$REPORT_DIR/order-before.json"
curl_work -H 'Content-Type: application/json' \
  --data-binary "@$REPORT_DIR/point-query-request.json" \
  "http://localhost/api/query" >"$REPORT_DIR/point-before.json"
jq -e '(.results | length) == 3' "$REPORT_DIR/order-before.json" >/dev/null \
  || fail "fresh molecule range-order read did not return all three records"
jq -e '(.results | length) == 1' "$REPORT_DIR/point-before.json" >/dev/null \
  || fail "fresh molecule point read did not return recent-3"

run_lastdb db compact-order-log --json --max-keys "$MAX_KEYS" \
  >"$REPORT_DIR/order-log-dry-run.json"
jq -e '.dry_run == true and (.tips_bytes_planned | type == "number")' \
  "$REPORT_DIR/order-log-dry-run.json" >/dev/null \
  || fail "compact-order-log dry-run response is incomplete"

PROOF_STATUS="DRY_RUN"
if [[ "$EXECUTE_COMPACTION" == "1" ]]; then
  run_lastdb db compact-order-log --json --max-keys "$MAX_KEYS" --execute \
    >"$REPORT_DIR/order-log-execute.json"
  jq -e '.dry_run == false and (.wall_ms | type == "number") and (.deletes_per_sec | type == "number")' \
    "$REPORT_DIR/order-log-execute.json" >/dev/null \
    || fail "compact-order-log execute response is incomplete"
  for collection in tips field_update_order_log field_update_order_count; do
    run_lastdb db compact --collection "$collection" --execute --json \
      >"$REPORT_DIR/compact-$collection.json"
    jq -e '.' "$REPORT_DIR/compact-$collection.json" >/dev/null \
      || fail "physical compact returned invalid JSON for $collection"
  done
  PROOF_STATUS="PASS"
else
  printf '{"skipped":true,"reason":"EXECUTE_COMPACTION is not 1"}\n' \
    >"$REPORT_DIR/order-log-execute.json"
  for collection in tips field_update_order_log field_update_order_count; do
    printf '{"skipped":true,"collection":"%s"}\n' "$collection" \
      >"$REPORT_DIR/compact-$collection.json"
  done
fi

curl_work -H 'Content-Type: application/json' \
  --data-binary "@$REPORT_DIR/order-query-request.json" \
  "http://localhost/api/query" >"$REPORT_DIR/order-after.json"
curl_work -H 'Content-Type: application/json' \
  --data-binary "@$REPORT_DIR/point-query-request.json" \
  "http://localhost/api/query" >"$REPORT_DIR/point-after.json"
jq -S '.results' "$REPORT_DIR/order-before.json" >"$REPORT_DIR/order-before.normalized.json"
jq -S '.results' "$REPORT_DIR/order-after.json" >"$REPORT_DIR/order-after.normalized.json"
jq -S '.results' "$REPORT_DIR/point-before.json" >"$REPORT_DIR/point-before.normalized.json"
jq -S '.results' "$REPORT_DIR/point-after.json" >"$REPORT_DIR/point-after.normalized.json"
cmp -s "$REPORT_DIR/order-before.normalized.json" "$REPORT_DIR/order-after.normalized.json" \
  || fail "fresh molecule range order changed after compaction"
cmp -s "$REPORT_DIR/point-before.normalized.json" "$REPORT_DIR/point-after.normalized.json" \
  || fail "fresh molecule point read changed after compaction"

PRIMARY_PID_AFTER="$(snapshot_primary_pid)"
IDENTITY_AFTER="$(file_fingerprint "$PRIMARY/identity.key")"
CLOUD_CONFIG_AFTER="$(file_fingerprint "$PRIMARY/cloud_sync.json")"
PRIMARY_CONTINUITY=true
if [[ -n "$PRIMARY_PID_BEFORE" ]]; then
  if [[ "$PRIMARY_PID_BEFORE" != "$PRIMARY_PID_AFTER" ]] \
    || ! kill -0 "$PRIMARY_PID_BEFORE" 2>/dev/null; then
    PRIMARY_CONTINUITY=false
  fi
fi
[[ "$PRIMARY_CONTINUITY" == "true" ]] || fail "source daemon pid changed or exited"
[[ "$IDENTITY_BEFORE" == "$IDENTITY_AFTER" ]] || fail "source identity changed"
[[ "$CLOUD_CONFIG_BEFORE" == "$CLOUD_CONFIG_AFTER" ]] || fail "source Cloud Sync config changed"

jq -n \
  --arg status "$PROOF_STATUS" \
  --arg run_id "$RUN_ID" \
  --arg source_home "$PRIMARY" \
  --arg work_home "$WORK_HOME" \
  --arg work_socket "$WORK_SOCKET" \
  --arg primary_socket "$PRIMARY_SOCKET" \
  --arg schema_ref "$SCHEMA_REF" \
  --arg report_dir "$REPORT_DIR" \
  --argjson primary_continuity "$PRIMARY_CONTINUITY" \
  --slurpfile cloud_off "$REPORT_DIR/cloud-off.json" \
  --slurpfile dry_run "$REPORT_DIR/order-log-dry-run.json" \
  --slurpfile execute "$REPORT_DIR/order-log-execute.json" \
  --slurpfile tips "$REPORT_DIR/compact-tips.json" \
  --slurpfile order_log "$REPORT_DIR/compact-field_update_order_log.json" \
  --slurpfile order_count "$REPORT_DIR/compact-field_update_order_count.json" \
  '{
    ok: ($status == "PASS"),
    status: $status,
    run_id: $run_id,
    source_home: $source_home,
    work_home: $work_home,
    work_socket: $work_socket,
    primary_socket: $primary_socket,
    schema_ref: $schema_ref,
    guards: {
      source_home_used_by_daemon: false,
      source_socket_used: false,
      unique_socket: ($work_socket != $primary_socket),
      cloud_sync_off: ($cloud_off[0].ok == true and $cloud_off[0].intent == "off" and $cloud_off[0].file_state == "off" and ($cloud_off[0].engine_paused == true or ($cloud_off[0].engine_note | contains("not running")))),
      source_daemon_continuity: $primary_continuity,
      source_identity_unchanged: true,
      source_cloud_config_unchanged: true,
      cleanup_default: true
    },
    order_log: {dry_run: $dry_run[0], execute: $execute[0]},
    physical_compact: {tips: $tips[0], field_update_order_log: $order_log[0], field_update_order_count: $order_count[0]},
    fresh_molecule: {records: 3, range_order_equal: true, point_read_equal: true},
    report_dir: $report_dir
  }' >"$PROOF_JSON"

{
  printf '%s\n' "$PROOF_STATUS"
  printf 'run_id=%s\n' "$RUN_ID"
  printf 'source_home=%s\n' "$PRIMARY"
  printf 'work_home=%s\n' "$WORK_HOME"
  printf 'work_socket=%s\n' "$WORK_SOCKET"
  printf 'cloud_sync_off=true\n'
  printf 'source_socket_used=false\n'
  printf 'source_identity_unchanged=true\n'
  printf 'fresh_molecule_range_order_equal=true\n'
  printf 'fresh_molecule_point_read_equal=true\n'
  printf 'proof_json=%s\n' "$PROOF_JSON"
} >"$DONE_WHEN_PROOF"

echo "$PROOF_STATUS order-log-retention-cow-proof report=$PROOF_JSON done_when=$DONE_WHEN_PROOF"
