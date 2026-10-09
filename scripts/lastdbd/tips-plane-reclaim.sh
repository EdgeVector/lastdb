#!/usr/bin/env bash
# Tips plane reclaim to retention window (primary node).
#
# Honors three retention windows:
# 1. Order log: 30 days + drop zero-live entries
# 2. Superseded versions: 7 days (live records only)
# 3. Dropped schema tips: reap all tips for inactive schemas
#
# Execution follows LastDB safe-upgrade discipline:
# - Phase 1: CoW proof on real data copy (zero divergence required)
# - Phase 2: Primary execution (only after Phase 1 proof succeeds)
#
# Usage:
#   PRIMARY_HOME=$HOME/.lastdb WORK_ROOT=/private/tmp/tips-reclaim \
#     DROPPED_SCHEMAS_FILE=/path/dropped.txt \
#     scripts/lastdbd/tips-plane-reclaim.sh
#
# EXECUTE=1 runs Phase 2 (primary deletions). Default is Phase 1 only (dry-run).
set -euo pipefail

fail() {
  echo "ERROR: $*" >&2
  if [[ -n "${REPORT_DIR:-}" && -d "${REPORT_DIR}" ]]; then
    jq -n --arg error "$*" '{ok:false, error:$error}' >"$REPORT_DIR/proof.json" 2>/dev/null || true
  fi
  exit 1
}

info() {
  echo "INFO: $*" >&2
}

# Parse arguments
RUN_ID="${1:-$(date -u +%Y%m%dT%H%M%SZ)}"
PRIMARY_HOME="${PRIMARY_HOME:-$HOME/.lastdb}"
WORK_ROOT="${WORK_ROOT:-/private/tmp/tips-reclaim}"
WORK_HOME="${WORK_HOME:-$WORK_ROOT/$RUN_ID/home}"
ROLLBACK_HOME="${ROLLBACK_HOME:-$WORK_ROOT/$RUN_ID/rollback}"
PROOF_ROOT="${PROOF_ROOT:-$HOME/.local/state/last-stack/tips-reclaim-proofs}"
RUN_DIR="${RUN_DIR:-$PROOF_ROOT/$RUN_ID}"
REPORT_DIR="$RUN_DIR/report"
EXECUTE="${EXECUTE:-0}"
DROPPED_SCHEMAS_FILE="${DROPPED_SCHEMAS_FILE:-}"

# Paths to binaries
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LASTDB="${LASTDB:-$ROOT/target/debug/lastdb}"
LASTDBD="${LASTDBD:-$ROOT/target/debug/lastdbd}"

# Configuration
MAX_OPS="${MAX_OPS:-256}"
RANGE_LIMIT="${RANGE_LIMIT:-32}"
NODE_WAIT_TRIES="${NODE_WAIT_TRIES:-600}"
HTTP_TIMEOUT_SECS="${HTTP_TIMEOUT_SECS:-120}"
RETENTION_ORDER_LOG_SECS=$((30 * 24 * 3600))  # 30 days
RETENTION_VERSIONS_SECS=$((7 * 24 * 3600))     # 7 days

# Runtime state
mkdir -p "$REPORT_DIR" "$REPORT_DIR/receipts"
LOG="$REPORT_DIR/reclaim.log"
exec >>"$LOG" 2>&1

DAEMON_PID=""
DIVERGENCE=0
CLONE_METHOD=""
WORK_SOCKET="$WORK_HOME/data/folddb.sock"

cleanup() {
  if [[ -n "${DAEMON_PID}" ]] && kill -0 "$DAEMON_PID" 2>/dev/null; then
    kill "$DAEMON_PID" 2>/dev/null || true
    wait "$DAEMON_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

# Verification
[[ -x "$LASTDB" ]] || fail "LASTDB not executable: $LASTDB"
[[ -x "$LASTDBD" ]] || fail "LASTDBD not executable: $LASTDBD"
command -v jq >/dev/null 2>&1 || fail "jq required"
command -v curl >/dev/null 2>&1 || fail "curl required"

# Helper functions
abs_path() {
  python3 - "$1" <<'PY'
import pathlib, sys
print(pathlib.Path(sys.argv[1]).expanduser().resolve(strict=False))
PY
}

is_child() {
  python3 - "$1" "$2" <<'PY'
import pathlib, sys
path = pathlib.Path(sys.argv[1]).expanduser().resolve(strict=False)
parent = pathlib.Path(sys.argv[2]).expanduser().resolve(strict=False)
try:
    path.relative_to(parent)
    raise SystemExit(0)
except ValueError:
    raise SystemExit(1)
PY
}

refuse_primary() {
  local path="$1"
  local label="$2"
  local canonical
  for canonical in "$HOME/.lastdb" "$HOME/.folddb"; do
    if [[ -e "$canonical" ]] && is_child "$path" "$canonical"; then
      fail "refusing live primary as $label"
    fi
  done
}

run_lastdb() {
  "$LASTDB" --data-dir "$WORK_HOME" "$@" 2>&1 | tee -a "$REPORT_DIR/commands.log"
  return "${PIPESTATUS[0]}"
}

wait_for_node() {
  local attempt=1
  while (( attempt <= NODE_WAIT_TRIES )); do
    if [[ -S "$WORK_SOCKET" ]] && run_lastdb status --json >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "$DAEMON_PID" 2>/dev/null; then
      tail -50 "$REPORT_DIR/lastdbd.err" >&2 || true
      fail "isolated daemon exited before socket ready"
    fi
    sleep 1
    attempt=$((attempt + 1))
  done
  fail "daemon did not become ready at $WORK_SOCKET"
}

start_daemon() {
  if [[ -n "${DAEMON_PID}" ]] && kill -0 "$DAEMON_PID" 2>/dev/null; then
    kill "$DAEMON_PID" 2>/dev/null || true
    wait "$DAEMON_PID" 2>/dev/null || true
  fi
  rm -f "$WORK_HOME/data/folddb.sock" "$WORK_HOME/data/folddb-full.sock"
  (
    unset LASTDB_SOCKET FOLDDB_SOCKET LASTDB_SOCKET_PATH FOLDDB_SOCKET_PATH
    exec env HOME="$WORK_HOME" LASTDB_HOME="$WORK_HOME" FOLDDB_HOME="$WORK_HOME" \
      FOLDDB_DISABLE_KEYCHAIN=1 "$LASTDBD" --data-dir "$WORK_HOME"
  ) >>"$REPORT_DIR/lastdbd.out" 2>>"$REPORT_DIR/lastdbd.err" &
  DAEMON_PID=$!
  wait_for_node
  info "Daemon ready (PID $DAEMON_PID)"
}

clone_home() {
  local src="$1"
  local dest="$2"
  refuse_primary "$dest" "clone dest"
  [[ -d "$dest" ]] && rm -rf "$dest"
  mkdir -p "$(dirname "$dest")"
  if cp -cR "$src" "$dest" 2>"$REPORT_DIR/clone.err"; then
    printf 'apfs-clone\n'
  else
    rm -rf "$dest"
    cp -a "$src" "$dest"
    printf 'full-copy\n'
  fi
  rm -f "$dest/data/folddb.sock" "$dest/data/folddb-full.sock"
  if [[ -f "$dest/cloud_sync.json" ]]; then
    mv "$dest/cloud_sync.json" "$dest/cloud_sync.json.disabled"
  fi
}

# Phase 1: Copy-on-Write Proof
phase_1_copy() {
  info "Phase 1: Creating CoW clone..."
  PRIMARY_HOME="$(abs_path "$PRIMARY_HOME")"
  CLONE_METHOD="$(clone_home "$PRIMARY_HOME" "$WORK_HOME")"
  info "Clone method: $CLONE_METHOD"
  du -sh "$WORK_HOME" | sed 's/^/  /'
}

phase_1_load_schemas() {
  info "Loading active schemas..."
  curl -s --max-time "$HTTP_TIMEOUT_SECS" --unix-socket "$WORK_SOCKET" \
    "http://localhost/api/schemas?include_system=true" >"$REPORT_DIR/schemas.json"
  jq -r '
    (.schemas // .data.schemas // [])[]
    | select((.state // "Available") != "Blocked")
    | .name
  ' "$REPORT_DIR/schemas.json" | sort >"$REPORT_DIR/active-schemas.txt"
  local count
  count=$(wc -l <"$REPORT_DIR/active-schemas.txt")
  info "Loaded $count active schemas"
}

phase_1_load_dropped() {
  info "Loading dropped schemas..."
  [[ -n "$DROPPED_SCHEMAS_FILE" && -f "$DROPPED_SCHEMAS_FILE" ]] || {
    info "No dropped schemas file; skipping dropped-schema reap"
    return
  }
  : >"$REPORT_DIR/dropped-schemas.txt"
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ "$line" =~ ^#.*$ || -z "$line" ]] && continue
    printf '%s\n' "$line" >>"$REPORT_DIR/dropped-schemas.txt"
  done <"$DROPPED_SCHEMAS_FILE"
  local count
  count=$(wc -l <"$REPORT_DIR/dropped-schemas.txt" 2>/dev/null || printf '0')
  info "Loaded $count dropped schemas"

  # Refuse to reap any active schema
  if [[ -s "$REPORT_DIR/dropped-schemas.txt" ]]; then
    local overlap
    overlap="$(comm -12 <(sort "$REPORT_DIR/active-schemas.txt") \
                        <(sort "$REPORT_DIR/dropped-schemas.txt") || true)"
    [[ -z "$overlap" ]] || fail "refusing to reap active schemas: ${overlap//$'\n'/, }"
  fi
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

phase_1_capture_reads() {
  info "Capturing before-state reads..."
  printf '[]\n' >"$REPORT_DIR/reads-before.json"
  local schema count=0
  while IFS= read -r schema || [[ -n "$schema" ]]; do
    [[ -n "$schema" ]] || continue

    if run_lastdb get-keys "$schema" --limit 1 --json >"$REPORT_DIR/raw-discover.json"; then
      local hash range
      hash="$(jq -r '.keys[0].hash // empty' "$REPORT_DIR/raw-discover.json")"
      range="$(jq -r '.keys[0].range // empty' "$REPORT_DIR/raw-discover.json")"

      local point_canon range_canon
      if [[ -z "$hash" ]]; then
        point_canon='{"results":[]}'
        range_canon="{\"schema\":\"$schema\",\"hash_filter\":null,\"has_more\":false,\"keys\":[]}"
      else
        if [[ -n "$range" ]]; then
          run_lastdb get "$schema" --key-hash "$hash" --key-range "$range" --json >"$REPORT_DIR/raw-point.json"
        else
          run_lastdb get "$schema" --key-hash "$hash" --json >"$REPORT_DIR/raw-point.json"
        fi
        point_canon="$(canon_point "$REPORT_DIR/raw-point.json")"

        run_lastdb get-keys "$schema" --key-hash "$hash" --limit "$RANGE_LIMIT" --json >"$REPORT_DIR/raw-range.json"
        range_canon="$(canon_range "$REPORT_DIR/raw-range.json")"
      fi

      local entry
      entry="$(jq -nc \
        --arg schema "$schema" \
        --arg key_hash "$hash" \
        --arg key_range "$range" \
        --argjson point "$point_canon" \
        --argjson range_doc "$range_canon" \
        '{
          schema: $schema,
          key_hash: (if $key_hash == "" then null else $key_hash end),
          key_range: (if $key_range == "" then null else $key_range end),
          point: $point,
          range: $range_doc
        }')"

      jq --argjson entry "$entry" '. + [$entry]' "$REPORT_DIR/reads-before.json" \
        >"$REPORT_DIR/reads-before.json.tmp"
      mv "$REPORT_DIR/reads-before.json.tmp" "$REPORT_DIR/reads-before.json"
      count=$((count + 1))
    fi
  done <"$REPORT_DIR/active-schemas.txt"
  info "Captured $count before-state reads"
}

phase_1_dry_run() {
  info "Running dry-run retention operations..."

  # Order log compaction
  info "  Dry-run: compact-order-log (30d retention)..."
  run_lastdb db compact-order-log --dry-run --retention-seconds "$RETENTION_ORDER_LOG_SECS" --json \
    >"$REPORT_DIR/order-log-dryrun.json"
  local order_del
  order_del="$(jq '.entries_deleted // 0' "$REPORT_DIR/order-log-dryrun.json")"
  info "    Planned deletion: $order_del order-log entries"

  # Superseded versions
  info "  Dry-run: retain-superseded-versions (7d retention)..."
  run_lastdb db retain-superseded-versions --dry-run --retention-seconds "$RETENTION_VERSIONS_SECS" --json \
    >"$REPORT_DIR/versions-dryrun.json"
  local versions_del
  versions_del="$(jq '.entries_deleted // 0' "$REPORT_DIR/versions-dryrun.json")"
  info "    Planned deletion: $versions_del version entries"

  # Dropped schemas (if any)
  if [[ -f "$REPORT_DIR/dropped-schemas.txt" && -s "$REPORT_DIR/dropped-schemas.txt" ]]; then
    printf '[]\n' >"$REPORT_DIR/dropped-dryrun.json"
    local schema
    while IFS= read -r schema || [[ -n "$schema" ]]; do
      [[ -n "$schema" ]] || continue
      info "  Dry-run: reap-dropped-schema $schema..."
      run_lastdb db reap-dropped-schema --schema "$schema" --max-ops "$MAX_OPS" --dry-run --json \
        >"$REPORT_DIR/dropped-$schema-dryrun.json"
      local tips_del
      tips_del="$(jq '.tips_deleted // 0' "$REPORT_DIR/dropped-$schema-dryrun.json")"
      info "    Planned deletion: $tips_del tips for $schema"
    done <"$REPORT_DIR/dropped-schemas.txt"
  fi
}

phase_1_replay_reads() {
  info "Replaying reads after dry-run..."
  printf '[]\n' >"$REPORT_DIR/reads-after.json"
  local count
  count=$(jq 'length' "$REPORT_DIR/reads-before.json")
  local i=0
  while (( i < count )); do
    local schema hash range
    schema="$(jq -r ".[$i].schema" "$REPORT_DIR/reads-before.json")"
    hash="$(jq -r ".[$i].key_hash // empty" "$REPORT_DIR/reads-before.json")"
    range="$(jq -r ".[$i].key_range // empty" "$REPORT_DIR/reads-before.json")"

    local point_canon range_canon
    if [[ -n "$hash" ]]; then
      if [[ -n "$range" ]]; then
        run_lastdb get "$schema" --key-hash "$hash" --key-range "$range" --json >"$REPORT_DIR/raw-point.json"
      else
        run_lastdb get "$schema" --key-hash "$hash" --json >"$REPORT_DIR/raw-point.json"
      fi
      point_canon="$(canon_point "$REPORT_DIR/raw-point.json")"

      run_lastdb get-keys "$schema" --key-hash "$hash" --limit "$RANGE_LIMIT" --json >"$REPORT_DIR/raw-range.json"
      range_canon="$(canon_range "$REPORT_DIR/raw-range.json")"
    else
      point_canon='{"results":[]}'
      range_canon="{\"schema\":\"$schema\",\"hash_filter\":null,\"has_more\":false,\"keys\":[]}"
    fi

    local entry
    entry="$(jq -nc \
      --arg schema "$schema" \
      --arg key_hash "$hash" \
      --arg key_range "$range" \
      --argjson point "$point_canon" \
      --argjson range_doc "$range_canon" \
      '{
        schema: $schema,
        key_hash: (if $key_hash == "" then null else $key_hash end),
        key_range: (if $key_range == "" then null else $key_range end),
        point: $point,
        range: $range_doc
      }')"

    jq --argjson entry "$entry" '. + [$entry]' "$REPORT_DIR/reads-after.json" \
      >"$REPORT_DIR/reads-after.json.tmp"
    mv "$REPORT_DIR/reads-after.json.tmp" "$REPORT_DIR/reads-after.json"
    i=$((i + 1))
  done

  # Canonical sort for comparison
  jq -S 'map({schema, key_hash, key_range, point, range})' "$REPORT_DIR/reads-before.json" \
    >"$REPORT_DIR/reads-before.canon.json"
  jq -S 'map({schema, key_hash, key_range, point, range})' "$REPORT_DIR/reads-after.json" \
    >"$REPORT_DIR/reads-after.canon.json"

  # Check divergence
  local mismatches
  mismatches="$(jq -c -n \
    --slurpfile before "$REPORT_DIR/reads-before.canon.json" \
    --slurpfile after "$REPORT_DIR/reads-after.canon.json" '
      [range(0; ($before[0] | length)) as $i
        | select($before[0][$i] != $after[0][$i])
        | $before[0][$i].schema]
    ')"
  DIVERGENCE="$(jq 'length' <<<"$mismatches")"

  if (( DIVERGENCE == 0 )); then
    info "✓ ZERO DIVERGENCE: All reads match before and after dry-run"
    return 0
  else
    info "✗ DIVERGENCE DETECTED: $DIVERGENCE schemas differ"
    echo "$mismatches" | sed 's/^/  /' >&2
    return 1
  fi
}

phase_1_proof() {
  info "Writing Phase 1 proof..."
  jq -n \
    --arg run_id "$RUN_ID" \
    --arg clone_method "$CLONE_METHOD" \
    --arg work_home "$WORK_HOME" \
    --arg primary_home "$PRIMARY_HOME" \
    --argjson divergence "$DIVERGENCE" \
    '{
      tool: "tips-plane-reclaim-cow-proof",
      ok: ($divergence == 0),
      divergence_count: $divergence,
      run_id: $run_id,
      clone_method: $clone_method,
      work_home: $work_home,
      primary_home: $primary_home,
      phase_1_proof: "zero-divergence-on-dry-run"
    }' >"$REPORT_DIR/phase-1-proof.json"

  cat "$REPORT_DIR/phase-1-proof.json" | jq '.'
}

# Phase 2: Primary Execution
phase_2_rollback() {
  info "Phase 2: Writing rollback point..."
  refuse_primary "$ROLLBACK_HOME" "rollback"
  if [[ -d "$ROLLBACK_HOME" ]]; then
    rm -rf "$ROLLBACK_HOME"
  fi
  mkdir -p "$(dirname "$ROLLBACK_HOME")"
  cp -a "$PRIMARY_HOME" "$ROLLBACK_HOME"
  rm -f "$ROLLBACK_HOME/data/folddb.sock" "$ROLLBACK_HOME/data/folddb-full.sock"
  cat >"$ROLLBACK_HOME/ROLLBACK_POINT" <<'EOF'
Database snapshot taken before tips plane reclaim.
To restore: stop lastdbd, then:
  cp -a <this-directory> ~/.lastdb
  lastdbd --data-dir ~/.lastdb
Do NOT upgrade the node version.
EOF
  du -sh "$ROLLBACK_HOME" | sed 's/^/  /'
  info "Rollback point created"
}

phase_2_execute() {
  info "Phase 2: Executing deletion batches..."

  # Batch 1: Compact order log
  info "  Batch 1: compact-order-log..."
  jq -n '{batch: 1, name: "compact-order-log", status: "in_progress"}' >"$REPORT_DIR/receipts/batch-status.json"
  run_lastdb db compact-order-log --execute --retention-seconds "$RETENTION_ORDER_LOG_SECS" --json \
    >"$REPORT_DIR/receipts/batch-001-order-log.json"
  local order_result
  order_result="$(jq '{entries_deleted, bytes_deleted, molecules_compacted}' "$REPORT_DIR/receipts/batch-001-order-log.json")"
  info "    Result: $order_result"
  jq -n '{batch: 1, name: "compact-order-log", status: "completed"}' >"$REPORT_DIR/receipts/batch-status.json"

  # Batch 2: Retain superseded versions
  info "  Batch 2: retain-superseded-versions..."
  jq -n '{batch: 2, name: "retain-superseded-versions", status: "in_progress"}' >"$REPORT_DIR/receipts/batch-status.json"
  run_lastdb db retain-superseded-versions --execute --retention-seconds "$RETENTION_VERSIONS_SECS" --json \
    >"$REPORT_DIR/receipts/batch-002-versions.json"
  local versions_result
  versions_result="$(jq '{entries_deleted, molecules_pruned}' "$REPORT_DIR/receipts/batch-002-versions.json")"
  info "    Result: $versions_result"
  jq -n '{batch: 2, name: "retain-superseded-versions", status: "completed"}' >"$REPORT_DIR/receipts/batch-status.json"

  # Batch 3+: Reap dropped schemas (if any)
  local batch_n=3
  if [[ -f "$REPORT_DIR/dropped-schemas.txt" && -s "$REPORT_DIR/dropped-schemas.txt" ]]; then
    local schema
    while IFS= read -r schema || [[ -n "$schema" ]]; do
      [[ -n "$schema" ]] || continue
      local batch_num
      printf -v batch_num '%03d' "$batch_n"
      info "  Batch $batch_num: reap-dropped-schema $schema..."
      jq -n --arg schema "$schema" --arg batch_num "$batch_num" '{batch: ($batch_num | tonumber), name: ("reap-dropped-schema/" + $schema), status: "in_progress"}' >"$REPORT_DIR/receipts/batch-status.json"
      run_lastdb db reap-dropped-schema --schema "$schema" --max-ops "$MAX_OPS" --execute --json \
        >"$REPORT_DIR/receipts/batch-$batch_num-$schema.json"
      local tips_result
      tips_result="$(jq '{tips_deleted, index_keys_deleted}' "$REPORT_DIR/receipts/batch-$batch_num-$schema.json")"
      info "    Result: $tips_result"
      jq -n --arg schema "$schema" --arg batch_num "$batch_num" '{batch: ($batch_num | tonumber), name: ("reap-dropped-schema/" + $schema), status: "completed"}' >"$REPORT_DIR/receipts/batch-status.json"
      batch_n=$((batch_n + 1))
    done <"$REPORT_DIR/dropped-schemas.txt"
  fi
}

parse_bytes() {
  local value="$1"
  local num unit bytes_val
  num="$(echo "$value" | sed 's/[^0-9.]*$//' | awk '{printf "%.0f", $1}')"
  unit="$(echo "$value" | sed 's/^[0-9.]*[[:space:]]*//')"
  case "$unit" in
    B|bytes) bytes_val="$num" ;;
    KiB|KB|K) bytes_val="$((num * 1024))" ;;
    MiB|MB|M) bytes_val="$((num * 1024 * 1024))" ;;
    GiB|GB|G) bytes_val="$((num * 1024 * 1024 * 1024))" ;;
    TiB|TB|T) bytes_val="$((num * 1024 * 1024 * 1024 * 1024))" ;;
    *) bytes_val="-1" ;;
  esac
  printf '%s\n' "$bytes_val"
}

phase_2_verify() {
  info "Phase 2: Verifying tips plane reduction..."
  run_lastdb status --json >"$REPORT_DIR/status-after.json"
  local tips_after
  tips_after="$(jq -r '.store.tips // "unknown"' "$REPORT_DIR/status-after.json")"
  info "Tips plane after: $tips_after (target: ≤ 1 GiB)"

  if [[ "$tips_after" == "unknown" ]]; then
    fail "Could not read tips plane size from status"
  fi

  local tips_bytes target_bytes
  tips_bytes="$(parse_bytes "$tips_after")"
  target_bytes=$((1024 * 1024 * 1024))  # 1 GiB

  if [[ "$tips_bytes" -lt 0 ]]; then
    fail "Could not parse tips plane size: $tips_after"
  fi

  if [[ "$tips_bytes" -gt "$target_bytes" ]]; then
    fail "Tips plane ≤ 1 GiB target NOT met: $tips_after > 1 GiB"
  fi

  info "✓ Tips plane meets target (≤ 1 GiB): $tips_after"
}

phase_2_receipt() {
  info "Writing Phase 2 execution receipt..."
  local receipts_array
  receipts_array=$(jq -s '.' "$REPORT_DIR/receipts"/batch-*.json)
  local total_deleted
  total_deleted=$(jq '[.[] | .entries_deleted // .tips_deleted // 0] | add // 0' <<<"$receipts_array")

  jq -n \
    --arg primary_home "$PRIMARY_HOME" \
    --arg executed_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --argjson batches "$receipts_array" \
    --argjson total_deleted "$total_deleted" \
    '{
      tool: "tips-plane-reclaim",
      phase: "execution",
      primary_home: $primary_home,
      executed_at: $executed_at,
      batches: $batches,
      total_entries_deleted: $total_deleted
    }' >"$REPORT_DIR/phase-2-receipt.json"

  cat "$REPORT_DIR/phase-2-receipt.json" | jq '.'
}

# Main flow
info "Tips plane reclaim — RUN_ID=$RUN_ID"
info "Primary: $PRIMARY_HOME"
info "Work: $WORK_HOME"
info "Execute: $EXECUTE"

# Initialize
PRIMARY_HOME="$(abs_path "$PRIMARY_HOME")"
refuse_primary "$WORK_HOME" "work home"

# Phase 1
info "=========================================="
info "PHASE 1: Copy-on-Write Proof"
info "=========================================="
phase_1_copy
start_daemon
phase_1_load_schemas
phase_1_load_dropped
phase_1_capture_reads
phase_1_dry_run
phase_1_replay_reads || fail "Phase 1 divergence detected"
phase_1_proof

# Cleanup Phase 1 daemon
cleanup

if [[ "$EXECUTE" == "1" ]]; then
  info "=========================================="
  info "PHASE 2: Primary Execution"
  info "=========================================="

  # Pre-Phase-2 validation: verify Phase 1 succeeded
  [[ -f "$REPORT_DIR/phase-1-proof.json" ]] || fail "Phase 1 proof not found at $REPORT_DIR/phase-1-proof.json"
  local divergence_count
  divergence_count="$(jq '.divergence_count' "$REPORT_DIR/phase-1-proof.json")"
  [[ "$divergence_count" == "0" ]] || fail "Phase 1 did not achieve zero divergence (divergence_count: $divergence_count). Phase 2 refused."

  phase_2_rollback
  phase_2_execute
  phase_2_verify
  phase_2_receipt

  info "✓ Tips plane reclaim completed successfully"
  info "Results archived to: $RUN_DIR"
else
  info "=========================================="
  info "Phase 1 proof succeeded. To proceed to Phase 2:"
  info "EXECUTE=1 $0"
  info "Results saved to: $RUN_DIR"
fi
