#!/usr/bin/env bash
# CoW-first proof harness for `lastdb db repair-dangling-tips`.
#
# The primary LastDB home is only used as the source for a copy-on-write clone.
# Every daemon boot and every repair command targets WORK_HOME, never PRIMARY_HOME.
#
# Usage:
#   cargo build -p lastdb_node --bin lastdb --bin lastdbd
#   LASTDB=./target/debug/lastdb LASTDBD=./target/debug/lastdbd \
#     ./scripts/lastdbd/repair-dangling-tips-cow-proof.sh [run-id]
#
# Env:
#   PRIMARY_HOME       default $HOME/.lastdb
#   PROOF_ROOT         default $HOME/.local/state/last-stack/repair-dangling-tips-cow-proof
#   WORK_ROOT          default $HOME/.lastdb-proofs/rdt
#   WORK_HOME          default $WORK_ROOT/<run-id>/home
#
# WORK_HOME is deliberately NOT under PROOF_ROOT: the work home hosts the node's
# Unix sockets, and PROOF_ROOT's path is long enough that `<work>/data/*.sock`
# overflows the 103-byte sockaddr_un limit — the node then boots fully and dies
# at bind, after the multi-GB clone (measured 2026-08-03, run
# grok-20260803T185633Z). Reports still land under PROOF_ROOT, where path length
# does not matter.
#   LASTDB             default target/debug/lastdb
#   LASTDBD            default target/debug/lastdbd
#   REUSE_WORK_HOME=1  reuse an existing WORK_HOME
#   EXECUTE_COW=1      run `repair-dangling-tips --execute` on the clone
#   TIP_PAGE           forwarded to repair-dangling-tips (default 256)
#   MAX_OPS            optional scan bound
#   AUDIT_LIMIT        unresolved sample bound (default 20)
#   REPAIR_SCHEMA      optional `--schema`: walk one schema's tips, not the store
#   REPAIR_HASH_KEY    optional `--hash-key` inside REPAIR_SCHEMA
#   PROOF_INVENTORY=1  collect `db inventory` around the repair (default 0)
#   PROOF_TEE=1        tee output to stdout; default appends directly to LOG
#
# The inventory is evidence gathered *around* the proof, not part of it. On a
# real multi-GB store it is a full-prefix walk that can outlast the admin
# deadline, and under `set -e` that took the whole run down before the thing
# being proved ever ran (measured 2026-08-03: 10 minutes spent, zero repair).
# It is therefore opt-in and, even when requested, never fatal — proof.json
# records whether each side actually produced a file.
set -euo pipefail

fail() {
  echo "RED repair-dangling-tips-cow-proof: $*" >&2
  exit 1
}

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUN_ID="${1:-$(date -u +%Y%m%dT%H%M%SZ)}"
PRIMARY="${PRIMARY_HOME:-$HOME/.lastdb}"
PROOF_ROOT="${PROOF_ROOT:-$HOME/.local/state/last-stack/repair-dangling-tips-cow-proof}"
RUN_DIR="${RUN_DIR:-$PROOF_ROOT/runs/$RUN_ID}"
WORK_ROOT="${WORK_ROOT:-$HOME/.lastdb-proofs/rdt}"
WORK_HOME="${WORK_HOME:-$WORK_ROOT/$RUN_ID/home}"
REPORT_DIR="$RUN_DIR/report"
LOG="$REPORT_DIR/repair-dangling-tips-cow-proof.log"
PROOF_JSON="$REPORT_DIR/proof.json"
LASTDB="${LASTDB:-$ROOT/target/debug/lastdb}"
LASTDBD="${LASTDBD:-$ROOT/target/debug/lastdbd}"
EXECUTE_COW="${EXECUTE_COW:-0}"
TIP_PAGE="${TIP_PAGE:-256}"
MAX_OPS="${MAX_OPS:-}"
AUDIT_LIMIT="${AUDIT_LIMIT:-20}"
REPAIR_SCHEMA="${REPAIR_SCHEMA:-}"
REPAIR_HASH_KEY="${REPAIR_HASH_KEY:-}"
PROOF_INVENTORY="${PROOF_INVENTORY:-0}"

mkdir -p "$REPORT_DIR"
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

refuse_work_home_if_primary() {
  local work="$1"
  local primary="$2"
  [[ -n "$work" ]] || fail "WORK_HOME resolved empty"
  [[ "$work" != "/" ]] || fail "WORK_HOME resolved to /"
  if is_same_or_child "$work" "$primary"; then
    fail "refusing work home under PRIMARY_HOME: $(abs_path "$work")"
  fi
  for name in .lastdb .folddb; do
    local canonical="$HOME/$name"
    if [[ -e "$canonical" ]] && is_same_or_child "$work" "$canonical"; then
      fail "refusing primary/legacy home as work home: $(abs_path "$work")"
    fi
  done
}

# Refuse a work home whose sockets cannot be bound — BEFORE the multi-GB clone.
# `lastdbd` now preflights this too, but it does so after the copy is already
# paid for, so the harness checks first and fails in milliseconds. Budget mirrors
# lastdb_uds::uds: 103-byte sockaddr_un limit, less the longest socket name
# (folddb-full.sock), its `.tmp` rename sibling, and the path separator.
refuse_work_home_if_socket_path_too_long() {
  local work="$1"
  local limit=103
  local overhead=$(( ${#work} + 5 + 1 + 16 + 4 ))  # work + /data + / + name + .tmp
  if (( overhead > limit )); then
    fail "work home is too deep for a Unix socket: $work/data/folddb-full.sock.tmp is \
$overhead bytes, over the ${limit}-byte sockaddr_un limit. Set WORK_ROOT to a shorter path \
(current WORK_ROOT=$WORK_ROOT)."
  fi
}

snapshot_pids() {
  local out="$1"
  pgrep -f 'lastdbd( |$)|target/.*/lastdbd' 2>/dev/null | while read -r pid; do
    if ! ps -wwp "$pid" -o command= 2>/dev/null | grep -qF "$RUN_DIR"; then
      echo "$pid"
    fi
  done | sort >"$out" || true
}

run_lastdb() {
  echo "+ $LASTDB --data-dir $WORK_HOME $*" >&2
  "$LASTDB" --data-dir "$WORK_HOME" "$@"
}

wait_for_node() {
  local tries="${NODE_WAIT_TRIES:-60}"
  local socket="$WORK_HOME/data/folddb.sock"
  for _ in $(seq 1 "$tries"); do
    if [[ -e "$socket" ]] && run_lastdb status --json >"$REPORT_DIR/status-ready.json" 2>"$REPORT_DIR/status-ready.err"; then
      return 0
    fi
    if ! kill -0 "$DAEMON_PID" 2>/dev/null; then
      tail -80 "$REPORT_DIR/lastdbd.err" >&2 || true
      fail "lastdbd exited before socket became ready"
    fi
    sleep 1
  done
  tail -80 "$REPORT_DIR/lastdbd.err" >&2 || true
  fail "lastdbd did not become ready at $socket"
}

# Optional, never fatal. A failed or skipped inventory leaves no file, which is
# what proof.json reports — it must not abort a proof that would otherwise be
# green.
collect_inventory() {
  local side="$1"
  if [[ "$PROOF_INVENTORY" != "1" ]]; then
    echo "inventory-$side skipped (PROOF_INVENTORY=$PROOF_INVENTORY)"
    return 0
  fi
  if run_lastdb db inventory --json --out "$REPORT_DIR/inventory-$side.json" \
      >"$REPORT_DIR/inventory-$side.out" 2>&1; then
    echo "inventory-$side ok"
  else
    echo "inventory-$side FAILED (non-fatal; see inventory-$side.out)"
    rm -f "$REPORT_DIR/inventory-$side.json"
  fi
  return 0
}

stop_daemon() {
  if [[ -n "${DAEMON_PID:-}" ]] && kill -0 "$DAEMON_PID" 2>/dev/null; then
    kill "$DAEMON_PID" 2>/dev/null || true
    wait "$DAEMON_PID" 2>/dev/null || true
  fi
}

echo "=== repair-dangling-tips CoW proof run=$RUN_ID ==="
echo "root=$ROOT"
echo "primary=$PRIMARY"
echo "work_home=$WORK_HOME"
echo "work_root=$WORK_ROOT"
echo "report_dir=$REPORT_DIR"
echo "lastdb=$LASTDB"
echo "lastdbd=$LASTDBD"
echo "execute_cow=$EXECUTE_COW tip_page=$TIP_PAGE max_ops=${MAX_OPS:-none} audit_limit=$AUDIT_LIMIT schema=${REPAIR_SCHEMA:-all} hash_key=${REPAIR_HASH_KEY:-all} inventory=$PROOF_INVENTORY"

[[ -d "$PRIMARY" ]] || fail "PRIMARY_HOME missing: $PRIMARY"
[[ -f "$PRIMARY/identity.key" ]] || fail "PRIMARY_HOME has no identity.key: $PRIMARY"
[[ -x "$LASTDB" ]] || fail "LASTDB is not executable: $LASTDB"
[[ -x "$LASTDBD" ]] || fail "LASTDBD is not executable: $LASTDBD"
refuse_work_home_if_primary "$WORK_HOME" "$PRIMARY"
refuse_work_home_if_socket_path_too_long "$WORK_HOME"

snapshot_pids "$REPORT_DIR/primary-pids-before.txt"

if [[ "${REUSE_WORK_HOME:-0}" == "1" && -d "$WORK_HOME" ]]; then
  echo "Reusing work home $WORK_HOME"
else
  rm -rf "$WORK_HOME"
  mkdir -p "$(dirname "$WORK_HOME")"
  if cp -cR "$PRIMARY" "$WORK_HOME" 2>/dev/null; then
    echo "CoW clone ok"
  else
    cp -a "$PRIMARY" "$WORK_HOME"
    echo "full copy ok"
  fi
fi

refuse_work_home_if_primary "$WORK_HOME" "$PRIMARY"
find "$WORK_HOME" -name '*.sock' -delete 2>/dev/null || true
find "$WORK_HOME" -name 'folddb.sock' -delete 2>/dev/null || true
if [[ -f "$WORK_HOME/cloud_sync.json" ]]; then
  mv "$WORK_HOME/cloud_sync.json" "$WORK_HOME/cloud_sync.json.disabled-by-repair-proof-$RUN_ID"
  echo "disabled inherited cloud_sync.json on CoW home"
fi

"$LASTDBD" --data-dir "$WORK_HOME" >"$REPORT_DIR/lastdbd.out" 2>"$REPORT_DIR/lastdbd.err" &
DAEMON_PID=$!
trap stop_daemon EXIT
wait_for_node

collect_inventory before

scope_args=()
if [[ -n "$REPAIR_SCHEMA" ]]; then
  scope_args+=(--schema "$REPAIR_SCHEMA")
  if [[ -n "$REPAIR_HASH_KEY" ]]; then
    scope_args+=(--hash-key "$REPAIR_HASH_KEY")
  fi
fi
repair_args=(db repair-dangling-tips --json --tip-page "$TIP_PAGE" --audit-limit "$AUDIT_LIMIT")
if [[ -n "$MAX_OPS" ]]; then
  repair_args+=(--max-ops "$MAX_OPS")
fi
repair_args+=(${scope_args[@]+"${scope_args[@]}"})

run_lastdb "${repair_args[@]}" >"$REPORT_DIR/repair-dry-run.json"

EXECUTE_EXIT=0
if [[ "$EXECUTE_COW" == "1" ]]; then
  run_lastdb "${repair_args[@]}" --execute >"$REPORT_DIR/repair-execute.json" || EXECUTE_EXIT=$?
  run_lastdb db repair-dangling-tips --json --tip-page "$TIP_PAGE" --audit-limit "$AUDIT_LIMIT" \
    ${scope_args[@]+"${scope_args[@]}"} \
    >"$REPORT_DIR/repair-after-execute-dry-run.json" || true
  run_lastdb db delete-ledger --json >"$REPORT_DIR/delete-ledger.json" || true
fi

collect_inventory after
run_lastdb status --json >"$REPORT_DIR/status-after.json" 2>"$REPORT_DIR/status-after.err" || true
snapshot_pids "$REPORT_DIR/primary-pids-after.txt"

python3 - "$PROOF_JSON" "$RUN_ID" "$PRIMARY" "$WORK_HOME" "$REPORT_DIR" "$LOG" "$EXECUTE_COW" "$EXECUTE_EXIT" "$PROOF_INVENTORY" <<'PY'
import json, pathlib, sys, time

(proof_path, run_id, primary, work_home, report_dir, log, execute_cow,
 execute_exit, inventory_requested) = sys.argv[1:]
report = pathlib.Path(report_dir)

def load(name):
    path = report / name
    if not path.exists() or path.stat().st_size == 0:
        return None
    return json.loads(path.read_text())

def inventory_path(name):
    """Name the file only if it exists — a path to a file the run never wrote
    reads as collected evidence when it is nothing of the sort."""
    path = report / name
    return str(path) if path.exists() else None

dry_run = load("repair-dry-run.json")
execute = load("repair-execute.json")
after_dry_run = load("repair-after-execute-dry-run.json")
proof = {
    "ok": int(execute_exit) == 0,
    "run_id": run_id,
    "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "primary": primary,
    "work_home": work_home,
    "report_dir": report_dir,
    "log": log,
    "guards": {
        "primary_home_used": False,
        "cow_home_only": True,
        "inherited_cloud_sync_disabled": bool(list(pathlib.Path(work_home).glob("cloud_sync.json.disabled-by-repair-proof-*"))),
        "primary_pids_recorded": True,
    },
    "repair": {
        "dry_run": dry_run,
        "execute_requested": execute_cow == "1",
        "execute_exit": int(execute_exit),
        "execute": execute,
        "after_execute_dry_run": after_dry_run,
    },
    "evidence": {
        "inventory_requested": inventory_requested == "1",
        "inventory_before": inventory_path("inventory-before.json"),
        "inventory_after": inventory_path("inventory-after.json"),
        "status_ready": str(report / "status-ready.json"),
        "status_after": str(report / "status-after.json"),
        "delete_ledger": str(report / "delete-ledger.json"),
        "primary_pids_before": str(report / "primary-pids-before.txt"),
        "primary_pids_after": str(report / "primary-pids-after.txt"),
    },
    "notes": [
        "The live primary is never opened by lastdbd in this harness.",
        "Run with EXECUTE_COW=1 only after the dry-run report matches the expected dangling-tip count.",
        "Live primary repair remains a separate operator-cleared action after lastdb-safe-upgrade.",
    ],
}
pathlib.Path(proof_path).write_text(json.dumps(proof, indent=2) + "\n")
print(f"PROOF_JSON={proof_path}")
print(json.dumps(proof, indent=2))
PY

if [[ "$EXECUTE_EXIT" -ne 0 ]]; then
  fail "repair execute failed with exit $EXECUTE_EXIT"
fi

echo "GREEN repair-dangling-tips-cow-proof report=$PROOF_JSON"
