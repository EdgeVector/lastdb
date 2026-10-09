#!/usr/bin/env bash
# Cloud-sync pin-mode CoW proof harness.
#
# This is intentionally CoW/copy-first. It may read PRIMARY_HOME as the source,
# but every executable action targets WORK_HOME, which must not resolve to the
# primary LastDB/FoldDB home.
#
# Usage:
#   ./scripts/cloud-sync-pin-mode-cow-proof.sh [run-id]
#
# Env:
#   PRIMARY_HOME         default $HOME/.lastdb
#   PROOF_ROOT           default $HOME/.local/state/last-stack/cloud-sync-pin-mode-cow-proof
#   WORK_HOME            default $PROOF_ROOT/runs/<run-id>/home
#   REUSE_WORK_HOME=1    reuse an existing WORK_HOME
#   ALLOW_COW_CLOUD_SYNC=1 keep inherited cloud_sync.json on the clone
#   EXECUTE_WRITES=1     run CSYNC_E2E_WRITE against WORK_HOME after F0
#   CSYNC_E2E_WRITE      writer binary, e.g. target/debug/csync_e2e_write
#   WRITE_COUNT          rows per write round, default 32
#   WRITE_ROUNDS         writer rounds, default 1
#   LASTDB               optional lastdb CLI for status --json evidence
#   PIN_PROOF_TEE=0      append directly to LOG instead of teeing stdout
set -euo pipefail

fail() {
  echo "RED cloud-sync-pin-mode-cow-proof: $*" >&2
  exit 1
}

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="${1:-$(date -u +%Y%m%dT%H%M%SZ)}"
PRIMARY="${PRIMARY_HOME:-$HOME/.lastdb}"
PROOF_ROOT="${PROOF_ROOT:-$HOME/.local/state/last-stack/cloud-sync-pin-mode-cow-proof}"
RUN_DIR="${RUN_DIR:-$PROOF_ROOT/runs/$RUN_ID}"
WORK_HOME="${WORK_HOME:-$RUN_DIR/home}"
REPORT_DIR="$RUN_DIR/report"
LOG="$REPORT_DIR/cloud-sync-pin-mode-cow-proof.log"
PROOF_JSON="$REPORT_DIR/proof.json"
F0_JSON="$REPORT_DIR/pin-boundary-F0.json"
F1_JSON="$REPORT_DIR/post-write-F1.json"
LASTDB="${LASTDB:-}"
EXECUTE_WRITES="${EXECUTE_WRITES:-0}"
CSYNC_E2E_WRITE="${CSYNC_E2E_WRITE:-$ROOT/target/debug/csync_e2e_write}"
WRITE_COUNT="${WRITE_COUNT:-32}"
WRITE_ROUNDS="${WRITE_ROUNDS:-1}"

mkdir -p "$REPORT_DIR"
if [[ "${PIN_PROOF_TEE:-1}" == "0" ]]; then
  exec >>"$LOG" 2>&1
else
  exec > >(tee -a "$LOG") 2>&1
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
path = pathlib.Path(sys.argv[1]).resolve(strict=False)
parent = pathlib.Path(sys.argv[2]).resolve(strict=False)
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

snapshot_pids() {
  local out="$1"
  pgrep -f 'lastdbd( |$)|target/.*/lastdbd' 2>/dev/null | while read -r pid; do
    if ! ps -wwp "$pid" -o command= 2>/dev/null | grep -qF "$RUN_DIR"; then
      echo "$pid"
    fi
  done | sort >"$out" || true
}

inventory_home() {
  local phase="$1"
  local home="$2"
  local out="$3"
  python3 - "$phase" "$home" "$out" <<'PY'
import json, os, pathlib, sys, time

phase, home_arg, out_arg = sys.argv[1:4]
home = pathlib.Path(home_arg).resolve(strict=False)
interesting = ("backup", "chunk", "cloud", "laststore", "manifest", "order", "sync")
files_total = 0
bytes_total = 0
candidate_files = []
top_level = []

if home.exists():
    top_level = sorted(p.name for p in home.iterdir())[:100]
    for root, dirs, files in os.walk(home):
        dirs[:] = [d for d in dirs if not d.endswith(".sock")]
        for name in files:
            path = pathlib.Path(root) / name
            try:
                st = path.stat()
            except OSError:
                continue
            files_total += 1
            bytes_total += st.st_size
            rel = path.relative_to(home).as_posix()
            if any(token in rel.lower() for token in interesting):
                candidate_files.append({
                    "path": rel,
                    "bytes": st.st_size,
                    "mtime": int(st.st_mtime),
                })

active = home / "cloud_sync.json"
paused = home / "cloud_sync.json.paused"
disabled = sorted(p.name for p in home.glob("cloud_sync.json.disabled-by-pin-proof-*"))
doc = {
    "phase": phase,
    "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "home": str(home),
    "exists": home.exists(),
    "identity_key": (home / "identity.key").exists(),
    "cloud_sync": {
        "active": active.exists(),
        "paused": paused.exists(),
        "disabled_by_harness": disabled,
    },
    "files_total": files_total,
    "bytes_total": bytes_total,
    "top_level_entries": top_level,
    "candidate_files": sorted(candidate_files, key=lambda x: x["path"])[:500],
}
pathlib.Path(out_arg).write_text(json.dumps(doc, indent=2) + "\n")
print(f"{phase}_INVENTORY={out_arg} files={files_total} bytes={bytes_total} candidates={len(doc['candidate_files'])}")
PY
}

run_lastdb_status() {
  local label="$1"
  local out="$REPORT_DIR/status-${label}.json"
  if [[ -n "$LASTDB" && -x "$LASTDB" ]]; then
    "$LASTDB" --data-dir "$WORK_HOME" status --json >"$out" 2>"$REPORT_DIR/status-${label}.err" || true
  fi
}

echo "=== cloud-sync pin-mode CoW proof run=$RUN_ID ==="
echo "root=$ROOT"
echo "primary=$PRIMARY"
echo "work_home=$WORK_HOME"
echo "report_dir=$REPORT_DIR"
echo "execute_writes=$EXECUTE_WRITES write_count=$WRITE_COUNT write_rounds=$WRITE_ROUNDS"

[[ -d "$PRIMARY" ]] || fail "PRIMARY_HOME missing: $PRIMARY"
[[ -f "$PRIMARY/identity.key" ]] || fail "PRIMARY_HOME has no identity.key: $PRIMARY"
refuse_work_home_if_primary "$WORK_HOME" "$PRIMARY"

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

if [[ "${ALLOW_COW_CLOUD_SYNC:-0}" != "1" && -f "$WORK_HOME/cloud_sync.json" ]]; then
  mv "$WORK_HOME/cloud_sync.json" "$WORK_HOME/cloud_sync.json.disabled-by-pin-proof-$RUN_ID"
  echo "disabled inherited cloud_sync.json on CoW home"
fi

inventory_home "F0" "$WORK_HOME" "$F0_JSON"
run_lastdb_status "F0"

WRITES_EXIT=0
if [[ "$EXECUTE_WRITES" == "1" ]]; then
  [[ -x "$CSYNC_E2E_WRITE" ]] || fail "CSYNC_E2E_WRITE is not executable: $CSYNC_E2E_WRITE"
  for round in $(seq 1 "$WRITE_ROUNDS"); do
    echo "+ $CSYNC_E2E_WRITE $WORK_HOME $WRITE_COUNT # round $round"
    "$CSYNC_E2E_WRITE" "$WORK_HOME" "$WRITE_COUNT" \
      >"$REPORT_DIR/writer-round-${round}.out" \
      2>"$REPORT_DIR/writer-round-${round}.err" || WRITES_EXIT=$?
    [[ "$WRITES_EXIT" -eq 0 ]] || break
  done
fi

inventory_home "F1" "$WORK_HOME" "$F1_JSON"
run_lastdb_status "F1"
snapshot_pids "$REPORT_DIR/primary-pids-after.txt"

python3 - "$PROOF_JSON" "$RUN_ID" "$PRIMARY" "$WORK_HOME" "$REPORT_DIR" "$LOG" "$F0_JSON" "$F1_JSON" "$EXECUTE_WRITES" "$WRITES_EXIT" "$WRITE_COUNT" "$WRITE_ROUNDS" <<'PY'
import json, pathlib, sys, time

(
    proof_path,
    run_id,
    primary,
    work_home,
    report_dir,
    log,
    f0_path,
    f1_path,
    execute_writes,
    writes_exit,
    write_count,
    write_rounds,
) = sys.argv[1:]
f0 = json.load(open(f0_path))
f1 = json.load(open(f1_path))
proof = {
    "ok": int(writes_exit) == 0,
    "run_id": run_id,
    "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "primary": primary,
    "work_home": work_home,
    "report_dir": report_dir,
    "log": log,
    "pin_boundary": "F0",
    "guards": {
        "primary_home_used": False,
        "inherited_cloud_sync_disabled": bool(f0["cloud_sync"]["disabled_by_harness"]),
        "primary_pids_recorded": True,
    },
    "writes": {
        "executed": execute_writes == "1",
        "exit_code": int(writes_exit),
        "count_per_round": int(write_count),
        "rounds": int(write_rounds),
    },
    "evidence": {
        "f0": f0_path,
        "f1": f1_path,
        "status_f0": str(pathlib.Path(report_dir) / "status-F0.json"),
        "status_f1": str(pathlib.Path(report_dir) / "status-F1.json"),
        "primary_pids_before": str(pathlib.Path(report_dir) / "primary-pids-before.txt"),
        "primary_pids_after": str(pathlib.Path(report_dir) / "primary-pids-after.txt"),
    },
    "summary": {
        "f0_candidate_files": len(f0["candidate_files"]),
        "f1_candidate_files": len(f1["candidate_files"]),
        "f0_bytes": f0["bytes_total"],
        "f1_bytes": f1["bytes_total"],
    },
    "notes": [
        "F0 is the sealed-base candidate inventory captured before proof writes.",
        "Executable writes, when enabled, target only WORK_HOME.",
        "Cloud sync config copied from the primary is disabled by default on the clone.",
    ],
}
pathlib.Path(proof_path).write_text(json.dumps(proof, indent=2) + "\n")
print(f"PROOF_JSON={proof_path}")
print(json.dumps(proof, indent=2))
PY

if [[ "$WRITES_EXIT" -ne 0 ]]; then
  fail "writer failed with exit $WRITES_EXIT"
fi

echo "GREEN cloud-sync-pin-mode-cow-proof report=$PROOF_JSON"
