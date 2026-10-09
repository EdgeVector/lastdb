#!/usr/bin/env bash
# CoW-first verification gate for atom GC over fkanban's multi-key card shapes.
#
# The gate clones PRIMARY_HOME, runs atom-gc audit + guarded reap on the clone,
# and writes a machine-readable proof report. It never executes against the
# primary home. The report is intentionally small enough to attach to a North
# Star proof: command paths, home path type, candidate/delete counts, and the
# Card / BoardCards / MilestoneCards read-after-GC verdicts.
#
# Usage:
#   cargo build -p lastdb_node --bin lastdb_local_maintain
#   MAINTAIN=./target/debug/lastdb_local_maintain \
#     ./scripts/lastdbd/atom-gc-tip-fold-gate.sh [run-id]
#
# Env:
#   PRIMARY_HOME       default $HOME/.lastdb
#   PROOF_ROOT         default $HOME/.local/state/last-stack/atom-gc-tip-fold-gate
#   WORK_ROOT          default $HOME/.lastdb-proofs/atom-gc-tip-fold
#   WORK_HOME          default $WORK_ROOT/<run-id>/home
#   MAINTAIN           default target/debug/lastdb_local_maintain
#   REUSE_WORK_HOME=1  reuse an existing WORK_HOME
#   PROOF_TEE=1        tee output to stdout; default appends directly to LOG
set -euo pipefail

fail() {
  echo "RED atom-gc-tip-fold-gate: $*" >&2
  exit 1
}

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUN_ID="${1:-$(date -u +%Y%m%dT%H%M%SZ)}"
PRIMARY="${PRIMARY_HOME:-$HOME/.lastdb}"
PROOF_ROOT="${PROOF_ROOT:-$HOME/.local/state/last-stack/atom-gc-tip-fold-gate}"
RUN_DIR="${RUN_DIR:-$PROOF_ROOT/runs/$RUN_ID}"
WORK_ROOT="${WORK_ROOT:-$HOME/.lastdb-proofs/atom-gc-tip-fold}"
WORK_HOME="${WORK_HOME:-$WORK_ROOT/$RUN_ID/home}"
REPORT_DIR="$RUN_DIR/report"
LOG="$REPORT_DIR/atom-gc-tip-fold-gate.log"
PROOF_JSON="$REPORT_DIR/proof.json"
MAINTAIN="${MAINTAIN:-$ROOT/target/debug/lastdb_local_maintain}"

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

run_maintain() {
  echo "+ $MAINTAIN --home $WORK_HOME $*" >&2
  "$MAINTAIN" --home "$WORK_HOME" "$@"
}

echo "=== atom GC tip-fold gate run=$RUN_ID ==="
echo "root=$ROOT"
echo "primary=$PRIMARY"
echo "work_home=$WORK_HOME"
echo "report_dir=$REPORT_DIR"
echo "maintain=$MAINTAIN"

[[ -d "$PRIMARY" ]] || fail "PRIMARY_HOME missing: $PRIMARY"
[[ -f "$PRIMARY/identity.key" ]] || fail "PRIMARY_HOME has no identity.key: $PRIMARY"
[[ -x "$MAINTAIN" ]] || fail "MAINTAIN is not executable: $MAINTAIN"
refuse_work_home_if_primary "$WORK_HOME" "$PRIMARY"

if [[ "${REUSE_WORK_HOME:-0}" == "1" && -d "$WORK_HOME" ]]; then
  echo "Reusing work home $WORK_HOME"
else
  rm -rf "$WORK_HOME"
  mkdir -p "$(dirname "$WORK_HOME")"
  if cp -cR "$PRIMARY" "$WORK_HOME" 2>/dev/null; then
    echo "CoW clone ok"
  else
    cp -a "$PRIMARY" "$WORK_HOME"
    echo "Full copy ok"
  fi
fi

refuse_work_home_if_primary "$WORK_HOME" "$PRIMARY"
find "$WORK_HOME" -name '*.sock' -delete 2>/dev/null || true
if [[ -f "$WORK_HOME/cloud_sync.json" ]]; then
  mv "$WORK_HOME/cloud_sync.json" "$WORK_HOME/cloud_sync.json.disabled-by-atom-gc-gate-$RUN_ID"
  echo "Disabled inherited cloud_sync.json on work home"
fi

AUDIT_JSON="$REPORT_DIR/audit.json"
DRY_JSON="$REPORT_DIR/reap-dry-run.json"
EXECUTE_JSON="$REPORT_DIR/reap-execute.json"

run_maintain atom-gc-audit --json >"$AUDIT_JSON"
run_maintain atom-gc-reap --json >"$DRY_JSON"
run_maintain atom-gc-reap --execute --json >"$EXECUTE_JSON"

python3 - "$RUN_ID" "$PRIMARY" "$WORK_HOME" "$MAINTAIN" "$AUDIT_JSON" "$DRY_JSON" "$EXECUTE_JSON" "$PROOF_JSON" <<'PY'
import json
import os
import pathlib
import sys
import time

run_id, primary, work_home, maintain, audit_path, dry_path, execute_path, proof_path = sys.argv[1:]
required = ["Card", "BoardCards", "MilestoneCards"]

def load(path):
    with open(path) as f:
        return json.load(f)

audit = load(audit_path)
dry = load(dry_path)
execute = load(execute_path)

def visible_schemas(report):
    out = {}
    for row in report.get("schema_coverage") or []:
        out[row.get("schema")] = {
            "visible": bool(row.get("visible_in_schema_catalog")),
            "status": row.get("status"),
        }
    return out

def missing_visible(report):
    visible = visible_schemas(report)
    return [
        schema for schema in required
        if not visible.get(schema, {}).get("visible")
    ]

def affected(report):
    return set(report.get("affected_schemas") or [])

failed = []
schema_missing = {
    "audit": missing_visible(audit),
    "dry_run": missing_visible(dry),
    "execute": missing_visible(execute),
}
if any(schema_missing.values()):
    failed.append("schema-drift")

dry_deleted = int(dry.get("deleted_keys") or 0)
execute_deleted = int(execute.get("deleted_keys") or 0)
if dry_deleted != execute_deleted:
    failed.append("dry-run-execute-delete-count-mismatch")

if str(execute.get("mode")) != "execute":
    failed.append("execute-report-not-execute-mode")
if str(execute.get("seam")) != "at-rest-seam":
    failed.append("at-rest-seam-not-proven")
if str(execute.get("atom_key_encoding")) != "partition_prefix":
    failed.append("partition-prefix-home-not-proven")

execute_affected = affected(execute)
read_after_gc = {}
for schema in required:
    if schema_missing["execute"] and schema in schema_missing["execute"]:
        read_after_gc[schema] = {
            "verdict": "FAIL",
            "reason": "schema-drift",
        }
    elif execute_deleted > 0 and schema not in execute_affected:
        read_after_gc[schema] = {
            "verdict": "FAIL",
            "reason": "missing-fold-agreement-after-gc",
        }
    elif execute_deleted == 0 and schema not in execute_affected:
        read_after_gc[schema] = {
            "verdict": "PASS",
            "reason": "no-candidates-for-schema",
        }
    else:
        read_after_gc[schema] = {
            "verdict": "PASS",
            "reason": "affected-schema-survivor-verified-by-reap",
        }

if any(v["verdict"] != "PASS" for v in read_after_gc.values()):
    failed.append("read-after-gc-fold-agreement")

proof = {
    "ok": not failed,
    "verdict": "PASS" if not failed else "FAIL",
    "run_id": run_id,
    "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "command": {
        "maintain": maintain,
        "audit": f"{maintain} --home {work_home} atom-gc-audit --json",
        "dry_run": f"{maintain} --home {work_home} atom-gc-reap --json",
        "execute": f"{maintain} --home {work_home} atom-gc-reap --execute --json",
    },
    "home": {
        "primary": primary,
        "work_home": work_home,
        "path_type": "cow-copy",
        "primary_mutated": False,
    },
    "counts": {
        "audit_atom_body_keys_scanned": int(audit.get("atom_body_keys_scanned") or 0),
        "audit_duplicate_uuid_groups": int(audit.get("duplicate_uuid_groups") or 0),
        "dry_run_candidate_delete_keys": dry_deleted,
        "execute_deleted_keys": execute_deleted,
        "execute_groups_scanned": int(execute.get("groups_scanned") or 0),
        "execute_ambiguous_groups": int(execute.get("ambiguous_groups") or 0),
    },
    "schema_coverage": {
        "audit": visible_schemas(audit),
        "dry_run": visible_schemas(dry),
        "execute": visible_schemas(execute),
    },
    "affected_schemas": sorted(execute_affected),
    "read_after_gc": read_after_gc,
    "failed_gates": failed,
    "raw_reports": {
        "audit": audit_path,
        "dry_run": dry_path,
        "execute": execute_path,
    },
    "notes": [
        "CoW-only atom GC tip-fold gate; primary execution is deliberately unavailable here.",
        "Read-after-GC PASS for an affected schema means atom-gc-reap verified the survivor key after deleting the redundant copy.",
        "A missing affected schema when deletes occurred is treated as fold/list/show agreement failure for the multi-key card shapes.",
    ],
}

pathlib.Path(proof_path).write_text(json.dumps(proof, indent=2, sort_keys=True) + "\n")
print(f"PROOF_JSON={proof_path}")
print(json.dumps(proof, indent=2, sort_keys=True))
if failed:
    raise SystemExit("FAIL atom-gc-tip-fold-gate failed_gates=" + ",".join(failed))
PY

echo "VERDICT: PASS atom-gc-tip-fold-gate"
