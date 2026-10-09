#!/usr/bin/env bash
# Ideal-storage plane residue: CoW-first proof (never primary-first).
#
# Card: lastdb-ideal-storage-primary-gated-apply
# Milestone: milestone-lastdb-ideal-storage-disk-matches-map
#
# Steps:
#   1. Refuse operating on live ~/.lastdb / ~/.folddb as the work home
#   2. APFS CoW (or full copy) of PRIMARY into a throwaway home
#   3. Dry-run tip / protein / index residue drains (paged)
#   4. Optional: --execute-cow to mutate only the CoW home (still never primary)
#   5. Emit plane inventory via lastdb status --json when lastdb binary is set
#   6. Write a machine-readable proof JSON under PROOF_DIR
#
# Aside delete is NEVER enabled by this script. Execute mode may drop only
# already-empty CoW residue sources after the bounded copy/delete page reports
# done; primary rollout remains separate.
# Primary rollout is a separate lastdb-safe-upgrade step after GREEN CoW.
#
# Usage:
#   MAINTAIN=./target/debug/lastdb_local_maintain \
#   LASTDB=./target/debug/lastdb \
#     ./scripts/ideal-storage-plane-residue-cow-proof.sh [run-id]
#
# Env:
#   PRIMARY_HOME   default $HOME/.lastdb
#   COW_ROOT       default $HOME/.lastdb-test-copies
#   MAINTAIN       path to lastdb_local_maintain (required)
#   LASTDB         optional path to lastdb CLI for status --json
#   LIMIT          page size (default 200)
#   EXECUTE_COW=1  also run --execute on the CoW home only
#   TIP_COLLECTIONS collections to drain (default: "headers versions"; the
#                   old `field_tips` / `mk:` fallback was pruned on 2026-07-31)
#   PROTEIN_PREFIXES protein prefixes to drain from tips
#   INDEX_TIPS_PREFIXES index prefixes to drain from tips
#   INDEX_LEGACY_SOURCES legacy split index sources to drain into indexes
#   PLANE_RESIDUE_TEE=0 append directly to LOG instead of teeing stdout
#   REUSE_COW=1    reuse existing CoW home for RUN_ID
set -euo pipefail

RUN_ID="${1:-$(date -u +%Y%m%dT%H%M%SZ)}"
PRIMARY="${PRIMARY_HOME:-$HOME/.lastdb}"
COW_ROOT="${COW_ROOT:-$HOME/.lastdb-test-copies}"
MAINTAIN="${MAINTAIN:?set MAINTAIN to lastdb_local_maintain binary}"
LASTDB="${LASTDB:-}"
LIMIT="${LIMIT:-200}"
EXECUTE_COW="${EXECUTE_COW:-0}"
TIP_COLLECTIONS="${TIP_COLLECTIONS:-headers versions}"
PROTEIN_PREFIXES="${PROTEIN_PREFIXES:-protein: molprot: fldprot: pfq:}"
INDEX_TIPS_PREFIXES="${INDEX_TIPS_PREFIXES:-mhr: mhk: mhi: schema_atoms: idx: schemaidx:}"
INDEX_LEGACY_SOURCES="${INDEX_LEGACY_SOURCES:-field-hashrange-page-index field-hashrange-hash-index field-hashrange-complete schema-atom-index legacy-schema-secondary-index}"
PROOF_DIR="${PROOF_DIR:-$COW_ROOT/proofs}"
mkdir -p "$COW_ROOT" "$PROOF_DIR"

PROOF_JSON="$PROOF_DIR/plane-residue-cow-${RUN_ID}.json"
LOG="$PROOF_DIR/plane-residue-cow-${RUN_ID}.log"
REPORTS_JSONL="$PROOF_DIR/plane-residue-cow-${RUN_ID}-reports.jsonl"
BEFORE_PLANES_JSON="$PROOF_DIR/plane-residue-cow-${RUN_ID}-before-status.json"
AFTER_PLANES_JSON="$PROOF_DIR/plane-residue-cow-${RUN_ID}-after-status.json"
: >"$REPORTS_JSONL"
if [[ "${PLANE_RESIDUE_TEE:-1}" == "0" ]]; then
  exec >>"$LOG" 2>&1
else
  exec > >(tee -a "$LOG") 2>&1
fi

echo "=== ideal-storage plane residue CoW proof run=$RUN_ID ==="
echo "primary=$PRIMARY"
echo "maintain=$MAINTAIN ($("$MAINTAIN" --help 2>&1 | head -1 || true))"
echo "EXECUTE_COW=$EXECUTE_COW LIMIT=$LIMIT"
echo "PROTEIN_PREFIXES=$PROTEIN_PREFIXES"
echo "INDEX_TIPS_PREFIXES=$INDEX_TIPS_PREFIXES"
echo "INDEX_LEGACY_SOURCES=$INDEX_LEGACY_SOURCES"

if [[ ! -d "$PRIMARY" || ! -f "$PRIMARY/identity.key" ]]; then
  echo "FAIL: primary home missing or no identity.key"
  exit 1
fi

# Refuse pointing MAINTAIN home at live primary without explicit CoW clone.
refuse_if_primary() {
  local path="$1"
  local abs
  abs=$(cd "$path" 2>/dev/null && pwd -P || echo "$path")
  for name in .lastdb .folddb; do
    local p
    p=$(cd "$HOME/$name" 2>/dev/null && pwd -P || true)
    if [[ -n "$p" && ( "$abs" == "$p" || "$abs" == "$p"/* ) ]]; then
      echo "FAIL: refused primary path $abs — CoW only"
      exit 3
    fi
  done
}

COW="$COW_ROOT/plane-residue-${RUN_ID}"
if [[ "${REUSE_COW:-0}" == "1" && -d "$COW" ]]; then
  echo "Reusing CoW home $COW"
else
  rm -rf "$COW"
  echo "CoW clone → $COW"
  if cp -cR "$PRIMARY" "$COW" 2>/dev/null; then
    echo "CoW: APFS clone ok"
  else
    cp -a "$PRIMARY" "$COW"
    echo "CoW: full copy ok"
  fi
  find "$COW" -name 'folddb.sock' -delete 2>/dev/null || true
  find "$COW" -name '*.sock' -delete 2>/dev/null || true
  # Never inherit cloud sync state onto a throwaway proof home.
  if [[ -f "$COW/cloud_sync.json" ]]; then
    python3 - <<'PY' "$COW/cloud_sync.json"
import json,sys
p=sys.argv[1]
try:
  d=json.load(open(p))
except Exception:
  raise SystemExit(0)
d["enabled"]=False
d["cloud_sync"]=False
json.dump(d, open(p,"w"), indent=2)
print("cloud_sync disabled on CoW home")
PY
  fi
fi

refuse_if_primary "$COW"

run_maintain() {
  local cmd=("$@")
  echo "+ $MAINTAIN --home $COW ${cmd[*]}" >&2
  "$MAINTAIN" --home "$COW" "${cmd[@]}"
}

json_get_bool() {
  python3 -c 'import json,sys; print("true" if json.load(sys.stdin).get(sys.argv[1]) else "false")' "$1"
}

json_get_string() {
  python3 -c 'import json,sys; v=json.load(sys.stdin).get(sys.argv[1]); print("" if v is None else v)' "$1"
}

record_report() {
  local mode="$1"
  local kind="$2"
  local label="$3"
  local report="$4"
  local tmp
  tmp="$(mktemp)"
  printf '%s\n' "$report" >"$tmp"
  python3 - "$REPORTS_JSONL" "$mode" "$kind" "$label" "$tmp" <<'PY'
import json
import sys

out, mode, kind, label, report_path = sys.argv[1:]
with open(report_path) as f:
    report = json.load(f)
required = ["done", "after", "keys_scanned", "skipped"]
missing = [k for k in required if k not in report]
if missing:
    raise SystemExit(f"drain report {mode}/{kind}/{label} missing keys: {missing}")
entry = {
    "mode": mode,
    "kind": kind,
    "label": label,
    "done": bool(report.get("done")),
    "after": report.get("after"),
    "keys_scanned": int(report.get("keys_scanned") or 0),
    "copied": int(report.get("copied_to_tips") or report.get("copied_to_target") or 0),
    "target_already_won": int(report.get("tips_already_won") or report.get("target_already_won") or 0),
    "deleted": int(report.get("deleted_from_legacy") or report.get("deleted_from_source") or 0),
    "skipped": int(report.get("skipped") or 0),
    "dropped": bool(report.get("collection_dropped") or report.get("source_dropped")),
    "report": report,
}
with open(out, "a") as f:
    f.write(json.dumps(entry, sort_keys=True) + "\n")
PY
  rm -f "$tmp"
}

utf8_to_hex() {
  python3 -c 'import sys; print(sys.stdin.buffer.read().decode("utf-8").encode("utf-8").hex())'
}

run_tip_residue_until_done() {
  local col="$1"
  local mode="$2"
  local after_hex=""
  local page=0
  local report is_done after
  while :; do
    page=$((page + 1))
    local args=(drain-tip-residue --collection "$col" --limit "$LIMIT" --json)
    if [[ -n "$after_hex" ]]; then
      args+=(--after-hex "$after_hex")
    fi
    if [[ "$mode" == "execute" ]]; then
      args+=(--execute --drop-empty-collection)
    fi
    report="$(run_maintain "${args[@]}")"
    printf '%s\n' "$report"
    record_report "$mode" tip "$col" "$report"
    is_done="$(printf '%s\n' "$report" | json_get_bool "done")"
    after="$(printf '%s\n' "$report" | json_get_string "after")"
    if [[ "$is_done" == "true" ]]; then
      break
    fi
    if [[ -z "$after" ]]; then
      echo "FAIL: tip residue $col page $page was not done but returned no cursor"
      exit 4
    fi
    after_hex="$(printf '%s' "$after" | utf8_to_hex)"
  done
}

run_protein_residue_until_done() {
  local prefix="$1"
  local mode="$2"
  local after_hex=""
  local page=0
  local report is_done after
  while :; do
    page=$((page + 1))
    local args=(drain-protein-residue --prefix "$prefix" --limit "$LIMIT" --json)
    if [[ -n "$after_hex" ]]; then
      args+=(--after-hex "$after_hex")
    fi
    if [[ "$mode" == "execute" ]]; then
      args+=(--execute)
    fi
    report="$(run_maintain "${args[@]}")"
    printf '%s\n' "$report"
    record_report "$mode" protein "$prefix" "$report"
    is_done="$(printf '%s\n' "$report" | json_get_bool "done")"
    after="$(printf '%s\n' "$report" | json_get_string "after")"
    if [[ "$is_done" == "true" ]]; then
      break
    fi
    if [[ -z "$after" ]]; then
      echo "FAIL: protein residue $prefix page $page was not done but returned no cursor"
      exit 4
    fi
    after_hex="$(printf '%s' "$after" | utf8_to_hex)"
  done
}

run_index_residue_until_done() {
  local source="$1"
  local prefix="$2"
  local mode="$3"
  local after_hex=""
  local page=0
  local report is_done after
  while :; do
    local label="$source"
    page=$((page + 1))
    local args=(drain-index-residue --source "$source" --limit "$LIMIT" --json)
    if [[ -n "$prefix" ]]; then
      args+=(--prefix "$prefix")
      label="$source/$prefix"
    fi
    if [[ -n "$after_hex" ]]; then
      args+=(--after-hex "$after_hex")
    fi
    if [[ "$mode" == "execute" ]]; then
      args+=(--execute --drop-empty-source)
    fi
    report="$(run_maintain "${args[@]}")"
    printf '%s\n' "$report"
    record_report "$mode" index "$label" "$report"
    is_done="$(printf '%s\n' "$report" | json_get_bool "done")"
    after="$(printf '%s\n' "$report" | json_get_string "after")"
    if [[ "$is_done" == "true" ]]; then
      break
    fi
    if [[ -z "$after" ]]; then
      echo "FAIL: index residue $label page $page was not done but returned no cursor"
      exit 4
    fi
    after_hex="$(printf '%s' "$after" | utf8_to_hex)"
  done
}

echo "=== plane map before (if LASTDB set) ==="
BEFORE_PLANES=""
if [[ -n "$LASTDB" && -x "$LASTDB" ]]; then
  BEFORE_PLANES=$("$LASTDB" --data-dir "$COW" status --json 2>/dev/null || true)
  if [[ -n "$BEFORE_PLANES" ]]; then
    printf '%s\n' "$BEFORE_PLANES" >"$BEFORE_PLANES_JSON"
  fi
  echo "$BEFORE_PLANES" | python3 -c "
import json,sys
raw=sys.stdin.read().strip()
if not raw:
  print('status --json unavailable on this binary/home')
  raise SystemExit(0)
d=json.loads(raw)
p=d.get('planes') or {}
print('collections', p.get('collection_count'), 'total_bytes', p.get('total_bytes'))
for r in (p.get('by_role') or [])[:12]:
  print(' ', r.get('label'), r.get('bytes'), r.get('collections'))
" || true
fi

echo "=== dry-run tip residue (${TIP_COLLECTIONS}) ==="
for col in $TIP_COLLECTIONS; do
  run_tip_residue_until_done "$col" dry-run
done

echo "=== dry-run protein residue (tips -> proteins) ==="
for prefix in $PROTEIN_PREFIXES; do
  run_protein_residue_until_done "$prefix" dry-run
done

echo "=== dry-run index residue (tips -> indexes) ==="
for prefix in $INDEX_TIPS_PREFIXES; do
  run_index_residue_until_done tips "$prefix" dry-run
done

echo "=== dry-run index residue (legacy splits -> indexes) ==="
for source in $INDEX_LEGACY_SOURCES; do
  run_index_residue_until_done "$source" "" dry-run
done

EXECUTED=0
if [[ "$EXECUTE_COW" == "1" ]]; then
  echo "=== EXECUTE on CoW only (drop emptied tip/index residue sources; no aside) ==="
  for col in $TIP_COLLECTIONS; do
    run_tip_residue_until_done "$col" execute
  done
  for prefix in $PROTEIN_PREFIXES; do
    run_protein_residue_until_done "$prefix" execute
  done
  for prefix in $INDEX_TIPS_PREFIXES; do
    run_index_residue_until_done tips "$prefix" execute
  done
  for source in $INDEX_LEGACY_SOURCES; do
    run_index_residue_until_done "$source" "" execute
  done
  EXECUTED=1
fi

echo "=== plane map after (if LASTDB set) ==="
AFTER_PLANES=""
if [[ -n "$LASTDB" && -x "$LASTDB" ]]; then
  AFTER_PLANES=$("$LASTDB" --data-dir "$COW" status --json 2>/dev/null || true)
  if [[ -n "$AFTER_PLANES" ]]; then
    printf '%s\n' "$AFTER_PLANES" >"$AFTER_PLANES_JSON"
  fi
  echo "$AFTER_PLANES" | python3 -c "
import json,sys
raw=sys.stdin.read().strip()
if not raw:
  print('status --json unavailable on this binary/home')
  raise SystemExit(0)
d=json.loads(raw)
p=d.get('planes') or {}
print('collections', p.get('collection_count'), 'total_bytes', p.get('total_bytes'))
for r in (p.get('by_role') or [])[:12]:
  print(' ', r.get('label'), r.get('bytes'), r.get('collections'))
" || true
fi

python3 - <<PY
import json, os, time
from collections import defaultdict

reports = []
with open("$REPORTS_JSONL") as f:
    for line in f:
        line = line.strip()
        if line:
            reports.append(json.loads(line))

latest = {}
totals = defaultdict(int)
for entry in reports:
    key = f"{entry['mode']}:{entry['kind']}:{entry['label']}"
    latest[key] = entry
    for field in ("keys_scanned", "copied", "target_already_won", "deleted", "skipped"):
        totals[field] += int(entry.get(field) or 0)

tip_collections = "$TIP_COLLECTIONS".split()
protein_prefixes = "$PROTEIN_PREFIXES".split()
index_tips_prefixes = "$INDEX_TIPS_PREFIXES".split()
index_legacy_sources = "$INDEX_LEGACY_SOURCES".split()
expected_dry_labels = (
  [f"dry-run:tip:{col}" for col in tip_collections]
  + [f"dry-run:protein:{prefix}" for prefix in protein_prefixes]
  + [f"dry-run:index:tips/{prefix}" for prefix in index_tips_prefixes]
  + [f"dry-run:index:{source}" for source in index_legacy_sources]
)
expected_execute_labels = []
if bool($EXECUTED):
  expected_execute_labels = [label.replace("dry-run:", "execute:", 1) for label in expected_dry_labels]
expected_labels = expected_dry_labels + expected_execute_labels
missing_labels = [label for label in expected_labels if label not in latest]
latest_not_done = [
  label for label, entry in latest.items()
  if label in expected_labels and (not entry.get("done") or entry.get("after") is not None)
]

order_log_sources = {"field_update_order_log", "field_update_order_count"}
field_hashrange_sources = {
  "field-hashrange-page-index",
  "field-hashrange-hash-index",
  "field-hashrange-complete",
}
gates = {
  "cow_home_only": os.path.abspath("$COW") != os.path.abspath("$PRIMARY"),
  "primary_not_mutated": True,
  "primary_refuse_guard_enabled": True,
  "reports_captured": bool(reports),
  "all_expected_reports_present": not missing_labels,
  "latest_pages_done_and_cursor_cleared": not latest_not_done,
  "order_log_excluded": not bool(order_log_sources.intersection(index_legacy_sources)),
  "field_hashrange_legacy_sources_covered": field_hashrange_sources.issubset(set(index_legacy_sources)),
  "tips_prefix_drains_are_scoped": all(prefix for prefix in protein_prefixes + index_tips_prefixes),
  "aside_delete_disabled": True,
  "drop_empty_collection_only_on_execute_cow": bool($EXECUTED) == bool("$EXECUTE_COW" == "1"),
}
failed_gates = [name for name, ok in gates.items() if not ok]
drain_summary = {
  "report_count": len(reports),
  "latest_by_mode_kind_label": latest,
  "totals": dict(totals),
  "missing_expected_labels": missing_labels,
  "latest_not_done": latest_not_done,
}
proof = {
  "run_id": "$RUN_ID",
  "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
  "primary": "$PRIMARY",
  "cow_home": "$COW",
  "limit": int("$LIMIT"),
  "protein_prefixes": "$PROTEIN_PREFIXES".split(),
  "index_tips_prefixes": "$INDEX_TIPS_PREFIXES".split(),
  "index_legacy_sources": "$INDEX_LEGACY_SOURCES".split(),
  "execute_cow": bool($EXECUTED),
  "aside_delete": False,
  "drop_empty_collection": bool($EXECUTED),
  "primary_mutated": False,
  "reports_jsonl": "$REPORTS_JSONL",
  "before_planes_json": "$BEFORE_PLANES_JSON" if os.path.exists("$BEFORE_PLANES_JSON") else None,
  "after_planes_json": "$AFTER_PLANES_JSON" if os.path.exists("$AFTER_PLANES_JSON") else None,
  "gates": gates,
  "failed_gates": failed_gates,
  "drain_summary": drain_summary,
  "verdict": "GREEN_COW_PROOF" if not failed_gates else "RED_COW_PROOF",
  "notes": [
    "CoW-only residue drain proof for ideal-storage primary-gated-apply",
    "field_tips/mk live fallback was pruned on 2026-07-31; remaining tip residue is mh/tv headers/versions",
    "Primary rollout requires lastdb-safe-upgrade after GREEN CoW + Situation clearance",
    "Aside reclaim remains Tom-gated after backup receipt",
    "Order-log is history-adjacent and is never drained by this proof",
  ],
  "log": "$LOG",
}
open("$PROOF_JSON","w").write(json.dumps(proof, indent=2) + "\n")
print("PROOF_JSON=$PROOF_JSON")
print(json.dumps(proof, indent=2))
if failed_gates:
  raise SystemExit("RED_COW_PROOF failed_gates=" + ",".join(failed_gates))
PY

echo "VERDICT: GREEN_COW_PROOF (primary not mutated; safe-upgrade separate)"
echo "Primary gated apply next:"
echo "  1) situations preflight --action safe-upgrade --system lastdbd"
echo "  2) bash ~/.last-stack/skills/lastdb-safe-upgrade/scripts/safe-upgrade-lastdb.sh --candidate <lastdbd with this main> --probe-only"
echo "  3) only if GREEN and Situation allows: same with --yes"
echo "  4) post-rollout: lastdb status --json | dual_read + planes"
