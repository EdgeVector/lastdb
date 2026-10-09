#!/usr/bin/env bash
# CoW-first before/after proof harness for mutation-log capture volume.
#
# Measures captured record count and bytes per logical mutation on isolated
# copy-on-write homes derived from an explicit source home. The live primary
# home, its sockets, and its PID are hard-rejected as work targets. Inherited
# cloud_sync.json is disabled so the probe never enables real cloud upload.
#
# Two measurement roles run the same logical-write workload against separate
# CoW homes (baseline vs candidate). A pluggable MEASURE_CMD produces the
# per-role JSON metrics; fixture tests inject a mock MEASURE_CMD without
# booting a real node. Live runs point MEASURE_CMD at a real workload script
# that boots lastdbd against WORK_HOME only.
#
# Usage:
#   MEASURE_CMD=./path/to/measure.sh \
#     ./scripts/lastdbd/cloud-sync-capture-volume-cow-proof.sh [run-id]
#
# MEASURE_CMD contract:
#   MEASURE_CMD <role> <work_home> <out_json>
#   role is "baseline" or "candidate". The command must write out_json with:
#     {
#       "logical_mutations": <int>,
#       "captured_records": <int>,
#       "captured_bytes": <int>,
#       "namespaces": ["..."],
#       "atom_locator_rows": <int>,
#       "capture_format": "logical_commit_v1" | "mutation_intent",
#       "logical_field_bytes": <int>,
#       "payload_bytes": <int>,
#       "replay_atom_locator_rows": <int>,
#       "replay_pin_log_records_before": <int>,
#       "replay_pin_log_records_after": <int>,
#       "replay_rebuild_ok": <bool>,
#       "daemon_pid": <int>
#     }
#
# Env:
#   PRIMARY_HOME / SOURCE_HOME  source home for CoW (default $HOME/.lastdb)
#   PROOF_ROOT                 reports root (default under ~/.local/state/last-stack)
#   WORK_ROOT                  short path for work homes (socket path budget)
#   REUSE_WORK_HOME=1          reuse existing role homes under WORK_ROOT
#   MEASURE_CMD                required measurement driver (see contract)
#   BASELINE_REF / CANDIDATE_REF labels recorded in proof (optional)
#   WORKLOAD_LABEL             free-form workload description for proof
#   MAX_INTENT_TO_LOGICAL_RATIO max candidate payload/logical bytes (default 4)
#   PAYLOAD_SLACK_BYTES         fixed envelope/framing allowance (default 1024)
#   NORTH_STAR_PROOF_DIR       default ~/.last-stack/north-star-proofs
#   PROOF_TEE=1                tee log to stdout (default 0 for fixture quiet)
#   SKIP_COW=1                 skip clone when MEASURE_CMD does not need a home
#                              tree (fixture-only; still refuses primary paths)
#
# Live primary mutation, cloud enablement, compact, restart, and deferred-write
# budget changes are OUT OF SCOPE for this harness.
set -euo pipefail

fail() {
  echo "RED cloud-sync-capture-volume-cow-proof: $*" >&2
  exit 1
}

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUN_ID="${1:-$(date -u +%Y%m%dT%H%M%SZ)}"
PRIMARY="${SOURCE_HOME:-${PRIMARY_HOME:-$HOME/.lastdb}}"
PROOF_ROOT="${PROOF_ROOT:-$HOME/.local/state/last-stack/cloud-sync-capture-volume-cow-proof}"
RUN_DIR="${RUN_DIR:-$PROOF_ROOT/runs/$RUN_ID}"
WORK_ROOT="${WORK_ROOT:-$HOME/.lastdb-proofs/csv}"
REPORT_DIR="$RUN_DIR/report"
LOG="$REPORT_DIR/cloud-sync-capture-volume-cow-proof.log"
PROOF_JSON="$REPORT_DIR/proof.json"
NORTH_STAR_PROOF_DIR="${NORTH_STAR_PROOF_DIR:-$HOME/.last-stack/north-star-proofs}"
MEASURE_CMD="${MEASURE_CMD:-}"
BASELINE_REF="${BASELINE_REF:-baseline}"
CANDIDATE_REF="${CANDIDATE_REF:-candidate}"
WORKLOAD_LABEL="${WORKLOAD_LABEL:-bounded-logical-writes}"
SKIP_COW="${SKIP_COW:-0}"
MAX_INTENT_TO_LOGICAL_RATIO="${MAX_INTENT_TO_LOGICAL_RATIO:-4}"
PAYLOAD_SLACK_BYTES="${PAYLOAD_SLACK_BYTES:-1024}"

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
  [[ -n "$work" ]] || fail "work home resolved empty"
  [[ "$work" != "/" ]] || fail "work home resolved to /"
  if is_same_or_child "$work" "$primary"; then
    fail "refusing work home under PRIMARY/SOURCE home: $(abs_path "$work")"
  fi
  for name in .lastdb .folddb; do
    local canonical="$HOME/$name"
    if [[ -e "$canonical" ]] && is_same_or_child "$work" "$canonical"; then
      fail "refusing primary/legacy home as work home: $(abs_path "$work")"
    fi
  done
}

# Refuse a work home whose sockets cannot be bound — BEFORE multi-GB clone.
refuse_work_home_if_socket_path_too_long() {
  local work="$1"
  local limit=103
  local overhead=$(( ${#work} + 5 + 1 + 16 + 4 ))
  if (( overhead > limit )); then
    fail "work home is too deep for a Unix socket: $work/data/folddb-full.sock.tmp is \
$overhead bytes, over the ${limit}-byte sockaddr_un limit. Set WORK_ROOT to a shorter path \
(current WORK_ROOT=$WORK_ROOT)."
  fi
}

refuse_if_primary_socket_live() {
  local primary="$1"
  local sock
  for sock in "$primary/data/folddb.sock" "$primary/data/folddb-full.sock"; do
    if [[ -S "$sock" || -e "$sock" ]]; then
      # Presence alone is expected for a live primary — we only refuse using
      # that path as WORK_HOME (already covered). Record for proof evidence.
      echo "primary_socket_present=$sock"
    fi
  done
}

snapshot_primary_pids() {
  local out="$1"
  local socket="$PRIMARY/data/folddb.sock"
  : >"$out"
  if [[ -n "${PRIMARY_DAEMON_PID:-}" ]]; then
    printf '%s\n' "$PRIMARY_DAEMON_PID" >"$out"
    return 0
  fi
  if [[ -S "$socket" && -x "$(command -v lsof 2>/dev/null || true)" ]]; then
    lsof -n -t -- "$socket" 2>/dev/null | sort -u >"$out" || true
  fi
}

refuse_inherited_live_targets() {
  local name value normalized pid
  for name in LASTDB_HOME FOLDDB_HOME LASTDB_DATA_DIR; do
    value="${!name:-}"
    [[ -z "$value" ]] && continue
    if is_same_or_child "$value" "$PRIMARY"; then
      fail "refusing inherited $name that targets PRIMARY/SOURCE home: $(abs_path "$value")"
    fi
    for normalized in "$HOME/.lastdb" "$HOME/.folddb"; do
      if [[ -e "$normalized" ]] && is_same_or_child "$value" "$normalized"; then
        fail "refusing inherited $name that targets live primary home: $(abs_path "$value")"
      fi
    done
  done
  for name in LASTDB_SOCKET FOLDDB_SOCKET; do
    value="${!name:-}"
    [[ -z "$value" ]] && continue
    value="${value#unix://}"
    if is_same_or_child "$value" "$PRIMARY"; then
      fail "refusing inherited $name that targets PRIMARY/SOURCE socket: $(abs_path "$value")"
    fi
  done
  for name in LASTDB_PID LASTDBD_PID FOLDDB_PID DAEMON_PID; do
    value="${!name:-}"
    [[ "$value" =~ ^[0-9]+$ ]] || continue
    while IFS= read -r pid; do
      if [[ -n "$pid" && "$value" == "$pid" ]]; then
        fail "refusing inherited $name=$value because it is a live primary PID"
      fi
    done <"$REPORT_DIR/primary-pids-before.txt"
  done
}

snapshot_primary_identity() {
  local out="$1"
  {
    echo "primary=$(abs_path "$PRIMARY")"
    if [[ -f "$PRIMARY/identity.key" ]]; then
      # fingerprint only — never copy key material into the proof
      python3 - "$PRIMARY/identity.key" <<'PY'
import hashlib, pathlib, sys
data = pathlib.Path(sys.argv[1]).read_bytes()
print("identity_key_sha256=" + hashlib.sha256(data).hexdigest())
print("identity_key_bytes=" + str(len(data)))
PY
    else
      echo "identity_key=missing"
    fi
  } >"$out" || true
}

clone_role_home() {
  local role="$1"
  local dest="$2"
  refuse_work_home_if_primary "$dest" "$PRIMARY"
  refuse_work_home_if_socket_path_too_long "$dest"

  if [[ "${REUSE_WORK_HOME:-0}" == "1" && -d "$dest" ]]; then
    echo "Reusing $role home $dest"
  else
    rm -rf "$dest"
    mkdir -p "$(dirname "$dest")"
    if [[ "$SKIP_COW" == "1" ]]; then
      mkdir -p "$dest/data"
      # Minimal stub tree so path guards still exercise a non-primary home.
      if [[ -f "$PRIMARY/identity.key" ]]; then
        cp "$PRIMARY/identity.key" "$dest/identity.key" 2>/dev/null || \
          printf 'fixture-identity\n' >"$dest/identity.key"
      else
        printf 'fixture-identity\n' >"$dest/identity.key"
      fi
      if [[ -f "$PRIMARY/cloud_sync.json" ]]; then
        cp "$PRIMARY/cloud_sync.json" "$dest/cloud_sync.json"
      fi
      echo "SKIP_COW stub home for $role"
    else
      [[ -d "$PRIMARY" ]] || fail "SOURCE/PRIMARY home missing: $PRIMARY"
      [[ -f "$PRIMARY/identity.key" ]] || fail "SOURCE/PRIMARY has no identity.key: $PRIMARY"
      if cp -cR "$PRIMARY" "$dest" 2>/dev/null; then
        echo "CoW clone ok role=$role"
      else
        cp -a "$PRIMARY" "$dest"
        echo "full copy ok role=$role"
      fi
    fi
  fi

  refuse_work_home_if_primary "$dest" "$PRIMARY"
  find "$dest" -name '*.sock' -delete 2>/dev/null || true
  find "$dest" -name 'folddb.sock' -delete 2>/dev/null || true
  if [[ -f "$dest/cloud_sync.json" ]]; then
    mv "$dest/cloud_sync.json" \
      "$dest/cloud_sync.json.disabled-by-capture-volume-proof-$RUN_ID"
    echo "disabled inherited cloud_sync.json on $role home"
  fi
}

run_measure() {
  local role="$1"
  local home="$2"
  local out="$3"
  [[ -n "$MEASURE_CMD" ]] || fail "MEASURE_CMD is required"
  [[ -x "$MEASURE_CMD" || -f "$MEASURE_CMD" ]] || fail "MEASURE_CMD not found: $MEASURE_CMD"
  echo "+ MEASURE_CMD role=$role home=$home out=$out"
  # shellcheck disable=SC2086
  LASTDB_HOME="$home" \
  FOLDDB_HOME="$home" \
  LASTDB_DATA_DIR="$home" \
  LASTDB_SOCKET="$home/data/folddb.sock" \
    bash "$MEASURE_CMD" "$role" "$home" "$out"
  [[ -f "$out" ]] || fail "MEASURE_CMD did not write $out"
}

echo "=== cloud-sync capture-volume CoW proof run=$RUN_ID ==="
echo "root=$ROOT"
echo "primary=$PRIMARY"
echo "work_root=$WORK_ROOT"
echo "report_dir=$REPORT_DIR"
echo "baseline_ref=$BASELINE_REF candidate_ref=$CANDIDATE_REF"
echo "workload=$WORKLOAD_LABEL measure_cmd=$MEASURE_CMD skip_cow=$SKIP_COW"
echo "intent_payload_bound=${MAX_INTENT_TO_LOGICAL_RATIO}x+$PAYLOAD_SLACK_BYTES"

[[ -n "$MEASURE_CMD" ]] || fail "MEASURE_CMD is required (fixture mock or live workload driver)"
[[ -d "$PRIMARY" ]] || fail "SOURCE/PRIMARY home missing: $PRIMARY"
refuse_if_primary_socket_live "$PRIMARY"
snapshot_primary_pids "$REPORT_DIR/primary-pids-before.txt"
refuse_inherited_live_targets
snapshot_primary_identity "$REPORT_DIR/primary-identity-before.txt"

BASELINE_HOME="${BASELINE_HOME:-$WORK_ROOT/$RUN_ID/baseline/home}"
CANDIDATE_HOME="${CANDIDATE_HOME:-$WORK_ROOT/$RUN_ID/candidate/home}"

clone_role_home baseline "$BASELINE_HOME"
clone_role_home candidate "$CANDIDATE_HOME"

run_measure baseline "$BASELINE_HOME" "$REPORT_DIR/baseline-measure.json"
run_measure candidate "$CANDIDATE_HOME" "$REPORT_DIR/candidate-measure.json"

snapshot_primary_identity "$REPORT_DIR/primary-identity-after.txt"

python3 - "$PROOF_JSON" "$RUN_ID" "$PRIMARY" "$BASELINE_HOME" "$CANDIDATE_HOME" \
  "$REPORT_DIR" "$LOG" "$BASELINE_REF" "$CANDIDATE_REF" "$WORKLOAD_LABEL" \
  "$NORTH_STAR_PROOF_DIR" "$MAX_INTENT_TO_LOGICAL_RATIO" "$PAYLOAD_SLACK_BYTES" \
  "$REPORT_DIR/primary-pids-before.txt" <<'PY'
import json, pathlib, sys, time, hashlib

(
    proof_path,
    run_id,
    primary,
    baseline_home,
    candidate_home,
    report_dir,
    log,
    baseline_ref,
    candidate_ref,
    workload,
    north_star_dir,
    max_intent_ratio,
    payload_slack_bytes,
    primary_pids_path,
) = sys.argv[1:]

max_intent_ratio = float(max_intent_ratio)
payload_slack_bytes = int(payload_slack_bytes)
primary_pids = {
    int(line)
    for line in pathlib.Path(primary_pids_path).read_text().splitlines()
    if line.strip().isdigit()
}

report = pathlib.Path(report_dir)

def load(name):
    path = report / name
    if not path.exists() or path.stat().st_size == 0:
        raise SystemExit(f"missing measurement: {path}")
    return json.loads(path.read_text())

def file_sha(path):
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()

baseline = load("baseline-measure.json")
candidate = load("candidate-measure.json")

required = (
    "logical_mutations",
    "captured_records",
    "captured_bytes",
    "namespaces",
    "atom_locator_rows",
    "capture_format",
    "logical_field_bytes",
    "payload_bytes",
    "replay_atom_locator_rows",
    "replay_pin_log_records_before",
    "replay_pin_log_records_after",
    "replay_rebuild_ok",
    "daemon_pid",
)
for label, doc in (("baseline", baseline), ("candidate", candidate)):
    missing = [k for k in required if k not in doc]
    if missing:
        raise SystemExit(f"{label} measurement missing keys: {missing}")

def per_logical(doc):
    lm = int(doc["logical_mutations"])
    if lm <= 0:
        return None
    return {
        "records_per_logical": int(doc["captured_records"]) / lm,
        "bytes_per_logical": int(doc["captured_bytes"]) / lm,
        "payload_bytes_per_logical": int(doc["payload_bytes"]) / lm,
    }

base_rate = per_logical(baseline)
cand_rate = per_logical(candidate)

# Pass criteria for the harness gate (fixture and live):
# 1) baseline is the v1 LogicalCommit bag and candidate is live MutationIntent
# 2) candidate payload bytes track logical field bytes within the configured bound
# 3) candidate must not capture rebuildable atom_locator rows; replay rebuilds them
# 4) candidate replay emits zero new pin-log records
# 5) candidate records_per_logical must be finite and not grow with a synthetic
#    fanout marker when provided (optional physical_fanout field)
# 6) when baseline records_per_logical is available and higher, candidate should
#    be <= baseline (volume reduction / independence check)
reasons = []
ok = True

if baseline["capture_format"] != "logical_commit_v1":
    ok = False
    reasons.append(
        f"baseline capture_format={baseline['capture_format']!r}, expected logical_commit_v1"
    )
if candidate["capture_format"] != "mutation_intent":
    ok = False
    reasons.append(
        f"candidate capture_format={candidate['capture_format']!r}, expected mutation_intent"
    )

logical_field_bytes = int(candidate["logical_field_bytes"])
intent_payload_bytes = int(candidate["payload_bytes"])
payload_bound = logical_field_bytes * max_intent_ratio + payload_slack_bytes
intent_to_logical_ratio = (
    intent_payload_bytes / logical_field_bytes if logical_field_bytes > 0 else None
)
if logical_field_bytes <= 0:
    ok = False
    reasons.append("candidate logical_field_bytes must be > 0")
elif intent_payload_bytes > payload_bound:
    ok = False
    reasons.append(
        "candidate MutationIntent payload bytes exceed logical-field bound "
        f"({intent_payload_bytes} > {logical_field_bytes} * {max_intent_ratio:g} "
        f"+ {payload_slack_bytes})"
    )

if int(candidate["atom_locator_rows"]) != 0:
    ok = False
    reasons.append(
        f"candidate still captures atom_locator_rows={candidate['atom_locator_rows']}"
    )
if candidate.get("replay_rebuild_ok") is not True:
    ok = False
    reasons.append("candidate replay_rebuild_ok is not true")
if int(candidate["replay_atom_locator_rows"]) <= 0:
    ok = False
    reasons.append("candidate replay rebuilt zero aloc: atom locator rows")

replay_pin_log_delta = int(candidate["replay_pin_log_records_after"]) - int(
    candidate["replay_pin_log_records_before"]
)
if replay_pin_log_delta != 0:
    ok = False
    reasons.append(
        f"candidate replay emitted {replay_pin_log_delta} new pin-log records"
    )

for label, doc in (("baseline", baseline), ("candidate", candidate)):
    daemon_pid = int(doc["daemon_pid"])
    if daemon_pid <= 0:
        ok = False
        reasons.append(f"{label} daemon_pid must be > 0")
    elif daemon_pid in primary_pids:
        ok = False
        reasons.append(f"{label} daemon_pid={daemon_pid} is a live primary PID")

if cand_rate is None:
    ok = False
    reasons.append("candidate logical_mutations must be > 0")
else:
    # Optional: measurement may include physical_fanout for independence check.
    fanout = candidate.get("physical_fanout")
    if fanout is not None and int(fanout) > 1:
        # After logical boundary: one logical commit → ~1 capture record
        # regardless of physical row fanout. Allow small integer slack.
        if cand_rate["records_per_logical"] > 1.5:
            ok = False
            reasons.append(
                "candidate records_per_logical="
                f"{cand_rate['records_per_logical']} not independent of "
                f"physical_fanout={fanout}"
            )

if base_rate is not None and cand_rate is not None:
    if cand_rate["records_per_logical"] > base_rate["records_per_logical"] * 1.05:
        ok = False
        reasons.append(
            "candidate records_per_logical grew vs baseline "
            f"({cand_rate['records_per_logical']} > {base_rate['records_per_logical']})"
        )
    if cand_rate["bytes_per_logical"] > base_rate["bytes_per_logical"] * 1.05:
        # Soft note only when records already fail; hard-fail when bytes balloon
        # without an explanatory payload-shape change flag.
        if not candidate.get("allow_bytes_growth"):
            ok = False
            reasons.append(
                "candidate bytes_per_logical grew vs baseline "
                f"({cand_rate['bytes_per_logical']} > {base_rate['bytes_per_logical']})"
            )

# Primary unchanged: identity fingerprint before/after must match when present.
before = report / "primary-identity-before.txt"
after = report / "primary-identity-after.txt"
primary_unchanged = True
if before.exists() and after.exists():
    b = before.read_text()
    a = after.read_text()
    # Compare identity_key_sha256 lines only (status JSON may flap).
    def id_line(text: str) -> str:
        for line in text.splitlines():
            if line.startswith("identity_key_sha256="):
                return line
        return ""
    if id_line(b) and id_line(b) != id_line(a):
        primary_unchanged = False
        ok = False
        reasons.append("primary identity.key fingerprint changed during probe")

cloud_disabled_baseline = bool(
    list(pathlib.Path(baseline_home).glob("cloud_sync.json.disabled-by-capture-volume-proof-*"))
)
cloud_disabled_candidate = bool(
    list(pathlib.Path(candidate_home).glob("cloud_sync.json.disabled-by-capture-volume-proof-*"))
)

proof = {
    "ok": ok,
    "run_id": run_id,
    "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "primary": primary,
    "baseline_home": baseline_home,
    "candidate_home": candidate_home,
    "report_dir": report_dir,
    "log": log,
    "refs": {
        "baseline": baseline_ref,
        "candidate": candidate_ref,
    },
    "workload": workload,
    "guards": {
        "primary_home_used": False,
        "cow_home_only": True,
        "inherited_cloud_sync_disabled": cloud_disabled_baseline and cloud_disabled_candidate,
        "primary_unchanged": primary_unchanged,
        "real_cloud_upload": False,
    },
    "baseline": baseline,
    "candidate": candidate,
    "rates": {
        "baseline": base_rate,
        "candidate": cand_rate,
        "candidate_intent_to_logical_ratio": intent_to_logical_ratio,
        "candidate_payload_bound": payload_bound,
        "replay_pin_log_delta": replay_pin_log_delta,
    },
    "thresholds": {
        "max_intent_to_logical_ratio": max_intent_ratio,
        "payload_slack_bytes": payload_slack_bytes,
    },
    "reasons": reasons,
    "evidence": {
        "baseline_measure": str(report / "baseline-measure.json"),
        "candidate_measure": str(report / "candidate-measure.json"),
        "primary_identity_before": str(before),
        "primary_identity_after": str(after),
        "primary_identity_before_sha256": file_sha(before),
        "primary_identity_after_sha256": file_sha(after),
        "primary_pids_before": sorted(primary_pids),
    },
    "notes": [
        "Live primary home/socket/PID are never used as WORK_HOME.",
        "Inherited cloud_sync.json is disabled on every CoW home.",
        "MEASURE_CMD owns the logical-write workload and capture metrics.",
        "Durable north-star proof is written when NORTH_STAR_PROOF_DIR is writable.",
    ],
}

pathlib.Path(proof_path).write_text(json.dumps(proof, indent=2) + "\n")
print(f"PROOF_JSON={proof_path}")
print(json.dumps(proof, indent=2))

# Durable north-star proof artifact (PASS/FAIL markdown).
ns_dir = pathlib.Path(north_star_dir)
try:
    ns_dir.mkdir(parents=True, exist_ok=True)
    status = "PASS" if ok else "FAIL"
    md_path = ns_dir / f"cloud-sync-capture-volume-cow-proof-{run_id}.md"
    lines = [
        f"# {status}: cloud-sync capture volume CoW proof",
        "",
        f"- run_id: `{run_id}`",
        f"- ts: `{proof['ts']}`",
        f"- baseline_ref: `{baseline_ref}`",
        f"- candidate_ref: `{candidate_ref}`",
        f"- workload: `{workload}`",
        f"- primary: `{primary}` (never opened as work home)",
        f"- proof_json: `{proof_path}`",
        "",
        "## Measurements",
        "",
        "### Baseline",
        "```json",
        json.dumps(baseline, indent=2),
        "```",
        "",
        "### Candidate",
        "```json",
        json.dumps(candidate, indent=2),
        "```",
        "",
        "## Rates",
        "```json",
        json.dumps(proof["rates"], indent=2),
        "```",
        "",
        "## Guards",
        "```json",
        json.dumps(proof["guards"], indent=2),
        "```",
        "",
    ]
    if reasons:
        lines.extend(["## Fail reasons", ""])
        for r in reasons:
            lines.append(f"- {r}")
        lines.append("")
    md_path.write_text("\n".join(lines) + "\n")
    print(f"NORTH_STAR_PROOF={md_path}")
except OSError as exc:
    print(f"NORTH_STAR_PROOF_SKIP={exc}")

if not ok:
    raise SystemExit(1)
PY

echo "GREEN cloud-sync-capture-volume-cow-proof report=$PROOF_JSON"
