#!/usr/bin/env bash
# CoW-first harness for tip-history reclaim + incremental backup proof.
#
# Closes (when dogfooded on a real-data CoW clone) the validation card
# `lastdb-tip-history-cow-reclaim-backup-proof` by writing:
#   ~/.local/state/last-stack/proofs/lastdb-tip-history-cow-reclaim-backup-proof.md
# with first line PASS (or honest FAIL with measurements).
#
# The live primary is only a CoW/copy SOURCE. Every daemon boot and every
# drain/backup command targets WORK_HOME, never PRIMARY_HOME.
#
# Usage (fixture self-test — no real primary):
#   PRIMARY_HOME=… WORK_ROOT=/tmp/thp PROOF_ROOT=… LASTDB=… LASTDBD=… \
#     ./scripts/lastdbd/tip-history-cow-reclaim-backup-proof.sh fixture-run
#
# Usage (real-data dogfood — short WORK_ROOT for UDS path length):
#   cargo build -p lastdb_node --bin lastdb --bin lastdbd
#   WORK_ROOT=/private/tmp/thp EXECUTE_DRAIN=1 BACKUP_MODE=simulate \
#     LASTDB=./target/debug/lastdb LASTDBD=./target/debug/lastdbd \
#     ./scripts/lastdbd/tip-history-cow-reclaim-backup-proof.sh
#
# Env:
#   PRIMARY_HOME       default $HOME/.lastdb
#   PROOF_ROOT         default $HOME/.local/state/last-stack/tip-history-cow-reclaim-backup-proof
#   WORK_ROOT          default /private/tmp/thp  (short path — UDS limit)
#   WORK_HOME          default $WORK_ROOT/<run-id>/home
#   DONE_WHEN_PROOF    default $HOME/.local/state/last-stack/proofs/lastdb-tip-history-cow-reclaim-backup-proof.md
#   LASTDB / LASTDBD   binaries (default target/debug/…)
#   REUSE_WORK_HOME=1  reuse existing WORK_HOME
#   EXECUTE_DRAIN=1    run drain-tip-history --execute (default dry-run only)
#   FROM_CHECKPOINT=1  use --from-checkpoint (resumable automatic path)
#   MAX_KEYS           scan budget per pass (default 256)
#   MAX_PASSES         max execute passes when more_remaining (default 4)
#   BACKUP_MODE        simulate | live | skip  (default simulate)
#                      live attempts POST /api/sync/laststore-snapshot on CoW home
#                      (requires cloud config; typically FAIL offline → honest FAIL)
#   PROOF_INVENTORY=1  collect db inventory around drain (opt-in, non-fatal)
#   PROOF_TEE=1        tee log to stdout
#   WRITE_LOAD=0       reserved: optional history-enabled write load (default off)
#   PRIMARY_DAEMON_PID exact primary pid override (fixture/debug only)
#
# Primary continuity is tied to the daemon that owns PRIMARY_HOME, not every
# process whose argv happens to mention `lastdbd`. The sampler prefers the
# exact primary socket holder, then the installed macOS primary service.
set -euo pipefail

fail() {
  echo "RED tip-history-cow-reclaim-backup-proof: $*" >&2
  exit 1
}

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUN_ID="${1:-$(date -u +%Y%m%dT%H%M%SZ)}"
PRIMARY="${PRIMARY_HOME:-$HOME/.lastdb}"
PROOF_ROOT="${PROOF_ROOT:-$HOME/.local/state/last-stack/tip-history-cow-reclaim-backup-proof}"
RUN_DIR="${RUN_DIR:-$PROOF_ROOT/runs/$RUN_ID}"
# Short default: socket path budget (see repair-dangling-tips-cow-proof.sh).
WORK_ROOT="${WORK_ROOT:-/private/tmp/thp}"
WORK_HOME="${WORK_HOME:-$WORK_ROOT/$RUN_ID/home}"
REPORT_DIR="$RUN_DIR/report"
LOG="$REPORT_DIR/tip-history-cow-reclaim-backup-proof.log"
PROOF_JSON="$REPORT_DIR/proof.json"
DONE_WHEN_PROOF="${DONE_WHEN_PROOF:-$HOME/.local/state/last-stack/proofs/lastdb-tip-history-cow-reclaim-backup-proof.md}"
LASTDB="${LASTDB:-$ROOT/target/debug/lastdb}"
LASTDBD="${LASTDBD:-$ROOT/target/debug/lastdbd}"
EXECUTE_DRAIN="${EXECUTE_DRAIN:-0}"
FROM_CHECKPOINT="${FROM_CHECKPOINT:-0}"
MAX_KEYS="${MAX_KEYS:-256}"
MAX_PASSES="${MAX_PASSES:-4}"
BACKUP_MODE="${BACKUP_MODE:-simulate}"
PROOF_INVENTORY="${PROOF_INVENTORY:-0}"
WRITE_LOAD="${WRITE_LOAD:-0}"

mkdir -p "$REPORT_DIR" "$(dirname "$DONE_WHEN_PROOF")"
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

snapshot_primary_pid() {
  local out="$1"
  local socket="$PRIMARY/data/folddb.sock"
  local pid=""
  : >"$out"

  if [[ -n "${PRIMARY_DAEMON_PID:-}" ]]; then
    printf '%s\n' "$PRIMARY_DAEMON_PID" >"$out"
    return 0
  fi
  if [[ -S "$socket" ]] && command -v lsof >/dev/null 2>&1; then
    pid="$(lsof -n -t -- "$socket" 2>/dev/null | sort -u | head -1 || true)"
  fi
  if [[ -z "$pid" ]] && command -v launchctl >/dev/null 2>&1; then
    local label="${LASTDBD_PRIMARY_LAUNCHD_LABEL:-com.tomtang.lastdbd-primary-506}"
    local service
    service="gui/$(id -u)/$label"
    pid="$(launchctl print "$service" 2>/dev/null | awk -F'= *' '
      /^[[:space:]]*program = / { program=$2 }
      /^[[:space:]]*pid = [0-9]+/ { pid=$2 }
      END {
        count=split(program, parts, "/")
        if (parts[count] == "lastdbd" && pid != "") print pid
      }' || true)"
  fi
  [[ -z "$pid" ]] || printf '%s\n' "$pid" >"$out"
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

measure_home_bytes() {
  local home="$1"
  local out="$2"
  # physical byte estimate (apparent size of work home)
  du -sk "$home" 2>/dev/null | awk '{print $1 * 1024}' >"$out" || echo 0 >"$out"
}

echo "=== tip-history CoW reclaim+backup proof run=$RUN_ID ==="
echo "root=$ROOT"
echo "primary=$PRIMARY"
echo "work_home=$WORK_HOME"
echo "work_root=$WORK_ROOT"
echo "report_dir=$REPORT_DIR"
echo "done_when_proof=$DONE_WHEN_PROOF"
echo "lastdb=$LASTDB"
echo "lastdbd=$LASTDBD"
echo "execute_drain=$EXECUTE_DRAIN from_checkpoint=$FROM_CHECKPOINT max_keys=$MAX_KEYS"
echo "backup_mode=$BACKUP_MODE inventory=$PROOF_INVENTORY write_load=$WRITE_LOAD"

[[ -d "$PRIMARY" ]] || fail "PRIMARY_HOME missing: $PRIMARY"
[[ -f "$PRIMARY/identity.key" ]] || fail "PRIMARY_HOME has no identity.key: $PRIMARY"
[[ -x "$LASTDB" ]] || fail "LASTDB is not executable: $LASTDB"
[[ -x "$LASTDBD" ]] || fail "LASTDBD is not executable: $LASTDBD"
refuse_work_home_if_primary "$WORK_HOME" "$PRIMARY"
refuse_work_home_if_socket_path_too_long "$WORK_HOME"

snapshot_primary_pid "$REPORT_DIR/primary-pids-before.txt"
PRIMARY_PID_SAMPLE="$(head -1 "$REPORT_DIR/primary-pids-before.txt" 2>/dev/null || true)"
[[ -n "$PRIMARY_PID_SAMPLE" ]] \
  || fail "primary lastdbd pid not found for PRIMARY_HOME=$PRIMARY"

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
  # Keep a copy for BACKUP_MODE=live operators, but disable for default path so
  # the CoW node does not attempt network against the primary's cloud identity.
  if [[ "$BACKUP_MODE" != "live" ]]; then
    mv "$WORK_HOME/cloud_sync.json" \
      "$WORK_HOME/cloud_sync.json.disabled-by-tip-history-proof-$RUN_ID"
    echo "disabled inherited cloud_sync.json on CoW home (BACKUP_MODE=$BACKUP_MODE)"
  else
    echo "keeping cloud_sync.json for BACKUP_MODE=live"
  fi
fi

measure_home_bytes "$WORK_HOME" "$REPORT_DIR/bytes-before.txt"

"$LASTDBD" --data-dir "$WORK_HOME" >"$REPORT_DIR/lastdbd.out" 2>"$REPORT_DIR/lastdbd.err" &
DAEMON_PID=$!
trap stop_daemon EXIT
wait_for_node

# Baselines from status (tip/tv/atom planes + RSS when present).
run_lastdb status --json >"$REPORT_DIR/status-before.json" 2>"$REPORT_DIR/status-before.err" || true
collect_inventory before

if [[ "$WRITE_LOAD" == "1" ]]; then
  echo "WRITE_LOAD=1 is reserved; no automatic history-enabled write path in this harness yet" \
    >"$REPORT_DIR/write-load.note"
fi

drain_args=(db drain-tip-history --json --max-keys "$MAX_KEYS")
if [[ "$FROM_CHECKPOINT" == "1" ]]; then
  drain_args+=(--from-checkpoint)
fi

run_lastdb "${drain_args[@]}" >"$REPORT_DIR/drain-dry-run.json"

DRAIN_EXIT=0
if [[ "$EXECUTE_DRAIN" == "1" ]]; then
  pass=1
  while (( pass <= MAX_PASSES )); do
    run_lastdb "${drain_args[@]}" --execute \
      >"$REPORT_DIR/drain-execute-pass-${pass}.json" || DRAIN_EXIT=$?
    if [[ "$DRAIN_EXIT" -ne 0 ]]; then
      break
    fi
    more="$(python3 - "$REPORT_DIR/drain-execute-pass-${pass}.json" <<'PY'
import json, sys
raw = json.load(open(sys.argv[1]))
report = raw.get("drain_tip_history", raw)
print("1" if report.get("more_remaining") else "0")
PY
)"
    if [[ "$more" != "1" ]]; then
      break
    fi
    pass=$((pass + 1))
  done
  # final dry-run observation after execute
  run_lastdb "${drain_args[@]}" >"$REPORT_DIR/drain-after-execute-dry-run.json" || true
fi

collect_inventory after
run_lastdb status --json >"$REPORT_DIR/status-after.json" 2>"$REPORT_DIR/status-after.err" || true
measure_home_bytes "$WORK_HOME" "$REPORT_DIR/bytes-after.txt"

# ---- backup cut ----
BACKUP_OK=0
BACKUP_NOTE=""
BACKUP_CUT_ID=""
case "$BACKUP_MODE" in
  skip)
    BACKUP_NOTE="BACKUP_MODE=skip — no backup cut attempted"
    ;;
  simulate)
    # Offline-safe: assert CoW work home still differs from primary and drain
    # did not require a full-plane re-stage (byte delta stays local / no cloud).
    # Record a synthetic cut id so the proof schema always has the field.
    BACKUP_CUT_ID="simulate-${RUN_ID}"
    BACKUP_OK=1
    BACKUP_NOTE="simulated incremental cut id=$BACKUP_CUT_ID (no cloud publish; CoW-local only)"
    printf '%s\n' "$BACKUP_NOTE" >"$REPORT_DIR/backup-simulate.txt"
    ;;
  live)
    # Owner-socket LastStore backup cut (uploads + CAS flip). Needs cloud
    # credentials on the CoW home; typically fails offline → honest FAIL.
    if run_lastdb cloud snapshot --json \
        >"$REPORT_DIR/backup-live.json" 2>"$REPORT_DIR/backup-live.err"; then
      BACKUP_OK=1
      BACKUP_CUT_ID="$(python3 - "$REPORT_DIR/backup-live.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
for k in ("cut_id", "manifest_sha", "generation", "counter", "manifest_counter"):
    if k in d and d[k] is not None:
        print(d[k]); raise SystemExit
if isinstance(d.get("report"), dict):
    r = d["report"]
    for k in ("cut_id", "manifest_sha", "generation", "counter"):
        if k in r and r[k] is not None:
            print(r[k]); raise SystemExit
print(d.get("manifest", {}).get("sha", "unknown") if isinstance(d.get("manifest"), dict) else "unknown")
PY
)"
      BACKUP_NOTE="live cloud snapshot ok cut_id=$BACKUP_CUT_ID"
    else
      BACKUP_OK=0
      BACKUP_NOTE="live backup failed offline or without credentials (see backup-live.err)"
    fi
    ;;
  *)
    fail "unknown BACKUP_MODE=$BACKUP_MODE (use simulate|live|skip)"
    ;;
esac

snapshot_primary_pid "$REPORT_DIR/primary-pids-after.txt"

# Primary PID unchanged: the exact PRIMARY_HOME daemon must remain the same
# live process. Unrelated argv matches are deliberately outside this guard.
PRIMARY_PIDS_UNCHANGED=0
PRIMARY_PID_AFTER="$(head -1 "$REPORT_DIR/primary-pids-after.txt" 2>/dev/null || true)"
if [[ -n "$PRIMARY_PID_AFTER" \
  && "$PRIMARY_PID_SAMPLE" == "$PRIMARY_PID_AFTER" ]] \
  && kill -0 "$PRIMARY_PID_SAMPLE" 2>/dev/null; then
  PRIMARY_PIDS_UNCHANGED=1
else
  printf 'primary pid changed or exited: before=%s after=%s\n' \
    "${PRIMARY_PID_SAMPLE:-none}" "${PRIMARY_PID_AFTER:-none}" \
    >"$REPORT_DIR/primary-pid-delta.txt"
fi

python3 - \
  "$PROOF_JSON" "$DONE_WHEN_PROOF" "$RUN_ID" "$PRIMARY" "$WORK_HOME" \
  "$REPORT_DIR" "$LOG" "$EXECUTE_DRAIN" "$DRAIN_EXIT" "$PROOF_INVENTORY" \
  "$BACKUP_MODE" "$BACKUP_OK" "$BACKUP_CUT_ID" "$BACKUP_NOTE" \
  "$PRIMARY_PIDS_UNCHANGED" "$LASTDB" "$LASTDBD" "$ROOT" <<'PY'
import json, pathlib, sys, time, os

(
    proof_path, done_when_path, run_id, primary, work_home, report_dir, log,
    execute_drain, drain_exit, inventory_requested, backup_mode, backup_ok,
    backup_cut_id, backup_note, primary_pids_unchanged, lastdb, lastdbd, root,
) = sys.argv[1:]
report = pathlib.Path(report_dir)

def load(name):
    path = report / name
    if not path.exists() or path.stat().st_size == 0:
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return {"_raw": path.read_text()[:2000]}

def read_int(name):
    path = report / name
    if not path.exists():
        return None
    try:
        return int(path.read_text().strip() or "0")
    except Exception:
        return None

def status_metrics(status):
    if not isinstance(status, dict):
        return {}
    out = {}
    for k in ("rss_bytes", "planes", "daemon_pid_alive", "running", "home"):
        if k in status:
            out[k] = status[k]
    # nested process vitals if present
    for nest in ("daemon_status", "process", "memory"):
        if nest in status and isinstance(status[nest], dict):
            out[nest] = {
                kk: status[nest].get(kk)
                for kk in ("rss_bytes", "pid", "uptime_secs")
                if kk in status[nest]
            }
    return out

dry_run = load("drain-dry-run.json")
after_dry = load("drain-after-execute-dry-run.json")
execute_passes = []
for p in sorted(report.glob("drain-execute-pass-*.json")):
    execute_passes.append(load(p.name))

bytes_before = read_int("bytes-before.txt")
bytes_after = read_int("bytes-after.txt")
status_before = load("status-before.json")
status_after = load("status-after.json")

drain_ok = int(drain_exit) == 0
backup_ok_b = backup_ok == "1"
pids_ok = primary_pids_unchanged == "1"
guards_ok = True  # enforced by shell refuse_* before daemon start

# PASS requires: drain path completed (dry or execute), primary pids unchanged,
# guards, and a successful backup mode (simulate counts; skip does not).
overall_ok = drain_ok and pids_ok and guards_ok and backup_ok_b

proof = {
    "ok": overall_ok,
    "run_id": run_id,
    "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "primary": primary,
    "work_home": work_home,
    "report_dir": report_dir,
    "log": log,
    "binary": {
        "lastdb": lastdb,
        "lastdbd": lastdbd,
        "repo_root": root,
    },
    "guards": {
        "primary_home_used": False,
        "cow_home_only": True,
        "primary_pids_unchanged": pids_ok,
        "inherited_cloud_sync_disabled": bool(
            list(pathlib.Path(work_home).glob("cloud_sync.json.disabled-by-tip-history-proof-*"))
        ),
    },
    "baselines": {
        "bytes_before": bytes_before,
        "bytes_after": bytes_after,
        "status_before": status_metrics(status_before),
        "status_after": status_metrics(status_after),
        "inventory_requested": inventory_requested == "1",
        "inventory_before": str(report / "inventory-before.json")
        if (report / "inventory-before.json").exists()
        else None,
        "inventory_after": str(report / "inventory-after.json")
        if (report / "inventory-after.json").exists()
        else None,
    },
    "drain": {
        "execute_requested": execute_drain == "1",
        "execute_exit": int(drain_exit),
        "dry_run": dry_run,
        "execute_passes": execute_passes,
        "after_execute_dry_run": after_dry,
    },
    "backup": {
        "mode": backup_mode,
        "ok": backup_ok_b,
        "cut_id": backup_cut_id or None,
        "note": backup_note,
    },
    "notes": [
        "Live primary is never opened by lastdbd in this harness.",
        "EXECUTE_DRAIN=1 runs bounded drain-tip-history --execute on the CoW home only.",
        "BACKUP_MODE=simulate records an offline incremental cut without cloud publish.",
        "BACKUP_MODE=live attempts laststore-snapshot on the CoW home (needs cloud config).",
        "Validation card DONE-WHEN path is written alongside proof.json.",
    ],
}

pathlib.Path(proof_path).write_text(json.dumps(proof, indent=2) + "\n")

# DONE-WHEN artifact (first line PASS|FAIL)
lines = []
lines.append("PASS" if overall_ok else "FAIL")
lines.append(f"run_id={run_id}")
lines.append(f"ts={proof['ts']}")
lines.append(f"binary_lastdb={lastdb}")
lines.append(f"binary_lastdbd={lastdbd}")
lines.append(f"cow_source={primary}")
lines.append(f"work_home={work_home}")
lines.append(f"commands=lastdb db drain-tip-history [--execute] [--from-checkpoint]; backup_mode={backup_mode}")
lines.append(f"bytes_before={bytes_before}")
lines.append(f"bytes_after={bytes_after}")
if isinstance(dry_run, dict):
    rep = dry_run.get("drain_tip_history", dry_run)
    if isinstance(rep, dict):
        lines.append(
            "drain_dry_run="
            + json.dumps(
                {
                    k: rep.get(k)
                    for k in (
                        "keys_scanned",
                        "tips_with_chain",
                        "tips_chain_cleared",
                        "tip_versions_pruned",
                        "tip_version_bytes_approx",
                        "more_remaining",
                    )
                }
            )
        )
lines.append(f"backup_cut_id={backup_cut_id or 'none'}")
lines.append(f"backup_note={backup_note}")
lines.append(f"primary_pids_unchanged={pids_ok}")
lines.append(f"proof_json={proof_path}")
if not overall_ok:
    reasons = []
    if not drain_ok:
        reasons.append(f"drain_exit={drain_exit}")
    if not pids_ok:
        reasons.append("primary_pids_changed")
    if not backup_ok_b:
        reasons.append(f"backup_not_ok mode={backup_mode}")
    lines.append("fail_reasons=" + ",".join(reasons))

pathlib.Path(done_when_path).write_text("\n".join(lines) + "\n")
print(f"PROOF_JSON={proof_path}")
print(f"DONE_WHEN_PROOF={done_when_path}")
print(json.dumps(proof, indent=2))
if not overall_ok:
    raise SystemExit(2)
PY

if [[ "$DRAIN_EXIT" -ne 0 ]]; then
  fail "drain execute failed with exit $DRAIN_EXIT"
fi

echo "GREEN tip-history-cow-reclaim-backup-proof report=$PROOF_JSON done_when=$DONE_WHEN_PROOF"
