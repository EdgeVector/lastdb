#!/usr/bin/env bash
# CoW replay of product reads with LASTDB_READS_REQUIRE_PARTITION=1.
#
# The primary home is only the clone source. Every boot and every read targets
# WORK_HOME. Live enablement of the flag is a later safe-upgrade, not this proof.
#
# Usage:
#   LASTDBD=~/.lastdb/current/lastdbd LASTDB=~/.lastdb/current/lastdb \
#     ./scripts/lastdbd/partition-guard-ring-replay-cow-proof.sh [run-id]
#
# Modes (first argument may be a mode instead of a run-id):
#   live          clone PRIMARY_HOME, boot with the flag, replay, score (default)
#   clone-only    clone and strip cloud_sync; do not boot
#   score         SCORE_JSON=<status-recent.json> only
#
# Env:
#   PRIMARY_HOME   default $HOME/.lastdb
#   PROOF_ROOT     default $HOME/.local/state/last-stack/partition-guard-ring-replay
#   WORK_ROOT      default $HOME/.lastdb-proofs/pgr  (short: Unix socket budget)
#   WORK_HOME      default $WORK_ROOT/<run-id>/home
#   LASTDB / LASTDBD
#   KEEP=1         leave WORK_HOME and the node up
#   NODE_WAIT_TRIES  default 300
#   PROOF_TEE=1    also print the log
set -euo pipefail

fail() {
  echo "RED partition-guard-ring-replay: $*" >&2
  exit 1
}

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCORE_PY="$ROOT/scripts/lastdbd/partition-guard-ring-replay-score.py"
MODE="live"
RUN_ID=""
if [[ "${1:-}" == "live" || "${1:-}" == "clone-only" || "${1:-}" == "score" ]]; then
  MODE="$1"
  RUN_ID="${2:-$(date -u +%Y%m%dT%H%M%SZ)}"
else
  RUN_ID="${1:-$(date -u +%Y%m%dT%H%M%SZ)}"
fi

PRIMARY="${PRIMARY_HOME:-$HOME/.lastdb}"
PROOF_ROOT="${PROOF_ROOT:-$HOME/.local/state/last-stack/partition-guard-ring-replay}"
RUN_DIR="${RUN_DIR:-$PROOF_ROOT/runs/$RUN_ID}"
WORK_ROOT="${WORK_ROOT:-$HOME/.lastdb-proofs/pgr}"
WORK_HOME="${WORK_HOME:-$WORK_ROOT/$RUN_ID/home}"
REPORT_DIR="$RUN_DIR/report"
LOG="$REPORT_DIR/partition-guard-ring-replay.log"
PROOF_JSON="$REPORT_DIR/proof.json"
LASTDB="${LASTDB:-${HOME}/.lastdb/current/lastdb}"
LASTDBD="${LASTDBD:-${HOME}/.lastdb/current/lastdbd}"
KEEP="${KEEP:-0}"
DAEMON_PID=""

mkdir -p "$REPORT_DIR"

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

teardown() {
  if [[ "$KEEP" == "1" ]]; then
    return 0
  fi
  if [[ -n "$DAEMON_PID" ]]; then
    kill "$DAEMON_PID" 2>/dev/null || true
    sleep 1
    kill -9 "$DAEMON_PID" 2>/dev/null || true
  fi
}

trap teardown EXIT

if [[ "$MODE" == "score" ]]; then
  [[ -n "${SCORE_JSON:-}" ]] || fail "SCORE_JSON is required for score mode"
  python3 "$SCORE_PY" "$SCORE_JSON" | tee "$PROOF_JSON"
  exit "${PIPESTATUS[0]}"
fi

refuse_work_home_if_primary "$WORK_HOME" "$PRIMARY"
refuse_work_home_if_socket_path_too_long "$WORK_HOME"

if [[ "${PROOF_TEE:-0}" == "1" ]]; then
  exec > >(tee -a "$LOG") 2>&1
else
  exec >>"$LOG" 2>&1
fi

echo "mode=$MODE run_id=$RUN_ID"
echo "primary=$(abs_path "$PRIMARY")"
echo "work_home=$(abs_path "$WORK_HOME")"

if [[ -d "$WORK_HOME" && "${REUSE_WORK_HOME:-0}" != "1" ]]; then
  rm -rf "$WORK_HOME"
fi
mkdir -p "$(dirname "$WORK_HOME")"
if [[ ! -d "$WORK_HOME" ]]; then
  [[ -d "$PRIMARY" ]] || fail "PRIMARY_HOME does not exist: $PRIMARY"
  echo "clone cp -cR $PRIMARY -> $WORK_HOME"
  cp -cR "$PRIMARY" "$WORK_HOME" 2>/dev/null || cp -R "$PRIMARY" "$WORK_HOME"
fi
[[ -d "$WORK_HOME" ]] || fail "clone failed"
[[ ! -L "$WORK_HOME" ]] || fail "copy is a symlink (would alias the live brain)"
WORK_DATA="$(cd "$WORK_HOME/data" 2>/dev/null && pwd -P || true)"
LIVE_DATA="$(cd "$PRIMARY/data" 2>/dev/null && pwd -P || true)"
if [[ -n "$WORK_DATA" && -n "$LIVE_DATA" && "$WORK_DATA" == "$LIVE_DATA" ]]; then
  fail "copy data dir aliases the LIVE primary ($LIVE_DATA)"
fi
rm -f "$WORK_HOME/cloud_sync.json" "$WORK_HOME/cloud_sync.json.side-during-org-check"
if [[ -f "$WORK_HOME/cloud_sync.json" ]]; then
  fail "cloud_sync.json still present on the work home"
fi
printf 'cloned\n' >"$REPORT_DIR/clone-ok"

if [[ "$MODE" == "clone-only" ]]; then
  python3 - "$PROOF_JSON" "$WORK_HOME" "$PRIMARY" <<'PY'
import json, sys
json.dump(
    {
        "ok": True,
        "mode": "clone-only",
        "work_home": sys.argv[2],
        "primary_home": sys.argv[3],
        "guards": {"primary_home_used": False, "cloud_sync_stripped": True},
    },
    open(sys.argv[1], "w"),
    indent=2,
    sort_keys=True,
)
open(sys.argv[1], "a").write("\n")
PY
  echo "GREEN clone-only"
  exit 0
fi

[[ -x "$LASTDBD" ]] || fail "LASTDBD not executable: $LASTDBD"
[[ -x "$LASTDB" ]] || fail "LASTDB not executable: $LASTDB"

# HR-N1: keep the caller's LASTDB_* (encoding, warm budget) and turn the
# partition guard on for this probe only.
export LASTDB_ATOM_KEY_ENCODING="${LASTDB_ATOM_KEY_ENCODING:-partition_prefix}"
export LASTDB_READS_REQUIRE_PARTITION=1
export LASTDB_HOME="$WORK_HOME"
export FOLDDB_HOME="$WORK_HOME"

echo "boot_env LASTDB_ATOM_KEY_ENCODING=$LASTDB_ATOM_KEY_ENCODING LASTDB_READS_REQUIRE_PARTITION=$LASTDB_READS_REQUIRE_PARTITION"
echo "binary LASTDBD=$LASTDBD LASTDB=$LASTDB"

env -u SENTRY_DSN -u FOLD_SENTRY_DSN -u OBS_SENTRY_DSN \
  "$LASTDBD" --data-dir "$WORK_HOME" \
  >"$REPORT_DIR/lastdbd.out" 2>"$REPORT_DIR/lastdbd.err" &
DAEMON_PID=$!
echo "node pid=$DAEMON_PID socket=$WORK_HOME/data/folddb.sock"

run_lastdb() {
  echo "+ $LASTDB --data-dir $WORK_HOME $*" >&2
  "$LASTDB" --data-dir "$WORK_HOME" "$@"
}

wait_for_node() {
  local tries="${NODE_WAIT_TRIES:-300}"
  local socket="$WORK_HOME/data/folddb.sock"
  local i
  for i in $(seq 1 "$tries"); do
    if ! kill -0 "$DAEMON_PID" 2>/dev/null; then
      tail -80 "$REPORT_DIR/lastdbd.err" >&2 || true
      fail "lastdbd exited before socket became ready"
    fi
    if [[ -S "$socket" ]] && run_lastdb status --json >"$REPORT_DIR/status-ready.json" 2>"$REPORT_DIR/status-ready.err"; then
      echo "identity ready after ${i}s"
      return 0
    fi
    sleep 1
  done
  tail -80 "$REPORT_DIR/lastdbd.err" >&2 || true
  fail "node not ready in ${tries}s"
}

wait_for_node

SOCK="$WORK_HOME/data/folddb.sock"
replay_log="$REPORT_DIR/replay.log"
: >"$replay_log"

replay_one() {
  echo "+ $*" >>"$replay_log"
  set +e
  "$@" >>"$replay_log" 2>&1
  local rc=$?
  set -e
  echo "rc=$rc" >>"$replay_log"
  return 0
}

replay_one run_lastdb status --json
replay_one run_lastdb list Card --limit 20 --json
replay_one run_lastdb list BoardCards --key-hash default --limit 20 --json
replay_one curl -sS --unix-socket "$SOCK" --max-time 60 \
  "http://localhost/api/list?schema=Card&limit=20"

# kanban is opt-in. A mis-pointed CLI hits the live primary. The scored
# product reads are the lastdb --data-dir list/status calls above.
if [[ "${KANBAN_ON_COPY:-0}" == "1" ]] && command -v kanban >/dev/null 2>&1; then
  replay_one env LASTDB_HOME="$WORK_HOME" FOLDDB_HOME="$WORK_HOME" \
    kanban ping
  replay_one env LASTDB_HOME="$WORK_HOME" FOLDDB_HOME="$WORK_HOME" \
    kanban list --column todo --json
fi

curl -sS --unix-socket "$SOCK" --max-time 60 \
  "http://localhost/api/status?recent=1" >"$REPORT_DIR/status-recent.json" \
  || fail "failed to read /api/status?recent=1"

set +e
python3 "$SCORE_PY" "$REPORT_DIR/status-recent.json" >"$PROOF_JSON"
score_rc=$?
set -e
cat "$PROOF_JSON"
if [[ "$score_rc" -ne 0 ]]; then
  fail "score bars failed (see $PROOF_JSON)"
fi
echo "GREEN partition-guard-ring-replay"
exit 0
