#!/usr/bin/env bash
#
# Validate that a CANDIDATE LastDB Mini binary can READ data written by the
# PREVIOUS released Mini binary (upgrade-decrypt gate).
#
# Incident 2026-07-13 (v0.22.6): the release pipeline never booted the
# candidate against EXISTING data, so a build that could not decrypt ≤0.22.5
# stores shipped to brew. This gate closes that hole.
#
# Flow (throwaway home only — never ~/.lastdb):
#   1. Boot PREVIOUS release lastdbd → write a marker schema + row over UDS.
#   2. Stop previous daemon (flush/drain via TERM).
#   3. Boot CANDIDATE lastdbd against the SAME home.
#   4. Query the marker row; fail if missing or if aead/decrypt errors appear
#      in the candidate log.
#
# Usage:
#   scripts/release/validate-mini-upgrade-decrypt.sh \
#     --previous-tag v0.22.8 \
#     --candidate-built target/aarch64-apple-darwin/release
#
#   scripts/release/validate-mini-upgrade-decrypt.sh \
#     --previous-built /path/to/old/release \
#     --candidate-built /path/to/new/release
#
# Options:
#   --previous-tag <vX.Y.Z>     Download previous binaries from the public
#                               EdgeVector/homebrew-lastdb release (default when
#                               neither --previous-tag nor --previous-built set:
#                               v0.22.8 — current brew stable as of 2026-07-14).
#   --previous-built <dir>      Use already-on-disk previous lastdb/lastdbd.
#   --candidate-built <dir>     Candidate binaries (required in CI).
#   --candidate-tarball <file>  Unpack candidate from a Mini tarball instead.
#   --triple <triple>           Target triple for --previous-tag download.
#   --report-dir <path>         Where to write logs/reports.
#   --work-root-base <dir>      Base for throwaway home (default $TMPDIR).
#   --help | -h

set -euo pipefail

# ---------------------------------------------------------------------------
# Pure helpers (sourced by validate-mini-upgrade-decrypt-test.sh).
# ---------------------------------------------------------------------------

detect_triple() {
  case "$(uname -s)-$(uname -m)" in
    Darwin-arm64) echo "aarch64-apple-darwin" ;;
    Darwin-x86_64) echo "x86_64-apple-darwin" ;;
    Linux-x86_64) echo "x86_64-unknown-linux-gnu" ;;
    Linux-aarch64 | Linux-arm64) echo "aarch64-unknown-linux-gnu" ;;
    *) return 1 ;;
  esac
}

normalize_tag() {
  case "$1" in
    v*) printf '%s\n' "$1" ;;
    *) printf 'v%s\n' "$1" ;;
  esac
}

download_url() {
  local tag triple
  tag="$(normalize_tag "$1")"
  triple="$2"
  printf 'https://github.com/EdgeVector/homebrew-lastdb/releases/download/%s/lastdb-%s.tar.gz\n' \
    "$tag" "$triple"
}

assert_work_root_safe() {
  local home_dir sock len
  home_dir="$1"
  case "$home_dir" in
    "$HOME" | "$HOME"/* )
      printf 'refusing throwaway home under $HOME: %s\n' "$home_dir" >&2
      return 1 ;;
  esac
  case "$home_dir" in
    "$HOME/.lastdb"* | "$HOME/.folddb"* )
      printf 'refusing throwaway home under the primary-brain dir: %s\n' "$home_dir" >&2
      return 1 ;;
  esac
  sock="$home_dir/data/folddb.sock"
  len="$(printf '%s' "$sock" | wc -c | tr -d '[:space:]')"
  if [ "$len" -gt 103 ]; then
    printf 'socket path too long for sockaddr_un (%s > 103): %s\n' "$len" "$sock" >&2
    return 1
  fi
}

# log_has_decrypt_failure <file> — true (exit 0) if the log looks like the
# 0.22.6 incident class (aead / wrong encryption key / cannot decrypt store).
log_has_decrypt_failure() {
  local f="$1"
  [ -f "$f" ] || return 1
  # Case-insensitive; keep the match set tight so normal INFO noise does not
  # trip the gate (e.g. "encrypt" in unrelated messages).
  if grep -Eiq \
    'aead(::Error)?|aes-gcm decrypt|wrong encryption key|cannot decrypt existing store|Decryption failed' \
    "$f"
  then
    return 0
  fi
  return 1
}

if [ "${MINI_UPGRADE_DECRYPT_LIB:-}" = "1" ]; then
  return 0 2>/dev/null || exit 0
fi

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

PREVIOUS_TAG=""
PREVIOUS_BUILT=""
CANDIDATE_BUILT=""
CANDIDATE_TARBALL=""
TRIPLE=""
REPORT_DIR=""
WORK_ROOT_BASE="${TMPDIR:-/tmp}"

while [ $# -gt 0 ]; do
  case "$1" in
    --previous-tag) PREVIOUS_TAG="$2"; shift 2 ;;
    --previous-built) PREVIOUS_BUILT="$2"; shift 2 ;;
    --candidate-built) CANDIDATE_BUILT="$2"; shift 2 ;;
    --candidate-tarball) CANDIDATE_TARBALL="$2"; shift 2 ;;
    --triple) TRIPLE="$2"; shift 2 ;;
    --report-dir) REPORT_DIR="$2"; shift 2 ;;
    --work-root-base) WORK_ROOT_BASE="$2"; shift 2 ;;
    --help|-h)
      sed -n '2,40p' "$0"
      exit 0
      ;;
    *) echo "validate-mini-upgrade-decrypt: unknown flag: $1" >&2; exit 2 ;;
  esac
done

if [ -z "$CANDIDATE_BUILT" ] && [ -z "$CANDIDATE_TARBALL" ]; then
  echo "validate-mini-upgrade-decrypt: --candidate-built or --candidate-tarball is required" >&2
  exit 2
fi
if [ -z "$PREVIOUS_BUILT" ] && [ -z "$PREVIOUS_TAG" ]; then
  PREVIOUS_TAG="v0.22.8"
fi
if [ -n "$PREVIOUS_BUILT" ] && [ -n "$PREVIOUS_TAG" ]; then
  echo "validate-mini-upgrade-decrypt: pass only one of --previous-built / --previous-tag" >&2
  exit 2
fi

for bin in curl jq tar mktemp; do
  command -v "$bin" >/dev/null || {
    echo "validate-mini-upgrade-decrypt: missing required tool: $bin" >&2
    exit 2
  }
done

REPORT_DIR="${REPORT_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/lmud-report.XXXXXX")}"
mkdir -p "$REPORT_DIR"
PREV_LOG="$REPORT_DIR/previous-lastdbd.log"
CAND_LOG="$REPORT_DIR/candidate-lastdbd.log"
DAEMON_PID=""

log() { printf '[upgrade-decrypt] %s\n' "$*" | tee -a "$REPORT_DIR/run.log"; }
fail() { log "FAIL: $*"; exit 1; }

cleanup() {
  if [ -n "${DAEMON_PID:-}" ]; then
    kill -TERM "$DAEMON_PID" >/dev/null 2>&1 || true
    wait "$DAEMON_PID" >/dev/null 2>&1 || true
    DAEMON_PID=""
  fi
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Acquire binaries
# ---------------------------------------------------------------------------

PREV_BIN="$REPORT_DIR/previous-bin"
CAND_BIN="$REPORT_DIR/candidate-bin"
mkdir -p "$PREV_BIN" "$CAND_BIN"

if [ -n "$PREVIOUS_BUILT" ]; then
  log "previous binaries from --previous-built $PREVIOUS_BUILT"
  for bin in lastdb lastdbd; do
    [ -x "$PREVIOUS_BUILT/$bin" ] || fail "previous binary missing: $PREVIOUS_BUILT/$bin"
    cp "$PREVIOUS_BUILT/$bin" "$PREV_BIN/$bin"
  done
else
  [ -n "$TRIPLE" ] || TRIPLE="$(detect_triple)" || fail "cannot detect triple; pass --triple"
  PREVIOUS_TAG="$(normalize_tag "$PREVIOUS_TAG")"
  url="$(download_url "$PREVIOUS_TAG" "$TRIPLE")"
  tb="$REPORT_DIR/previous-$TRIPLE.tar.gz"
  log "downloading previous release $PREVIOUS_TAG from $url"
  curl -fsSL -o "$tb" "$url" || fail "download failed: $url"
  tar -xzf "$tb" -C "$PREV_BIN"
  for bin in lastdb lastdbd; do
    [ -x "$PREV_BIN/$bin" ] || fail "extracted previous binary missing: $bin"
  done
fi

if [ -n "$CANDIDATE_BUILT" ]; then
  log "candidate binaries from --candidate-built $CANDIDATE_BUILT"
  for bin in lastdb lastdbd; do
    [ -x "$CANDIDATE_BUILT/$bin" ] || fail "candidate binary missing: $CANDIDATE_BUILT/$bin"
    cp "$CANDIDATE_BUILT/$bin" "$CAND_BIN/$bin"
  done
else
  log "candidate binaries from --candidate-tarball $CANDIDATE_TARBALL"
  tar -xzf "$CANDIDATE_TARBALL" -C "$CAND_BIN"
  for bin in lastdb lastdbd; do
    [ -x "$CAND_BIN/$bin" ] || fail "extracted candidate binary missing: $bin"
  done
fi

PREV_VER="$("$PREV_BIN/lastdbd" --version | awk '{print $NF}')"
CAND_VER="$("$CAND_BIN/lastdbd" --version | awk '{print $NF}')"
log "previous lastdbd --version -> $PREV_VER"
log "candidate lastdbd --version -> $CAND_VER"
[ "$PREV_VER" != "0.1.0" ] || fail "previous reports placeholder 0.1.0"
[ "$CAND_VER" != "0.1.0" ] || fail "candidate reports placeholder 0.1.0"

# ---------------------------------------------------------------------------
# Throwaway home + seed with PREVIOUS
# ---------------------------------------------------------------------------

assert_work_root_safe "$WORK_ROOT_BASE/probe" || fail "work-root base unsafe: $WORK_ROOT_BASE"
WORK_ROOT="$(mktemp -d "${WORK_ROOT_BASE%/}/lmud.XXXXXX")"
HOME_DIR="$WORK_ROOT/h"
mkdir -p "$HOME_DIR"
assert_work_root_safe "$HOME_DIR" || fail "throwaway home unsafe: $HOME_DIR"
SOCKET="$HOME_DIR/data/folddb.sock"
FULL_SOCKET="$HOME_DIR/data/folddb-full.sock"
log "throwaway home: $HOME_DIR"

boot_daemon() {
  local bin="$1" logf="$2"
  # Truncate log so each boot phase has a clean failure scan.
  : >"$logf"
  (
    cd "$WORK_ROOT"
    env \
      HOME="$HOME_DIR" \
      FOLDDB_DISABLE_KEYCHAIN=1 \
      RUST_LOG="${RUST_LOG:-info}" \
      "$bin" --data-dir "$HOME_DIR" </dev/null
  ) >"$logf" 2>&1 &
  DAEMON_PID="$!"
  python3 - "$SOCKET" "$DAEMON_PID" "$logf" <<'PY'
import os, sys, time
socket_path, pid, logf = sys.argv[1], int(sys.argv[2]), sys.argv[3]
deadline = time.monotonic() + 45.0
while time.monotonic() < deadline:
    try:
        os.kill(pid, 0)
    except OSError as exc:
        tail = ""
        try:
            with open(logf, "r", errors="replace") as f:
                tail = f.read()[-2000:]
        except OSError:
            pass
        raise SystemExit(
            f"daemon exited before socket appeared: {exc}\n--- log tail ---\n{tail}"
        )
    if os.path.exists(socket_path):
        raise SystemExit(0)
    time.sleep(0.05)
raise SystemExit(f"timed out waiting for {socket_path}")
PY
}

stop_daemon() {
  local pid="${DAEMON_PID:-}"
  DAEMON_PID=""
  if [ -n "$pid" ]; then
    kill -TERM "$pid" >/dev/null 2>&1 || true
    # Give a clean shutdown a few seconds, then escalate.
    python3 - "$pid" <<'PY' || true
import os, signal, sys, time
pid = int(sys.argv[1])
deadline = time.monotonic() + 8.0
while time.monotonic() < deadline:
    try:
        os.kill(pid, 0)
    except OSError:
        raise SystemExit(0)
    time.sleep(0.05)
try:
    os.kill(pid, signal.SIGKILL)
except OSError:
    pass
PY
    wait "$pid" >/dev/null 2>&1 || true
  fi
  # Wait for BOTH Mini sockets to free; lastdbd binds folddb.sock and
  # folddb-full.sock, and a live full-surface sock makes the next boot refuse.
  # Only unlink under the throwaway home after the process is dead.
  python3 - "$HOME_DIR" <<'PY'
import os, sys, time
home = sys.argv[1]
socks = [
    os.path.join(home, "data", "folddb.sock"),
    os.path.join(home, "data", "folddb-full.sock"),
]
for sock in socks:
    if not sock.startswith(home + os.sep) and sock != home:
        raise SystemExit(f"refusing to touch path outside throwaway home: {sock}")
deadline = time.monotonic() + 10.0
while time.monotonic() < deadline:
    if all(not os.path.exists(s) for s in socks):
        raise SystemExit(0)
    time.sleep(0.05)
for sock in socks:
    try:
        os.unlink(sock)
    except FileNotFoundError:
        pass
leftover = [s for s in socks if os.path.exists(s)]
if leftover:
    raise SystemExit(f"sockets still present after stop+unlink: {leftover}")
PY
}

C() { curl --unix-socket "$SOCKET" -fsS "$@"; }

log "seeding data with PREVIOUS lastdbd ($PREV_VER)"
boot_daemon "$PREV_BIN/lastdbd" "$PREV_LOG"
[ -f "$HOME_DIR/identity.key" ] || fail "previous daemon did not create identity.key"

C "http://lastdb/health" > "$REPORT_DIR/health-prev.json"
jq -e '.status == "ok"' "$REPORT_DIR/health-prev.json" >/dev/null \
  || fail "previous /health not ok"

MARKER_ID="upgrade-decrypt-$(date -u +%Y%m%dT%H%M%SZ)"
cat > "$REPORT_DIR/declare.json" <<'JSON'
{
  "namespace": "validation",
  "schema": {
    "name": "UpgradeDecryptGate",
    "descriptive_name": "Upgrade Decrypt Gate",
    "schema_type": "HashRange",
    "key": { "hash_field": "bucket", "range_field": "id" },
    "fields": ["bucket", "id", "title", "body"]
  }
}
JSON
C -H 'Content-Type: application/json' \
  --data-binary "@$REPORT_DIR/declare.json" \
  "http://lastdb/api/schemas/declare" > "$REPORT_DIR/declare-out.json"
jq -e '.ok == true' "$REPORT_DIR/declare-out.json" >/dev/null \
  || fail "previous schema declare failed (body: $(head -c 400 "$REPORT_DIR/declare-out.json" 2>/dev/null || true))"
# IMPORTANT: this phase talks to the PREVIOUS release binary (e.g. v0.22.8),
# not the candidate. Older Mini reports identity_hash on declare but still
# stores/resolves the schema under the namespaced name (proved live: hash
# mutation 404s; namespace/Name mutation succeeds). Using .canonical /
# .identity_hash here breaks the upgrade-decrypt gate (Release run
# 29885381187). Fresh-install (current candidate only) still pins identity
# hash — see validate-mini-fresh-install.sh.
SCHEMA_REF="$(jq -r '.schema_name // empty' "$REPORT_DIR/declare-out.json")"
[ -n "$SCHEMA_REF" ] && [ "$SCHEMA_REF" != "null" ] \
  || fail "previous schema declare missing schema_name"

cat > "$REPORT_DIR/mutation.json" <<JSON
{
  "type": "mutation",
  "schema": "$SCHEMA_REF",
  "fields_and_values": {
    "bucket": "release",
    "id": "$MARKER_ID",
    "title": "upgrade-decrypt marker",
    "body": "written-by-previous-$PREV_VER"
  },
  "key_value": { "hash": "release", "range": "$MARKER_ID" },
  "mutation_type": "create"
}
JSON
C -H 'Content-Type: application/json' \
  --data-binary "@$REPORT_DIR/mutation.json" \
  "http://lastdb/api/mutation" > "$REPORT_DIR/mutation-out.json"
jq -e '.ok == true and .success == true' "$REPORT_DIR/mutation-out.json" >/dev/null \
  || fail "previous mutation failed (body: $(head -c 400 "$REPORT_DIR/mutation-out.json" 2>/dev/null || true))"

cat > "$REPORT_DIR/query.json" <<JSON
{
  "schema_name": "$SCHEMA_REF",
  "fields": ["bucket", "id", "title", "body"],
  "filter": { "HashRangeKey": { "hash": "release", "range": "$MARKER_ID" } }
}
JSON
C -H 'Content-Type: application/json' \
  --data-binary "@$REPORT_DIR/query.json" \
  "http://lastdb/api/query" > "$REPORT_DIR/query-prev.json"
jq -e --arg id "$MARKER_ID" \
  '.ok == true and (.results | length) == 1 and .results[0].fields.id == $id' \
  "$REPORT_DIR/query-prev.json" >/dev/null \
  || fail "previous could not read back its own marker"

log "stopping previous daemon; rebooting same home with CANDIDATE ($CAND_VER)"
stop_daemon

boot_daemon "$CAND_BIN/lastdbd" "$CAND_LOG"

if log_has_decrypt_failure "$CAND_LOG"; then
  log "candidate log contains decrypt-class failure (incident class):"
  grep -Ein 'aead|aes-gcm decrypt|wrong encryption key|cannot decrypt existing store|Decryption failed' \
    "$CAND_LOG" | head -20 | tee -a "$REPORT_DIR/run.log" || true
  fail "candidate cannot decrypt data written by previous release (see $CAND_LOG)"
fi

C "http://lastdb/health" > "$REPORT_DIR/health-cand.json"
jq -e '.status == "ok"' "$REPORT_DIR/health-cand.json" >/dev/null \
  || fail "candidate /health not ok after upgrade boot"

C -H 'Content-Type: application/json' \
  --data-binary "@$REPORT_DIR/query.json" \
  "http://lastdb/api/query" > "$REPORT_DIR/query-cand.json"
jq -e --arg id "$MARKER_ID" --arg body "written-by-previous-$PREV_VER" \
  '.ok == true
   and (.results | length) == 1
   and .results[0].fields.id == $id
   and .results[0].fields.body == $body' \
  "$REPORT_DIR/query-cand.json" >/dev/null \
  || fail "candidate did not return the marker written by previous (upgrade-decrypt failed)"

# Re-check log after query path (schema load is where 0.22.6 often died).
if log_has_decrypt_failure "$CAND_LOG"; then
  fail "candidate log grew decrypt-class failures after query"
fi

stop_daemon
log "PASS: candidate $CAND_VER reads data written by previous $PREV_VER (marker $MARKER_ID)"
printf 'ok previous=%s candidate=%s marker=%s\n' "$PREV_VER" "$CAND_VER" "$MARKER_ID" \
  > "$REPORT_DIR/result.txt"
