#!/usr/bin/env bash
#
# Measure the phys_footprint cost of one plane compaction on a copy-on-write
# clone of a REAL LastDB home.
#
# Why this exists. `vendor/laststore/tests/compact_memory_bound.rs` proves the
# bounded rewrite keeps peak live heap flat as a synthetic plane grows. That is
# a regression bar, not a claim about the primary. The claim the memory guard
# actually cares about is "the 11M-key / 5.9 GiB tips plane rewrites without
# crossing LASTDBD_RSS_LIMIT_MB at the primary's steady baseline", and only a
# real-data measurement can make it. See
# `docs/lastdb-bounded-tips-compaction-cow-proof.md`.
#
# The probe never touches the live primary. It clones the home with APFS
# clonefile (`cp -cR`), strips the cloud-sync config and the inherited sockets,
# and boots the candidate against the clone on its own socket. The external
# `lastdbd-memory-guard` LaunchAgent watches LASTDBD_PRIMARY_HOME only, so the
# probe process is never a guard target and can exceed the limit safely.
#
# Footprint comes from the node's own /api/status, the same source the guard
# reads: `phys_footprint_bytes` and the kernel's lifetime maximum
# `phys_footprint_peak_bytes`. `ps -o rss=` is NOT a substitute — it excludes
# compressed anonymous pages and reads several times under the footprint.
#
# Usage:
#   scripts/run-tips-compaction-memory-probe.sh [--collection tips]
#
# Env:
#   LASTDBD            candidate daemon binary (default: target/release/lastdbd)
#   PROBE_SOURCE_HOME  home to clone           (default: $HOME/.lastdb)
#   PROBE_ROOT         clone parent, outside $HOME (default: /private/tmp/ltcp)
#   PROBE_SETTLE_SECS  idle seconds before the baseline read (default: 90)
#   PROBE_KEEP         1 to keep the clone for follow-up reads
#   PROBE_PRIMARY_CEILING_MB  kill the probe if the LIVE primary's footprint
#                      reaches this (default 15000; 0 disables the watchdog)
#   PROBE_CLONE_ONLY   1 to build the clone, print its path, and stop
#   PROBE_CLONE_PATH   run against a clone a previous --clone-only run made
#   PROBE_SKIP_DIRS    home subtrees the clone omits (logged, never silent)
#   PROBE_CLONE_JOBS   concurrent per-plane clonefile jobs (default 8)
#   PROBE_ENV_OVERRIDE space-separated KEY=VAL applied AFTER the mirrored
#                      LaunchAgent env, so a probe can be made to fit next to a
#                      live primary (logged, never silent)
#   PROBE_READBACK_SCHEMA  schema to read back through the compacted plane
#   PROBE_READBACK_KEY     hash key of a record that schema is known to hold
#   PROBE_READBACK_FILTER  full JSON filter, when the key alone is not enough
#   PROBE_REPORT       path for the JSON report (default: <clone>.report.json)
set -euo pipefail

fail() { echo "RED tips-compaction-probe: $*" >&2; exit 1; }
log()  { printf '%s tips-compaction-probe: %s\n' "$(date -u +%H:%M:%SZ)" "$*" >&2; }

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LASTDBD="${LASTDBD:-$ROOT/target/release/lastdbd}"
COMPACT_TIMEOUT="${PROBE_COMPACT_TIMEOUT_SECS:-5400}"
SOURCE_HOME="${PROBE_SOURCE_HOME:-$HOME/.lastdb}"
# Outside $HOME and short: the daemon binds "$home/data/folddb.sock" and
# sockaddr_un caps at ~103 bytes, so a $TMPDIR-shaped root overflows silently.
PROBE_ROOT="${PROBE_ROOT:-/private/tmp/ltcp}"
SETTLE_SECS="${PROBE_SETTLE_SECS:-90}"
COLLECTION="tips"

while [ $# -gt 0 ]; do
  case "$1" in
    --collection) COLLECTION="${2:?--collection needs a value}"; shift 2 ;;
    *) fail "unknown argument: $1" ;;
  esac
done

[ -x "$LASTDBD" ] || fail "missing candidate daemon: $LASTDBD"
for bin in curl jq; do command -v "$bin" >/dev/null 2>&1 || fail "missing $bin"; done
[ -d "$SOURCE_HOME/data" ] || fail "source home has no data dir: $SOURCE_HOME"

case "$PROBE_ROOT" in
  "$HOME"|"$HOME"/*) fail "PROBE_ROOT must live outside \$HOME (got $PROBE_ROOT)" ;;
esac

# Cloning the store is minutes of pure metadata work, and booting two probe
# nodes at once on one machine is not an option — a second multi-GiB node next
# to the live primary is how you make the primary's own memory guard fire. So
# the clone is separable: PROBE_CLONE_ONLY prepares one while another run is
# busy compacting, PROBE_CLONE_PATH consumes it later.
CLONE="${PROBE_CLONE_PATH:-$PROBE_ROOT/c$$}"
SOCK="$CLONE/data/folddb.sock"
BOOT_LOG="$CLONE.boot.log"
REPORT="${PROBE_REPORT:-$CLONE.report.json}"
sock_len=$(( ${#SOCK} ))
[ "$sock_len" -le 100 ] || fail "clone path too deep for a Unix socket ($SOCK is $sock_len bytes)"

REUSING_CLONE=0
[ -n "${PROBE_CLONE_PATH:-}" ] && REUSING_CLONE=1

# One probe per clone, enforced.
#
# Measured 2026-08-31: two workers ran this script against the same
# PROBE_CLONE_PATH at once. The second one's `rm -rf` of the plane raced the
# first one's `cp -cR` into it and died with "Directory not empty", and for the
# ~90s before that both were preparing to boot a multi-GiB node on a host that
# only has headroom for one. Neither worker could see the other, because a
# clone directory carries no claim.
#
# mkdir is the atomic primitive here: it either creates the lock or it does
# not, with no read-then-write window. The lock records the owner so a stale
# one can be identified, and a lock whose owner is gone is reclaimed rather
# than left to block every later run.
LOCK="$CLONE.lock"
LOCK_HELD=0
acquire_clone_lock() {
  if mkdir "$LOCK" 2>/dev/null; then
    LOCK_HELD=1
    printf '%s %s\n' "$$" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$LOCK/owner"
    return 0
  fi
  local owner_pid
  owner_pid="$(awk '{print $1; exit}' "$LOCK/owner" 2>/dev/null)"
  if [ -n "$owner_pid" ] && kill -0 "$owner_pid" 2>/dev/null; then
    fail "another probe holds $CLONE (pid $owner_pid, since $(awk '{print $2; exit}' "$LOCK/owner" 2>/dev/null)); refusing to boot a second node"
  fi
  log "reclaiming a stale clone lock (owner pid ${owner_pid:-unknown} is gone)"
  rm -rf "$LOCK"
  mkdir "$LOCK" 2>/dev/null || fail "could not take the clone lock $LOCK"
  LOCK_HELD=1
  printf '%s %s\n' "$$" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$LOCK/owner"
}

PRIMARY_CEILING_MB="${PROBE_PRIMARY_CEILING_MB:-15000}"
PRIMARY_SOCK="${PROBE_PRIMARY_SOCK:-$HOME/.lastdb/data/folddb.sock}"
GUARD_PID=""

DAEMON_PID=""
cleanup() {
  [ "$LOCK_HELD" = "1" ] && rm -rf "$LOCK"
  [ -n "$GUARD_PID" ] && kill "$GUARD_PID" 2>/dev/null
  if [ -n "$DAEMON_PID" ]; then
    kill -TERM "$DAEMON_PID" >/dev/null 2>&1 || true
    for _ in $(seq 1 20); do kill -0 "$DAEMON_PID" 2>/dev/null || break; sleep 1; done
    kill -9 "$DAEMON_PID" >/dev/null 2>&1 || true
  fi
  if [ "${PROBE_KEEP:-0}" != "1" ]; then
    rm -rf "$CLONE" "$BOOT_LOG" 2>/dev/null || true
  fi
}
trap cleanup EXIT
acquire_clone_lock

# --- clone -----------------------------------------------------------------
# A flat `cp -cR` of the whole home is not viable here. clonefile is per-file
# metadata work, and the primary home is ~168k files of which ~126k are the
# published `apps/` asset tree that a plane compaction never reads. Measured
# 2026-08-31 on a 95%-full volume: ~900 files/min serial, so the whole home
# took hours while `data/` alone is ~41k files. So: skip the excluded trees,
# and clone the planes concurrently — clonefile is metadata-bound and scales
# with parallelism where a single `cp` does not.
#
# What is skipped is logged, never silent: a probe that quietly dropped a tree
# the node then needed would read as "the clone was complete".
SKIP_DIRS="${PROBE_SKIP_DIRS:-apps lastgit-pack-cas lastgit-pack-manifests crash-reports backup-cut-freeze}"
CLONE_JOBS="${PROBE_CLONE_JOBS:-8}"

if [ "$REUSING_CLONE" = "1" ]; then
  [ -f "$CLONE/identity.key" ] && [ -d "$CLONE/data/data" ] \
    || fail "PROBE_CLONE_PATH is not a usable clone: $CLONE"
  # A clone is single-use. Refreshing only the plane under test is NOT enough
  # to make a used clone measurable again, and the failure is silent rather
  # than loud. Measured 2026-08-31: after one interrupted tips rewrite, a rerun
  # with a freshly re-cloned 5.3 GiB tips plane reported `live_keys: 2` and
  # `bytes_before: 66619`, compacted in 0s, and deleted the plane as dead. Tip
  # liveness is decided by the REST of the store, and the interrupted rewrite
  # had already moved that. The number looked like a spectacular pass.
  #
  # So: a clone that has ever booted a node is refused for measurement. Only
  # PROBE_CLONE_ONLY output that has not been used is eligible.
  [ -e "$CLONE/.probe-consumed" ] && fail \
    "clone $CLONE already ran a probe; tip liveness depends on the whole store, so a used clone cannot be refreshed into a valid rerun — make a fresh one"
  log "reusing unused clone $CLONE; refreshing the $COLLECTION plane from source"
  rm -rf "$CLONE/data/data/$COLLECTION"
  cp -cR "$SOURCE_HOME/data/data/$COLLECTION" "$CLONE/data/data/$COLLECTION" 2>/dev/null || true
  rm -f "$CLONE/cloud_sync.json" "$CLONE/data/"*.sock 2>/dev/null || true
else
mkdir -p "$PROBE_ROOT"
rm -rf "$CLONE"
mkdir -p "$CLONE/data"
log "cloning $SOURCE_HOME -> $CLONE (APFS clonefile, ${CLONE_JOBS}-way; skipping: $SKIP_DIRS)"

# Top-level plain files (identity.key, at_rest_key, install_id, …). One `cp`
# per file: BSD `xargs -J` silently dropped the whole batch here on 2026-08-31,
# and a clone missing identity.key does not boot.
while IFS= read -r f; do
  cp -c "$f" "$CLONE/" 2>/dev/null || true
done < <(find "$SOURCE_HOME" -maxdepth 1 -type f)

# Top-level dirs except the skipped trees and `data`, which is handled below.
for d in "$SOURCE_HOME"/*/; do
  name="$(basename "$d")"
  [ "$name" = "data" ] && continue
  case " $SKIP_DIRS " in *" $name "*) continue ;; esac
  cp -cR "$d" "$CLONE/$name" 2>/dev/null || true
done

# The store: small files at data/, then every plane under data/data in parallel.
while IFS= read -r f; do
  cp -c "$f" "$CLONE/data/" 2>/dev/null || true
done < <(find "$SOURCE_HOME/data" -maxdepth 1 -type f)
for d in "$SOURCE_HOME"/data/*/; do
  name="$(basename "$d")"
  [ "$name" = "data" ] && continue
  cp -cR "$d" "$CLONE/data/$name" 2>/dev/null || true
done
if [ -d "$SOURCE_HOME/data/data" ]; then
  mkdir -p "$CLONE/data/data"
  clone_started="$(date -u +%s)"
  running=0
  for d in "$SOURCE_HOME"/data/data/*/; do
    name="$(basename "$d")"
    cp -cR "$d" "$CLONE/data/data/$name" 2>/dev/null &
    running=$((running + 1))
    if [ "$running" -ge "$CLONE_JOBS" ]; then wait; running=0; fi
  done
  wait
  log "planes cloned in $(( $(date -u +%s) - clone_started ))s"
fi

[ -f "$CLONE/identity.key" ] && [ -d "$CLONE/data/data" ] || fail "clone incomplete: $CLONE"
# Never let a probe join the real account's cloud sync, and never inherit the
# primary's live socket inodes.
rm -f "$CLONE/cloud_sync.json" "$CLONE/data/"*.sock 2>/dev/null || true
fi
log "clone holds $(find "$CLONE" -type f | wc -l | tr -d ' ') files"

if [ "${PROBE_CLONE_ONLY:-0}" = "1" ]; then
  # The caller owns this clone now; do not delete it on the way out.
  PROBE_KEEP=1
  DAEMON_PID=""
  echo "CLONE $CLONE"
  exit 0
fi

plane_bytes="$(/usr/bin/du -sk "$CLONE/data/data/$COLLECTION" 2>/dev/null | awk '{print $1*1024}')"
plane_bytes="${plane_bytes:-0}"
log "plane $COLLECTION allocated on disk before: $plane_bytes bytes"

# --- boot ------------------------------------------------------------------
# Mirror the primary's LASTDB_* tuning from its LaunchAgent, minus every
# home-shaped key: a probe that boots on default config measures a node the
# live primary is not (the 4 GiB hash-group warm budget alone moves it).
PLIST="${PROBE_LAUNCHD_PLIST:-$HOME/Library/LaunchAgents/com.tomtang.lastdbd-primary-506.plist}"
env_pairs=()
if [ -f "$PLIST" ]; then
  while IFS= read -r line; do
    [ -n "$line" ] && env_pairs+=("$line")
  done <<EOF_ENV
$(/usr/libexec/PlistBuddy -c 'Print :EnvironmentVariables' "$PLIST" 2>/dev/null \
  | awk -F' = ' '
      $1 ~ /^ *LASTDB/ {
        key=$1; gsub(/^ +| +$/,"",key)
        if (key == "LASTDB_HOME" || key == "FOLDDB_HOME" || key == "LASTDB_DATA_DIR") next
        val=$2; gsub(/^ +| +$/,"",val)
        if (key != "" && val != "") print key "=" val
      }')
EOF_ENV
fi
# Silence the UNATTENDED tips compaction for the settle window. A clone boots
# with a small footprint, so the headroom gate would happily let the automatic
# probe rewrite the plane before the baseline read — and then the deliberate
# compaction below would measure an already-compacted plane. Zero on the
# overhang knobs disables the unattended trigger; the operator path this probe
# drives is the same rewrite, just on our clock.
# The mirrored env sizes the probe like the primary. On a host that is ALREADY
# holding the primary, two full-size nodes do not fit: measured 2026-08-31, a
# probe mirroring the 4 GiB hash-group warm budget booted next to a 12.6 GiB
# primary, swap went 8.3 -> 14.4 GiB, and the kernel SIGKILLed the probe ~10s
# after boot. Overrides let the caller shrink the probe's *baseline* while the
# rewrite's transient cost -- the number the guard arithmetic needs -- is still
# measured on the real plane. What is overridden is logged, never silent.
override_pairs=()
if [ -n "${PROBE_ENV_OVERRIDE:-}" ]; then
  for kv in $PROBE_ENV_OVERRIDE; do
    case "$kv" in
      LASTDB_HOME=*|FOLDDB_HOME=*|LASTDB_DATA_DIR=*)
        fail "PROBE_ENV_OVERRIDE must not carry a home-shaped key (got $kv)" ;;
      *=*) override_pairs+=("$kv") ;;
      *) fail "PROBE_ENV_OVERRIDE entries must be KEY=VAL (got $kv)" ;;
    esac
  done
fi
log "booting candidate with mirrored env: ${env_pairs[*]:-none}"
[ ${#override_pairs[@]} -gt 0 ] && log "env overrides applied after the mirror: ${override_pairs[*]}"
env -u SENTRY_DSN -u FOLD_SENTRY_DSN -u OBS_SENTRY_DSN \
    -u LASTDB_HOME -u FOLDDB_HOME \
    ${env_pairs[@]+"${env_pairs[@]}"} \
    ${override_pairs[@]+"${override_pairs[@]}"} \
    LASTDB_TIPS_COMPACT_MIN_OVERHANG_BYTES=0 \
    LASTDB_TIPS_COMPACT_MIN_OVERHANG_BPS=0 \
    "$LASTDBD" --data-dir "$CLONE" >"$BOOT_LOG" 2>&1 &
DAEMON_PID=$!

# Protect the live primary from THIS probe.
#
# Measured 2026-08-31: a probe mirroring the primary's tuning reached 13.9 GiB
# next to a primary already holding ~15 GiB on a 36 GiB host. The machine
# swapped, the primary's own footprint climbed to 18734 MiB, and its
# memory guard SIGKILLed and kickstarted it. The probe is supposed to answer a
# question ABOUT the guard, not trip it. So the probe watches the primary and
# dies first. It never signals the primary — only its own child.
start_primary_watchdog() {
  [ "$PRIMARY_CEILING_MB" -gt 0 ] 2>/dev/null || return 0
  [ -S "$PRIMARY_SOCK" ] || { log "no live primary socket; watchdog not armed"; return 0; }
  (
    # The watchdog runs with the script's `set -euo pipefail` INHERITED, so it
    # must never let a routine failure end the subshell. Measured 2026-08-31:
    # the previous body read the primary through
    # `curl … | sed … | head -1` inside a command substitution. When the host
    # is under memory pressure the primary answers /api/status slowly, curl
    # exits 28 (timeout), pipefail promotes 28 to the pipeline, and `set -e`
    # killed the watchdog on its FIRST iteration — silently, right after
    # logging "armed". The guard was therefore inert in exactly the condition
    # it exists for, and a probe ran unwatched beside a 14.8 GiB primary.
    #
    # So: no pipeline under pipefail, every step failure-tolerant, and a
    # timeout is LOGGED rather than fatal. `return` is also wrong here (a
    # subshell is not a function); `break` ends the loop.
    set +e
    set +o pipefail
    misses=0
    while kill -0 "$DAEMON_PID" 2>/dev/null; do
      body="$(curl -s --max-time 10 --unix-socket "$PRIMARY_SOCK" \
                -H 'X-LastDB-Client: tips-compaction-probe' \
                http://localhost/api/status 2>/dev/null)"
      pfp="$(printf '%s' "$body" | sed -n 's/.*"phys_footprint_bytes":\([0-9][0-9]*\).*/\1/p' | head -1)"
      if [ -z "$pfp" ]; then
        # A primary too slow to answer IS the pressure signal. Say so, and
        # treat a sustained blackout as a reason to stand the probe down
        # rather than as a reason to stop watching.
        misses=$((misses + 1))
        [ $((misses % 6)) -eq 1 ] && log "primary status unreadable (miss $misses) — probe still running"
        if [ "$misses" -ge 30 ]; then
          log "PRIMARY UNREADABLE for $misses polls — killing the probe node rather than run unwatched"
          kill -TERM "$DAEMON_PID" 2>/dev/null
          break
        fi
        sleep 2
        continue
      fi
      misses=0
      if [ "$((pfp / 1048576))" -ge "$PRIMARY_CEILING_MB" ]; then
        log "PRIMARY CEILING $((pfp / 1048576))MiB >= ${PRIMARY_CEILING_MB}MiB — killing the probe node"
        kill -TERM "$DAEMON_PID" 2>/dev/null
        break
      fi
      sleep 2
    done
  ) &
  GUARD_PID=$!
  log "primary watchdog armed at ${PRIMARY_CEILING_MB}MiB (pid $GUARD_PID)"
  # "armed" must mean "still running". The old watchdog logged armed and was
  # already dead; a caller could not tell the difference.
  sleep 3
  if kill -0 "$GUARD_PID" 2>/dev/null; then
    log "primary watchdog confirmed live (pid $GUARD_PID)"
  else
    fail "primary watchdog died immediately after arming; refusing to run a probe beside an unwatched primary"
  fi
}

status() {
  curl -s --max-time 30 --unix-socket "$SOCK" \
    -H 'X-LastDB-Client: tips-compaction-probe' \
    http://localhost/api/status 2>/dev/null || true
}

ready=""
for i in $(seq 1 300); do
  kill -0 "$DAEMON_PID" 2>/dev/null || fail "candidate exited during boot: $(tail -3 "$BOOT_LOG" | tr '\n' ' ')"
  if [ -S "$SOCK" ]; then
    ready="$(curl -s --max-time 5 --unix-socket "$SOCK" -H 'Host: localhost' \
      -H 'X-LastDB-Client: tips-compaction-probe' \
      http://x/api/system/auto-identity 2>/dev/null | jq -r '.user_hash // empty' 2>/dev/null || true)"
    [ -n "$ready" ] && break
  fi
  sleep 1
done
[ -n "$ready" ] || fail "candidate identity not ready in 300s"
log "identity ready after ${i}s (pid $DAEMON_PID)"
# From here the store is mutable state, not a pristine copy of the source.
date -u +%Y-%m-%dT%H:%M:%SZ >"$CLONE/.probe-consumed" 2>/dev/null || true
start_primary_watchdog

fp()      { printf '%s' "$1" | jq -r '.status.phys_footprint_bytes // empty'; }
fp_peak() { printf '%s' "$1" | jq -r '.status.phys_footprint_peak_bytes // empty'; }
limit()   { printf '%s' "$1" | jq -r '.status.memory_limit_bytes // empty'; }
build()   { printf '%s' "$1" | jq -r '.status.build.version // empty'; }

# --- baseline --------------------------------------------------------------
# A freshly booted node is still faulting pages in. Idle it so the baseline is
# a settled number, not the tail of boot.
log "settling ${SETTLE_SECS}s before baseline"
sleep "$SETTLE_SECS"
BASE_BODY="$(status)"
[ -n "$BASE_BODY" ] || fail "no /api/status from the probe socket"
BASE_FP="$(fp "$BASE_BODY")"; BASE_PEAK="$(fp_peak "$BASE_BODY")"
GUARD_LIMIT="$(limit "$BASE_BODY")"; BUILD="$(build "$BASE_BODY")"
[ -n "$BASE_FP" ] || fail "status carried no phys_footprint_bytes (not macOS?)"
log "baseline footprint $((BASE_FP/1048576)) MiB (peak $((BASE_PEAK/1048576)) MiB) build=$BUILD"

# --- compact ---------------------------------------------------------------
# Sample while the rewrite runs: the lifetime peak alone cannot show whether
# the spike was one step or a staircase.
SAMPLES="$CLONE.samples"
: >"$SAMPLES"
(
  while :; do
    b="$(status)"
    v="$(fp "$b")"; p="$(fp_peak "$b")"
    [ -n "$v" ] && printf '%s %s %s\n' "$(date -u +%s)" "$v" "${p:-0}" >>"$SAMPLES"
    sleep 2
  done
) &
SAMPLER_PID=$!

COMPACT_OUT="$CLONE.compact.json"
started="$(date -u +%s)"
log "compacting collection=$COLLECTION (operator path, deliberately not headroom-gated)"
# Straight to the owner socket rather than through the `lastdb` CLI: the CLI
# only POSTs this same body, and calling it directly keeps the probe runnable
# from a daemon-only build and lets the caller own the timeout.
set +e
curl -sS --max-time "$COMPACT_TIMEOUT" --unix-socket "$SOCK" \
  -H 'Host: localhost' -H 'Content-Type: application/json' \
  -H 'X-LastDB-Client: tips-compaction-probe' \
  -X POST --data "{\"collection\":\"$COLLECTION\",\"dry_run\":false}" \
  http://localhost/api/db/compact >"$COMPACT_OUT" 2>"$CLONE.compact.err"
COMPACT_RC=$?
set -e
finished="$(date -u +%s)"
kill "$SAMPLER_PID" 2>/dev/null || true
wait "$SAMPLER_PID" 2>/dev/null || true
log "compact exited rc=$COMPACT_RC after $((finished-started))s"

AFTER_BODY="$(status)"
AFTER_FP="$(fp "$AFTER_BODY")"; AFTER_PEAK="$(fp_peak "$AFTER_BODY")"
SAMPLE_MAX="$(awk '{if($2>m)m=$2}END{print m+0}' "$SAMPLES")"
SAMPLE_N="$(wc -l <"$SAMPLES" | tr -d ' ')"
plane_after="$(/usr/bin/du -sk "$CLONE/data/data/$COLLECTION" 2>/dev/null | awk '{print $1*1024}')"
plane_after="${plane_after:-0}"

# --- read-back -------------------------------------------------------------
# A rewrite that loses records is not a pass: read real rows back THROUGH the
# compacted tips plane, not just a status field.
#
# The request shape is not free-form. Measured 2026-08-31 against the primary:
#
#   {"type":"query","schema":S,...}  -> missing_required_key: schema_name
#   {"schema_name":S,"fields":["*"]} -> full_schema_scan_not_allowed
#   {"schema_name":S,"fields":["*"],"filter":{"HashKey":K}} -> no field(s): *
#   {"schema_name":S,"fields":[],"filter":{"HashKey":K}}    -> ok, results:[...]
#
# LastDB is keyed-access only, so the read-back MUST name a key. The previous
# form asked for `schema` and counted a `data` key; the API wants `schema_name`
# and answers with `results`. That combination cannot return a row for any
# input, so the read-back reported RED-or-skipped whatever the rewrite did —
# a verification step that could not pass. `PROBE_READBACK_SCHEMA` and
# `PROBE_READBACK_KEY` name a record the source home is known to hold.
RB_SCHEMA="${PROBE_READBACK_SCHEMA:-}"
RB_KEY="${PROBE_READBACK_KEY:-}"
RB_FILTER="${PROBE_READBACK_FILTER:-}"
READBACK="skipped"
if [ -n "$RB_SCHEMA" ]; then
  if [ -z "$RB_FILTER" ] && [ -n "$RB_KEY" ]; then
    RB_FILTER="$(jq -nc --arg k "$RB_KEY" '{HashKey:$k}')"
  fi
  [ -n "$RB_FILTER" ] || fail "PROBE_READBACK_SCHEMA needs PROBE_READBACK_KEY or PROBE_READBACK_FILTER; LastDB refuses an unfiltered schema query"
  # `fields: []` returns every field. A literal "*" is rejected as a field name.
  rb_body="$(jq -n --arg s "$RB_SCHEMA" --argjson f "$RB_FILTER" \
    '{schema_name:$s, fields:[], filter:$f}')"
  rb_out="$(curl -sS --max-time 120 --unix-socket "$SOCK" \
    -H 'Host: localhost' -H 'Content-Type: application/json' \
    -H 'X-LastDB-Client: tips-compaction-probe' \
    -X POST --data "$rb_body" http://localhost/api/query 2>/dev/null || true)"
  rb_n="$(printf '%s' "$rb_out" | jq -r '(.results // []) | length' 2>/dev/null || echo 0)"
  rb_err="$(printf '%s' "$rb_out" | jq -r '.error // empty' 2>/dev/null || true)"
  if [ "${rb_n:-0}" -gt 0 ]; then
    READBACK="ok rows=$rb_n schema=$RB_SCHEMA"
  elif [ -n "$rb_err" ]; then
    # Distinguish "the query was refused" from "the rewrite lost the row".
    READBACK="RED read-back query rejected for schema=$RB_SCHEMA: $rb_err"
  else
    READBACK="RED no rows for schema=$RB_SCHEMA"
  fi
fi
log "read-back: $READBACK"

jq -n \
  --arg collection "$COLLECTION" \
  --arg build "$BUILD" \
  --arg readback "$READBACK" \
  --argjson base_fp "${BASE_FP:-0}" \
  --argjson base_peak "${BASE_PEAK:-0}" \
  --argjson after_fp "${AFTER_FP:-0}" \
  --argjson after_peak "${AFTER_PEAK:-0}" \
  --argjson sample_max "${SAMPLE_MAX:-0}" \
  --argjson sample_count "${SAMPLE_N:-0}" \
  --argjson guard_limit "${GUARD_LIMIT:-0}" \
  --argjson plane_before "${plane_bytes:-0}" \
  --argjson plane_after "${plane_after:-0}" \
  --argjson compact_rc "${COMPACT_RC:-1}" \
  --argjson duration_s "$((finished-started))" \
  --slurpfile compact "$COMPACT_OUT" \
  '{
     collection: $collection, build: $build, readback: $readback,
     guard_limit_bytes: $guard_limit,
     baseline_footprint_bytes: $base_fp, baseline_peak_bytes: $base_peak,
     after_footprint_bytes: $after_fp, after_peak_bytes: $after_peak,
     sampled_max_footprint_bytes: $sample_max, samples: $sample_count,
     # Kernel lifetime max, so a sampling gap cannot hide the spike. Reads 0
     # when boot already peaked higher than the rewrite ever did.
     peak_growth_bytes: ($after_peak - $base_peak),
     # What the guard arithmetic actually needs: how far above the settled
     # baseline the rewrite pushed the process.
     spike_over_baseline_bytes: (([$sample_max, $after_peak] | max) - $base_fp),
     plane_bytes_before: $plane_before, plane_bytes_after: $plane_after,
     reclaimed_bytes: ($plane_before - $plane_after),
     compact_rc: $compact_rc, compact_seconds: $duration_s,
     compact_report: ($compact[0] // null)
   }' >"$REPORT" 2>/dev/null || {
     # jq --slurpfile chokes on a non-JSON compact stdout; still emit the numbers.
     jq -n --argjson base_peak "${BASE_PEAK:-0}" --argjson after_peak "${AFTER_PEAK:-0}" \
       '{transient_spike_bytes: ($after_peak - $base_peak)}' >"$REPORT"
   }

# Guard the headline against the failure above: a rewrite that "succeeded"
# while seeing almost no live keys measured a broken store, not the plane.
live_keys="$(jq -r '.compact.live_keys // empty' "$COMPACT_OUT" 2>/dev/null)"
if [ -n "$live_keys" ] && [ "$live_keys" -lt 1000 ] 2>/dev/null; then
  log "RED live_keys=$live_keys on a $((plane_bytes / 1048576)) MiB plane — the store this clone booted is not the source store; the measurement is void"
fi

cp "$SAMPLES" "${REPORT%.json}.samples" 2>/dev/null || true
echo "REPORT $REPORT"
cat "$REPORT"
[ "$COMPACT_RC" -eq 0 ] || fail "compact exited $COMPACT_RC: $(tail -3 "$CLONE.compact.err" | tr '\n' ' ')"
