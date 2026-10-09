#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: lastdbd-memory-guard.sh [--resolve-pid]

Watches the supervised primary lastdbd process and restarts that LaunchAgent
when phys_footprint exceeds LASTDBD_RSS_LIMIT_MB.

The guard reads phys_footprint through proc_pid_rusage, not /api/status.
On trip it posts POST /api/admin/shed and waits up to 60s for the footprint
to fall. If it does not fall, it captures vmmap -summary, sends SIGTERM with
a 60s clean-shutdown window, then SIGKILL.

Environment:
  LASTDBD_RSS_LIMIT_MB             Footprint ceiling in MiB (default 16384)
  LASTDBD_SWAP_WARN_MB             Swap warning threshold in MiB (default 20480)
  LASTDBD_GUARD_COOLDOWN_SEC       Minimum seconds between restarts (default 120)
  LASTDBD_PRIMARY_HOME             Primary home (default ~/.lastdb)
  LASTDBD_PRIMARY_LAUNCHD_LABEL    LaunchAgent label (default com.tomtang.lastdbd-primary-506)
  LASTDBD_GUARD_DRY_RUN            Log restart intent without kill/kickstart
  LASTDBD_GUARD_SHED_WAIT_SEC      Seconds to wait after shed (default 60)
  LASTDBD_GUARD_TERM_WAIT_SEC      SIGTERM window before SIGKILL (default 60)
  LASTDBD_GUARD_RESPAWN_WAIT_SEC   Window to observe the KeepAlive respawn
                                   before a plain kickstart (default 15)
EOF
}

RSS_LIMIT_MB="${LASTDBD_RSS_LIMIT_MB:-16384}"
SWAP_WARN_MB="${LASTDBD_SWAP_WARN_MB:-20480}"
COOLDOWN_SEC="${LASTDBD_GUARD_COOLDOWN_SEC:-120}"
SHED_WAIT_SEC="${LASTDBD_GUARD_SHED_WAIT_SEC:-60}"
TERM_WAIT_SEC="${LASTDBD_GUARD_TERM_WAIT_SEC:-60}"
# launchd KeepAlive throttles a respawn to ~10s after a short-lived exit, so
# the observation window has to outlast that throttle or a healthy respawn
# reads as "launchd did not restart it".
RESPAWN_WAIT_SEC="${LASTDBD_GUARD_RESPAWN_WAIT_SEC:-15}"
PRIMARY_HOME="${LASTDBD_PRIMARY_HOME:-$HOME/.lastdb}"
PRIMARY_LABEL="${LASTDBD_PRIMARY_LAUNCHD_LABEL:-com.tomtang.lastdbd-primary-506}"
LOG_DIR="${LASTDBD_GUARD_LOG_DIR:-${PRIMARY_HOME}/monitoring}"
LOG="${LOG_DIR}/lastdbd-memory-guard.log"
STATE="${LOG_DIR}/lastdbd-memory-guard.state"
LAUNCHCTL_BIN="${LAUNCHCTL_BIN:-launchctl}"
PS_BIN="${PS_BIN:-ps}"
KILL_BIN="${KILL_BIN:-kill}"
SYSCTL_BIN="${SYSCTL_BIN:-/usr/sbin/sysctl}"
CURL_BIN="${CURL_BIN:-curl}"
VMMAP_BIN="${VMMAP_BIN:-/usr/bin/vmmap}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

mkdir -p "$LOG_DIR"

ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
log() { echo "$(ts) $*" | tee -a "$LOG" >/dev/null; echo "$(ts) $*"; }

target_domain() {
  printf 'gui/%s' "$(id -u)"
}

target_service() {
  printf '%s/%s' "$(target_domain)" "$PRIMARY_LABEL"
}

command_for_pid() {
  "$PS_BIN" -p "$1" -o command= 2>/dev/null | sed -n '1p' || true
}

comm_for_pid() {
  "$PS_BIN" -p "$1" -o comm= 2>/dev/null | sed -n '1p' || true
}

is_lastdbd_pid() {
  local pid="$1"
  local comm
  comm="$(comm_for_pid "$pid")"
  [ "${comm##*/}" = "lastdbd" ]
}

launchd_primary_pid() {
  local service out
  service="$(target_service)"
  if ! out="$("$LAUNCHCTL_BIN" print "$service" 2>/dev/null)"; then
    return 2
  fi
  printf '%s\n' "$out" | awk -F'= *' '
    /^[[:space:]]*program = / { program=$2 }
    /^[[:space:]]*pid = [0-9]+/ { pid=$2 }
    END {
      if (program != "") {
        n=split(program, parts, "/")
        if (parts[n] != "lastdbd") exit 65
      }
      if (pid != "") {
        print pid
        exit 0
      }
      exit 1
    }'
}

unique_bare_lastdbd_pid() {
  "$PS_BIN" -Ao pid=,comm=,command= 2>/dev/null | awk '
    {
      pid=$1
      comm=$2
      line=$0
      sub(/^[[:space:]]*[0-9]+[[:space:]]+[^[:space:]]+[[:space:]]*/, "", line)
      n=split(comm, parts, "/")
      base=parts[n]
      if (base == "lastdbd" && line !~ /(^|[[:space:]])--data-dir([[:space:]]|=|$)/ && line !~ /lastdbd-memory-guard/) {
        found[++count]=pid
      }
    }
    END {
      if (count == 1) print found[1]
    }'
}

primary_pid() {
  local pid launchd_status
  set +e
  pid="$(launchd_primary_pid)"
  launchd_status=$?
  set -e
  case "$launchd_status" in
    0)
      echo "$pid"
      return 0
      ;;
    65)
      log "warn launchd_program_not_lastdbd service=$(target_service)" >&2
      return 1
      ;;
    1)
      return 1
      ;;
  esac

  # Launchd is authoritative. This fallback is intentionally conservative for
  # non-launchd test/dev hosts: exact executable name only, and exactly one
  # candidate. It never substring-matches shell command lines that mention
  # "lastdbd".
  pid="$(unique_bare_lastdbd_pid || true)"
  if [ -n "${pid:-}" ]; then
    echo "$pid"
    return 0
  fi
  return 1
}

rss_mb_of() {
  local pid="$1"
  local rss_kb
  rss_kb=$("$PS_BIN" -p "$pid" -o rss= 2>/dev/null | tr -d ' ' || echo 0)
  if [ -z "$rss_kb" ] || [ "$rss_kb" = "0" ]; then
    echo 0
  else
    echo $((rss_kb / 1024))
  fi
}

# phys_footprint via proc_pid_rusage(RUSAGE_INFO_V4). Falls back to RSS when
# the pid is gone or the platform has no footprint accounting.
phys_footprint_mb_of() {
  local pid="$1"
  local bytes
  # Unquoted $(...) — bash 3.2 treats quotes inside a double-quoted
  # "$( <<'PY' )" as closers, so the python must not sit in an extra
  # quoted substitution.
  bytes=$("$PYTHON_BIN" - "$pid" <<'PY' 2>/dev/null || true
import ctypes
import ctypes.util
import struct
import sys

pid = int(sys.argv[1])
libc_name = ctypes.util.find_library("c")
if not libc_name:
    raise SystemExit(1)
libc = ctypes.CDLL(libc_name)
# Darwin rusage_info_v4 is 296 bytes (macOS 26 SDK). A 248-byte ctypes
# Structure overflowed the heap and SIGSEGV'd in PyGC_Collect on finalize.
# An oversized buffer survives later field growth (v5=304, v6=464).
RUSAGE_INFO_V4 = 4
BUF_SIZE = 1024
PHYS_FOOTPRINT_OFFSET = 72
buf = (ctypes.c_uint8 * BUF_SIZE)()
fn = libc.proc_pid_rusage
fn.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
fn.restype = ctypes.c_int
rc = fn(pid, RUSAGE_INFO_V4, buf)
if rc != 0:
    raise SystemExit(1)
footprint = struct.unpack_from('<Q', bytes(buf), PHYS_FOOTPRINT_OFFSET)[0]
if footprint <= 0:
    raise SystemExit(1)
print(int(footprint))
PY
)
  if [ -n "${bytes:-}" ] && [ "$bytes" -gt 0 ] 2>/dev/null; then
    echo $((bytes / 1024 / 1024))
    return 0
  fi
  rss_mb_of "$pid"
}

owner_socket() {
  printf '%s/data/folddb.sock' "$PRIMARY_HOME"
}

post_shed() {
  local sock
  sock="$(owner_socket)"
  if [ ! -S "$sock" ]; then
    log "shed_skip reason=no_socket path=$sock"
    return 1
  fi
  if "$CURL_BIN" --unix-socket "$sock" -sS -m 5 -X POST "http://localhost/api/admin/shed" >/dev/null; then
    log "shed_ok socket=$sock"
    return 0
  fi
  log "shed_failed socket=$sock"
  return 1
}

capture_vmmap() {
  local pid="$1"
  local dest="$LOG_DIR/footprint-guard-$(date -u +%Y%m%dT%H%M%SZ).txt"
  mkdir -p "$LOG_DIR"
  if [ -x "$VMMAP_BIN" ] || command -v "$VMMAP_BIN" >/dev/null 2>&1; then
    "$VMMAP_BIN" -summary "$pid" >"$dest" 2>&1 || true
    log "vmmap_capture path=$dest pid=$pid"
  else
    log "vmmap_skip reason=no_vmmap pid=$pid"
  fi
}

swap_used_mb() {
  "$SYSCTL_BIN" -n vm.swapusage 2>/dev/null \
    | sed -n 's/.*used = \([0-9.]*\)M.*/\1/p' \
    | awk '{printf "%d\n", $1+0}'
}

cooldown_ok() {
  if [ ! -f "$STATE" ]; then
    return 0
  fi
  local last now
  last=$(cat "$STATE" 2>/dev/null || echo 0)
  now=$(date +%s)
  if [ $((now - last)) -ge "$COOLDOWN_SEC" ]; then
    return 0
  fi
  return 1
}

mark_restart() {
  date +%s >"$STATE"
}

write_restart_intent() {
  local cause="$1"
  local previous_pid="$2"
  local path="$PRIMARY_HOME/restart-intent.json"
  local tmp="${path}.tmp.$$"
  mkdir -p "$PRIMARY_HOME"
  (
    umask 077
    printf '{"cause":"%s","previous_pid":%s,"created_at":%s}\n' \
      "$cause" "$previous_pid" "$(date +%s)" >"$tmp"
    mv -f "$tmp" "$path"
  )
  log "restart_intent cause=$cause previous_pid=$previous_pid path=$path"
}

restart_primary() {
  local pid="$1"
  local footprint_mb="$2"
  local service now deadline
  log "RESTART primary lastdbd pid=$pid footprint_mb=$footprint_mb limit_mb=$RSS_LIMIT_MB (shed, then SIGTERM, then observe respawn)"
  mark_restart
  if [ "${LASTDBD_GUARD_DRY_RUN:-0}" = "1" ]; then
    log "dry_run skip shed/kill/kickstart service=$(target_service)"
    return 0
  fi

  write_restart_intent guard-memory "$pid"
  if post_shed; then
    now=$(date +%s)
    deadline=$((now + SHED_WAIT_SEC))
    while [ "$(date +%s)" -lt "$deadline" ]; do
      footprint_mb="$(phys_footprint_mb_of "$pid")"
      if [ "$footprint_mb" -lt "$RSS_LIMIT_MB" ]; then
        log "shed_recovered pid=$pid footprint_mb=$footprint_mb limit_mb=$RSS_LIMIT_MB"
        return 0
      fi
      sleep 1
    done
    log "shed_timeout pid=$pid footprint_mb=$footprint_mb limit_mb=$RSS_LIMIT_MB wait_sec=$SHED_WAIT_SEC"
  fi

  capture_vmmap "$pid"
  "$KILL_BIN" -TERM "$pid" 2>/dev/null || true
  now=$(date +%s)
  deadline=$((now + TERM_WAIT_SEC))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if ! "$KILL_BIN" -0 "$pid" 2>/dev/null; then
      break
    fi
    sleep 1
  done
  if "$KILL_BIN" -0 "$pid" 2>/dev/null; then
    log "SIGKILL pid=$pid (did not exit after SIGTERM window ${TERM_WAIT_SEC}s)"
    "$KILL_BIN" -KILL "$pid" 2>/dev/null || true
  else
    log "SIGTERM_clean pid=$pid"
  fi

  service="$(target_service)"
  # KeepAlive respawns the daemon within about a second of the old pid dying.
  # A `kickstart -k` here used to SIGKILL THAT fresh daemon (its session-ledger
  # line already written, no exit record), so every guard restart burned two
  # boots and the next boot promoted a phantom "unclean exit" crash to Sentry
  # (issue 7601263508: 9 of the 10 retained events were this double kill).
  # Observe the respawn instead; only start (never kill) when launchd did not.
  local new_pid=""
  now=$(date +%s)
  deadline=$((now + RESPAWN_WAIT_SEC))
  while :; do
    new_pid="$(launchd_primary_pid 2>/dev/null || true)"
    if [ -n "$new_pid" ] && [ "$new_pid" != "$pid" ]; then
      break
    fi
    new_pid=""
    if [ "$(date +%s)" -ge "$deadline" ]; then
      break
    fi
    sleep 1
  done
  if [ -n "$new_pid" ]; then
    log "respawn_observed old_pid=$pid new_pid=$new_pid service=$service (KeepAlive restarted it; no kickstart)"
    return 0
  fi
  # Plain kickstart starts a stopped service and leaves a running one alone.
  # Never pass -k: that is the double kill this block exists to prevent.
  "$LAUNCHCTL_BIN" kickstart "$service" 2>/dev/null || true
  log "kickstart issued service=$service (no respawn seen within ${RESPAWN_WAIT_SEC}s); next probe will verify socket"
}

main() {
  local resolve_only=0
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --resolve-pid) resolve_only=1; shift ;;
      -h|--help) usage; exit 0 ;;
      *) echo "unknown argument: $1" >&2; usage >&2; exit 64 ;;
    esac
  done

  local pid
  pid="$(primary_pid || true)"
  if [ -z "${pid:-}" ]; then
    log "ok no_primary_lastdbd reason=unresolved"
    exit 0
  fi
  if [ "$resolve_only" -eq 1 ]; then
    echo "$pid"
    exit 0
  fi

  local rss footprint swap cmd
  rss=$(rss_mb_of "$pid")
  footprint=$(phys_footprint_mb_of "$pid")
  swap=$(swap_used_mb || echo 0)
  cmd=$(command_for_pid "$pid" | head -c 200)

  if [ "$swap" -ge "$SWAP_WARN_MB" ] 2>/dev/null; then
    log "warn swap_used_mb=$swap (>= $SWAP_WARN_MB) primary_pid=$pid footprint_mb=$footprint rss_mb=$rss"
  fi

  if [ "$footprint" -lt "$RSS_LIMIT_MB" ]; then
    log "ok pid=$pid footprint_mb=$footprint rss_mb=$rss limit_mb=$RSS_LIMIT_MB swap_mb=$swap"
    exit 0
  fi

  if ! cooldown_ok; then
    log "over_limit pid=$pid footprint_mb=$footprint rss_mb=$rss but cooldown active skip_restart"
    exit 0
  fi

  log "OVER_LIMIT pid=$pid footprint_mb=$footprint rss_mb=$rss limit_mb=$RSS_LIMIT_MB swap_mb=$swap cmd=$cmd"
  restart_primary "$pid" "$footprint"
}

main "$@"
