#!/usr/bin/env bash

set -uo pipefail

label="ci command"
timeout_duration=""

while [[ "$#" -gt 0 ]]; do
  case "${1:-}" in
    --label)
      label="${2:-ci command}"
      shift 2
      ;;
    --timeout)
      timeout_duration="${2:-}"
      shift 2
      ;;
    --)
      shift
      break
      ;;
    *)
      break
      ;;
  esac
done

if [[ -z "$timeout_duration" || "$#" -eq 0 ]]; then
  echo "usage: $0 --label LABEL --timeout DURATION -- COMMAND [ARGS...]" >&2
  exit 2
fi

started_at_epoch="$(date +%s)"
diagnostics_emitted=0

emit_section() {
  local title="$1"
  shift

  echo "::group::${title}"
  "$@" || true
  echo "::endgroup::"
}

emit_diagnostics() {
  local exit_code="$1"
  local reason="$2"
  local finished_at_epoch elapsed

  if [[ "$diagnostics_emitted" -eq 1 ]]; then
    return
  fi
  diagnostics_emitted=1
  shift 2

  finished_at_epoch="$(date +%s)"
  elapsed=$((finished_at_epoch - started_at_epoch))

  echo "::group::ci command interruption diagnostics (${label})"
  echo "reason=${reason}"
  echo "exit_code=${exit_code}"
  echo "elapsed_seconds=${elapsed}"
  echo "timeout_duration=${timeout_duration}"
  echo "command=$*"
  echo "utc_now=$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  echo "github_run_id=${GITHUB_RUN_ID:-}"
  echo "github_run_attempt=${GITHUB_RUN_ATTEMPT:-}"
  echo "github_job=${GITHUB_JOB:-}"
  echo "github_ref=${GITHUB_REF:-}"
  echo "github_sha=${GITHUB_SHA:-}"
  echo "runner_name=${RUNNER_NAME:-}"
  echo "runner_os=${RUNNER_OS:-}"
  echo "runner_arch=${RUNNER_ARCH:-}"
  echo "runner_temp=${RUNNER_TEMP:-}"
  echo "::endgroup::"

  emit_section "kernel and disk snapshot" bash -c '
    uname -a
    df -h
  '

  emit_section "memory snapshot" bash -c '
    if command -v free >/dev/null 2>&1; then
      free -h
    fi
    if [[ -r /proc/meminfo ]]; then
      cat /proc/meminfo
    fi
  '

  emit_section "pressure stall information" bash -c '
    for path in /proc/pressure/cpu /proc/pressure/io /proc/pressure/memory; do
      if [[ -r "$path" ]]; then
        echo "### $path"
        cat "$path"
      fi
    done
  '

  emit_section "largest processes by RSS" bash -c '
    if ps -eo pid,ppid,stat,pcpu,pmem,rss,etime,comm --sort=-rss >/dev/null 2>&1; then
      ps -eo pid,ppid,stat,pcpu,pmem,rss,etime,comm --sort=-rss | head -n 40
    else
      ps -axo pid,ppid,stat,%cpu,%mem,rss,etime,comm | sort -k6 -nr | head -n 40
    fi
  '

  emit_section "cgroup limits" bash -c '
    for path in \
      /sys/fs/cgroup/memory.max \
      /sys/fs/cgroup/memory.current \
      /sys/fs/cgroup/memory.events \
      /sys/fs/cgroup/cpu.max \
      /sys/fs/cgroup/pids.current \
      /sys/fs/cgroup/pids.max; do
      if [[ -r "$path" ]]; then
        echo "### $path"
        cat "$path"
      else
        echo "$path: unavailable"
      fi
    done
  '

  emit_section "recent kernel log" bash -c '
    if command -v dmesg >/dev/null 2>&1; then
      dmesg -T 2>/dev/null | tail -n 120 || dmesg 2>/dev/null | tail -n 120 || true
    fi
  '
}

on_int() {
  emit_diagnostics 130 "wrapper received SIGINT" "$@"
  exit 130
}

on_term() {
  emit_diagnostics 143 "wrapper received SIGTERM" "$@"
  exit 143
}

trap 'on_int "$@"' INT
trap 'on_term "$@"' TERM

timeout --preserve-status --kill-after=10s "$timeout_duration" "$@"
exit_code="$?"

case "$exit_code" in
  124|125|130|131|137|143)
    emit_diagnostics "$exit_code" "command timed out or exited with signal-like status" "$@"
    ;;
esac

exit "$exit_code"
