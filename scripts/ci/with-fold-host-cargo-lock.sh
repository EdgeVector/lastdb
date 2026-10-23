#!/usr/bin/env bash
#
# Optional host-local coordination for one bounded performance measurement.
# Ordinary builds, checks, lints, and local app runs do not require this wrapper.
# Build first; do not hold this lock through rebuilds or a measurement suite.
#
# Usage:
#   scripts/ci/with-fold-host-cargo-lock.sh [--timeout SECS] -- COMMAND [ARGS...]
#   COMMAND must have its own finite deadline; --timeout limits acquisition only.
#
# Env:
#   FOLD_HOST_CARGO_LOCK_PATH   override lock file (default:
#                               ~/.cache/fold-db-perf-guard/host-cargo.lock)
#   FOLD_HOST_CARGO_LOCK_TIMEOUT  acquire timeout seconds (default 60; 0 = forever)
#
# Exit codes:
#   4  lock acquire timeout
#   2  usage error
#   otherwise the wrapped command's exit code

set -euo pipefail

timeout_secs="${FOLD_HOST_CARGO_LOCK_TIMEOUT:-60}"
lock_path="${FOLD_HOST_CARGO_LOCK_PATH:-${HOME}/.cache/fold-db-perf-guard/host-cargo.lock}"

usage() {
  cat >&2 <<'USAGE'
usage: scripts/ci/with-fold-host-cargo-lock.sh [--timeout SECS] [--lock PATH] -- COMMAND [ARGS...]

Optional coordination for one bounded performance measurement.
Ordinary builds, checks, lints, and local app runs do not require this wrapper.
Build first. Do not wrap rebuilds or a full measurement suite.

Waits up to 60 seconds by default, then exits 4 if the lock is busy.
--timeout or FOLD_HOST_CARGO_LOCK_TIMEOUT overrides that acquisition wait.
The lock stays held until COMMAND exits; give COMMAND its own finite deadline.
This lock does not guarantee an idle host. See CLAUDE.md for the local policy.
USAGE
}

while [[ "$#" -gt 0 ]]; do
  case "${1:-}" in
    --timeout)
      timeout_secs="${2:-}"
      shift 2
      ;;
    --lock)
      lock_path="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    *)
      # Allow bare command without -- for convenience
      break
      ;;
  esac
done

if [[ "$#" -lt 1 ]]; then
  usage
  exit 2
fi

repo_root=""
if git rev-parse --show-toplevel >/dev/null 2>&1; then
  repo_root="$(git rev-parse --show-toplevel)"
fi

helper=""
if [[ -n "$repo_root" && -f "$repo_root/scripts/ci/lib/host-cargo-lock.py" ]]; then
  helper="$repo_root/scripts/ci/lib/host-cargo-lock.py"
elif [[ -f "$(dirname "$0")/lib/host-cargo-lock.py" ]]; then
  helper="$(cd "$(dirname "$0")" && pwd)/lib/host-cargo-lock.py"
else
  echo "::error::with-fold-host-cargo-lock: host-cargo-lock.py not found" >&2
  exit 2
fi

py=""
if command -v python3 >/dev/null 2>&1; then
  py="python3"
elif command -v python >/dev/null 2>&1; then
  py="python"
else
  echo "::error::with-fold-host-cargo-lock: python3 required for portable fcntl lock" >&2
  exit 2
fi

exec "$py" "$helper" run --path "$lock_path" --timeout "$timeout_secs" -- "$@"
