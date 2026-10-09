#!/usr/bin/env bash
#
# Serialize heavy fold cargo behind a host-local exclusive lock so daily
# db-perf-guard can claim a free window instead of competing with N pickup
# workers.
#
# Usage:
#   scripts/ci/with-fold-host-cargo-lock.sh [--timeout SECS] -- cargo test -p fold_db
#   scripts/ci/with-fold-host-cargo-lock.sh --timeout 600 -- cargo check -p fold_db --tests
#
# Env:
#   FOLD_HOST_CARGO_LOCK_PATH   override lock file (default:
#                               ~/.cache/fold-db-perf-guard/host-cargo.lock)
#   FOLD_HOST_CARGO_LOCK_TIMEOUT  default acquire timeout seconds (0 = forever)
#
# Exit codes:
#   4  lock acquire timeout
#   2  usage error
#   otherwise the wrapped command's exit code

set -euo pipefail

timeout_secs="${FOLD_HOST_CARGO_LOCK_TIMEOUT:-0}"
lock_path="${FOLD_HOST_CARGO_LOCK_PATH:-${HOME}/.cache/fold-db-perf-guard/host-cargo.lock}"

usage() {
  cat >&2 <<'USAGE'
usage: scripts/ci/with-fold-host-cargo-lock.sh [--timeout SECS] [--lock PATH] -- COMMAND [ARGS...]

Acquires the host fold-cargo exclusive lock and holds it until COMMAND exits.
Agents and scheduled routines should wrap heavy fold cargo:
  build | test | bench | check | clippy | nextest
so db-perf-guard can run measurements without permanent concurrent-cargo starve.
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
