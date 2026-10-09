#!/usr/bin/env bash
#
# Run the fold_db Criterion performance guard with explicit setup/build and
# measurement phases.
#
# Defaults:
#   - durable probe-owned CARGO_TARGET_DIR (~/.cache/fold-db-perf-guard/target)
#     so daily runs reuse bench-profile artifacts without thrashing ad-hoc agent
#     worktree caches (see 2026-08-05 setup-budget-exceeded under concurrent cargo)
#   - wait for a free heavy-cargo window (default 900s), then refuse with exit 3
#     (named concurrent-cargo) if still busy; emit FINDING lines with pid age
#   - hold host-local exclusive cargo lock while measuring so agents that wrap
#     cargo via scripts/ci/with-fold-host-cargo-lock.sh queue behind the probe
#   - classify cold/warm target + HEAD before setup so over-budget failures are
#     attributable (cold-compile vs product regression)
#   - setup timeouts report as setup-budget failures instead of empty logs

set -euo pipefail

setup_timeout="${FOLD_DB_PERF_SETUP_TIMEOUT:-1200s}"
bench_timeout="${FOLD_DB_PERF_BENCH_TIMEOUT:-1200s}"
log_dir="${FOLD_DB_PERF_LOG_DIR:-}"
criterion_dir="${FOLD_DB_PERF_CRITERION_DIR:-}"

usage() {
  cat >&2 <<'USAGE'
usage: scripts/ci/run-db-perf-guard.sh [--setup-timeout DURATION] [--bench-timeout DURATION] [--log-dir DIR] [--criterion-dir DIR]

Runs:
  preflight:   concurrent-cargo wait/refuse + host cargo lock + cold/warm classify
  setup:       cargo bench -p fold_db --bench query_path_bench --bench db_operations_bench --no-run
  measurement: FOLD_BENCH_FAST_GUARD=1 FOLD_DISABLE_NATIVE_INDEX=1 cargo bench -p fold_db --bench query_path_bench --bench db_operations_bench
  compare:     scripts/lints/bench-compare.py --criterion-dir <criterion-dir>

Env:
  CARGO_TARGET_DIR / FOLD_DB_PERF_SHARED_TARGET_DIR
      Default target is ~/.cache/fold-db-perf-guard/target (probe-owned warm path).
  FOLD_DB_PERF_USE_GIT_COMMON_TARGET=1
      Legacy: use git common-dir sibling target/ instead of the probe cache.
  FOLD_DB_PERF_ALLOW_CONCURRENT_CARGO=1
      Skip concurrent-cargo refuse (not recommended on busy hosts).
  FOLD_DB_PERF_CONCURRENT_WAIT_SECONDS
      Seconds to wait for a free heavy-cargo window before exit 3 (default 900).
  FOLD_DB_PERF_CONCURRENT_POLL_SECONDS
      Poll interval while waiting (default 15).
  FOLD_HOST_CARGO_LOCK_PATH / FOLD_DB_PERF_HOST_LOCK_ACQUIRE_SECONDS
      Exclusive host cargo lock held for the suite; agents should wrap cargo
      with scripts/ci/with-fold-host-cargo-lock.sh.
  FOLD_DB_PERF_SKIP_HOST_CARGO_LOCK=1
      Skip exclusive lock acquisition (self-tests).
USAGE
}

while [[ "$#" -gt 0 ]]; do
  case "${1:-}" in
    --setup-timeout)
      setup_timeout="${2:-}"
      shift 2
      ;;
    --bench-timeout)
      bench_timeout="${2:-}"
      shift 2
      ;;
    --log-dir)
      log_dir="${2:-}"
      shift 2
      ;;
    --criterion-dir)
      criterion_dir="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage
      exit 2
      ;;
  esac
done

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

# shellcheck source=lib/db-perf-guard-common.sh
source "$repo_root/scripts/ci/lib/db-perf-guard-common.sh"

export CARGO_TARGET_DIR="$(db_perf_resolve_cargo_target_dir)"

if [[ -z "$criterion_dir" ]]; then
  criterion_dir="$CARGO_TARGET_DIR/criterion"
fi

if [[ -z "$log_dir" ]]; then
  log_dir="${TMPDIR:-/tmp}/fold-db-perf-guard"
fi

mkdir -p "$CARGO_TARGET_DIR" "$log_dir"
rm -rf "$criterion_dir"

cargo_bin="${FOLD_DB_PERF_GUARD_CARGO_BIN:-cargo}"
compare_bin="${FOLD_DB_PERF_GUARD_COMPARE_BIN:-$repo_root/scripts/lints/bench-compare.py}"
timeout_bin="${FOLD_DB_PERF_GUARD_TIMEOUT_BIN:-}"
if [[ -z "$timeout_bin" ]]; then
  if command -v timeout >/dev/null 2>&1; then
    timeout_bin="timeout"
  elif command -v gtimeout >/dev/null 2>&1; then
    timeout_bin="gtimeout"
  else
    echo "::error::db-perf-guard timeout-unavailable: install GNU timeout or set FOLD_DB_PERF_GUARD_TIMEOUT_BIN" >&2
    exit 2
  fi
fi

echo "repo_root=$repo_root"
echo "CARGO_TARGET_DIR=$CARGO_TARGET_DIR"
echo "criterion_dir=$criterion_dir"

# Always release the host lock if we acquired it (including early exit paths).
trap 'db_perf_release_host_cargo_lock' EXIT INT TERM

# Preflight: wait for a free window, then claim the host exclusive cargo lock.
# Do not start a multi-minute cold compile while another cargo owns the machine.
db_perf_refuse_if_concurrent_cargo "db-perf-guard"
db_perf_acquire_host_cargo_lock "db-perf-guard"

# Classification for cold-compile vs product-regression attribution.
target_state_line="$(db_perf_classify_target_state "$CARGO_TARGET_DIR" release 'query_path_bench-*')"
echo "$target_state_line"
# Also expose a machine-friendly key for log scrapers.
echo "db_perf_preflight ${target_state_line}"

run_phase() {
  local phase="$1"
  local timeout_duration="$2"
  local log_path="$3"
  shift 3

  local started_at finished_at elapsed rc
  started_at="$(date +%s)"
  printf '\n== db-perf-guard %s ==\n' "$phase"
  printf 'timeout=%s\n' "$timeout_duration"
  printf 'log=%s\n' "$log_path"
  printf 'command='
  printf '%q ' "$@"
  printf '\n'

  set +e
  "$timeout_bin" -k 30s "$timeout_duration" "$@" >"$log_path" 2>&1
  rc="$?"
  set -e

  finished_at="$(date +%s)"
  elapsed=$((finished_at - started_at))
  printf '%s_elapsed_seconds=%s\n' "$phase" "$elapsed"
  cat "$log_path"

  case "$rc" in
    0)
      return 0
      ;;
    124|125|130|131|137|143)
      echo "::error::db-perf-guard ${phase}-budget-exceeded elapsed_seconds=${elapsed} timeout=${timeout_duration} log=${log_path} ${target_state_line}" >&2
      return 2
      ;;
    *)
      echo "::error::db-perf-guard ${phase}-failed exit_code=${rc} elapsed_seconds=${elapsed} log=${log_path} ${target_state_line}" >&2
      return "$rc"
      ;;
  esac
}

run_phase setup "$setup_timeout" "$log_dir/criterion-setup.log" \
  "$cargo_bin" bench -p fold_db --bench query_path_bench --bench db_operations_bench --no-run

# Successful setup means this HEAD is warm for the next fire.
db_perf_stamp_warm_head "$CARGO_TARGET_DIR"

run_phase criterion "$bench_timeout" "$log_dir/criterion-measurement.log" \
  env FOLD_BENCH_FAST_GUARD=1 FOLD_DISABLE_NATIVE_INDEX=1 \
  "$cargo_bin" bench -p fold_db --bench query_path_bench --bench db_operations_bench

"$compare_bin" --criterion-dir "$criterion_dir"
