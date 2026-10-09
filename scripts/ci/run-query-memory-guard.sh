#!/usr/bin/env bash
#
# Run the query memory regression guard from either the main checkout or a
# linked agent worktree.
#
# Defaults:
#   - durable probe-owned CARGO_TARGET_DIR (~/.cache/fold-db-perf-guard/target)
#     so the daily probe reuses debug artifacts instead of cold-compiling ~340
#     deps into a disposable per-worktree target (2026-08-05: 1500s timeout at
#     ~209/346 under concurrent cargo).
#   - wait for a free heavy-cargo window, then refuse (exit 3) if still busy.
#   - hold host-local exclusive cargo lock while the memory guard runs.
#   - classify cold/warm + HEAD for over-budget attribution.

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

# shellcheck source=lib/db-perf-guard-common.sh
source "$repo_root/scripts/ci/lib/db-perf-guard-common.sh"

export CARGO_TARGET_DIR="$(db_perf_resolve_cargo_target_dir)"
mkdir -p "$CARGO_TARGET_DIR"

export CARGO_PROFILE_DEV_DEBUG="${CARGO_PROFILE_DEV_DEBUG:-0}"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}"

echo "repo_root=$repo_root"
echo "CARGO_TARGET_DIR=$CARGO_TARGET_DIR"

trap 'db_perf_release_host_cargo_lock' EXIT INT TERM

db_perf_refuse_if_concurrent_cargo "query-memory-guard"
db_perf_acquire_host_cargo_lock "query-memory-guard"

target_state_line="$(db_perf_classify_target_state "$CARGO_TARGET_DIR" debug 'query_memory_guard-*')"
echo "$target_state_line"
echo "db_perf_preflight ${target_state_line}"

cargo_bin="${FOLD_DB_PERF_GUARD_CARGO_BIN:-cargo}"

set +e
"$cargo_bin" test -p fold_db --test query_memory_guard -- --nocapture
rc=$?
set -e

if [[ "$rc" -eq 0 ]]; then
  db_perf_stamp_warm_head "$CARGO_TARGET_DIR"
  exit 0
fi

echo "::error::query-memory-guard failed exit_code=${rc} ${target_state_line}" >&2
exit "$rc"
