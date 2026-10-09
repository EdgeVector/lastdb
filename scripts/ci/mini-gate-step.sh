#!/usr/bin/env bash
# Record the active Mini-lane step name, run the command, and keep a short
# failure excerpt in the runner state for diagnosis.
#
# usage:
#   scripts/ci/mini-gate-step.sh "cargo fmt --check" -- cargo fmt --all -- --check
set -uo pipefail

# Every gated step opens a store per test under real parallelism, and the
# Docker runner's default fd ulimit is far below what that needs (measured:
# `cargo test -p fold_db --lib` here failing 5 tests on `os error 24, Too
# many open files` while the identical raise already protects the
# `lastdb_node` step and the heavy-clippy/lambda-artifact/cloud-gc-mutations
# workflows). Apply the same floor here once, for every step this script
# gates, instead of repeating it per call site.
ulimit -n 8192 || true

if [[ "$#" -lt 3 || "$2" != "--" ]]; then
  echo "usage: $0 STEP_NAME -- COMMAND [ARGS...]" >&2
  exit 2
fi

step_name="$1"
shift 2

state_dir="${MINI_GATE_STATE_DIR:-${RUNNER_TEMP:-/tmp}/mini-gate}"
mkdir -p "$state_dir"
printf '%s\n' "$step_name" >"$state_dir/last_step"
printf '%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >"$state_dir/last_step_started_at"
if [[ ! -f "$state_dir/job_started_at_epoch" ]]; then
  date +%s >"$state_dir/job_started_at_epoch"
fi

echo "::notice title=Mini gate step::${step_name}"
echo "MINI_GATE_STEP=${step_name}"

log_file="$state_dir/step.log"
: >"$log_file"

# Silent native compiles (aws-lc-sys) have killed this lane at ~80-90s of no
# stdout — duration_mode mystery-90s, job timeout-minutes is 20. Emit a
# heartbeat so the docker runner sees activity. 0 disables (tests).
heartbeat_secs="${MINI_GATE_HEARTBEAT_SECS:-20}"
heartbeat_pid=""
if [[ "$heartbeat_secs" =~ ^[1-9][0-9]*$ ]]; then
  (
    n=0
    while sleep "$heartbeat_secs"; do
      n=$((n + heartbeat_secs))
      echo "MINI_GATE_HEARTBEAT step=${step_name} elapsed_s=${n}"
    done
  ) &
  heartbeat_pid=$!
fi
stop_heartbeat() {
  if [[ -n "$heartbeat_pid" ]]; then
    kill "$heartbeat_pid" 2>/dev/null || true
    wait "$heartbeat_pid" 2>/dev/null || true
    heartbeat_pid=""
  fi
}
trap stop_heartbeat EXIT

set +e
set -o pipefail
"$@" 2>&1 | tee -a "$log_file"
rc=${PIPESTATUS[0]}
set +o pipefail
set -e
stop_heartbeat
trap - EXIT

if [[ "$rc" -ne 0 ]]; then
  tail -n 200 "$log_file" >"$state_dir/fail_excerpt.txt" || true

  # Keep the most actionable identifier in a tiny, status-safe sidecar. Rust
  # unit-test output names a failing test as `test ... FAILED`; serialized
  # integration-test loops announce each test binary with an Actions group.
  failed_test="$({ sed -n 's/^test \(.*\) \.\.\. FAILED$/\1/p' "$log_file" || true; } | head -n 1)"
  failed_group="$({ sed -n 's/^::group:://p' "$log_file" || true; } | tail -n 1)"
  if [[ -n "$failed_test" ]]; then
    printf 'test=%s\n' "$failed_test" >"$state_dir/fail_summary.txt"
  elif [[ -n "$failed_group" ]]; then
    printf 'target=%s\n' "$failed_group" >"$state_dir/fail_summary.txt"
  else
    printf 'step=%s\n' "$step_name" >"$state_dir/fail_summary.txt"
  fi
  {
    echo "step=${step_name}"
    echo "exit_code=${rc}"
    echo "finished_at_utc=$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  } >"$state_dir/fail_meta.txt"
  echo "::error title=Mini gate step failed::${step_name} (exit ${rc})"
fi

exit "$rc"
