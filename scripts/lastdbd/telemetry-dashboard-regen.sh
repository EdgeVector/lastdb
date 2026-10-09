#!/usr/bin/env bash
set -euo pipefail

last_stack="${LAST_STACK_ROOT:-$HOME/.last-stack}"
if [ -f "$last_stack/bin/last-stack-shell-prelude" ]; then
  # shellcheck disable=SC1091
  . "$last_stack/bin/last-stack-shell-prelude"
fi

lastdb_bin="${LASTDB_BIN:-lastdb}"
command_name="${LASTDB_TELEMETRY_DASHBOARD_COMMAND:-telemetry-dashboard}"

skip() {
  local reason="$1"
  printf 'DASHBOARD_SKIP=%s\n' "$reason"
}

args=()
if [ -n "${LASTDB_HOME:-}" ]; then
  args+=(--data-dir "$LASTDB_HOME")
fi

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/lastdb-telemetry-dashboard.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT
stdout_file="$work_dir/stdout"
stderr_file="$work_dir/stderr"

if ! command -v "$lastdb_bin" >/dev/null 2>&1; then
  skip "lastdb-command-not-found"
  exit 0
fi

set +e
"$lastdb_bin" "${args[@]}" "$command_name" "$@" >"$stdout_file" 2>"$stderr_file"
status=$?
set -e

if [ "$status" -ne 0 ]; then
  if grep -Eiq 'unrecognized subcommand|invalid subcommand|unexpected argument|unknown command|was not recognized|No such command' "$stderr_file" "$stdout_file"; then
    skip "telemetry-dashboard-command-unavailable"
  else
    skip "telemetry-dashboard-unavailable(status=$status)"
  fi
  exit 0
fi

cat "$stdout_file"
if ! grep -Eq '^DASHBOARD_HTML=.' "$stdout_file"; then
  skip "telemetry-dashboard-no-html"
fi
