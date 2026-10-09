#!/usr/bin/env bash
set -euo pipefail

last_stack="${LAST_STACK_ROOT:-$HOME/.last-stack}"
if [ -f "$last_stack/bin/last-stack-shell-prelude" ]; then
  # shellcheck disable=SC1091
  . "$last_stack/bin/last-stack-shell-prelude"
fi

lastdb_bin="${LASTDB_BIN:-lastdb}"
heartbeat_helper="${LASTDB_HEALTH_ALERT_HEARTBEAT_HELPER:-$last_stack/bin/last-stack-fbrain-append-heartbeat}"

args=(alert-check)
if [ -n "${LASTDB_HOME:-}" ]; then
  args=(--data-dir "$LASTDB_HOME" "${args[@]}")
fi
if [ -n "${LASTDB_HEALTH_ALERT_STATE_FILE:-}" ]; then
  args+=("--state-file" "$LASTDB_HEALTH_ALERT_STATE_FILE")
fi
if [ -n "${LASTDB_HEALTH_ALERT_NOTIFICATION_LOG:-}" ]; then
  args+=("--notification-log" "$LASTDB_HEALTH_ALERT_NOTIFICATION_LOG")
fi
if [ -n "${LASTDB_HEALTH_ALERT_FAILURES_BEFORE_ALERT:-}" ]; then
  args+=("--failures-before-alert" "$LASTDB_HEALTH_ALERT_FAILURES_BEFORE_ALERT")
fi
if [ -n "${LASTDB_HEALTH_ALERT_COOLDOWN_SECS:-}" ]; then
  args+=("--cooldown-secs" "$LASTDB_HEALTH_ALERT_COOLDOWN_SECS")
fi
if [ -n "${LASTDB_HEALTH_ALERT_ACK_FILE:-}" ]; then
  args+=("--acknowledged-incident-file" "$LASTDB_HEALTH_ALERT_ACK_FILE")
fi
if [ -x "$heartbeat_helper" ]; then
  args+=("--heartbeat-command" "$heartbeat_helper")
fi

exec "$lastdb_bin" "${args[@]}"
