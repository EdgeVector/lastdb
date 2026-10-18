#!/usr/bin/env bash
# Shared helpers for schema comparison: boot lastdbd on a FRESH EMPTY home and
# load synthetic data. The bars never copy ~/.lastdb (Tom, 2026-10-12: a copy
# of the primary's home costs disk the machine does not have).
#
# Source this file; it defines functions only. The caller owns `set -euo
# pipefail`, `log`, and the EXIT trap (call `syn_cleanup` from it).
#
# Synthetic schema: validation/MiniFreshInstall, HashRange(bucket, id) with
# title and body. The schema service reuses this identity on every run, so a
# bar run adds no new catalog entry after the first.

SYN_SCHEMA_NAMESPACE="validation"
SYN_SCHEMA_NAME="MiniFreshInstall"
SYN_HOME=""
SYN_PID=""
SYN_SOCK=""
SYN_SCHEMA_ID=""
SYN_ENV_PAIRS=()
SYN_NOFILE_LIMIT=""
SYN_OP_TIMEOUT_SECS="${LASTDB_BAR_OP_TIMEOUT_SECS:-30}"
SYN_BOOT_TIMEOUT_SECS="${LASTDB_BAR_BOOT_TIMEOUT_SECS:-120}"
SYN_LAUNCHD_PLIST="${LASTDB_LAUNCHD_PLIST:-$HOME/Library/LaunchAgents/com.tomtang.lastdbd-primary-506.plist}"

# LASTDB_* tuning from the live LaunchAgent, minus the keys the bar owns, so
# the probe runs the config that serves. The home path is
# set by the caller. The status sampler is set to 1 s so /api/status
# counters are current when the bar reads them (the default is 60 s).
syn_live_env_pairs() {
  [ -f "$SYN_LAUNCHD_PLIST" ] || return 0
  /usr/libexec/PlistBuddy -c 'Print :EnvironmentVariables' "$SYN_LAUNCHD_PLIST" 2>/dev/null \
    | awk -F' = ' '
        $1 ~ /^ *LASTDB_/ {
          key=$1; gsub(/^ +| +$/,"",key)
          if (key == "LASTDB_HOME" || key == "FOLDDB_HOME" || key == "LASTDB_DATA_DIR") next
          if (key == "LASTDB_RESIDENT_MODE" || key == "LASTDB_ENGINE" || key == "LASTDB_DISK_PACK" || key == "LASTDB_SELF_METRICS_SAMPLE_SECS") next
          val=$2; gsub(/^ +| +$/,"",val)
          if (key != "" && val != "") print key "=" val
        }'
}

syn_load_env_pairs() {
  SYN_ENV_PAIRS=()
  local line
  while IFS= read -r line; do
    [ -n "$line" ] && SYN_ENV_PAIRS+=("$line")
  done <<EOF_ENV
$(syn_live_env_pairs)
EOF_ENV
  SYN_NOFILE_LIMIT=""
  if [ -f "$SYN_LAUNCHD_PLIST" ]; then
    local n
    n="$(plutil -extract SoftResourceLimits.NumberOfFiles raw "$SYN_LAUNCHD_PLIST" 2>/dev/null || true)"
    case "$n" in ''|*[!0-9]*) ;; *) SYN_NOFILE_LIMIT="$n" ;; esac
  fi
}

syn_curl() {
  curl -sS --max-time "$SYN_OP_TIMEOUT_SECS" --unix-socket "$SYN_SOCK" -H 'Host: localhost' "$@"
}

# syn_boot <bin> [ENV=VAL ...]: fresh home, native node up, identity ready.
# The home path stays short: sockaddr_un caps the socket path at 103 bytes.
syn_boot() {
  local bin="$1"
  shift
  syn_load_env_pairs
  SYN_HOME="$(mktemp -d /private/tmp/lbar.XXXXXX)"
  SYN_SOCK="$SYN_HOME/data/folddb.sock"
  (
    cd "$SYN_HOME"
    [ -z "$SYN_NOFILE_LIMIT" ] || ulimit -n "$SYN_NOFILE_LIMIT"
    exec env -u SENTRY_DSN -u FOLD_SENTRY_DSN \
      -u LASTDB_RESIDENT_MODE -u LASTDB_ENGINE -u LASTDB_DISK_PACK \
      HOME="$SYN_HOME" FOLDDB_DISABLE_KEYCHAIN=1 \
      ${SYN_ENV_PAIRS[@]+"${SYN_ENV_PAIRS[@]}"} \
      LASTDB_SELF_METRICS_SAMPLE_SECS=1 \
      "$@" \
      "$bin" --data-dir "$SYN_HOME"
  ) >"$SYN_HOME.boot.log" 2>&1 </dev/null &
  SYN_PID=$!
  local i=0 ok=""
  while [ "$i" -lt "$SYN_BOOT_TIMEOUT_SECS" ]; do
    i=$((i + 1))
    if ! kill -0 "$SYN_PID" 2>/dev/null; then
      echo "node exited during boot: $(tail -3 "$SYN_HOME.boot.log" 2>/dev/null | tr '\n' ' ')" >&2
      return 1
    fi
    if [ -S "$SYN_SOCK" ]; then
      ok="$(syn_curl http://x/health 2>/dev/null | jq -r '.status // empty' 2>/dev/null || true)"
      [ "$ok" = "ok" ] && break
    fi
    sleep 1
  done
  [ "$ok" = "ok" ] || { echo "node not healthy in ${SYN_BOOT_TIMEOUT_SECS}s" >&2; return 1; }
  log "boot: $bin native home=$SYN_HOME ready after ${i}s"
}

# syn_declare: register the synthetic schema; sets SYN_SCHEMA_ID.
syn_declare() {
  local body resp
  body='{"namespace":"validation","schema":{"name":"MiniFreshInstall","descriptive_name":"Mini Fresh Install","schema_type":"HashRange","key":{"hash_field":"bucket","range_field":"id"},"fields":["bucket","id","title","body"],"field_descriptions":{"bucket":"hash partition for the validation record","id":"range key id","title":"short title","body":"long body text for semantic search"}}}'
  resp="$(SYN_OP_TIMEOUT_SECS=300 syn_curl -H 'Content-Type: application/json' \
    --data "$body" http://lastdb/api/schemas/declare 2>/dev/null || true)"
  SYN_SCHEMA_ID="$(printf '%s' "$resp" | jq -r '.canonical // .identity_hash // empty' 2>/dev/null || true)"
  [ -n "$SYN_SCHEMA_ID" ] || { echo "schema declare failed: $(printf '%s' "$resp" | head -c 400)" >&2; return 1; }
  log "schema declared: $SYN_SCHEMA_NAMESPACE/$SYN_SCHEMA_NAME -> $SYN_SCHEMA_ID"
}

# syn_mutate <create|update|delete> <bucket> <id> [title]: exit 0 when ok.
syn_mutate() {
  local mtype="$1" bucket="$2" id="$3" title="${4:-t}" fields
  if [ "$mtype" = "delete" ]; then
    fields='{}'
  else
    fields="{\"bucket\":\"$bucket\",\"id\":\"$id\",\"title\":\"$title\",\"body\":\"x\"}"
  fi
  SYN_LAST_BODY="$(syn_curl -H 'Content-Type: application/json' \
    --data "{\"type\":\"mutation\",\"schema\":\"$SYN_SCHEMA_ID\",\"fields_and_values\":$fields,\"key_value\":{\"hash\":\"$bucket\",\"range\":\"$id\"},\"mutation_type\":\"$mtype\"}" \
    http://x/api/mutation 2>/dev/null || true)"
  printf '%s' "$SYN_LAST_BODY" | jq -e '.ok == true' >/dev/null 2>&1
}

# syn_query_point <bucket> <id>: raw response body on stdout.
syn_query_point() {
  syn_curl -H 'Content-Type: application/json' \
    --data "{\"schema_name\":\"$SYN_SCHEMA_ID\",\"fields\":[\"title\"],\"filter\":{\"HashRangeKey\":{\"hash\":\"$1\",\"range\":\"$2\"}}}" \
    http://x/api/query 2>/dev/null || true
}

# syn_query_range <bucket>: every record under one hash (range list).
syn_query_range() {
  syn_curl -H 'Content-Type: application/json' \
    --data "{\"schema_name\":\"$SYN_SCHEMA_ID\",\"fields\":[\"title\"],\"filter\":{\"HashKey\":\"$1\"}}" \
    http://x/api/query 2>/dev/null || true
}

# syn_status: the full /api/status JSON on stdout.
syn_status() {
  syn_curl http://x/api/status 2>/dev/null || true
}

# syn_status_sampled_wait: block until the status sampler has published a
# sample newer than the one at call time (default sampler is 1 s here).
syn_status_sampled_wait() {
  local t0 now i=0
  t0="$(date +%s)"
  while [ "$i" -lt 15 ]; do
    now="$(syn_status | jq -r '.status.sampled_at // 0' 2>/dev/null || echo 0)"
    [ "${now:-0}" -gt "$t0" ] && return 0
    i=$((i + 1))
    sleep 1
  done
  return 0
}

syn_cleanup() {
  if [ -n "${SYN_PID:-}" ] && kill -0 "$SYN_PID" 2>/dev/null; then
    kill "$SYN_PID" 2>/dev/null || true
    sleep 1
    kill -9 "$SYN_PID" 2>/dev/null || true
  fi
  if [ "${KEEP:-0}" != "1" ] && [ -n "${SYN_HOME:-}" ]; then
    case "$SYN_HOME" in
      /private/tmp/lbar.*) rm -rf "$SYN_HOME" "$SYN_HOME.boot.log" 2>/dev/null || true ;;
    esac
  fi
}
