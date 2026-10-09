#!/usr/bin/env bash
#
# Shared helpers for db-perf-guard wrappers (Criterion + query memory).
# Sourced only — do not execute directly.
#
# Goals:
#   1) Prefer a durable probe-owned CARGO_TARGET_DIR so daily runs stay warm
#      without fighting ad-hoc agent worktree target dirs.
#   2) Wait for a free cargo window (then refuse with a named exit if the wait
#      budget is exhausted) so the daily probe is not permanently starved by
#      concurrent agent cargo; keep the exit-3 refuse as the hard stop.
#   3) Hold a host-local exclusive cargo lock while the probe measures, and
#      expose scripts/ci/with-fold-host-cargo-lock.sh so agent/routine cargo
#      can queue behind the probe instead of thrashing the host.
#   4) Classify cold vs warm target + record HEAD so over-budget failures can
#      be attributed to cold-compile vs product regression.

# shellcheck shell=bash

db_perf_default_shared_target_dir() {
  printf '%s\n' "${FOLD_DB_PERF_SHARED_TARGET_DIR:-${HOME}/.cache/fold-db-perf-guard/target}"
}

db_perf_host_cargo_lock_path() {
  printf '%s\n' "${FOLD_HOST_CARGO_LOCK_PATH:-${HOME}/.cache/fold-db-perf-guard/host-cargo.lock}"
}

# Resolve scripts/ci/lib/host-cargo-lock.py relative to this sourced file when
# possible; callers may also set FOLD_DB_PERF_HOST_LOCK_PY.
db_perf_host_cargo_lock_py() {
  if [[ -n "${FOLD_DB_PERF_HOST_LOCK_PY:-}" && -f "${FOLD_DB_PERF_HOST_LOCK_PY}" ]]; then
    printf '%s\n' "$FOLD_DB_PERF_HOST_LOCK_PY"
    return 0
  fi
  local here
  here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  if [[ -f "$here/host-cargo-lock.py" ]]; then
    printf '%s\n' "$here/host-cargo-lock.py"
    return 0
  fi
  return 1
}

db_perf_python() {
  if command -v python3 >/dev/null 2>&1; then
    printf '%s\n' "python3"
    return 0
  fi
  if command -v python >/dev/null 2>&1; then
    printf '%s\n' "python"
    return 0
  fi
  return 1
}

# Resolve CARGO_TARGET_DIR when unset.
# Order:
#   1. existing CARGO_TARGET_DIR
#   2. FOLD_DB_PERF_SHARED_TARGET_DIR / ~/.cache/fold-db-perf-guard/target (default)
#   3. legacy: git common-dir sibling target/ (opt-in via FOLD_DB_PERF_USE_GIT_COMMON_TARGET=1)
db_perf_resolve_cargo_target_dir() {
  if [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
    printf '%s\n' "$CARGO_TARGET_DIR"
    return 0
  fi
  if [[ "${FOLD_DB_PERF_USE_GIT_COMMON_TARGET:-0}" == "1" ]]; then
    local git_common_dir common_root
    git_common_dir="$(git rev-parse --git-common-dir)"
    common_root="$(cd "$git_common_dir/.." && pwd)"
    printf '%s\n' "$common_root/target"
    return 0
  fi
  db_perf_default_shared_target_dir
}

# Print one concurrent heavy-cargo line per process (pid + command), or nothing.
# Overridable probe for self-tests: FOLD_DB_PERF_CONCURRENT_PROBE (command).
db_perf_list_concurrent_cargo() {
  if [[ -n "${FOLD_DB_PERF_CONCURRENT_PROBE:-}" ]]; then
    # shellcheck disable=SC2086
    eval "$FOLD_DB_PERF_CONCURRENT_PROBE" || true
    return 0
  fi

  local self_pid="${1:-$$}"
  local line pid cmd
  # macOS/BSD and Linux: pgrep -lf prints "PID command..."
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    pid="${line%% *}"
    cmd="${line#* }"
    # Skip non-numeric / our process tree roots when possible
    [[ "$pid" =~ ^[0-9]+$ ]] || continue
    if [[ "$pid" -eq "$self_pid" ]]; then
      continue
    fi
    # Skip this shell's cargo stub invocations that are children of the guard
    # itself (best-effort: ignore if parent is us).
    if [[ -r "/proc/$pid/stat" ]]; then
      # Linux only
      local ppid
      ppid="$(awk '{print $4}' "/proc/$pid/stat" 2>/dev/null || true)"
      if [[ -n "$ppid" && "$ppid" -eq "$self_pid" ]]; then
        continue
      fi
    fi
    # Only heavy cargo jobs (build/test/bench/check/nextest)
    case "$cmd" in
      *' cargo '*|*/cargo\ *|cargo\ *)
        case "$cmd" in
          *\ build\ *|*\ test\ *|*\ bench\ *|*\ check\ *|*\ nextest\ *|*\ clippy\ *)
            printf '%s\n' "$line"
            ;;
        esac
        ;;
    esac
  done < <(pgrep -lf 'cargo' 2>/dev/null || true)
}

# Enrich a concurrent-cargo line with etime/user when ps is available.
# Input: "PID command..." → "PID etime=… user=… command…"
db_perf_enrich_concurrent_cargo_line() {
  local line="$1"
  local pid cmd etime user
  pid="${line%% *}"
  cmd="${line#* }"
  [[ "$pid" =~ ^[0-9]+$ ]] || {
    printf '%s\n' "$line"
    return 0
  }
  etime="$(ps -p "$pid" -o etime= 2>/dev/null | tr -d ' ' || true)"
  user="$(ps -p "$pid" -o user= 2>/dev/null | tr -d ' ' || true)"
  printf '%s etime=%s user=%s %s\n' "$pid" "${etime:-unknown}" "${user:-unknown}" "$cmd"
}

db_perf_emit_concurrent_cargo_finding() {
  local label="$1"
  local list="$2"
  local waited="${3:-0}"
  local count
  count="$(printf '%s\n' "$list" | grep -c . || true)"
  echo "concurrent_cargo=present count=${count} waited_seconds=${waited}"
  local line enriched
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    enriched="$(db_perf_enrich_concurrent_cargo_line "$line")"
    printf '  concurrent_cargo_proc: %s\n' "$enriched"
  done <<<"$list"
  # Structured FINDING for routine scrapers / card filing.
  echo "FINDING: concurrent-cargo label=${label} count=${count} waited_seconds=${waited}"
  echo "FINDING: concurrent-cargo use scripts/ci/with-fold-host-cargo-lock.sh for agent cargo; refuse is exit 3"
  echo "::error::${label} concurrent-cargo count=${count} waited_seconds=${waited} (abort to preserve setup budget; wait window exhausted — re-run when host is idle, wrap agent cargo with with-fold-host-cargo-lock.sh, or set FOLD_DB_PERF_ALLOW_CONCURRENT_CARGO=1)" >&2
}

# Wait for a free heavy-cargo window, then return 0; or refuse with exit 3.
#
# Env:
#   FOLD_DB_PERF_ALLOW_CONCURRENT_CARGO=1  — skip (not recommended)
#   FOLD_DB_PERF_CONCURRENT_WAIT_SECONDS  — total wait budget (default 900)
#   FOLD_DB_PERF_CONCURRENT_POLL_SECONDS  — poll interval (default 15)
#
# Backward-compatible name: callers still use db_perf_refuse_if_concurrent_cargo.
db_perf_refuse_if_concurrent_cargo() {
  local label="${1:-db-perf-guard}"
  if [[ "${FOLD_DB_PERF_ALLOW_CONCURRENT_CARGO:-0}" == "1" ]]; then
    echo "concurrent_cargo=allowed (FOLD_DB_PERF_ALLOW_CONCURRENT_CARGO=1)"
    return 0
  fi

  local wait_budget poll_s
  wait_budget="${FOLD_DB_PERF_CONCURRENT_WAIT_SECONDS:-900}"
  poll_s="${FOLD_DB_PERF_CONCURRENT_POLL_SECONDS:-15}"
  # Defensive: non-numeric → immediate refuse path (wait 0).
  [[ "$wait_budget" =~ ^[0-9]+$ ]] || wait_budget=0
  [[ "$poll_s" =~ ^[0-9]+$ ]] || poll_s=15
  if [[ "$poll_s" -lt 1 ]]; then
    poll_s=1
  fi

  local started_at now elapsed list count saw_busy
  started_at="$(date +%s)"
  saw_busy=0
  echo "concurrent_cargo_wait_budget_seconds=${wait_budget} poll_seconds=${poll_s}"

  while true; do
    list="$(db_perf_list_concurrent_cargo "$$" || true)"
    now="$(date +%s)"
    elapsed=$((now - started_at))
    if [[ -z "$list" ]]; then
      # Prefer saw_busy over wall-clock elapsed: Docker/act can report the same
      # date +%s across a 1s poll sleep, which previously printed
      # concurrent_cargo=none after concurrent_cargo=waiting and failed case7.
      if [[ "$saw_busy" -eq 1 ]]; then
        echo "concurrent_cargo=cleared waited_seconds=${elapsed}"
      else
        echo "concurrent_cargo=none"
      fi
      return 0
    fi

    saw_busy=1
    count="$(printf '%s\n' "$list" | grep -c . || true)"
    if [[ "$elapsed" -ge "$wait_budget" ]]; then
      db_perf_emit_concurrent_cargo_finding "$label" "$list" "$elapsed"
      return 3
    fi

    echo "concurrent_cargo=waiting count=${count} elapsed_seconds=${elapsed} remaining_seconds=$((wait_budget - elapsed))"
    # Short final sleep so we do not overshoot the budget by a full poll.
    local sleep_for="$poll_s"
    if [[ $((elapsed + sleep_for)) -gt "$wait_budget" ]]; then
      sleep_for=$((wait_budget - elapsed))
      [[ "$sleep_for" -lt 1 ]] && sleep_for=1
    fi
    sleep "$sleep_for"
  done
}

# Acquire the host cargo exclusive lock (hold process in background).
# Sets FOLD_DB_PERF_HOST_LOCK_PID for later release. Exit 4 on acquire timeout.
#
# Env:
#   FOLD_DB_PERF_HOST_LOCK_ACQUIRE_SECONDS  — default 60
#   FOLD_DB_PERF_SKIP_HOST_CARGO_LOCK=1     — skip (tests / emergency)
db_perf_acquire_host_cargo_lock() {
  local label="${1:-db-perf-guard}"
  if [[ "${FOLD_DB_PERF_SKIP_HOST_CARGO_LOCK:-0}" == "1" ]]; then
    echo "host_cargo_lock=skipped (FOLD_DB_PERF_SKIP_HOST_CARGO_LOCK=1)"
    return 0
  fi
  if [[ "${FOLD_HOST_CARGO_LOCK_HELD:-0}" == "1" ]]; then
    echo "host_cargo_lock=already-held-by-parent path=${FOLD_HOST_CARGO_LOCK_PATH:-unknown}"
    return 0
  fi

  local py helper path timeout_s ready holder
  if ! py="$(db_perf_python)"; then
    echo "host_cargo_lock=unavailable reason=no-python (continuing without exclusive lock)"
    return 0
  fi
  if ! helper="$(db_perf_host_cargo_lock_py)"; then
    echo "host_cargo_lock=unavailable reason=no-helper (continuing without exclusive lock)"
    return 0
  fi

  path="$(db_perf_host_cargo_lock_path)"
  timeout_s="${FOLD_DB_PERF_HOST_LOCK_ACQUIRE_SECONDS:-60}"
  [[ "$timeout_s" =~ ^[0-9]+$ ]] || timeout_s=60
  ready="$(mktemp "${TMPDIR:-/tmp}/fold-host-cargo-ready.XXXXXX")"
  holder="$(mktemp "${TMPDIR:-/tmp}/fold-host-cargo-holder.XXXXXX")"
  rm -f "$ready" "$holder"

  echo "host_cargo_lock=acquiring path=${path} timeout_seconds=${timeout_s} label=${label}"
  "$py" "$helper" hold --path "$path" --timeout "$timeout_s" \
    --ready-file "$ready" --holder-file "$holder" &
  FOLD_DB_PERF_HOST_LOCK_PID=$!
  export FOLD_DB_PERF_HOST_LOCK_PID

  local deadline now hold_rc
  deadline=$(( $(date +%s) + timeout_s + 5 ))
  while [[ ! -f "$ready" ]]; do
    if ! kill -0 "$FOLD_DB_PERF_HOST_LOCK_PID" 2>/dev/null; then
      set +e
      wait "$FOLD_DB_PERF_HOST_LOCK_PID"
      hold_rc=$?
      set -e
      echo "host_cargo_lock=acquire-failed exit=${hold_rc} path=${path}"
      rm -f "$ready" "$holder"
      unset FOLD_DB_PERF_HOST_LOCK_PID
      return 4
    fi
    now="$(date +%s)"
    if [[ "$now" -ge "$deadline" ]]; then
      kill "$FOLD_DB_PERF_HOST_LOCK_PID" 2>/dev/null || true
      wait "$FOLD_DB_PERF_HOST_LOCK_PID" 2>/dev/null || true
      echo "host_cargo_lock=acquire-timeout path=${path}"
      rm -f "$ready" "$holder"
      unset FOLD_DB_PERF_HOST_LOCK_PID
      return 4
    fi
    sleep 0.2
  done

  echo "host_cargo_lock=held path=${path} holder_pid=$(tr -d '[:space:]' <"$holder" 2>/dev/null || echo "$FOLD_DB_PERF_HOST_LOCK_PID")"
  rm -f "$ready" "$holder"
  return 0
}

db_perf_release_host_cargo_lock() {
  if [[ -n "${FOLD_DB_PERF_HOST_LOCK_PID:-}" ]]; then
    kill "$FOLD_DB_PERF_HOST_LOCK_PID" 2>/dev/null || true
    wait "$FOLD_DB_PERF_HOST_LOCK_PID" 2>/dev/null || true
    echo "host_cargo_lock=released holder_pid=${FOLD_DB_PERF_HOST_LOCK_PID}"
    unset FOLD_DB_PERF_HOST_LOCK_PID
  fi
}

# Classify whether a profile looks warm enough that setup should not dominate.
# profile: debug | release (bench profile lands under release/ by default)
db_perf_classify_target_state() {
  local target_dir="$1"
  local profile="${2:-release}"
  local marker_glob="$3"

  local state="cold"
  local matched=""
  local search_root="${target_dir}/${profile}/deps"
  if [[ -d "$search_root" ]]; then
    # shellcheck disable=SC2086
    matched="$(find "$search_root" -maxdepth 1 -type f -name "$marker_glob" 2>/dev/null | head -n 1 || true)"
    if [[ -n "$matched" ]]; then
      state="warm"
    fi
  fi

  local stamp_dir="${target_dir}/.fold-db-perf-guard"
  local stamp_file="${stamp_dir}/last-warm-head"
  local stamped_head=""
  if [[ -f "$stamp_file" ]]; then
    stamped_head="$(tr -d '[:space:]' <"$stamp_file" || true)"
  fi

  local head_sha=""
  head_sha="$(git rev-parse HEAD 2>/dev/null || true)"

  local target_mtime="unknown"
  if [[ -d "$target_dir" ]]; then
    # portable-ish mtime (GNU/BSD stat)
    target_mtime="$(stat -f %m "$target_dir" 2>/dev/null || stat -c %Y "$target_dir" 2>/dev/null || echo unknown)"
  fi

  local head_match="unknown"
  if [[ -n "$head_sha" && -n "$stamped_head" ]]; then
    if [[ "$head_sha" == "$stamped_head" ]]; then
      head_match="yes"
      # stamp alone can promote cold→warm for classification logging
      if [[ "$state" == "cold" ]]; then
        state="warm-stamp"
      fi
    else
      head_match="no"
    fi
  elif [[ -n "$stamped_head" ]]; then
    head_match="stale-or-missing-head"
  else
    head_match="unstamped"
  fi

  printf 'target_state=%s profile=%s head=%s stamped_head=%s head_match=%s target_mtime=%s marker=%s\n' \
    "$state" "$profile" "${head_sha:-unknown}" "${stamped_head:-none}" "$head_match" "$target_mtime" "${matched:-none}"
}

db_perf_stamp_warm_head() {
  local target_dir="$1"
  local stamp_dir="${target_dir}/.fold-db-perf-guard"
  mkdir -p "$stamp_dir"
  git rev-parse HEAD >"${stamp_dir}/last-warm-head" 2>/dev/null || true
  date -u +%Y-%m-%dT%H:%M:%SZ >"${stamp_dir}/last-warm-at" 2>/dev/null || true
}
