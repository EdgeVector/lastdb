#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: schema_pow_dev_proof_harness.sh --url URL [--environment dev] [--run-id ID] [--evidence FILE] [--report FILE] [--build-timeout-seconds N] [--probe-timeout-seconds N] [--target-dir DIR]

Runs the Schema Service client PoW proof suite against a dev/test endpoint:
  1. valid node-key PoW registration is accepted
  2. missing PoW headers are rejected
  3. invalid PoW is rejected

The harness writes JSON lines to the evidence file plus a redacted summary
report that explicitly proves challenge, grind, signed retry, and idempotent
repost. It exits nonzero on a failing probe or an incomplete report. Production
requires running the underlying Rust probe directly with its explicit
--allow-prod guard.
USAGE
}

url=""
environment="dev"
run_id="schema-pow-proof-$(date -u +%Y%m%dT%H%M%SZ)"
evidence=""
report=""
build_timeout_seconds="${SCHEMA_POW_BUILD_TIMEOUT_SECONDS:-1200}"
build_timeout_reserve_seconds="${SCHEMA_POW_BUILD_TIMEOUT_RESERVE_SECONDS:-30}"
probe_timeout_seconds="${SCHEMA_POW_PROBE_TIMEOUT_SECONDS:-300}"
cargo_bin="${SCHEMA_POW_HARNESS_CARGO_BIN:-cargo}"
target_dir="${SCHEMA_POW_HARNESS_TARGET_DIR:-${CARGO_TARGET_DIR:-}}"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --url)
      url="${2:-}"
      shift 2
      ;;
    --environment)
      environment="${2:-}"
      shift 2
      ;;
    --run-id)
      run_id="${2:-}"
      shift 2
      ;;
    --evidence)
      evidence="${2:-}"
      shift 2
      ;;
    --report)
      report="${2:-}"
      shift 2
      ;;
    --build-timeout-seconds)
      build_timeout_seconds="${2:-}"
      shift 2
      ;;
    --probe-timeout-seconds)
      probe_timeout_seconds="${2:-}"
      shift 2
      ;;
    --target-dir)
      target_dir="${2:-}"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "schema_pow_dev_proof_harness: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [ -z "$url" ]; then
  echo "schema_pow_dev_proof_harness: --url is required" >&2
  usage >&2
  exit 2
fi

if [ "$environment" != "dev" ]; then
  echo "schema_pow_dev_proof_harness: only --environment dev is supported" >&2
  exit 2
fi

canonical_dev_schema_url() {
  jq -r '.environments.dev.schema_service // empty' \
    "$repo_root/folddb_profile/environments.json"
}

case "$url" in
  https://schema-dev.folddb.com|https://schema-dev.folddb.com/)
    canonical_url="$(canonical_dev_schema_url)"
    if [ -z "$canonical_url" ]; then
      echo "schema_pow_dev_proof_harness: missing dev schema_service URL in folddb_profile/environments.json" >&2
      exit 2
    fi
    echo "schema_pow_dev_proof_harness: resolved retired dev alias through folddb_profile/environments.json" >&2
    url="$canonical_url"
    ;;
esac

if [ -z "$evidence" ]; then
  evidence="schema_service/target/schema-pow-dev-proof-${run_id}.jsonl"
fi
if [ -z "$report" ]; then
  report="${evidence%.jsonl}.report.json"
fi

validate_positive_integer() {
  local label="$1"
  local value="$2"
  case "$value" in
    ''|*[!0-9]*)
      echo "schema_pow_dev_proof_harness: $label must be a positive integer" >&2
      exit 2
      ;;
  esac
  if [ "$value" -lt 1 ]; then
    echo "schema_pow_dev_proof_harness: $label must be >= 1" >&2
    exit 2
  fi
}

validate_positive_integer "--build-timeout-seconds" "$build_timeout_seconds"
case "$build_timeout_reserve_seconds" in
  ''|*[!0-9]*)
    echo "schema_pow_dev_proof_harness: SCHEMA_POW_BUILD_TIMEOUT_RESERVE_SECONDS must be a non-negative integer" >&2
    exit 2
    ;;
esac
validate_positive_integer "--probe-timeout-seconds" "$probe_timeout_seconds"

effective_build_timeout_seconds="$build_timeout_seconds"
if [ "$build_timeout_seconds" -gt "$build_timeout_reserve_seconds" ]; then
  effective_build_timeout_seconds=$((build_timeout_seconds - build_timeout_reserve_seconds))
fi

if [ -z "$target_dir" ]; then
  if [ -n "${HOME:-}" ]; then
    target_dir="$HOME/.cache/edgevector-git/fold-schema-pow-dev-proof-target"
  else
    target_dir="schema_service/target/schema-pow-dev-proof-target"
  fi
fi
mkdir -p "$target_dir"

timeout_bin=""
if command -v gtimeout >/dev/null 2>&1; then
  timeout_bin="gtimeout"
elif command -v timeout >/dev/null 2>&1; then
  timeout_bin="timeout"
else
  echo "schema_pow_dev_proof_harness: gtimeout or timeout is required for bounded dev proof probes" >&2
  exit 2
fi

mkdir -p "$(dirname "$evidence")"
mkdir -p "$(dirname "$report")"
: > "$evidence"

json_event() {
  local status="$1"
  local probe="$2"
  local phase="$3"
  local reason="${4:-}"
  local timeout_seconds="${5:-$probe_timeout_seconds}"
  local detail="${6:-}"
  jq -cn \
    --arg status "$status" \
    --arg environment "$environment" \
    --arg run_id "$run_id" \
    --arg probe "$probe" \
    --arg phase "$phase" \
    --arg reason "$reason" \
    --arg detail "$detail" \
    --argjson timeout_seconds "$timeout_seconds" \
    '{
      status: $status,
      environment: $environment,
      run_id: $run_id,
      probe: $probe,
      phase: $phase,
      timeout_seconds: $timeout_seconds,
      private_key_persisted: false
    } + (if $reason == "" then {} else {reason: $reason} end)
      + (if $detail == "" then {} else {detail: $detail} end)'
}

append_event() {
  json_event "$@" >> "$evidence"
}

probe_bin=""

build_probe() {
  local output_json
  local output_log
  output_json="$(mktemp "${TMPDIR:-/tmp}/schema-pow-build-json.XXXXXX")"
  output_log="$(mktemp "${TMPDIR:-/tmp}/schema-pow-build-log.XXXXXX")"
  append_event "RUNNING" "schema-pow-live-probe" "cargo-build-target-dir" "using_target_dir" "$effective_build_timeout_seconds" "$target_dir"
  append_event "RUNNING" "schema-pow-live-probe" "cargo-build-start" "" "$effective_build_timeout_seconds"
  set +e
  CARGO_TERM_COLOR=never "$timeout_bin" -k 10s "${effective_build_timeout_seconds}s" \
    "$cargo_bin" build --target-dir "$target_dir" -p schema_service_client --bin schema_pow_live_probe --message-format=json \
    >"$output_json" 2>"$output_log"
  local rc="$?"
  set -e
  if [ "$rc" -ne 0 ]; then
    if [ "$rc" -eq 124 ] || [ "$rc" -eq 137 ]; then
      append_event "FAIL" "schema-pow-live-probe" "cargo-build-timeout" "build_timeout" "$effective_build_timeout_seconds"
      printf 'schema_pow_dev_proof_harness: build timed out after %ss; evidence=%s\n' \
        "$effective_build_timeout_seconds" "$evidence" >&2
    else
      append_event "FAIL" "schema-pow-live-probe" "cargo-build-exit" "build_exited_nonzero" "$effective_build_timeout_seconds"
    fi
    cat "$output_json" >&2
    cat "$output_log" >&2
    rm -f "$output_json" "$output_log"
    return 1
  fi

  probe_bin="$(jq -r '
    select(.reason == "compiler-artifact")
    | select(.target.name == "schema_pow_live_probe")
    | select(.executable != null)
    | .executable
  ' "$output_json" | tail -n 1)"
  if [ -z "$probe_bin" ] || [ ! -x "$probe_bin" ]; then
    append_event "FAIL" "schema-pow-live-probe" "cargo-build-executable" "missing_executable" "$effective_build_timeout_seconds"
    cat "$output_json" >&2
    cat "$output_log" >&2
    rm -f "$output_json" "$output_log"
    return 1
  fi
  append_event "PASS" "schema-pow-live-probe" "cargo-build" "" "$effective_build_timeout_seconds"
  rm -f "$output_json" "$output_log"
}

run_probe() {
  local label="$1"
  shift
  local output
  output="$(mktemp "${TMPDIR:-/tmp}/schema-pow-${label}.XXXXXX")"
  append_event "RUNNING" "$label" "probe-start"
  set +e
  CARGO_TERM_COLOR=never "$timeout_bin" -k 10s "${probe_timeout_seconds}s" \
    "$probe_bin" "$@" 2>&1 \
    | tee "$output" \
    | awk -v evidence="$evidence" '/^\{.*\}$/ { print >> evidence; fflush(evidence) } { print }'
  local rc="${PIPESTATUS[0]}"
  set -e
  if [ "$rc" -eq 0 ]; then
    printf 'PASS %s\n' "$label"
  else
    local report
    report="$(awk '/^\{.*\}$/ { line = $0 } END { print line }' "$output")"
    if [ "$rc" -eq 124 ] || [ "$rc" -eq 137 ]; then
      append_event "FAIL" "$label" "probe-timeout" "probe_timeout"
      printf 'schema_pow_dev_proof_harness: %s timed out after %ss; evidence=%s\n' \
        "$label" "$probe_timeout_seconds" "$evidence" >&2
    elif [ -n "$report" ]; then
      :
    else
      append_event "FAIL" "$label" "cargo-run-exit" "probe_exited_nonzero"
    fi
    cat "$output" >&2
    rm -f "$output"
    return 1
  fi
  rm -f "$output"
}

build_probe

run_probe "valid-registration" \
  --url "$url" \
  --environment "$environment" \
  --run-id "$run_id"

run_probe "missing-proof-rejected" \
  --url "$url" \
  --environment "$environment" \
  --run-id "${run_id}-missing" \
  --negative-proof missing

run_probe "invalid-proof-rejected" \
  --url "$url" \
  --environment "$environment" \
  --run-id "${run_id}-invalid" \
  --negative-proof invalid

report_tmp="${report}.tmp"
if ! jq -e -s \
  --arg run_id "$run_id" \
  --arg evidence_file "$(basename "$evidence")" '
    ([.[] | select(
      .status == "PASS"
      and .environment == "dev"
      and .private_key_persisted == false
      and .protocol_steps.challenge == "PASS"
      and .protocol_steps.grind == "PASS"
      and .protocol_steps.signed_retry == "PASS"
      and .protocol_steps.idempotent_repost == "PASS"
    )][0]) as $valid
    | ([.[] | select(
        .status == "PASS"
        and .negative_proof == "missing"
        and .rejection == "node_key_required"
        and .private_key_persisted == false
      )] | length) as $missing_proof_passes
    | ([.[] | select(
        .status == "PASS"
        and .negative_proof == "invalid"
        and .rejection == "proof_of_work_invalid"
        and .private_key_persisted == false
      )] | length) as $invalid_proof_passes
    | select($valid != null and $missing_proof_passes > 0 and $invalid_proof_passes > 0)
    | {
        status: "PASS",
        environment: "dev",
        run_id: $run_id,
        evidence_file: $evidence_file,
        redacted: true,
        protocol_steps: $valid.protocol_steps,
        negative_controls: {
          missing_proof_rejected: "PASS",
          invalid_proof_rejected: "PASS"
        },
        private_key_persisted: false
      }
  ' "$evidence" >"$report_tmp"; then
  rm -f "$report_tmp"
  append_event "FAIL" "schema-pow-live-proof-report" "report-validation" "required_protocol_step_missing"
  printf 'schema_pow_dev_proof_harness: required proof stage missing; evidence=%s\n' "$evidence" >&2
  exit 1
fi
mv "$report_tmp" "$report"

printf 'Evidence: %s\n' "$evidence"
printf 'Report: %s\n' "$report"
