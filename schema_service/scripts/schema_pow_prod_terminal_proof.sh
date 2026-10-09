#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
prod_url="$(jq -r '.environments.prod.schema_service // empty' "$repo_root/folddb_profile/environments.json")"
attestation=""
evidence="$repo_root/schema_service/target/schema-pow-prod-terminal-proof.jsonl"
report="$repo_root/proofs/schema-pow-prod-terminal-proof.md"
probe_bin="${SCHEMA_POW_PROD_PROBE_BIN:-}"
timeout_seconds="${SCHEMA_POW_PROD_TIMEOUT_SECONDS:-300}"
allow_prod=0

usage() {
  cat <<'USAGE'
Usage: schema_pow_prod_terminal_proof.sh --allow-prod --attestation FILE [--evidence FILE] [--report FILE] [--probe-bin FILE] [--timeout-seconds N]

Runs the real Schema Service client against the canonical production endpoint,
then writes a redacted PASS report only when the positive path, all three
negative controls, and the operator-supplied deployment attestation pass.
USAGE
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --allow-prod) allow_prod=1; shift ;;
    --attestation) attestation="${2:-}"; shift 2 ;;
    --evidence) evidence="${2:-}"; shift 2 ;;
    --report) report="${2:-}"; shift 2 ;;
    --probe-bin) probe_bin="${2:-}"; shift 2 ;;
    --timeout-seconds) timeout_seconds="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "schema_pow_prod_terminal_proof: unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [ "$allow_prod" -ne 1 ]; then
  echo "schema_pow_prod_terminal_proof: production proof requires --allow-prod" >&2
  exit 2
fi
if [ -z "$attestation" ] || [ ! -r "$attestation" ]; then
  echo "schema_pow_prod_terminal_proof: --attestation must name a readable redacted JSON file" >&2
  exit 2
fi
case "$timeout_seconds" in
  ''|*[!0-9]*) echo "schema_pow_prod_terminal_proof: --timeout-seconds must be a positive integer" >&2; exit 2 ;;
esac
if [ "$timeout_seconds" -lt 1 ]; then
  echo "schema_pow_prod_terminal_proof: --timeout-seconds must be >= 1" >&2
  exit 2
fi
if [ -z "$prod_url" ]; then
  echo "schema_pow_prod_terminal_proof: production schema_service URL is missing from folddb_profile/environments.json" >&2
  exit 2
fi

timeout_bin=""
command -v gtimeout >/dev/null 2>&1 && timeout_bin="gtimeout"
command -v timeout >/dev/null 2>&1 && timeout_bin="timeout"
if [ -z "$timeout_bin" ]; then
  echo "schema_pow_prod_terminal_proof: gtimeout or timeout is required" >&2
  exit 2
fi

if [ -z "$probe_bin" ]; then
  target_dir="${SCHEMA_POW_PROD_TARGET_DIR:-${HOME}/.cache/edgevector-git/fold-schema-pow-prod-proof-target}"
  mkdir -p "$target_dir"
  "$timeout_bin" -k 10s "${timeout_seconds}s" \
    cargo build --target-dir "$target_dir" -p schema_service_client --bin schema_pow_live_probe
  probe_bin="$target_dir/debug/schema_pow_live_probe"
fi
if [ ! -x "$probe_bin" ]; then
  echo "schema_pow_prod_terminal_proof: probe binary is not executable" >&2
  exit 2
fi

# The attestation is deliberately narrow and rejects secret-shaped field names
# anywhere in the document before any live request is sent.
jq -e '
  .environment == "prod"
  and .enforcement_enabled == true
  and (.deployment_revision | type == "string" and length > 0)
  and (.verified_at | type == "string" and length > 0)
  and .canary.promoted == true
  and .canary.mutation_gate_alarms == "OK"
  and .canary.rollback_status == "ready"
  and .quota_alarm_evidence.minute_hour_day_telemetry == true
  and .quota_alarm_evidence.quota_rejection_signal == true
  and .production_scope.local == true
  and .production_scope.shared_discovery == true
  and .production_scope.unowned == true
  and .rollback.documented == true
  and (.rollback.path | type == "string" and length > 0)
  and ([.. | objects | keys[]
        | select(test("authorization|credential|header|private_key|raw_payload|request_body|response_body|secret|token"; "i"))]
       | length == 0)
' "$attestation" >/dev/null || {
  echo "schema_pow_prod_terminal_proof: attestation is incomplete or contains forbidden secret-shaped fields" >&2
  exit 1
}

mkdir -p "$(dirname "$evidence")" "$(dirname "$report")"
evidence_tmp="${evidence}.tmp"
report_tmp="${report}.tmp"
trap 'rm -f "$evidence_tmp" "$report_tmp"' EXIT
: > "$evidence_tmp"

run_probe() {
  local mode="$1"
  shift
  local output
  output="$(mktemp "${TMPDIR:-/tmp}/schema-pow-prod-${mode}.XXXXXX")"
  set +e
  "$timeout_bin" -k 10s "${timeout_seconds}s" \
    "$probe_bin" --url "$prod_url" --environment prod --allow-prod "$@" \
    >"$output" 2>&1
  local rc=$?
  set -e
  awk '/^\{.*\}$/ { print }' "$output" >> "$evidence_tmp"
  if [ "$rc" -ne 0 ]; then
    rm -f "$output"
    mv "$evidence_tmp" "$evidence"
    echo "schema_pow_prod_terminal_proof: ${mode} failed; redacted evidence=$evidence" >&2
    return 1
  fi
  rm -f "$output"
}

run_probe valid-registration
run_probe missing-proof --negative-proof missing
run_probe invalid-proof --negative-proof invalid
run_probe expired-proof --negative-proof expired

jq -e -s '
  any(.[]; .status == "PASS" and .environment == "prod"
    and .private_key_persisted == false
    and .protocol_steps.challenge == "PASS"
    and .protocol_steps.grind == "PASS"
    and .protocol_steps.signed_retry == "PASS"
    and .protocol_steps.idempotent_repost == "PASS")
  and any(.[]; .status == "PASS" and .environment == "prod"
    and .negative_proof == "missing" and .rejection == "node_key_required"
    and .private_key_persisted == false)
  and any(.[]; .status == "PASS" and .environment == "prod"
    and .negative_proof == "invalid" and .rejection == "proof_of_work_invalid"
    and .private_key_persisted == false)
  and any(.[]; .status == "PASS" and .environment == "prod"
    and .negative_proof == "expired" and .rejection == "proof_of_work_expired"
    and .private_key_persisted == false)
' "$evidence_tmp" >/dev/null || {
  echo "schema_pow_prod_terminal_proof: required production probe evidence is incomplete" >&2
  exit 1
}

deployment_revision="$(jq -r '.deployment_revision' "$attestation")"
verified_at="$(jq -r '.verified_at' "$attestation")"
rollback_path="$(jq -r '.rollback.path' "$attestation")"
cat >"$report_tmp" <<EOF
PASS

# Schema PoW production terminal proof

- Environment: production
- Verified at: ${verified_at}
- Deployment revision: ${deployment_revision}
- Evidence: $(basename "$evidence") (redacted JSONL; not committed)
- Enforcement enabled: PASS
- Real client challenge, grind, signed retry, and idempotent repost: PASS
- Missing, invalid, and expired proof rejection: PASS
- Minute/hour/day quota telemetry and quota-rejection signal: PASS
- Canary mutation-gate alarms: OK
- Canary promotion: PASS
- Production scope (local, shared-discovery, and unowned intents): PASS
- Rollback readiness: PASS
- Rollback path: ${rollback_path}

No credential, request payload, response payload, signing key, or secret value is recorded in this report.
EOF

mv "$evidence_tmp" "$evidence"
mv "$report_tmp" "$report"
printf 'PASS report=%s evidence=%s\n' "$report" "$evidence"
