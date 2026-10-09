#!/usr/bin/env bash
# P2 storage metering drift probe — NOT on the sync hot path.
#
# Calls storage_service `audit_storage` (full R2+B2 list vs live Dynamo meters).
# Optional heal: APPLY=1 rewrites absolute totals (same as recalculate_storage).
#
# Usage:
#   EXEMEM_API_KEY=em_… ./scripts/agent/audit-storage-metering.sh
#   BASE_URL=… EXEMEM_API_KEY=… EPSILON=1048576 APPLY=0 ./scripts/agent/audit-storage-metering.sh
#
# Exit codes:
#   0 — ok and within epsilon
#   2 — ok response but drift exceeds epsilon
#   1 — request/HTTP/parse failure
#
# Env:
#   BASE_URL          optional; default = folddb_profile environments.json exemem_api for ENV
#   ENV               default dev (selects environments.json entry)
#   EXEMEM_API_KEY    required (X-API-Key)
#   EPSILON           drift_epsilon_bytes (default 0 = exact)
#   APPLY             1 to heal after probe (default 0)
#   STORAGE_EXTRA_SCOPES optional comma/space/newline-separated org_hash scopes

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ENV_NAME="${ENV:-dev}"
EPSILON="${EPSILON:-0}"
APPLY="${APPLY:-0}"

if [[ -z "${BASE_URL:-}" ]]; then
  BASE_URL=$(jq -r --arg e "$ENV_NAME" '
    .environments[$e].exemem_api // empty
  ' "$ROOT/folddb_profile/environments.json")
  if [[ -z "$BASE_URL" || "$BASE_URL" == "null" ]]; then
    echo "Could not resolve BASE_URL for ENV=$ENV_NAME from folddb_profile/environments.json" >&2
    exit 1
  fi
fi

if [[ -z "${EXEMEM_API_KEY:-}" ]]; then
  echo "EXEMEM_API_KEY is required" >&2
  exit 1
fi

apply_json=false
if [[ "$APPLY" == "1" || "$APPLY" == "true" || "$APPLY" == "yes" ]]; then
  apply_json=true
fi

body=$(jq -nc \
  --argjson epsilon "$EPSILON" \
  --argjson apply "$apply_json" \
  --arg scopes "${STORAGE_EXTRA_SCOPES:-}" \
  '{
    action:"audit_storage",
    drift_epsilon_bytes:$epsilon,
    apply:$apply,
    extra_scopes: ($scopes | gsub("[,[:space:]]+";" ") | split(" ") | map(select(length > 0)))
  }')

tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT

http_code=$(curl -sS -o "$tmp" -w '%{http_code}' \
  -X POST "${BASE_URL}/api/sync/storage" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: ${EXEMEM_API_KEY}" \
  -d "$body")

if [[ "$http_code" != "200" ]]; then
  echo "HTTP $http_code" >&2
  cat "$tmp" >&2
  exit 1
fi

if ! jq -e '.ok == true' "$tmp" >/dev/null 2>&1; then
  echo "audit_storage failed:" >&2
  cat "$tmp" >&2
  exit 1
fi

jq '{
  live_used: .audit.live.used_bytes,
  scanned_used: .audit.scanned.used_bytes,
  drift_bytes: .audit.drift_bytes,
  abs_drift_bytes: .audit.abs_drift_bytes,
  within_epsilon: .audit.within_epsilon,
  epsilon_bytes: .audit.epsilon_bytes,
  scanned_b2: .audit.scanned_b2,
  applied: .audit.applied,
  live: .audit.live,
  scanned: .audit.scanned
}' "$tmp"

within=$(jq -r '.audit.within_epsilon' "$tmp")
if [[ "$within" != "true" ]]; then
  echo "DRIFT: abs_drift_bytes exceeds epsilon ($EPSILON)" >&2
  exit 2
fi

exit 0
