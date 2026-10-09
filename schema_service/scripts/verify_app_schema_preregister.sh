#!/usr/bin/env bash
# Verify known app schema identities still resolve on Schema Service.
#
# Usage:
#   bash schema_service/scripts/verify_app_schema_preregister.sh
#   SCHEMA_SERVICE_URL=https://... bash schema_service/scripts/verify_app_schema_preregister.sh
#
# Default URL is prod from folddb_profile/environments.json (not hardcoded
# elsewhere — override with SCHEMA_SERVICE_URL for dev/staging).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ENV_JSON="${ROOT}/folddb_profile/environments.json"

if [[ -z "${SCHEMA_SERVICE_URL:-}" ]]; then
  if command -v python3 >/dev/null 2>&1 && [[ -f "$ENV_JSON" ]]; then
    SCHEMA_SERVICE_URL="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["environments"]["prod"]["schema_service"].rstrip("/"))' "$ENV_JSON")"
  else
    echo "error: set SCHEMA_SERVICE_URL or provide folddb_profile/environments.json" >&2
    exit 2
  fi
fi

SCHEMA_SERVICE_URL="${SCHEMA_SERVICE_URL%/}"

# identity_hash values pinned in docs/app_schema_preregister_proof.md
declare -a CHECKS=(
  "lastsecrets/LastSecret:7f2f8d56b5b22ca1ba4a27ced754d80bab3d2defe92ac379e1d85327c5271b82"
  "fbrain/concept:8838f066b34a72fd0ad3d4c22e36a09c1c1150d81d1c6bf5bff4176276ed4fe7"
  "fbrain/task:4a67db42689be7c1c85b51df6d2f9dab8c292596805b6885286e9652cd3b2d0e"
  "fkanban/card:eacad7322a1eb2daa26e389426c160e522c682d4cbcdf601c6df7093421122db"
  "fkanban/board:53bef1f61388fe0c219f5e6310f68f2bcfeef5d9513477b09bb8b3d6b4584275"
)

fail=0
echo "schema service: $SCHEMA_SERVICE_URL"
for entry in "${CHECKS[@]}"; do
  label="${entry%%:*}"
  hash="${entry#*:}"
  code="$(curl -sS -o /tmp/schema-preregister-body.json -w '%{http_code}' \
    "${SCHEMA_SERVICE_URL}/v1/schemas/${hash}" || echo "000")"
  if [[ "$code" == "200" ]]; then
    echo "OK  $code  $label  ${hash:0:16}…"
  else
    echo "FAIL $code  $label  $hash" >&2
    fail=1
  fi
done

if [[ "$fail" -ne 0 ]]; then
  echo "one or more schema identities missing on Schema Service" >&2
  exit 1
fi

echo "all checked identities present"
exit 0
