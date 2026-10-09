#!/usr/bin/env bash
# Paid cloud sync + metering dogfood on Exemem **dev** (Stripe test mode).
#
# END STATE (cards metering-dogfood-paid-e2e + metering-org-multi-db-dogfood-e2e):
#   isolated home → setup-paid → Stripe test 4242 → register → upload DB+file
#   → upload an org_hash-scoped DB log → plan=paid / access_allowed
#   → storage.databases[] has personal+org rows → audit_storage within epsilon
#
# NEVER points at the primary brain (~/.lastdb). Always uses a throwaway
# --data-dir (default: mktemp under /tmp).
#
# Phases:
#   1) setup-paid      — create identity + open Checkout; print URL; wait for
#                        operator (or AGENT_STRIPE_DONE=1 after browser pay)
#   2) status          — require plan=paid + access_allowed
#   3) put-objects     — minimal billable put via storage API
#                        (personal log + cas, org_hash log)
#   4) assert-multidb  — require personal+org rows in storage.databases[]
#   5) audit           — scripts/agent/audit-storage-metering.sh within epsilon
#   6) assert-suspended — after dev suspension, prove status + write rejection
#
# Usage:
#   # Full interactive (human pays Checkout with 4242):
#   ./scripts/agent/dogfood-paid-cloud-sync.sh
#
#   # Resume after paying Checkout (home already has identity + cloud_sync.json
#   # partially written only after register succeeds):
#   HOME_DIR=/tmp/paid-dogfood-XXXX ./scripts/agent/dogfood-paid-cloud-sync.sh --from status
#
#   # Build + use in-tree lastdb (required until brew ships `cloud setup-paid`):
#   LASTDB_BIN=./target/release/lastdb ./scripts/agent/dogfood-paid-cloud-sync.sh
#
# Env:
#   ENV                 default dev (prod refused)
#   HOME_DIR            throwaway node home (created if missing)
#   LASTDB_BIN          path to lastdb with `cloud setup-paid` (default: lastdb on PATH)
#   BASE_URL            optional; default from folddb_profile/environments.json
#   EPSILON             audit drift bytes (default 1048576 = 1 MiB)
#   SKIP_UPLOAD         1 = stop after paid status (no put/audit)
#   SKIP_AUDIT          1 = stop after upload
#   SKIP_MULTIDB_ASSERT 1 = skip storage.databases[] assertion
#   AGENT_NONINTERACTIVE 1 = fail instead of waiting on stdin for setup-paid
#
# Exit: 0 success · 2 paid status not ready · 3 audit drift · 1 other

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ENV_NAME="${ENV:-dev}"
EPSILON="${EPSILON:-1048576}"
FROM_PHASE="${FROM:-setup}"
LASTDB_BIN="${LASTDB_BIN:-lastdb}"

if [[ "$ENV_NAME" != "dev" ]]; then
  echo "REFUSED: ENV=$ENV_NAME — this dogfood is dev/Stripe-test only" >&2
  exit 1
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --from)
      FROM_PHASE="$2"
      shift 2
      ;;
    --home)
      HOME_DIR="$2"
      shift 2
      ;;
    -h|--help)
      sed -n '2,40p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 1
      ;;
  esac
done

if [[ -z "${HOME_DIR:-}" ]]; then
  HOME_DIR="$(mktemp -d /tmp/paid-dogfood-XXXXXX)"
fi
mkdir -p "$HOME_DIR"
export FOLDDB_DISABLE_KEYCHAIN=1

if [[ -z "${BASE_URL:-}" ]]; then
  BASE_URL=$(jq -r --arg e "$ENV_NAME" '
    .environments[$e].exemem_api // empty
  ' "$ROOT/folddb_profile/environments.json")
  if [[ -z "$BASE_URL" || "$BASE_URL" == "null" ]]; then
    echo "Could not resolve BASE_URL for ENV=$ENV_NAME" >&2
    exit 1
  fi
fi

echo "=== paid cloud dogfood ==="
echo "ENV=$ENV_NAME BASE_URL=$BASE_URL"
echo "HOME_DIR=$HOME_DIR"
echo "LASTDB_BIN=$LASTDB_BIN"
echo "FROM=$FROM_PHASE EPSILON=$EPSILON"
echo

need_bin() {
  if ! command -v "$LASTDB_BIN" >/dev/null 2>&1 && [[ ! -x "$LASTDB_BIN" ]]; then
    echo "lastdb not found: $LASTDB_BIN" >&2
    echo "Build: cargo build -p lastdb_node --bin lastdb --release" >&2
    echo "Then: LASTDB_BIN=\$PWD/target/release/lastdb $0" >&2
    exit 1
  fi
  if ! "$LASTDB_BIN" cloud --help >/dev/null 2>&1; then
    echo "ERROR: $LASTDB_BIN has no 'cloud' subcommand (brew Mini is too old)." >&2
    echo "Build from fold main: cargo build -p lastdb_node --bin lastdb --release" >&2
    exit 1
  fi
}

phase_setup() {
  need_bin
  echo "--- phase: setup-paid ---"
  if [[ "${AGENT_NONINTERACTIVE:-0}" == "1" ]]; then
    echo "AGENT_NONINTERACTIVE=1: not running interactive setup-paid." >&2
    echo "Open Checkout yourself, then re-run with HOME_DIR=$HOME_DIR --from status" >&2
    echo "Or run without AGENT_NONINTERACTIVE and complete 4242 in the browser." >&2
    exit 2
  fi
  echo "Stripe test card: 4242 4242 4242 4242 · any future expiry · any CVC"
  echo "After payment succeeds, press Enter in the setup-paid prompt."
  "$LASTDB_BIN" --data-dir "$HOME_DIR" cloud setup-paid --env "$ENV_NAME"
  echo "setup-paid finished."
}

phase_status() {
  need_bin
  echo "--- phase: status ---"
  if [[ ! -f "$HOME_DIR/cloud_sync.json" ]]; then
    echo "missing $HOME_DIR/cloud_sync.json — run setup-paid first" >&2
    exit 2
  fi
  local out
  out=$("$LASTDB_BIN" --data-dir "$HOME_DIR" cloud status --env "$ENV_NAME" 2>&1) || {
    echo "$out" >&2
    exit 2
  }
  echo "$out"
  local plan access
  plan=$(echo "$out" | awk -F': *' '/^plan:/{print $2; exit}' | tr -d '[:space:]')
  access=$(echo "$out" | awk -F': *' '/^access_allowed:/{print $2; exit}' | tr -d '[:space:]')
  if [[ "$plan" != "paid" || "$access" != "true" ]]; then
    echo "NOT READY: need plan=paid access_allowed=true (got plan=$plan access_allowed=$access)" >&2
    echo "If Checkout just completed, wait ~10–30s for webhook and re-run --from status" >&2
    exit 2
  fi
  echo "status OK: plan=paid access_allowed=true"
}

api_key_from_home() {
  local api_key
  api_key=$(jq -r '.api_key // empty' "$HOME_DIR/cloud_sync.json")
  if [[ -z "$api_key" || "$api_key" == "null" ]]; then
    echo "cloud_sync.json missing api_key" >&2
    exit 1
  fi
  printf '%s\n' "$api_key"
}

subscription_status_json() {
  local api_key tmp http
  api_key=$(api_key_from_home)
  tmp=$(mktemp)
  http=$(curl -sS -o "$tmp" -w '%{http_code}' \
    -H "X-API-Key: ${api_key}" \
    "${BASE_URL}/api/subscription/status")
  if [[ "$http" != "200" ]] || ! jq -e '.ok == true' "$tmp" >/dev/null 2>&1; then
    echo "subscription status failed HTTP $http" >&2
    cat "$tmp" >&2
    rm -f "$tmp"
    exit 1
  fi
  cat "$tmp"
  rm -f "$tmp"
}

# Minimal billable objects via storage_service (does not require full lastdbd sync).
# Log: POST /api/sync/presign action=presign_log_upload
# File: POST /api/sync/presign action=presign_file_upload (CAS sha256)
# Credit: POST /api/sync/storage action=confirm_upload (HEAD + ledger)
phase_upload() {
  echo "--- phase: put-objects ---"
  local api_key
  api_key=$(api_key_from_home)
  export EXEMEM_API_KEY="$api_key"

  local stamp payload_log payload_file payload_org_log file_hash org_hash
  stamp=$(date -u +%Y%m%dT%H%M%SZ)
  payload_log=$(mktemp)
  payload_file=$(mktemp)
  payload_org_log=$(mktemp)
  printf 'dogfood-log %s\n' "$stamp" >"$payload_log"
  printf 'dogfood-file %s\n' "$stamp" >"$payload_file"
  printf 'dogfood-org-log %s\n' "$stamp" >"$payload_org_log"
  file_hash=$(shasum -a 256 "$payload_file" | awk '{print $1}')
  org_hash=$(printf 'dogfood-org %s\n' "$stamp" | shasum -a 256 | awk '{print $1}')
  printf '%s\n' "$org_hash" >"$HOME_DIR/dogfood-org-hash.txt"

  local tmp http
  tmp=$(mktemp)

  # --- log ---
  local log_size seq=1
  log_size=$(wc -c <"$payload_log" | tr -d ' ')
  http=$(curl -sS -o "$tmp" -w '%{http_code}' \
    -X POST "${BASE_URL}/api/sync/presign" \
    -H "Content-Type: application/json" \
    -H "X-API-Key: ${EXEMEM_API_KEY}" \
    -d "$(jq -nc --argjson seq "$seq" --argjson size "$log_size" \
      '{action:"presign_log_upload", seq_numbers:[$seq], estimated_size_bytes:$size}')")
  if [[ "$http" != "200" ]] || ! jq -e '.ok == true' "$tmp" >/dev/null 2>&1; then
    echo "presign_log_upload failed HTTP $http" >&2
    cat "$tmp" >&2
    rm -f "$tmp" "$payload_log" "$payload_file" "$payload_org_log"
    exit 1
  fi
  local log_url
  log_url=$(jq -r '.urls[0].url // empty' "$tmp")
  curl -sS -o /dev/null -f -X PUT -T "$payload_log" "$log_url"
  # Derive key from presigned URL path: .../{user_hash}/log/{seq}.enc
  local log_key
  log_key=$(python3 -c "from urllib.parse import urlparse; p=urlparse('$log_url').path; print('/'.join(p.strip('/').split('/')[-3:]))")
  # confirm_upload is routed under /api/sync/presign (same handler as presign_*)
  http=$(curl -sS -o "$tmp" -w '%{http_code}' \
    -X POST "${BASE_URL}/api/sync/presign" \
    -H "Content-Type: application/json" \
    -H "X-API-Key: ${EXEMEM_API_KEY}" \
    -d "$(jq -nc --arg k "$log_key" '{action:"confirm_upload", key:$k}')")
  echo "log put+confirm key=$log_key http=$http $(jq -c . "$tmp" 2>/dev/null || true)"

  # --- file (CAS) ---
  local file_size
  file_size=$(wc -c <"$payload_file" | tr -d ' ')
  http=$(curl -sS -o "$tmp" -w '%{http_code}' \
    -X POST "${BASE_URL}/api/sync/presign" \
    -H "Content-Type: application/json" \
    -H "X-API-Key: ${EXEMEM_API_KEY}" \
    -d "$(jq -nc --arg h "$file_hash" --argjson size "$file_size" \
      '{action:"presign_file_upload", file_hash:$h, estimated_size_bytes:$size}')")
  if [[ "$http" != "200" ]] || ! jq -e '.ok == true' "$tmp" >/dev/null 2>&1; then
    echo "presign_file_upload failed HTTP $http" >&2
    cat "$tmp" >&2
    rm -f "$tmp" "$payload_log" "$payload_file" "$payload_org_log"
    exit 1
  fi
  local file_url file_key
  file_url=$(jq -r '.urls[0].url // empty' "$tmp")
  file_key=$(jq -r '.key // empty' "$tmp")
  if [[ -z "$file_key" ]]; then
    file_key=$(python3 -c "from urllib.parse import urlparse; p=urlparse('$file_url').path; print('/'.join(p.strip('/').split('/')[-4:]))")
  fi
  curl -sS -o /dev/null -f -X PUT -T "$payload_file" "$file_url"
  http=$(curl -sS -o "$tmp" -w '%{http_code}' \
    -X POST "${BASE_URL}/api/sync/presign" \
    -H "Content-Type: application/json" \
    -H "X-API-Key: ${EXEMEM_API_KEY}" \
    -d "$(jq -nc --arg k "$file_key" '{action:"confirm_upload", key:$k}')")
  echo "file put+confirm key=$file_key http=$http $(jq -c . "$tmp" 2>/dev/null || true)"

  # --- org database log ---
  local org_log_size
  org_log_size=$(wc -c <"$payload_org_log" | tr -d ' ')
  http=$(curl -sS -o "$tmp" -w '%{http_code}' \
    -X POST "${BASE_URL}/api/sync/presign" \
    -H "Content-Type: application/json" \
    -H "X-API-Key: ${EXEMEM_API_KEY}" \
    -d "$(jq -nc --arg org "$org_hash" --argjson count 1 --argjson size "$org_log_size" \
      '{action:"presign_log_upload", org_hash:$org, count:$count, estimated_size_bytes:$size}')")
  if [[ "$http" != "200" ]] || ! jq -e '.ok == true' "$tmp" >/dev/null 2>&1; then
    echo "org presign_log_upload failed HTTP $http" >&2
    cat "$tmp" >&2
    rm -f "$tmp" "$payload_log" "$payload_file" "$payload_org_log"
    exit 1
  fi
  local org_log_url org_log_key
  org_log_url=$(jq -r '.urls[0].url // empty' "$tmp")
  curl -sS -o /dev/null -f -X PUT -T "$payload_org_log" "$org_log_url"
  org_log_key=$(python3 -c "from urllib.parse import urlparse; p=urlparse('$org_log_url').path; print('/'.join(p.strip('/').split('/')[-3:]))")
  http=$(curl -sS -o "$tmp" -w '%{http_code}' \
    -X POST "${BASE_URL}/api/sync/presign" \
    -H "Content-Type: application/json" \
    -H "X-API-Key: ${EXEMEM_API_KEY}" \
    -d "$(jq -nc --arg org "$org_hash" --arg k "$org_log_key" \
      '{action:"confirm_upload", org_hash:$org, key:$k}')")
  echo "org log put+confirm org_hash=$org_hash key=$org_log_key http=$http $(jq -c . "$tmp" 2>/dev/null || true)"
  if [[ "$http" != "200" ]] || ! jq -e '.ok == true' "$tmp" >/dev/null 2>&1; then
    echo "org confirm_upload failed HTTP $http" >&2
    rm -f "$tmp" "$payload_log" "$payload_file" "$payload_org_log"
    exit 1
  fi

  rm -f "$tmp" "$payload_log" "$payload_file" "$payload_org_log"
  echo "upload phase done (personal log+cas, org log)"
}

phase_assert_multidb() {
  echo "--- phase: assert-multidb ---"
  if [[ ! -f "$HOME_DIR/dogfood-org-hash.txt" ]]; then
    echo "missing $HOME_DIR/dogfood-org-hash.txt — run upload first" >&2
    exit 1
  fi
  local org_hash status_tmp
  org_hash=$(cat "$HOME_DIR/dogfood-org-hash.txt")
  status_tmp=$(mktemp)
  subscription_status_json >"$status_tmp"
  jq --arg org "$org_hash" '{
    plan,
    access_allowed,
    storage: {
      used_bytes: .storage.used_bytes,
      database_bytes: .storage.database_bytes,
      file_reference_bytes: .storage.file_reference_bytes,
      databases: .storage.databases
    },
    expected_org_hash: $org
  }' "$status_tmp"
  jq -e --arg org "$org_hash" '
    .plan == "paid"
    and .access_allowed == true
    and ([.storage.databases[]? | select(.kind == "personal" and .used_bytes > 0)] | length >= 1)
    and ([.storage.databases[]? | select(.scope == $org and .kind == "org" and .used_bytes > 0)] | length == 1)
    and (([.storage.databases[]?.used_bytes] | add // 0) == .storage.used_bytes)
  ' "$status_tmp" >/dev/null || {
    echo "multi-db assertion failed: expected paid status plus personal and org database rows" >&2
    rm -f "$status_tmp"
    exit 1
  }
  rm -f "$status_tmp"
  echo "multi-db status OK: personal+org rows and account total roll-up"
}

phase_assert_suspended() {
  need_bin
  echo "--- phase: assert-suspended ---"
  if [[ ! -f "$HOME_DIR/cloud_sync.json" ]]; then
    echo "missing $HOME_DIR/cloud_sync.json — run setup-paid first" >&2
    exit 2
  fi

  local status_tmp fix_text
  status_tmp=$(mktemp)
  subscription_status_json >"$status_tmp"
  jq '{
    plan,
    access_allowed,
    access_denied_reason,
    how_to_fix,
    storage: {
      used_bytes: .storage.used_bytes,
      quota_bytes: .storage.quota_bytes
    }
  }' "$status_tmp"
  jq -e '
    .plan == "suspended"
    and .access_allowed == false
    and (.access_denied_reason == "payment_failed_or_inactive")
    and ((.how_to_fix // []) | map(tostring) | join(" ") | contains("lastdb cloud fix-billing"))
    and ((.how_to_fix // []) | map(tostring) | join(" ") | contains("lastdb cloud status"))
  ' "$status_tmp" >/dev/null || {
    echo "suspended status assertion failed: expected plan=suspended, access_allowed=false, and fix-billing/status steps" >&2
    rm -f "$status_tmp"
    exit 1
  }
  fix_text=$(jq -r '(.how_to_fix // []) | join(" | ")' "$status_tmp")

  local api_key tmp http
  api_key=$(api_key_from_home)
  tmp=$(mktemp)
  http=$(curl -sS -o "$tmp" -w '%{http_code}' \
    -X POST "${BASE_URL}/api/sync/presign" \
    -H "Content-Type: application/json" \
    -H "X-API-Key: ${api_key}" \
    -d '{"action":"presign_log_upload","seq_numbers":[9001],"estimated_size_bytes":1}')
  if [[ "$http" != "403" ]]; then
    echo "suspended write assertion failed: expected HTTP 403, got HTTP $http" >&2
    cat "$tmp" >&2
    rm -f "$status_tmp" "$tmp"
    exit 1
  fi
  jq -e '
    .ok == false
    and .statusCode == 403
    and (.details.reason == "payment_failed_or_inactive")
    and (.details.plan == "suspended")
    and ((.details.how_to_fix // []) | map(tostring) | join(" ") | contains("lastdb cloud fix-billing"))
    and ((.details.how_to_fix // []) | map(tostring) | join(" ") | contains("lastdb cloud status"))
  ' "$tmp" >/dev/null || {
    echo "suspended write assertion failed: 403 body missing payment/fix details" >&2
    cat "$tmp" >&2
    rm -f "$status_tmp" "$tmp"
    exit 1
  }

  echo "suspended status OK: plan=suspended access_allowed=false"
  echo "suspended write gate OK: presign rejected with payment_failed_or_inactive"
  echo "fix steps: $fix_text"
  rm -f "$status_tmp" "$tmp"
}

phase_audit() {
  echo "--- phase: audit_storage ---"
  local api_key
  local extra_scopes=""
  api_key=$(jq -r '.api_key // empty' "$HOME_DIR/cloud_sync.json")
  if [[ -f "$HOME_DIR/dogfood-org-hash.txt" ]]; then
    extra_scopes=$(tr '\n' ' ' <"$HOME_DIR/dogfood-org-hash.txt")
  fi
  EXEMEM_API_KEY="$api_key" ENV="$ENV_NAME" EPSILON="$EPSILON" APPLY=0 STORAGE_EXTRA_SCOPES="$extra_scopes" \
    "$ROOT/scripts/agent/audit-storage-metering.sh"
}

case "$FROM_PHASE" in
  setup)
    phase_setup
    phase_status
    if [[ "${SKIP_UPLOAD:-0}" != "1" ]]; then phase_upload; fi
    if [[ "${SKIP_MULTIDB_ASSERT:-0}" != "1" && "${SKIP_UPLOAD:-0}" != "1" ]]; then phase_assert_multidb; fi
    if [[ "${SKIP_AUDIT:-0}" != "1" && "${SKIP_UPLOAD:-0}" != "1" ]]; then phase_audit; fi
    ;;
  status)
    phase_status
    if [[ "${SKIP_UPLOAD:-0}" != "1" ]]; then phase_upload; fi
    if [[ "${SKIP_MULTIDB_ASSERT:-0}" != "1" && "${SKIP_UPLOAD:-0}" != "1" ]]; then phase_assert_multidb; fi
    if [[ "${SKIP_AUDIT:-0}" != "1" && "${SKIP_UPLOAD:-0}" != "1" ]]; then phase_audit; fi
    ;;
  upload)
    phase_upload
    if [[ "${SKIP_MULTIDB_ASSERT:-0}" != "1" ]]; then phase_assert_multidb; fi
    if [[ "${SKIP_AUDIT:-0}" != "1" ]]; then phase_audit; fi
    ;;
  assert-multidb)
    phase_assert_multidb
    ;;
  assert-suspended)
    phase_assert_suspended
    ;;
  audit)
    phase_audit
    ;;
  *)
    echo "Unknown --from phase: $FROM_PHASE (setup|status|upload|assert-multidb|assert-suspended|audit)" >&2
    exit 1
    ;;
esac

echo
echo "=== DOGFOOD PASS ==="
echo "HOME_DIR=$HOME_DIR (throwaway; safe to rm -rf)"
echo "Record non-secret evidence (plan/access, personal+org database rows, audit drift) on North Star north-star-storage-metering-correctness."
exit 0
