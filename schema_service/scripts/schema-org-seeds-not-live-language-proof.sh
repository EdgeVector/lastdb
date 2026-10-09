#!/usr/bin/env bash
# Ephemeral proof: Schema.org types/fields are not live language.
# Never touches ~/.lastdb or prod Lambda.
set -euo pipefail

ROOT="$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)"
DB="$(mktemp -d /tmp/schema-org-not-live-XXXXXX)"
PORT="${SCHEMA_ORG_PROOF_PORT:-19117}"
BIN="${ROOT}/target/debug/schema_service"
PID=""

cleanup() {
  if [ -n "${PID}" ] && kill -0 "${PID}" 2>/dev/null; then
    kill "${PID}" 2>/dev/null || true
    wait "${PID}" 2>/dev/null || true
  fi
  rm -rf "${DB}"
}
trap cleanup EXIT

fail() {
  printf 'FAIL %s\n' "$*" >&2
  exit 1
}

case "${DB}" in
  "${HOME}/.lastdb"|"${HOME}/.lastdb"/*|"${HOME}/.folddb"|"${HOME}/.folddb"/*)
    fail "refusing to use primary LastDB home"
    ;;
esac

if [ ! -x "${BIN}" ]; then
  cargo build -p schema_service_server_http --bin schema_service --manifest-path "${ROOT}/Cargo.toml"
fi

"${BIN}" --port "${PORT}" --db-path "${DB}" >/tmp/schema-org-not-live-server.log 2>&1 &
PID=$!

ok=0
for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
  if curl -sf "http://127.0.0.1:${PORT}/v1/health" >/dev/null; then
    ok=1
    break
  fi
  if ! kill -0 "${PID}" 2>/dev/null; then
    fail "server exited early; log: $(tail -20 /tmp/schema-org-not-live-server.log)"
  fi
  # portable short wait without sleep-looping the agent contract — this is a
  # local health poll with a hard cap, not an unattended CI watcher.
  perl -e 'select(undef,undef,undef,0.25)'
done
[ "${ok}" = 1 ] || fail "server never became healthy"

curl -sS "http://127.0.0.1:${PORT}/v1/schemas/available" >/tmp/schema-org-not-live-avail.json
curl -sS "http://127.0.0.1:${PORT}/v1/snapshot" >/tmp/schema-org-not-live-snap.json

jq -e '
  .schemas as $s
  | ($s | map(select((.descriptive_name // "") == "Person" or (.schema.descriptive_name // "") == "Person")) | length) == 0
    and ($s | map(select((.descriptive_name // .schema.descriptive_name // "") == "Product")) | length) == 0
    and ($s | map(select((.descriptive_name // .schema.descriptive_name // "") == "CreativeWork")) | length) == 0
    and (
      [$s[] | select((.source // .schema.source) == "starter_seed")
            | (.owner_app_id // .schema.owner_app_id // "")]
      | all(. == "templates")
    )
' /tmp/schema-org-not-live-avail.json >/dev/null \
  || fail "available still lists Schema.org types or non-template starter_seed"

jq -e '
  .canonical_fields.accepted_payment_method == null
  and .canonical_fields.health_plan_network_id == null
  and .canonical_fields.address_state != null
  and .canonical_fields.state.classification.data_domain == "general"
  and .canonical_fields.state.classification.sensitivity_level == 1
  and (.canonical_fields.state.description | test("lifecycle"))
  and .canonical_fields.identity_hash.classification.data_domain == "general"
  and .canonical_fields.identity_hash.classification.sensitivity_level == 0
  and .canonical_fields.display_name.classification.sensitivity_level == 1
  and .canonical_fields.subject.classification.data_domain == "general"
  and .canonical_fields.subject.classification.sensitivity_level == 1
' /tmp/schema-org-not-live-snap.json >/dev/null \
  || fail "snapshot field registry still has Schema.org-only names or unreminted builtins"

jq -e '
  [.schemas[] | select((.source // "") == "system_seed")] | length >= 1
' /tmp/schema-org-not-live-snap.json >/dev/null \
  || fail "system_seed schemas missing from snapshot"

printf 'PASS schema.org types and fields are not live language; builtins reminted; templates/system_seed remain\n'
exit 0
