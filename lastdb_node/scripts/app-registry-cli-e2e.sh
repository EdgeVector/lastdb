#!/usr/bin/env bash
# `lastdb app …` e2e on an EPHEMERAL node (never the primary Mini). Hermetic:
# a local mock stands in for the schema service + exemem auth service, so the
# script needs no network and no credentials.
#
# Proves the sandbox publish → schema registration → promote flow (design:
# brain `design-lastdb-app-registry`) plus the hard promote gate:
#
#   Phase A — covered:  injected compose resolution → `lastdb app check` passes
#                       (catalog components fetched from the mock).
#   Phase B — novel:    resolver says novel → `check --sync` fails; `lastdb app publish`
#                       still reserves a sandbox row; `lastdb app promote`
#                       is REJECTED by the novel-schema gate BEFORE calling
#                       /v1/apps/{id}/promote.
#   Phase C — register: `lastdb app register-schemas` claims the manifest
#                       schema and an authoritative resolve re-check passes.
#   Phase D — promote:  mock /v1/dev-cert + /v1/apps/{id}/promote →
#                       promote round-trips (200 live).
#   Phase E — live dev (optional): EXEMEM_DEV_API_KEY set → real
#                       publish/register/promote to the DEV registry +
#                       `lastdb app info` readback.
#
# Usage: ./lastdb_node/scripts/app-registry-cli-e2e.sh
#   LASTDBD=target/debug/lastdbd LASTDB_CLI=target/debug/lastdb (defaults)
set -euo pipefail

DAEMON="${LASTDBD:-target/debug/lastdbd}"
CLI="${LASTDB_CLI:-target/debug/lastdb}"
MOCK_PORT="${MOCK_PORT:-8971}"
MOCK_URL="http://127.0.0.1:$MOCK_PORT"
HASH_A="e2e0000000000000000000000000000000000000000000000000000000000001"
HASH_B="e2e0000000000000000000000000000000000000000000000000000000000002"
# The identity the mock catalog STORES for a registered schema — deliberately
# different from any locally computed hash, simulating the service folding the
# proposal into an expanded canonical (the 2026-07-17 live dogfood case).
FOLD_HASH="e2e00000000000000000000000000000000000000000000000000000000f01d0"

NODE=$(mktemp -d /tmp/app-registry-e2e.XXXXXX)
SOCK="$NODE/data/folddb.sock"
LOG="$NODE/lastdbd.log"
PID=""
MOCK_PID=""
cleanup() {
  [ -n "$PID" ] && kill "$PID" 2>/dev/null || true
  [ -n "$MOCK_PID" ] && kill "$MOCK_PID" 2>/dev/null || true
  if [ "${KEEP_E2E_DIR:-0}" = "1" ]; then echo "kept: $NODE"; else rm -rf "$NODE"; fi
}
trap cleanup EXIT

fail() { echo "FAIL: $1" >&2; exit 1; }

# ── Mock schema service + exemem auth service ──────────────────────────────
python3 - "$NODE" "$MOCK_PORT" <<'PY' >"$NODE/mock.log" 2>&1 &
import http.server, json, sys, os
node_dir, port = sys.argv[1], int(sys.argv[2])
hits_path = os.path.join(node_dir, "mock-hits")
registered_path = os.path.join(node_dir, "schema-registered")
install_source_path = os.path.join(node_dir, "install-source-url")
app_version_path = os.path.join(node_dir, "app-version")

def parse_semver(v):
    parts = v.split(".")
    if len(parts) != 3 or any(not p.isdigit() for p in parts):
        raise ValueError(v)
    return tuple(int(p) for p in parts)

def current_version():
    if os.path.exists(app_version_path):
        return open(app_version_path).read().strip()
    return None

def store_version(v):
    open(app_version_path, "w").write(v)

class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def _hit(self):
        with open(hits_path, "a") as f: f.write(self.path + "\n")
    def _json(self, code, obj):
        out = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)
    def do_GET(self):
        self._hit()
        known = {"e2e0000000000000000000000000000000000000000000000000000000000001",
                 "e2e0000000000000000000000000000000000000000000000000000000000002",
                 "e2e00000000000000000000000000000000000000000000000000000000f01d0"}
        if self.path == "/v1/apps/e2e-demo":
            source = "https://github.com/EdgeVector/e2e-demo"
            if os.path.exists(install_source_path):
                source = open(install_source_path).read().strip()
            version = current_version() or "0.0.0"
            self._json(200, {"app_id": "e2e-demo", "env": "dev",
                             "metadata": {"display_name": "E2E Demo",
                                          "description": "Ephemeral-node e2e exercise app for the lastdb app CLI.",
                                          "homepage_url": "https://example.com/e2e-demo"},
                             "uses": [], "owner_dev_pubkey": "mock",
                             "version": version,
                             "source": source, "artifact": None,
                             "registered_at": "2026-01-01T00:00:00Z",
                             "code_signature": None, "tier": "live",
                             "revoked": False})
        elif self.path.startswith("/v1/schema/") and self.path.rsplit("/", 1)[-1] in known:
            h = self.path.rsplit("/", 1)[-1]
            self._json(200, {"name": h, "descriptive_name": f"mock component {h[-1]}",
                             "schema_type": "Single",
                             "fields": ["customer_id", "name", "email"],
                             "identity_hash": h, "system": False})
        else:
            self._json(404, {})
    def do_POST(self):
        self._hit()
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length) or b"{}")
        if self.path == "/v1/dev-cert":
            if not self.headers.get("Authorization", "").startswith("Bearer em_"):
                return self._json(401, {"reason": "api_key_invalid"})
            cert = {"version": 1, "purpose": "dev_cert", "alg": "ES256",
                    "key_id": "mock", "dev_pubkey": body.get("dev_pubkey", ""),
                    "user_hash": "mock-user", "issued_at": "2026-01-01T00:00:00Z",
                    "expires_at": "2099-01-01T00:00:00Z", "env": "dev",
                    "authorized_publisher": True, "sig": "bW9jaw=="}
            self._json(200, {"cert": cert, "expires_at": cert["expires_at"]})
        elif self.path == "/v1/apps":
            if not (self.headers.get("X-Exemem-Dev-Cert") and self.headers.get("X-Signature")):
                return self._json(401, {"reason": "cert_invalid"})
            requested = body.get("version", "0.0.0")
            existing = current_version()
            if existing is not None and parse_semver(requested) <= parse_semver(existing):
                return self._json(409, {"reason": "non_monotonic_version",
                                        "current_version": existing,
                                        "requested_version": requested})
            store_version(requested)
            status = 201 if existing is None else 200
            self._json(status, {"app_id": body.get("app_id"), "env": "dev",
                             "status": "created" if existing is None else "updated",
                             "metadata": body.get("metadata"), "uses": body.get("uses", []),
                             "version": requested,
                             "source": body.get("source"), "artifact": body.get("artifact"),
                             "owner_dev_pubkey": "mock",
                             "registered_at": "2026-01-01T00:00:00Z",
                             "code_signature": None, "tier": "sandbox"})
        elif self.path == "/v1/schemas":
            if not (self.headers.get("X-Exemem-Dev-Cert") and self.headers.get("X-Signature")):
                return self._json(401, {"reason": "cert_invalid"})
            open(registered_path, "w").write("1")
            schema = dict(body.get("schema", {}))
            # Simulate reuse-maximization: the stored canonical differs from
            # the proposal's local identity.
            schema["name"] = "e2e00000000000000000000000000000000000000000000000000000000f01d0"
            schema["identity_hash"] = "e2e00000000000000000000000000000000000000000000000000000000f01d0"
            self._json(201, {"schema": schema, "mutation_mappers": {},
                             "replaced_schema": None, "system": False})
        elif self.path == "/v1/schemas/resolve":
            # Always novel: fresh canonicals are not semantically matched by
            # resolve (observed live 2026-07-17), so post-registration
            # coverage MUST come from the lockfile's exact catalog fetch.
            results = {}
            for proposal in body.get("proposals", []):
                results[proposal.get("descriptive_name", "")] = {"outcome": "novel"}
            self._json(200, {"registry_version": 1, "cache_stale": False, "results": results})
        elif self.path == "/v1/apps/e2e-demo/promote":
            if not (self.headers.get("X-Exemem-Dev-Cert") and self.headers.get("X-Signature")):
                return self._json(401, {"reason": "cert_invalid"})
            self._json(200, {"app_id": "e2e-demo", "env": "dev",
                             "metadata": {"display_name": "E2E Demo",
                                          "description": "Ephemeral-node e2e exercise app for the lastdb app CLI.",
                                          "homepage_url": "https://example.com/e2e-demo"},
                             "uses": [], "owner_dev_pubkey": "mock",
                             "version": current_version() or "0.0.0",
                             "source": "https://github.com/EdgeVector/e2e-demo",
                             "artifact": None,
                             "registered_at": "2026-01-01T00:00:00Z",
                             "code_signature": None, "tier": "live"})
        else:
            self._json(404, {})

http.server.HTTPServer(("127.0.0.1", port), H).serve_forever()
PY
MOCK_PID=$!
for _ in $(seq 1 50); do
  curl -fsS "$MOCK_URL/v1/schema/$HASH_A" >/dev/null 2>&1 && break; sleep 0.2
done
curl -fsS "$MOCK_URL/v1/schema/$HASH_A" >/dev/null 2>&1 || fail "mock never came up"

start_node() { # $1 = resolve-json path (empty = none)
  if [ -n "$PID" ]; then kill "$PID" 2>/dev/null || true; wait "$PID" 2>/dev/null || true; fi
  rm -f "$SOCK"
  if [ -n "$1" ]; then
    FOLD_SCHEMA_SERVICE_URL="$MOCK_URL" LASTDB_TEST_RESOLVE_JSON="$1" \
      "$DAEMON" --data-dir "$NODE" >>"$LOG" 2>&1 &
  else
    FOLD_SCHEMA_SERVICE_URL="$MOCK_URL" "$DAEMON" --data-dir "$NODE" >>"$LOG" 2>&1 &
  fi
  PID=$!
  for _ in $(seq 1 100); do [ -S "$SOCK" ] && return 0; sleep 0.2; done
  fail "ephemeral lastdbd socket never appeared (log: $LOG)"
}

# ── Fixtures ────────────────────────────────────────────────────────────────
MANIFEST="$NODE/lastdb-app.json"
cat >"$MANIFEST" <<EOF
{
  "app_id": "e2e-demo",
  "metadata": {
    "display_name": "E2E Demo",
    "description": "Ephemeral-node e2e exercise app for the lastdb app CLI.",
    "homepage_url": "https://example.com/e2e-demo"
  },
  "version": "0.1.0",
  "source": "https://github.com/EdgeVector/e2e-demo",
  "run": {
    "runtime": "sh",
    "entrypoint": "app-run.sh",
    "args": ["manifest-arg"]
  },
  "schemas": [
    {
      "name": "CustomerWithAddress",
      "descriptive_name": "Customer with mailing address",
      "schema_type": "Single",
      "fields": ["customer_id", "name", "email", "line1", "city", "postal_code", "country"],
      "field_descriptions": {
        "customer_id": "unique identifier of the customer",
        "name": "the customer's full name",
        "email": "the customer's email address",
        "line1": "first line of the mailing address",
        "city": "mailing address city",
        "postal_code": "mailing address postal code",
        "country": "mailing address country"
      }
    }
  ]
}
EOF

RESOLVE_COMPOSE="$NODE/resolve-compose.json"
cat >"$RESOLVE_COMPOSE" <<EOF
{
  "outcome": "candidate_equivalent",
  "candidate_shared_schema_hashes": ["$HASH_A", "$HASH_B"],
  "candidates": [],
  "confidence": 0.99
}
EOF

RESOLVE_NOVEL="$NODE/resolve-novel.json"
cat >"$RESOLVE_NOVEL" <<EOF
{ "outcome": "novel", "candidates": [], "confidence": 0.0 }
EOF

export LASTDB_HOME="$NODE"
# Hermetic: the CLI's catalog cross-check (app check) must hit the mock too.
export FOLD_SCHEMA_SERVICE_URL="$MOCK_URL"
"$CLI" --data-dir "$NODE" app dev-init --key-file "$NODE/dev-signing.key" >/dev/null

# ── Phase A: covered check ─────────────────────────────────────────────────
start_node "$RESOLVE_COMPOSE"
"$CLI" --data-dir "$NODE" app check --manifest "$MANIFEST" --json \
  >"$NODE/check-a.json" 2>&1 \
  || { cat "$NODE/check-a.json" >&2; fail "phase A: check failed"; }
grep -q '"resolution": "compose"' "$NODE/check-a.json" \
  || fail "phase A: expected compose coverage (see $NODE/check-a.json)"
echo "PASS phase A: check reports catalog coverage (compose)"

# ── Phase B: novel → publish reserves sandbox; promote gate rejects ────────
start_node "$RESOLVE_NOVEL"
# `app check` is read-only by default (intent `check`) and reports a novel
# schema as would-register. Phase B exercises the writing sync path, so it
# passes --sync (intent `catalog_sync`) as `app check` did before.
if "$CLI" --data-dir "$NODE" app check --sync --manifest "$MANIFEST" >"$NODE/check-b.out" 2>&1; then
  fail "phase B: check unexpectedly passed on a novel schema"
fi
grep -q "NOVEL" "$NODE/check-b.out" || fail "phase B: check output missing NOVEL marker"

rm -f "$NODE/mock-hits"
"$CLI" --data-dir "$NODE" app publish --manifest "$MANIFEST" \
  --schema-url "$MOCK_URL" --api-url "$MOCK_URL" \
  --api-key em_mock --key-file "$NODE/dev-signing.key" >"$NODE/publish-b.out" 2>&1 \
  || { cat "$NODE/publish-b.out" >&2; fail "phase B: sandbox publish failed"; }
grep -q "published (created)" "$NODE/publish-b.out" || fail "phase B: expected sandbox publish"
grep -q '"version": "0.1.0"' "$NODE/publish-b.out" || fail "phase B: publish output missing version"
grep -q "/v1/apps" "$NODE/mock-hits" || fail "phase B: app reservation never happened"

if "$CLI" --data-dir "$NODE" app publish --manifest "$MANIFEST" \
    --schema-url "$MOCK_URL" --api-url "$MOCK_URL" \
    --api-key em_mock --key-file "$NODE/dev-signing.key" >"$NODE/publish-b-repeat.out" 2>&1; then
  fail "phase B: re-publishing the same version unexpectedly succeeded"
fi
grep -q "non_monotonic_version" "$NODE/publish-b-repeat.out" \
  || fail "phase B: same-version publish did not return the named monotonic error"

rm -f "$NODE/mock-hits"
if "$CLI" --data-dir "$NODE" app promote --manifest "$MANIFEST" \
    --schema-url "$MOCK_URL" --api-url "$MOCK_URL" \
    --api-key em_mock --key-file "$NODE/dev-signing.key" >"$NODE/promote-b.out" 2>&1; then
  fail "phase B: promote unexpectedly succeeded with a novel schema"
fi
grep -q "promote rejected: novel schemas present" "$NODE/promote-b.out" \
  || fail "phase B: promote failure is not the novel-schema gate (see $NODE/promote-b.out)"
if [ -s "$NODE/mock-hits" ] && grep -q "/v1/apps/e2e-demo/promote" "$NODE/mock-hits"; then
  fail "phase B: gate leaked a promote call before rejecting"
fi
echo "PASS phase B: novel schema can reserve sandbox, but promote is gated"

# ── Phase C: register still-novel schema ──────────────────────────────────
"$CLI" --data-dir "$NODE" app register-schemas --manifest "$MANIFEST" \
  --schema-url "$MOCK_URL" --api-url "$MOCK_URL" \
  --api-key em_mock --key-file "$NODE/dev-signing.key" >"$NODE/register-c.out" 2>&1 \
  || { cat "$NODE/register-c.out" >&2; fail "phase C: register-schemas failed"; }
grep -q "registered:" "$NODE/register-c.out" || fail "phase C: expected registered hash"
grep -q "folded into an expanded canonical" "$NODE/register-c.out" \
  || fail "phase C: expected folded-canonical annotation (stored != local)"
grep -q "ready to publish" "$NODE/register-c.out" || fail "phase C: expected catalog membership re-check success"
grep -q "/v1/schemas" "$NODE/mock-hits" || fail "phase C: schema registration never happened"
[ -f "$MANIFEST.lock.json" ] || fail "phase C: lockfile not written"
grep -q "$FOLD_HASH" "$MANIFEST.lock.json" || fail "phase C: lockfile missing stored catalog identity"
grep -q "/v1/schema/$FOLD_HASH" "$NODE/mock-hits" || fail "phase C: stored identity never verified against the catalog"
echo "PASS phase C: register-schemas records stored canonical in lockfile and verifies it"

# ── Phase D: promote round-trip ────────────────────────────────────────────
rm -f "$NODE/mock-hits"
"$CLI" --data-dir "$NODE" app promote --manifest "$MANIFEST" \
  --schema-url "$MOCK_URL" --api-url "$MOCK_URL" \
  --api-key em_mock --key-file "$NODE/dev-signing.key" >"$NODE/promote-d.out" 2>&1 \
  || { cat "$NODE/promote-d.out" >&2; fail "phase D: promote failed"; }
grep -q "promoted:" "$NODE/promote-d.out" || fail "phase D: expected promoted output"
grep -q '"tier": "live"' "$NODE/promote-d.out" || fail "phase D: expected live tier"
grep -q "/v1/dev-cert" "$NODE/mock-hits" || fail "phase D: dev-cert mint never happened"
grep -q "/v1/apps/e2e-demo/promote" "$NODE/mock-hits" || fail "phase D: app promotion never happened"
echo "PASS phase D: promote round-trip (dev-cert mint + signed /v1/apps/{id}/promote)"

# ── Phase D2: source-first install from registry record ────────────────────
INSTALL_SOURCE="$NODE/install-source"
mkdir -p "$INSTALL_SOURCE"
git -C "$INSTALL_SOURCE" init -q
git -C "$INSTALL_SOURCE" config user.email "app-registry-e2e@example.invalid"
git -C "$INSTALL_SOURCE" config user.name "App Registry E2E"
cp "$MANIFEST" "$INSTALL_SOURCE/lastdb-app.json"
echo "console.log('e2e demo')" >"$INSTALL_SOURCE/index.mjs"
cat >"$INSTALL_SOURCE/app-run.sh" <<'EOF'
#!/usr/bin/env sh
set -eu
printf 'app_id=%s\n' "$LASTDB_APP_ID"
printf 'socket=%s\n' "$LASTDB_SOCKET"
printf 'data_dir=%s\n' "$LASTDB_DATA_DIR"
printf 'arg1=%s\n' "$1"
printf 'arg2=%s\n' "$2"
test "$LASTDB_APP_ID" = "e2e-demo"
test -n "$LASTDB_SOCKET"
test -n "$LASTDB_DATA_DIR"
test "$1" = "manifest-arg"
test "$2" = "--probe"
EOF
git -C "$INSTALL_SOURCE" add lastdb-app.json index.mjs app-run.sh
git -C "$INSTALL_SOURCE" commit -q -m "seed e2e app source"
printf '%s\n' "$INSTALL_SOURCE" >"$NODE/install-source-url"
rm -f "$NODE/mock-hits"
"$CLI" --data-dir "$NODE" app install e2e-demo \
  --schema-url "$MOCK_URL" --dir "$NODE/installed/e2e-demo" --json \
  >"$NODE/install-d2.json" 2>&1 \
  || { cat "$NODE/install-d2.json" >&2; fail "phase D2: app install failed"; }
grep -q '"app_id": "e2e-demo"' "$NODE/install-d2.json" || fail "phase D2: install output missing app id"
grep -q '"version": "0.1.0"' "$NODE/install-d2.json" || fail "phase D2: install output missing version"
grep -q '"tier": "live"' "$NODE/install-d2.json" || fail "phase D2: install output missing live tier"
grep -q "/v1/apps/e2e-demo" "$NODE/mock-hits" || fail "phase D2: app lookup never happened"
[ -f "$NODE/installed/e2e-demo/source/lastdb-app.json" ] || fail "phase D2: source checkout missing manifest"
[ -f "$NODE/installed/e2e-demo/lastdb-app-install.json" ] || fail "phase D2: install receipt missing"
grep -q '"owner_dev_pubkey": "mock"' "$NODE/installed/e2e-demo/lastdb-app-install.json" \
  || fail "phase D2: install receipt missing publisher key"
grep -q '"version": "0.1.0"' "$NODE/installed/e2e-demo/lastdb-app-install.json" \
  || fail "phase D2: install receipt missing version"
"$CLI" --data-dir "$NODE" app run e2e-demo \
  --dir "$NODE/installed/e2e-demo" -- --probe >"$NODE/run-d2.out" 2>&1 \
  || { cat "$NODE/run-d2.out" >&2; fail "phase D2: app run failed"; }
grep -q "app_id=e2e-demo" "$NODE/run-d2.out" || fail "phase D2: run output missing app id"
grep -q "socket=$SOCK" "$NODE/run-d2.out" || fail "phase D2: run output missing socket"
grep -q "arg1=manifest-arg" "$NODE/run-d2.out" || fail "phase D2: run output missing manifest arg"
grep -q "arg2=--probe" "$NODE/run-d2.out" || fail "phase D2: run output missing cli arg"
echo "PASS phase D2: app install clones source, writes local receipt, and runs manifest entrypoint"

# ── Phase D3: monotonic publish + local upgrade ─────────────────────────────
perl -0pi -e 's/"version": "0\.1\.0"/"version": "0.2.0"/' "$MANIFEST"
cp "$MANIFEST" "$INSTALL_SOURCE/lastdb-app.json"
git -C "$INSTALL_SOURCE" add lastdb-app.json
git -C "$INSTALL_SOURCE" commit -q -m "bump e2e app version"
"$CLI" --data-dir "$NODE" app publish --manifest "$MANIFEST" \
  --schema-url "$MOCK_URL" --api-url "$MOCK_URL" \
  --api-key em_mock --key-file "$NODE/dev-signing.key" >"$NODE/publish-d3.out" 2>&1 \
  || { cat "$NODE/publish-d3.out" >&2; fail "phase D3: newer publish failed"; }
grep -q "published (updated)" "$NODE/publish-d3.out" \
  || fail "phase D3: expected updated publish output"
grep -q '"version": "0.2.0"' "$NODE/publish-d3.out" || fail "phase D3: newer publish output missing version"
"$CLI" --data-dir "$NODE" app upgrade e2e-demo \
  --schema-url "$MOCK_URL" --dir "$NODE/installed/e2e-demo" --json \
  >"$NODE/upgrade-d3.json" 2>&1 \
  || { cat "$NODE/upgrade-d3.json" >&2; fail "phase D3: app upgrade failed"; }
grep -q '"upgraded": true' "$NODE/upgrade-d3.json" || fail "phase D3: upgrade did not run"
grep -q '"installed_version": "0.1.0"' "$NODE/upgrade-d3.json" || fail "phase D3: upgrade missing old version"
grep -q '"registry_version": "0.2.0"' "$NODE/upgrade-d3.json" || fail "phase D3: upgrade missing new version"
grep -q '"version": "0.2.0"' "$NODE/installed/e2e-demo/lastdb-app-install.json" \
  || fail "phase D3: upgraded receipt missing new version"
echo "PASS phase D3: same-version publish rejects, newer publish accepts, upgrade installs newer source"

# ── Phase E: live DEV registry (optional) ──────────────────────────────────
if [ -n "${EXEMEM_DEV_API_KEY:-}" ]; then
  "$CLI" --data-dir "$NODE" app publish --manifest "$MANIFEST" --env dev \
    --key-file "$NODE/dev-signing.key" >"$NODE/publish-d.out" 2>&1 \
    || { cat "$NODE/publish-d.out" >&2; fail "phase E: live dev publish failed"; }
  "$CLI" --data-dir "$NODE" app register-schemas --manifest "$MANIFEST" --env dev \
    --key-file "$NODE/dev-signing.key" >"$NODE/register-d.out" 2>&1 \
    || { cat "$NODE/register-d.out" >&2; fail "phase E: live dev register-schemas failed"; }
  "$CLI" --data-dir "$NODE" app promote --manifest "$MANIFEST" --env dev \
    --key-file "$NODE/dev-signing.key" >"$NODE/promote-d.out" 2>&1 \
    || { cat "$NODE/promote-d.out" >&2; fail "phase E: live dev promote failed"; }
  "$CLI" app info e2e-demo --env dev >"$NODE/info-d.out" 2>&1 \
    || fail "phase E: app info readback failed"
  grep -q '"app_id": "e2e-demo"' "$NODE/info-d.out" || fail "phase E: info missing app record"
  grep -q '"tier": "live"' "$NODE/info-d.out" || fail "phase E: info missing live tier"
  grep -q '"source": "https://github.com/EdgeVector/e2e-demo"' "$NODE/info-d.out" || fail "phase E: info missing source"
  echo "PASS phase E: live DEV publish + register + promote + info readback"
else
  echo "SKIP phase E: EXEMEM_DEV_API_KEY not set (live dev publish/register/promote)"
fi

echo "PASS app-registry-cli-e2e ($("$CLI" --version 2>/dev/null || true))"
