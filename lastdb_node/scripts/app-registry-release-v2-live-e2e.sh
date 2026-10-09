#!/usr/bin/env bash
# Exemem app registry — the nine proofs, end to end, on the live surfaces.
#
# What "live" means here: the REAL schema service HTTP binary serving the
# real `/v1` + `/v2` route table, the REAL `lastdb app …` CLI, and real
# Ed25519 / ES256 verification. No registry route and no verification step
# is stubbed.
#
# Two things stand in.
#
#   1. `POST /v1/dev-cert`. The production exemem auth service signs
#      DevCerts with a KMS-held ES256 root key, and a local run has no KMS.
#      The script generates its own P-256 root, configures it into the
#      schema service through `APP_IDENTITY_ROOT_PUBKEYS`, and mints certs
#      with it — so the schema service still verifies every cert and every
#      envelope for real.
#   2. The artifact transport. Production reads `artifact_url` over HTTPS
#      from R2. This script publishes a `file://` URL, which
#      `fetch_artifact` reads from disk, so the run needs no object store.
#
# Neither stand-in weakens a proof. The digest check and the signature check
# of proof 4 run on the artifact bytes after the fetch returns, so they are
# the same comparison on either transport.
#
# The nine proofs:
#
#   1  No schema registration during release or install
#   2  No CURRENT in development
#   3  No CURRENT from an old release process
#   4  A digest fault or a signature fault stops activation
#   5  Generation conflicts fail
#   6  Anonymous reads work
#   7  Writes need a DevCert
#   8  desired = installed = active = observed after success
#   9  Forced drift restores the prior verified release
#  10  The recurring check cycle restores on drift without a new invocation
#
# Usage:
#   ./lastdb_node/scripts/app-registry-release-v2-live-e2e.sh [--live-dev]
#
# `--live-dev` is accepted and does not change what runs: the default run is
# already against live binaries. The flag exists because the design's graph
# proof command passes it.
#
# Env:
#   LASTDB_CLI       path to the `lastdb` binary   (default target/debug/lastdb)
#   SCHEMA_SERVICE   path to the schema service    (default target/debug/schema_service)
#   KEEP_E2E_DIR=1   keep the scratch directory for inspection
set -euo pipefail

CLI="${LASTDB_CLI:-target/debug/lastdb}"
SERVICE="${SCHEMA_SERVICE:-target/debug/schema_service}"
APP_ID="release-e2e-demo"
APP_UUID="0f6c2d5e-0000-4000-8000-00000000e2e0"
CHANNEL="stable"
SOURCE_COMMIT="0123456789abcdef0123456789abcdef01234567"

for arg in "$@"; do
  case "$arg" in
    --live-dev) ;;
    *) echo "unknown argument: $arg" >&2; exit 2 ;;
  esac
done

WORK=$(mktemp -d /tmp/app-registry-release-e2e.XXXXXX)
HOST_TRACK="$WORK/host-track"
KEY_FILE="$WORK/dev-signing.key"
MANIFEST="$WORK/app.json"
SERVICE_PID=""
AUTH_PID=""
OBSERVER_ONE_PID=""
OBSERVER_TWO_PID=""
PASSED=0

cleanup() {
  for pid in "$OBSERVER_ONE_PID" "$OBSERVER_TWO_PID" "$AUTH_PID" "$SERVICE_PID"; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  done
  if [ "${KEEP_E2E_DIR:-0}" = "1" ]; then echo "kept: $WORK"; else rm -rf "$WORK"; fi
}
trap cleanup EXIT

fail() { echo "FAIL: $1" >&2; exit 1; }
prove() { PASSED=$((PASSED + 1)); echo "  PROOF $1 PASSED — $2"; }
step() { echo; echo "── $* ─────────────────────────────────────────"; }

free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }

[ -x "$CLI" ] || fail "no lastdb CLI at $CLI (cargo build -p lastdb_node --bin lastdb)"
[ -x "$SERVICE" ] || fail "no schema service at $SERVICE (cargo build -p schema_service_server_http)"
python3 -c 'import cryptography' 2>/dev/null || fail "python3 needs the 'cryptography' package to mint DevCerts"

SCHEMA_PORT=$(free_port)
AUTH_PORT=$(free_port)
SCHEMA_URL="http://127.0.0.1:$SCHEMA_PORT"
AUTH_URL="http://127.0.0.1:$AUTH_PORT"

# ── The crypto helper: JCS, the ES256 root, DevCert minting, and Ed25519 ──
# envelope signing. The same primitives the product uses, so what this
# script signs is what the schema service verifies.
cat >"$WORK/devcert.py" <<'PY'
import base64, datetime, hashlib, json, os, sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, ed25519
from cryptography.hazmat.primitives.asymmetric.utils import Prehashed

WORK = os.environ["PROOF_WORK"]
ROOT_KEY_PATH = os.path.join(WORK, "root-p256.pem")


def jcs(value):
    """RFC 8785 for the value shapes used here (objects, strings, bools, ints)."""
    return json.dumps(value, separators=(",", ":"), sort_keys=True,
                      ensure_ascii=False).encode("utf-8")


def root_key():
    if os.path.exists(ROOT_KEY_PATH):
        with open(ROOT_KEY_PATH, "rb") as f:
            return serialization.load_pem_private_key(f.read(), password=None)
    key = ec.generate_private_key(ec.SECP256R1())
    with open(ROOT_KEY_PATH, "wb") as f:
        f.write(key.private_bytes(serialization.Encoding.PEM,
                                  serialization.PrivateFormat.PKCS8,
                                  serialization.NoEncryption()))
    return key


def root_spki_der(key):
    return key.public_key().public_bytes(serialization.Encoding.DER,
                                         serialization.PublicFormat.SubjectPublicKeyInfo)


def mint_cert(dev_pubkey_b64):
    key = root_key()
    der = root_spki_der(key)
    now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
    cert = {
        "version": 1,
        "purpose": "dev_cert",
        "alg": "ES256",
        "key_id": hashlib.sha256(der).hexdigest(),
        "dev_pubkey": dev_pubkey_b64,
        "user_hash": hashlib.sha256(b"release-e2e-developer").hexdigest(),
        "issued_at": now.isoformat().replace("+00:00", "Z"),
        "expires_at": (now + datetime.timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
        "env": "dev",
        "authorized_publisher": True,
    }
    digest = hashlib.sha256(jcs(cert)).digest()
    cert["sig"] = base64.b64encode(
        key.sign(digest, ec.ECDSA(Prehashed(hashes.SHA256())))
    ).decode()
    return cert


def dev_key():
    with open(os.path.join(WORK, "dev-signing.key")) as f:
        return ed25519.Ed25519PrivateKey.from_private_bytes(base64.b64decode(f.read().strip()))


def dev_pubkey_b64(key):
    return base64.b64encode(key.public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw)).decode()


def sign_envelope(purpose, payload):
    key = dev_key()
    raw_pub = key.public_key().public_bytes(serialization.Encoding.Raw,
                                            serialization.PublicFormat.Raw)
    now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
    envelope = {
        "version": 1,
        "purpose": purpose,
        "alg": "Ed25519",
        "key_id": hashlib.sha256(raw_pub).hexdigest(),
        "issued_at": now.isoformat().replace("+00:00", "Z"),
        "env": "dev",
        "payload_hash": hashlib.sha256(jcs(payload)).hexdigest(),
    }
    envelope["sig"] = base64.b64encode(key.sign(jcs(envelope))).decode()
    return base64.b64encode(jcs(envelope)).decode()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_POST(self):
        if self.path != "/v1/dev-cert":
            return self._json(404, {"reason": "not_found"})
        if not self.headers.get("Authorization", "").startswith("Bearer em_"):
            return self._json(401, {"reason": "api_key_invalid"})
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        return self._json(200, {"cert": mint_cert(body["dev_pubkey"])})

    def _json(self, code, obj):
        out = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)


def main():
    mode = sys.argv[1]
    if mode == "root-pubkey":
        print(base64.b64encode(root_spki_der(root_key())).decode())
    elif mode == "serve":
        HTTPServer(("127.0.0.1", int(sys.argv[2])), Handler).serve_forever()
    elif mode == "signed-write":
        # A hand-built, correctly SIGNED `/v2` write. Used to publish a
        # release whose manifest carries a corrupted artifact signature —
        # the CLI signs artifacts correctly, so proving the host's
        # signature check needs a publisher that does not.
        import urllib.request
        method, url, purpose = sys.argv[2], sys.argv[3], sys.argv[4]
        payload = json.load(sys.stdin)
        request = urllib.request.Request(
            url, data=json.dumps(payload).encode(), method=method,
            headers={
                "Content-Type": "application/json",
                "X-Exemem-Dev-Cert": base64.b64encode(
                    jcs(mint_cert(dev_pubkey_b64(dev_key())))).decode(),
                "X-Signature": sign_envelope(purpose, payload),
            })
        try:
            with urllib.request.urlopen(request) as response:
                print(response.read().decode())
        except urllib.error.HTTPError as e:
            print(e.read().decode())
            sys.exit(1)
    else:
        raise SystemExit(f"unknown mode {mode}")


main()
PY

export PROOF_WORK="$WORK"
ROOT_PUBKEY=$(python3 "$WORK/devcert.py" root-pubkey)

step "start the real schema service and the DevCert minter"
python3 "$WORK/devcert.py" serve "$AUTH_PORT" >"$WORK/auth.log" 2>&1 &
AUTH_PID=$!
APP_IDENTITY_ROOT_PUBKEYS="$ROOT_PUBKEY" ENVIRONMENT=dev OBS_FILE_PATH="$WORK/obs.jsonl" \
  "$SERVICE" --port "$SCHEMA_PORT" --db-path "$WORK/registry" >"$WORK/service.log" 2>&1 &
SERVICE_PID=$!

for _ in $(seq 1 120); do
  if curl -fsS "$SCHEMA_URL/v1/health" >/dev/null 2>&1; then break; fi
  sleep 0.5
done
curl -fsS "$SCHEMA_URL/v1/health" >/dev/null || fail "schema service did not come up (see $WORK/service.log)"
curl -fsS -X POST "$AUTH_URL/v1/dev-cert" -H 'Authorization: Bearer em_probe' \
  -H 'Content-Type: application/json' -d '{"dev_pubkey":"AAAA"}' >/dev/null \
  || fail "DevCert minter did not come up (see $WORK/auth.log)"

# ── Development: declare a schema, resolve it, lock the identity ──────────
step "development — resolve a schema and lock its identity"
"$CLI" app dev-init --key-file "$KEY_FILE" >"$WORK/dev-init.log"
DEV_PUBKEY=$(grep '^dev_pubkey:' "$WORK/dev-init.log" | awk '{print $2}')
[ -n "$DEV_PUBKEY" ] || fail "dev-init produced no pubkey"

cat >"$WORK/schema.json" <<JSON
{
  "schema": {
    "name": "ReleaseE2ENote",
    "schema_type": "Single",
    "fields": {
      "note_id": {"field_type": "Single", "writable": true},
      "body": {"field_type": "Single", "writable": true}
    },
    "descriptive_name": "release e2e note",
    "field_descriptions": {
      "note_id": "The identifier of one note.",
      "body": "The text of one note."
    },
    "purpose_statement": "Notes written by the release registry end-to-end proof."
  },
  "mutation_mappers": {}
}
JSON
SCHEMA_RESPONSE=$(curl -fsS -X POST "$SCHEMA_URL/v1/schemas" \
  -H 'Content-Type: application/json' --data @"$WORK/schema.json") \
  || fail "development schema registration failed"
SCHEMA_IDENTITY=$(printf '%s' "$SCHEMA_RESPONSE" | python3 -c 'import json,sys; print(json.load(sys.stdin)["schema"]["identity_hash"])')
[ -n "$SCHEMA_IDENTITY" ] || fail "schema registration returned no identity_hash"
echo "  locked schema identity: $SCHEMA_IDENTITY"

cat >"$MANIFEST" <<JSON
{
  "app_id": "$APP_ID",
  "version": "1.0.0",
  "metadata": {
    "display_name": "Release E2E Demo",
    "description": "Exercises the Exemem app registry release, channel, and install path.",
    "homepage_url": "https://example.com/release-e2e-demo"
  },
  "uses": [],
  "source": "https://example.com/release-e2e-demo.git",
  "schemas": [{"name": "$APP_ID/Note"}]
}
JSON
# The lockfile the release copies from. This is the existing app lockfile
# shape — the release adds no field to it.
printf '{\n  "schemas": {\n    "%s/Note": "%s"\n  }\n}\n' "$APP_ID" "$SCHEMA_IDENTITY" \
  >"$MANIFEST.lock.json"

step "reserve the app namespace (POST /v1/apps, real DevCert)"
EXEMEM_DEV_API_KEY=em_release_e2e "$CLI" app publish --manifest "$MANIFEST" \
  --schema-url "$SCHEMA_URL" --api-url "$AUTH_URL" --key-file "$KEY_FILE" \
  >"$WORK/publish.log" 2>&1 || { cat "$WORK/publish.log"; fail "app namespace reservation failed"; }

schema_writes() {
  curl -fsS "$SCHEMA_URL/v1/health" | python3 -c 'import json,sys; print(json.load(sys.stdin)["schema_writes"])'
}
WRITES_BEFORE_RELEASE=$(schema_writes)
echo "  catalog write count before the release: $WRITES_BEFORE_RELEASE"

# ── Build a signed artifact ───────────────────────────────────────────────
step "build and sign the release artifact"
mkdir -p "$WORK/payload"
echo '#!/bin/sh' >"$WORK/payload/probe.sh"
echo 'exit 0' >>"$WORK/payload/probe.sh"
echo 'release one' >"$WORK/payload/VERSION"
tar -czf "$WORK/artifact-1.tar.gz" -C "$WORK/payload" .
echo 'release two' >"$WORK/payload/VERSION"
tar -czf "$WORK/artifact-2.tar.gz" -C "$WORK/payload" .
echo 'release three' >"$WORK/payload/VERSION"
tar -czf "$WORK/artifact-3.tar.gz" -C "$WORK/payload" .

publish_release() {
  EXEMEM_DEV_API_KEY=em_release_e2e "$CLI" app release-publish \
    --manifest "$MANIFEST" --app-uuid "$APP_UUID" --source-commit "$SOURCE_COMMIT" \
    --artifact "$1" --artifact-url "file://$1" \
    --schema-url "$SCHEMA_URL" --api-url "$AUTH_URL" --key-file "$KEY_FILE" --json \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["release_id"])'
}

set_channel() {
  EXEMEM_DEV_API_KEY=em_release_e2e "$CLI" app release-channel "$APP_ID" \
    --channel "$CHANNEL" --release-id "$1" ${2:+--generation "$2"} \
    --schema-url "$SCHEMA_URL" --api-url "$AUTH_URL" --key-file "$KEY_FILE" --json
}

RELEASE_1=$(publish_release "$WORK/artifact-1.tar.gz")
[ ${#RELEASE_1} -eq 64 ] || fail "release 1 id is not a sha256 hex value: $RELEASE_1"
echo "  release 1: $RELEASE_1"

# A release that references an unresolved schema must fail at publish.
cp "$MANIFEST.lock.json" "$WORK/lock.backup"
printf '{\n  "schemas": {\n    "%s/Note": "%s"\n  }\n}\n' "$APP_ID" "$(printf 'f%.0s' $(seq 64))" \
  >"$MANIFEST.lock.json"
if publish_release "$WORK/artifact-1.tar.gz" >/dev/null 2>&1; then
  fail "a release referencing an unresolved schema must not publish"
fi
cp "$WORK/lock.backup" "$MANIFEST.lock.json"
echo "  a release with an unresolved schema identity is refused"

# ── PROOF 5: generation conflicts fail ────────────────────────────────────
step "PROOF 5 — generation conflicts fail"
set_channel "$RELEASE_1" 0 >"$WORK/channel-1.json" || fail "the first channel write must succeed"
GENERATION=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["generation"])' "$WORK/channel-1.json")
[ "$GENERATION" = "1" ] || fail "the first channel write must store generation 1, got $GENERATION"
if set_channel "$RELEASE_1" 0 >"$WORK/channel-conflict.json" 2>&1; then
  fail "a second write at the same generation must be refused"
fi
grep -q 'generation_conflict' "$WORK/channel-conflict.json" \
  || { cat "$WORK/channel-conflict.json"; fail "the refusal must be a generation_conflict"; }
prove 5 "two writes at the same generation: the second is a conflict"

# ── PROOF 6: anonymous reads work ─────────────────────────────────────────
step "PROOF 6 — anonymous reads work"
for path in "/v2/apps/$APP_ID" "/v2/releases/$RELEASE_1" "/v2/apps/$APP_ID/channels/$CHANNEL"; do
  code=$(curl -s -o /dev/null -w '%{http_code}' "$SCHEMA_URL$path")
  [ "$code" = "200" ] || fail "anonymous GET $path returned $code, want 200"
done
prove 6 "the three GET routes return 200 with no credential"

# ── PROOF 7: writes need a DevCert ────────────────────────────────────────
step "PROOF 7 — writes need a DevCert"
uncredentialed() {
  curl -s -o /dev/null -w '%{http_code}' -X "$1" "$SCHEMA_URL$2" \
    -H 'Content-Type: application/json' -d "$3"
}
code=$(uncredentialed POST "/v2/apps/$APP_ID/releases" '{"manifest":{}}')
[ "$code" = "401" ] || fail "POST /v2/apps/$APP_ID/releases without a cert returned $code, want 401"
code=$(uncredentialed PUT "/v2/apps/$APP_ID/channels/$CHANNEL" '{"app_id":"x","channel":"y","release_id":"z","generation":0}')
[ "$code" = "401" ] || fail "PUT the channel without a cert returned $code, want 401"
code=$(uncredentialed POST "/v2/apps/$APP_ID/revocations" '{"app_id":"x","release_id":"y"}')
[ "$code" = "401" ] || fail "POST a revocation without a cert returned $code, want 401"
# The same calls WITH a DevCert already succeeded above (publish + channel).
prove 7 "each write route is rejected without a DevCert and accepted with one"

# ── Install and activate ──────────────────────────────────────────────────
step "install — resolve the channel, verify the release, activate"
"$CLI" app release-install "$APP_ID" --channel "$CHANNEL" --schema-url "$SCHEMA_URL" \
  --host-track-root "$HOST_TRACK" --json >"$WORK/install-1.json" \
  || { cat "$WORK/install-1.json"; fail "install failed"; }
grep -q "$RELEASE_1" "$WORK/install-1.json" || fail "install did not activate release 1"

# ── PROOF 1: no schema registration during release or install ─────────────
step "PROOF 1 — no schema registration during release or install"
WRITES_AFTER_INSTALL=$(schema_writes)
[ "$WRITES_BEFORE_RELEASE" = "$WRITES_AFTER_INSTALL" ] \
  || fail "catalog writes moved from $WRITES_BEFORE_RELEASE to $WRITES_AFTER_INSTALL across release + install"
prove 1 "catalog write count is $WRITES_AFTER_INSTALL before and after"

# ── PROOF 2: no CURRENT in development ────────────────────────────────────
step "PROOF 2 — no CURRENT in development"
mkdir -p "$WORK/workspace"
DEV_STATUS=$("$CLI" app dev-status "$APP_ID" --workspace-id ws1 --dev-session-id s1 \
  --workspace "$WORK/workspace" --json \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')
[ "$DEV_STATUS" = "DEV" ] || fail "a managed development workspace must report DEV, got $DEV_STATUS"
DEV_STATUS_GONE=$("$CLI" app dev-status "$APP_ID" --workspace-id ws1 --dev-session-id s1 \
  --workspace "$WORK/workspace-does-not-exist" --json \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')
[ "$DEV_STATUS_GONE" = "UNMANAGED" ] || fail "an unmanaged workspace must report UNMANAGED, got $DEV_STATUS_GONE"
prove 2 "a dev:<app_id>:<workspace_id>:<dev_session_id> session reads DEV or UNMANAGED"

# ── PROOF 8: desired = installed = active = observed ──────────────────────
step "PROOF 8 — desired = installed = active = observed, probe green"
ACTIVATION_EPOCH=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["activation_epoch"])' "$WORK/install-1.json")
"$CLI" app release-observe "$APP_ID" --app-uuid "$APP_UUID" --release-id "$RELEASE_1" \
  --activation-epoch "$ACTIVATION_EPOCH" --host-track-root "$HOST_TRACK" --hold-secs 600 \
  >"$WORK/observer-1.log" 2>&1 &
OBSERVER_ONE_PID=$!
sleep 1

status_json() {
  "$CLI" app release-status "$APP_ID" --channel "$CHANNEL" --schema-url "$SCHEMA_URL" \
    --host-track-root "$HOST_TRACK" --json
}
status_json >"$WORK/status-1.json"
python3 - "$WORK/status-1.json" "$RELEASE_1" <<'PY' || fail "the four release ids did not all match"
import json, sys
proof = json.load(open(sys.argv[1]))
expected = sys.argv[2]
for key in ("desired", "installed", "active", "observed"):
    print(f"  {key:<10} {proof[key]}")
    if proof[key] != expected:
        raise SystemExit(f"{key} is {proof[key]}, expected {expected}")
print(f"  probe      {proof['probe']}")
print(f"  status     {proof['status_label']}")
if proof["status_label"] != "CURRENT":
    raise SystemExit(f"status is {proof['status_label']}, expected CURRENT")
PY
prove 8 "all four release ids equal $RELEASE_1 and the probe is green"

# ── PROOF 3: no CURRENT from an old release process ───────────────────────
step "PROOF 3 — an old release process cannot report CURRENT"
RELEASE_2=$(publish_release "$WORK/artifact-2.tar.gz")
echo "  release 2: $RELEASE_2"
set_channel "$RELEASE_2" >/dev/null || fail "pointing the channel at release 2 failed"
"$CLI" app release-install "$APP_ID" --channel "$CHANNEL" --schema-url "$SCHEMA_URL" \
  --host-track-root "$HOST_TRACK" --json >"$WORK/install-2.json" \
  || { cat "$WORK/install-2.json"; fail "installing release 2 failed"; }
EPOCH_2=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["activation_epoch"])' "$WORK/install-2.json")
"$CLI" app release-observe "$APP_ID" --app-uuid "$APP_UUID" --release-id "$RELEASE_2" \
  --activation-epoch "$EPOCH_2" --host-track-root "$HOST_TRACK" --hold-secs 600 \
  >"$WORK/observer-2.log" 2>&1 &
OBSERVER_TWO_PID=$!
sleep 1
# Observer one is still alive on release 1. Its observed release id differs.
STATUS=$(status_json | python3 -c 'import json,sys; print(json.load(sys.stdin)["status_label"])')
if [ "$STATUS" = "CURRENT" ]; then fail "an old release process must remove CURRENT"; fi
echo "  with the old process alive, status is: $STATUS"
kill "$OBSERVER_ONE_PID" 2>/dev/null || true
wait "$OBSERVER_ONE_PID" 2>/dev/null || true
OBSERVER_ONE_PID=""
sleep 1
STATUS_AFTER=$(status_json | python3 -c 'import json,sys; print(json.load(sys.stdin)["status_label"])')
[ "$STATUS_AFTER" = "CURRENT" ] || fail "after the old process exits the app must be CURRENT again, got $STATUS_AFTER"
prove 3 "an old release process observes an older release id, so the status is not CURRENT"

# ── PROOF 4: a digest fault or a signature fault stops activation ─────────
step "PROOF 4 — a digest fault and a signature fault both stop activation"
ACTIVE_BEFORE=$(readlink "$HOST_TRACK/apps/$APP_ID/current")

# 4a — digest fault. Publish release 3 over artifact 3, then corrupt the
# bytes the artifact URL serves. The digest no longer matches the manifest.
RELEASE_3=$(publish_release "$WORK/artifact-3.tar.gz")
set_channel "$RELEASE_3" >/dev/null || fail "pointing the channel at release 3 failed"
echo 'corrupted bytes' >"$WORK/artifact-3.tar.gz"
if "$CLI" app release-install "$APP_ID" --channel "$CHANNEL" --schema-url "$SCHEMA_URL" \
     --host-track-root "$HOST_TRACK" --json >"$WORK/install-digest-fault.log" 2>&1; then
  fail "a corrupted artifact must not activate"
fi
grep -q 'digest fault' "$WORK/install-digest-fault.log" \
  || { cat "$WORK/install-digest-fault.log"; fail "the refusal must name the digest fault"; }
ACTIVE_AFTER_DIGEST=$(readlink "$HOST_TRACK/apps/$APP_ID/current")
[ "$ACTIVE_BEFORE" = "$ACTIVE_AFTER_DIGEST" ] \
  || fail "the current pointer moved for a faulty artifact"

# 4b — signature fault. The CLI signs artifacts correctly, so publish a
# manifest with a corrupted signature directly, correctly ENVELOPE-signed so
# the registry accepts the write and the fault is the host's to catch.
tar -czf "$WORK/artifact-4.tar.gz" -C "$WORK/payload" .
python3 - "$WORK/artifact-4.tar.gz" "$APP_ID" "$APP_UUID" "$SOURCE_COMMIT" "$SCHEMA_IDENTITY" \
    >"$WORK/bad-signature-manifest.json" <<'PY'
import base64, hashlib, json, sys
artifact, app_id, app_uuid, commit, identity = sys.argv[1:6]
digest = hashlib.sha256(open(artifact, "rb").read()).hexdigest()
print(json.dumps({"manifest": {
    "app_id": app_id,
    "app_uuid": app_uuid,
    "schemas": {f"{app_id}/Note": identity},
    "source_commit": commit,
    "artifact_digest": digest,
    "artifact_url": f"file://{artifact}",
    # A syntactically valid, cryptographically wrong signature.
    "artifact_signature": base64.b64encode(b"\x00" * 64).decode(),
}}))
PY
python3 "$WORK/devcert.py" signed-write POST "$SCHEMA_URL/v2/apps/$APP_ID/releases" \
  app_release_publish <"$WORK/bad-signature-manifest.json" >"$WORK/bad-signature-publish.json" \
  || { cat "$WORK/bad-signature-publish.json"; fail "the bad-signature release did not publish"; }
RELEASE_4=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["release_id"])' "$WORK/bad-signature-publish.json")
set_channel "$RELEASE_4" >/dev/null || fail "pointing the channel at release 4 failed"
if "$CLI" app release-install "$APP_ID" --channel "$CHANNEL" --schema-url "$SCHEMA_URL" \
     --host-track-root "$HOST_TRACK" --json >"$WORK/install-signature-fault.log" 2>&1; then
  fail "an artifact with a bad signature must not activate"
fi
grep -q 'signature fault' "$WORK/install-signature-fault.log" \
  || { cat "$WORK/install-signature-fault.log"; fail "the refusal must name the signature fault"; }
ACTIVE_AFTER_SIGNATURE=$(readlink "$HOST_TRACK/apps/$APP_ID/current")
[ "$ACTIVE_BEFORE" = "$ACTIVE_AFTER_SIGNATURE" ] \
  || fail "the current pointer moved for an artifact with a bad signature"
prove 4 "both faults stop the flow before the current pointer moves"

# ── PROOF 9: forced drift restores the prior verified release ─────────────
step "PROOF 9 — forced drift restores the prior verified release"
# Point the channel back at release 2 so the desired release is the one the
# host actually holds, then take a clean check to record it as verified.
set_channel "$RELEASE_2" >/dev/null || fail "pointing the channel back at release 2 failed"
"$CLI" app release-check "$APP_ID" --channel "$CHANNEL" --schema-url "$SCHEMA_URL" \
  --host-track-root "$HOST_TRACK" --json >"$WORK/check-clean.json" \
  || { cat "$WORK/check-clean.json"; fail "the clean drift check failed"; }
PRIOR=$(cat "$HOST_TRACK/apps/$APP_ID/prior-verified")
[ "$PRIOR" = "$RELEASE_2" ] || fail "prior-verified is $PRIOR, expected $RELEASE_2"

# Force drift: point current at a directory that is not the desired release.
mkdir -p "$HOST_TRACK/apps/$APP_ID/versions/$RELEASE_1"
ln -sfn "versions/$RELEASE_1" "$HOST_TRACK/apps/$APP_ID/current"
"$CLI" app release-check "$APP_ID" --channel "$CHANNEL" --schema-url "$SCHEMA_URL" \
  --host-track-root "$HOST_TRACK" --json >"$WORK/check-drift.json" \
  || { cat "$WORK/check-drift.json"; fail "the drift check failed"; }
python3 - "$WORK/check-drift.json" "$RELEASE_2" <<'PY' || fail "the host did not restore and re-prove"
import json, sys
outcome = json.load(open(sys.argv[1]))
expected = sys.argv[2]
if outcome["before"]["status_label"] == "CURRENT":
    raise SystemExit("the forced-drift check must not read CURRENT")
if outcome["restored_to"] != expected:
    raise SystemExit(f"restored_to is {outcome['restored_to']}, expected {expected}")
after = outcome["after"]
if after is None:
    raise SystemExit("the host must re-prove after a restore")
for key in ("desired", "installed", "active", "observed"):
    if after[key] != expected:
        raise SystemExit(f"after the restore {key} is {after[key]}, expected {expected}")
if after["status_label"] != "CURRENT":
    raise SystemExit(f"after the restore the status is {after['status_label']}, expected CURRENT")
print(f"  restored to {outcome['restored_to']} and proved the four-way match again")
PY
prove 9 "forced drift restored the prior verified release and re-proved the match"

# ── PROOF 10: the recurring check cycle runs on the operator cadence ──────
step "PROOF 10 — the recurring check cycle restores on drift without a new invocation"
# Proof 9 restored release 2 and re-proved the match, so the host is clean
# and release 2 is the rollback target. Force drift once more, then hand the
# host ONE invocation that runs the cycle repeatedly. Nothing calls the CLI
# again: the restore has to come from that single run.
#
# The interval is overridden to 1 s so the proof does not wait two minutes.
# `check_interval` resolves it; the operator default is PROBE_INTERVAL (60 s).
ln -sfn "versions/$RELEASE_1" "$HOST_TRACK/apps/$APP_ID/current"
"$CLI" app release-check "$APP_ID" --channel "$CHANNEL" --schema-url "$SCHEMA_URL" \
  --host-track-root "$HOST_TRACK" --watch --interval-secs 1 --cycles 2 --json \
  >"$WORK/check-watch.json" || { cat "$WORK/check-watch.json"; fail "the watch run failed"; }
python3 - "$WORK/check-watch.json" "$RELEASE_2" <<'PYCYCLE' || fail "the recurring cycle did not converge"
import json, sys

raw = open(sys.argv[1]).read()
expected = sys.argv[2]
decoder, cycles, at = json.JSONDecoder(), [], 0
while at < len(raw):
    while at < len(raw) and raw[at].isspace():
        at += 1
    if at >= len(raw):
        break
    value, at = decoder.raw_decode(raw, at)
    cycles.append(value)

if len(cycles) != 2:
    raise SystemExit(f"one --cycles 2 run emitted {len(cycles)} cycles, expected 2")

first, second = cycles
if first["before"]["status_label"] == "CURRENT":
    raise SystemExit("cycle 1 ran against forced drift and must not read CURRENT")
if first["restored_to"] != expected:
    raise SystemExit(f"cycle 1 restored_to is {first['restored_to']}, expected {expected}")

# The second cycle is the one that matters. It came from the SAME invocation,
# so a green reading here proves the cadence is live, not a dead constant.
if second["restored_to"] is not None:
    raise SystemExit("cycle 2 had nothing to restore, so restored_to must be null")
if second["before"]["status_label"] != "CURRENT":
    raise SystemExit(f"cycle 2 reads {second['before']['status_label']}, expected CURRENT")
for key in ("desired", "installed", "active", "observed"):
    if second["before"][key] != expected:
        raise SystemExit(f"cycle 2 {key} is {second['before'][key]}, expected {expected}")
print("  cycle 1 restored the prior verified release; cycle 2 of the same run read CURRENT")
PYCYCLE
prove 10 "one watch invocation restored on drift and re-proved CURRENT on its next cycle"

# ── Settled operator behavior: a revoked active release drifts ────────
# Not one of the nine, but the recommendation the design leaves to the
# operator (decision 03): the revocation read rides the same cycle as the
# channel read, and a revoked active release runs the drift path.
step "revocation — a revoked active release runs the drift path"
EXEMEM_DEV_API_KEY=em_release_e2e "$CLI" app release-revoke "$APP_ID" \
  --release-id "$RELEASE_2" --reason "an end-to-end proof revocation" \
  --schema-url "$SCHEMA_URL" --api-url "$AUTH_URL" --key-file "$KEY_FILE" --json \
  >"$WORK/revoke.json" || { cat "$WORK/revoke.json"; fail "the revocation was refused"; }
"$CLI" app release-check "$APP_ID" --channel "$CHANNEL" --schema-url "$SCHEMA_URL" \
  --host-track-root "$HOST_TRACK" --desired "$RELEASE_2" --json >"$WORK/check-revoked.json" \
  || { cat "$WORK/check-revoked.json"; fail "the revocation drift check failed"; }
python3 - "$WORK/check-revoked.json" <<'REVOKED' || fail "a revoked active release must not read CURRENT"
import json, sys
outcome = json.load(open(sys.argv[1]))
if outcome["before"]["status_label"] == "CURRENT":
    raise SystemExit("a revoked active release must not read CURRENT")
print(f"  a revoked active release reads {outcome['before']['status_label']}")
REVOKED

step "result"
[ "$PASSED" = "10" ] || fail "only $PASSED of 10 proofs ran"
echo "ALL 10 PROOFS PASSED"
