#!/usr/bin/env bash
# Proof: Publish an app release to /v2 registry in DEV
#
# This script demonstrates the complete flow for publishing an app release
# (Brain, LastSecrets, or lastseek) to the /v2 app registry in DEV.
#
# Prerequisites:
#   - LASTDB_API_KEY: Developer API key from https://exemem.com
#   - LASTDB_DEV_KEY_FILE: Path to Ed25519 private key file (64-byte base64)
#   - Built lastdb CLI in target/debug/lastdb
#
# Usage:
#   export LASTDB_API_KEY="your-api-key"
#   export LASTDB_DEV_KEY_FILE="$HOME/.lastdb-dev-key"
#   bash scripts/proofs/app-release-v2-publish-dev-proof.sh brain

set -euo pipefail

APP_ID="${1:-brain}"
PROOF_DIR="/tmp/lastdb-proof-v2-publish-$(date +%s)"
EXEMEM_ENV=dev

echo "=== App Release /v2 Publishing Proof (DEV) ==="
echo "App: $APP_ID"
echo "Proof directory: $PROOF_DIR"
echo

# Check prerequisites
if [[ -z "${LASTDB_API_KEY:-}" ]]; then
  echo "❌ LASTDB_API_KEY not set"
  echo "   Obtain from: https://exemem.com (paid plan or developer_access grant)"
  exit 1
fi

if [[ -z "${LASTDB_DEV_KEY_FILE:-}" ]] || [[ ! -f "${LASTDB_DEV_KEY_FILE}" ]]; then
  echo "❌ LASTDB_DEV_KEY_FILE not set or file not found: ${LASTDB_DEV_KEY_FILE:-<unset>}"
  echo "   Generate with: dd if=/dev/urandom bs=32 count=1 2>/dev/null | base64 > ~/.lastdb-dev-key"
  exit 1
fi

if [[ ! -f target/debug/lastdb ]]; then
  echo "❌ target/debug/lastdb not found"
  echo "   Build with: cargo build -p lastdb_node --bin lastdb"
  exit 1
fi

mkdir -p "$PROOF_DIR"
cd "$PROOF_DIR"

echo "--- Step 1: Create test manifest and lockfile ---"

# Create a minimal test manifest for the app
cat > manifest.json <<EOF
{
  "app_id": "$APP_ID",
  "app_uuid": "test-uuid-$(uuidgen | tr '[:upper:]' '[:lower:]')",
  "version": "0.1.0-test.$(date +%Y%m%d)",
  "metadata": {
    "display_name": "$APP_ID Test",
    "description": "Test release for /v2 publishing proof"
  },
  "schemas": [
    {
      "name": "${APP_ID}/TestSchema",
      "hash_key": ["id"],
      "fields": ["id", "value"]
    }
  ],
  "source": {
    "repo": "https://github.com/EdgeVector/${APP_ID}",
    "branch": "main"
  }
}
EOF

# Create a lockfile with a test schema identity
# NOTE: In a real workflow, this comes from `lastdb app check --sync`
cat > manifest.json.lock.json <<EOF
{
  "schemas": {
    "${APP_ID}/TestSchema": "sha256:$(echo -n "${APP_ID}/TestSchema" | shasum -a 256 | cut -d' ' -f1)"
  }
}
EOF

echo "✓ Manifest created: $(jq -r .app_id manifest.json)"
echo "  App UUID: $(jq -r .app_uuid manifest.json)"
echo "  Version: $(jq -r .version manifest.json)"
echo "  Locked schemas: $(jq -r '.schemas | keys | join(", ")' manifest.json.lock.json)"
echo

echo "--- Step 2: Create test artifact ---"

# Create a minimal test artifact
mkdir -p artifact
echo "Test artifact for $APP_ID release $(date)" > artifact/README.txt
tar czf artifact.tar.gz -C artifact .
ARTIFACT_DIGEST="$(shasum -a 256 artifact.tar.gz | cut -d' ' -f1)"

echo "✓ Artifact created: artifact.tar.gz"
echo "  Size: $(stat -f%z artifact.tar.gz 2>/dev/null || stat -c%s artifact.tar.gz) bytes"
echo "  SHA-256: $ARTIFACT_DIGEST"
echo

echo "--- Step 3: Check DEV /v2 app registration ---"

DEV_SCHEMA_SERVICE="https://y0q3m6vk75.execute-api.us-west-2.amazonaws.com"

if curl -sf "$DEV_SCHEMA_SERVICE/v2/apps/$APP_ID" > app-record.json; then
  echo "✓ App is registered in DEV /v2"
  echo "  Publisher: $(jq -r .owner_dev_pubkey app-record.json | cut -c1-16)..."
  echo "  Registered: $(jq -r .registered_at app-record.json)"
else
  echo "❌ App '$APP_ID' not registered in DEV /v2"
  echo "   Register with: lastdb app publish --env dev ..."
  echo "   Or use a different app that is already registered"
  exit 1
fi
echo

echo "--- Step 4: Publish release to DEV /v2 ---"

APP_UUID="$(jq -r .app_uuid manifest.json)"
SOURCE_COMMIT="$(git -C "$(dirname "$0")/../.." rev-parse HEAD)"
ARTIFACT_URL="https://r2.folddb.com/apps/$APP_ID/test/artifact-$(date +%Y%m%d).tar.gz"

# NOTE: This will fail if the dev key's verifying pubkey does not match
# the registered publisher for the app. In that case, you need to either:
# 1. Use the correct dev key that was used to register the app
# 2. Re-register the app with POST /v2/apps using the current dev key

set +e
"$(dirname "$0")/../../target/debug/lastdb" app release-publish \
  --manifest manifest.json \
  --app-uuid "$APP_UUID" \
  --source-commit "$SOURCE_COMMIT" \
  --artifact artifact.tar.gz \
  --artifact-url "$ARTIFACT_URL" \
  --env dev \
  --key-file "$LASTDB_DEV_KEY_FILE" \
  --api-key "$LASTDB_API_KEY" \
  --json \
  | tee release-result.json
PUBLISH_EXIT=$?
set -e

if [[ $PUBLISH_EXIT -ne 0 ]]; then
  echo
  echo "❌ Release publish failed (exit $PUBLISH_EXIT)"
  echo
  echo "Common causes:"
  echo "  1. Publisher key mismatch: dev key pubkey != registered publisher"
  echo "     Check: jq -r .owner_dev_pubkey app-record.json"
  echo "     Your key: (compute verifying key from LASTDB_DEV_KEY_FILE)"
  echo
  echo "  2. Missing or unresolved schema in lockfile"
  echo "     Fix: Run 'lastdb app check --sync' in the app repo"
  echo
  echo "  3. Invalid or expired API key"
  echo "     Fix: Obtain fresh key from https://exemem.com"
  echo
  echo "  4. Network or service error"
  echo "     Check: curl -sf $DEV_SCHEMA_SERVICE/v2/apps/$APP_ID"
  echo
  exit 1
fi

RELEASE_ID="$(jq -r .release_id release-result.json)"

echo
echo "✓ Release published successfully"
echo "  Release ID: $RELEASE_ID"
echo "  Source: $SOURCE_COMMIT"
echo "  Artifact digest: $(jq -r .manifest.artifact_digest release-result.json)"
echo

echo "--- Step 5: Verify release is readable ---"

if curl -sf "$DEV_SCHEMA_SERVICE/v2/releases/$RELEASE_ID" | jq . > release-read.json; then
  echo "✓ Release is readable via anonymous GET"
  echo "  App: $(jq -r .app_id release-read.json)"
  echo "  Publisher: $(jq -r .publisher_dev_pubkey release-read.json | cut -c1-16)..."
  echo "  Published: $(jq -r .published_at release-read.json)"
  echo "  Schemas: $(jq -r '.manifest.schemas | length' release-read.json)"
else
  echo "❌ Release not readable: GET /v2/releases/$RELEASE_ID failed"
  exit 1
fi
echo

echo "--- Step 6: Set channel to point at release ---"

CHANNEL="test-$(date +%Y%m%d)"

set +e
"$(dirname "$0")/../../target/debug/lastdb" app release-channel \
  --app-id "$APP_ID" \
  --channel "$CHANNEL" \
  --release-id "$RELEASE_ID" \
  --env dev \
  --key-file "$LASTDB_DEV_KEY_FILE" \
  --api-key "$LASTDB_API_KEY" \
  --json \
  | tee channel-result.json
CHANNEL_EXIT=$?
set -e

if [[ $CHANNEL_EXIT -ne 0 ]]; then
  echo
  echo "❌ Channel set failed (exit $CHANNEL_EXIT)"
  echo "   This might be a publisher key mismatch (same as Step 4)"
  exit 1
fi

echo
echo "✓ Channel set successfully"
echo "  Channel: $APP_ID/$CHANNEL"
echo "  Release: $RELEASE_ID"
echo "  Generation: $(jq -r .generation channel-result.json)"
echo

echo "--- Step 7: Verify channel is readable ---"

if curl -sf "$DEV_SCHEMA_SERVICE/v2/apps/$APP_ID/channels/$CHANNEL" | jq . > channel-read.json; then
  echo "✓ Channel is readable via anonymous GET"
  echo "  Desired release: $(jq -r .release_id channel-read.json)"
  echo "  Generation: $(jq -r .generation channel-read.json)"
  echo "  Updated: $(jq -r .updated_at channel-read.json)"
else
  echo "❌ Channel not readable: GET /v2/apps/$APP_ID/channels/$CHANNEL failed"
  exit 1
fi
echo

echo "=== ✅ Proof complete ==="
echo
echo "Summary:"
echo "  • App '$APP_ID' has a release published to DEV /v2"
echo "  • Release ID: $RELEASE_ID"
echo "  • Channel '$CHANNEL' points to this release"
echo "  • Verifier can now import and sign compat rows for this release"
echo
echo "Next steps for app workflows:"
echo "  1. Add LASTDB_API_KEY and LASTDB_DEV_KEY_FILE secrets to app repo"
echo "  2. Call 'lastdb app release-publish' after building artifact"
echo "  3. Call 'lastdb app release-channel' to update desired channel"
echo "  4. Verifier reads release and POSTs compat rows"
echo
echo "Proof artifacts retained in: $PROOF_DIR"
echo "  manifest.json"
echo "  manifest.json.lock.json"
echo "  artifact.tar.gz"
echo "  release-result.json"
echo "  release-read.json"
echo "  channel-result.json"
echo "  channel-read.json"
