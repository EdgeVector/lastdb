#!/usr/bin/env bash
# Proof: Publish an app release to /v2 registry in DEV
#
# This script demonstrates the complete flow for publishing an app release
# to the /v2 app registry in DEV using a dedicated test app.
#
# Prerequisites:
#   - LASTDB_API_KEY: Developer API key from https://exemem.com
#   - LASTDB_DEV_KEY_B64: Base64-encoded Ed25519 private key (32-byte secret)
#   - Built lastdb CLI in target/debug/lastdb
#   - TEST_APP_ID: A registered test app ID (default: lastdb-test-app)
#   - TEST_APP_UUID: The registered UUID for the test app
#
# Usage:
#   export LASTDB_API_KEY="your-api-key"
#   export LASTDB_DEV_KEY_B64="your-base64-encoded-key"
#   export TEST_APP_ID="lastdb-test-app"
#   export TEST_APP_UUID="01234567-89ab-cdef-0123-456789abcdef"
#   bash scripts/proofs/app-release-v2-publish-dev-proof.sh

set -euo pipefail

# Compute absolute repo root before changing directory
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

TEST_APP_ID="${TEST_APP_ID:-lastdb-test-app}"
PROOF_DIR="/tmp/lastdb-proof-v2-publish-$(date +%s)"

echo "=== App Release /v2 Publishing Proof (DEV) ==="
echo "App: $TEST_APP_ID"
echo "Proof directory: $PROOF_DIR"
echo

# Check prerequisites
if [[ -z "${LASTDB_API_KEY:-}" ]]; then
  echo "❌ LASTDB_API_KEY not set"
  echo "   Obtain from: https://exemem.com (paid plan or developer_access grant)"
  exit 1
fi

if [[ -z "${LASTDB_DEV_KEY_B64:-}" ]]; then
  echo "❌ LASTDB_DEV_KEY_B64 not set"
  echo "   Generate with: dd if=/dev/urandom bs=32 count=1 2>/dev/null | base64"
  exit 1
fi

if [[ -z "${TEST_APP_UUID:-}" ]]; then
  echo "❌ TEST_APP_UUID not set"
  echo "   This should be the registered UUID for the test app in DEV"
  exit 1
fi

if [[ ! -f "$REPO_ROOT/target/debug/lastdb" ]]; then
  echo "❌ $REPO_ROOT/target/debug/lastdb not found"
  echo "   Build with: cargo build -p lastdb_node --bin lastdb"
  exit 1
fi

mkdir -p "$PROOF_DIR"
cd "$PROOF_DIR"

# Write dev key to temporary file with 0600 permissions
DEV_KEY_FILE="$(mktemp)"
trap 'rm -f "$DEV_KEY_FILE"' EXIT
echo "$LASTDB_DEV_KEY_B64" > "$DEV_KEY_FILE"
chmod 0600 "$DEV_KEY_FILE"

echo "--- Step 1: Create test manifest and lockfile ---"

# Create a minimal test manifest for the app
cat > manifest.json <<EOF
{
  "app_id": "$TEST_APP_ID",
  "app_uuid": "$TEST_APP_UUID",
  "version": "0.1.0-test.$(date +%Y%m%d)",
  "metadata": {
    "display_name": "$TEST_APP_ID Test",
    "description": "Test release for /v2 publishing proof"
  },
  "schemas": [
    {
      "name": "${TEST_APP_ID}/TestSchema",
      "schema_type": "Hash",
      "key": {
        "hash_field": "id"
      },
      "fields": [
        {"name": "id", "type": "String"},
        {"name": "value", "type": "String"}
      ]
    }
  ],
  "source": "https://github.com/EdgeVector/lastdb"
}
EOF

echo "✓ Manifest created: $(jq -r .app_id manifest.json)"
echo "  App UUID: $(jq -r .app_uuid manifest.json)"
echo "  Version: $(jq -r .version manifest.json)"
echo

echo "--- Step 2: Generate lockfile with real schema identities ---"

# Use lastdb app check --sync to generate a real lockfile with resolved identities
# This requires a running lastdb node with DEV catalog access
if "$REPO_ROOT/target/debug/lastdb" app check --manifest manifest.json --json > check-result.json 2>&1; then
  echo "✓ Schema check passed"
  
  # The lockfile should now exist with real identities
  if [[ -f manifest.json.lock.json ]]; then
    echo "✓ Lockfile created: manifest.json.lock.json"
    echo "  Locked schemas: $(jq -r '.schemas | keys | join(", ")' manifest.json.lock.json)"
  else
    echo "⚠ Lockfile was not created by check command"
    echo "  Creating a placeholder lockfile (may cause publish to fail)"
    cat > manifest.json.lock.json <<EOF
{
  "schemas": {}
}
EOF
  fi
else
  echo "⚠ Schema check command failed (this is expected if no node is running)"
  echo "  Creating a placeholder lockfile (publish will fail without real identities)"
  cat > manifest.json.lock.json <<EOF
{
  "schemas": {}
}
EOF
fi
echo

echo "--- Step 3: Create test artifact ---"

# Create a minimal test artifact
mkdir -p artifact
echo "Test artifact for $TEST_APP_ID release $(date)" > artifact/README.txt
tar czf artifact.tar.gz -C artifact .
ARTIFACT_DIGEST="$(shasum -a 256 artifact.tar.gz | cut -d' ' -f1)"

echo "✓ Artifact created: artifact.tar.gz"
echo "  Size: $(stat -f%z artifact.tar.gz 2>/dev/null || stat -c%s artifact.tar.gz) bytes"
echo "  SHA-256: $ARTIFACT_DIGEST"
echo

echo "--- Step 4: Check DEV /v2 app registration ---"

DEV_SCHEMA_SERVICE="https://y0q3m6vk75.execute-api.us-west-2.amazonaws.com"

if curl -sf "$DEV_SCHEMA_SERVICE/v2/apps/$TEST_APP_ID" > app-record.json; then
  echo "✓ App is registered in DEV /v2"
  echo "  Publisher: $(jq -r .owner_dev_pubkey app-record.json | cut -c1-16)..."
  echo "  Registered: $(jq -r .registered_at app-record.json)"
else
  echo "❌ App '$TEST_APP_ID' not registered in DEV /v2"
  echo "   Register with: lastdb app publish --env dev ..."
  echo "   Or use a different test app that is already registered"
  exit 1
fi
echo

echo "--- Step 5: Publish release to DEV /v2 ---"

SOURCE_COMMIT="$(git -C "$REPO_ROOT" rev-parse HEAD)"
ARTIFACT_URL="https://r2.folddb.com/apps/$TEST_APP_ID/test/artifact-$(date +%Y%m%d).tar.gz"

# NOTE: This will fail if the dev key's verifying pubkey does not match
# the registered publisher for the app. In that case, you need to either:
# 1. Use the correct dev key that was used to register the app
# 2. Re-register the app with POST /v2/apps using the current dev key

set +e
"$REPO_ROOT/target/debug/lastdb" app release-publish \
  --manifest manifest.json \
  --app-uuid "$TEST_APP_UUID" \
  --source-commit "$SOURCE_COMMIT" \
  --artifact artifact.tar.gz \
  --artifact-url "$ARTIFACT_URL" \
  --env dev \
  --key-file "$DEV_KEY_FILE" \
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

echo "--- Step 6: Verify release is readable ---"

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

echo "--- Step 7: Set channel to point at release ---"

CHANNEL="test-$(date +%Y%m%d)"

set +e
"$REPO_ROOT/target/debug/lastdb" app release-channel \
  "$TEST_APP_ID" \
  --channel "$CHANNEL" \
  --release-id "$RELEASE_ID" \
  --env dev \
  --key-file "$DEV_KEY_FILE" \
  --api-key "$LASTDB_API_KEY" \
  --json \
  | tee channel-result.json
CHANNEL_EXIT=$?
set -e

if [[ $CHANNEL_EXIT -ne 0 ]]; then
  echo
  echo "❌ Channel set failed (exit $CHANNEL_EXIT)"
  echo "   This might be a publisher key mismatch (same as Step 5)"
  exit 1
fi

echo
echo "✓ Channel set successfully"
echo "  Channel: $TEST_APP_ID/$CHANNEL"
echo "  Release: $RELEASE_ID"
echo "  Generation: $(jq -r .generation channel-result.json)"
echo

echo "--- Step 8: Verify channel is readable ---"

if curl -sf "$DEV_SCHEMA_SERVICE/v2/apps/$TEST_APP_ID/channels/$CHANNEL" | jq . > channel-read.json; then
  echo "✓ Channel is readable via anonymous GET"
  echo "  Desired release: $(jq -r .release_id channel-read.json)"
  echo "  Generation: $(jq -r .generation channel-read.json)"
  echo "  Updated: $(jq -r .updated_at channel-read.json)"
else
  echo "❌ Channel not readable: GET /v2/apps/$TEST_APP_ID/channels/$CHANNEL failed"
  exit 1
fi
echo

echo "=== ✅ Proof complete ==="
echo
echo "Summary:"
echo "  • App '$TEST_APP_ID' has a release published to DEV /v2"
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
