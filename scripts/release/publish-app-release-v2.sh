#!/usr/bin/env bash
# Helper script for app release workflows to publish to /v2 registry
#
# This script wraps the lastdb CLI to publish an app release to the /v2
# registry (DEV or PROD) and optionally update a channel.
#
# Required environment variables:
#   LASTDB_API_KEY: Developer API key from https://exemem.com
#   LASTDB_DEV_KEY_FILE: Path to Ed25519 private key file
#   APP_ID: App identifier (e.g. brain, lastsecrets, lastseek)
#   APP_UUID: Stable app UUID from manifest
#   MANIFEST_PATH: Path to app manifest (e.g. brain/lastdb-app.json)
#   ARTIFACT_PATH: Path to built artifact (e.g. brain.tar.gz)
#   ARTIFACT_URL: Public URL where artifact will be available
#   SOURCE_COMMIT: Git commit SHA that produced the artifact
#
# Optional environment variables:
#   REGISTRY_ENV: dev or prod (default: dev)
#   CHANNEL: Channel to update after publish (default: none)
#   LASTDB_BIN: Path to lastdb binary (default: lastdb)
#
# Usage in GitHub Actions:
#   - name: Publish to /v2 registry
#     env:
#       LASTDB_API_KEY: ${{ secrets.LASTDB_API_KEY }}
#       LASTDB_DEV_KEY_FILE: ${{ secrets.LASTDB_DEV_KEY_FILE }}
#       APP_ID: brain
#       APP_UUID: ${{ secrets.BRAIN_APP_UUID }}
#       MANIFEST_PATH: brain/lastdb-app.json
#       ARTIFACT_PATH: brain.tar.gz
#       ARTIFACT_URL: https://r2.folddb.com/apps/brain/releases/brain-${{ env.VERSION }}.tar.gz
#       SOURCE_COMMIT: ${{ github.sha }}
#       REGISTRY_ENV: dev
#       CHANNEL: nightly
#     run: bash scripts/release/publish-app-release-v2.sh

set -euo pipefail

# Required variables
: "${LASTDB_API_KEY:?LASTDB_API_KEY not set}"
: "${LASTDB_DEV_KEY_FILE:?LASTDB_DEV_KEY_FILE not set}"
: "${APP_ID:?APP_ID not set}"
: "${APP_UUID:?APP_UUID not set}"
: "${MANIFEST_PATH:?MANIFEST_PATH not set}"
: "${ARTIFACT_PATH:?ARTIFACT_PATH not set}"
: "${ARTIFACT_URL:?ARTIFACT_URL not set}"
: "${SOURCE_COMMIT:?SOURCE_COMMIT not set}"

# Optional variables
REGISTRY_ENV="${REGISTRY_ENV:-dev}"
CHANNEL="${CHANNEL:-}"
LASTDB_BIN="${LASTDB_BIN:-lastdb}"

# Validate inputs
if [[ ! -f "$LASTDB_DEV_KEY_FILE" ]]; then
  echo "❌ Dev key file not found: $LASTDB_DEV_KEY_FILE" >&2
  exit 1
fi

if [[ ! -f "$MANIFEST_PATH" ]]; then
  echo "❌ Manifest not found: $MANIFEST_PATH" >&2
  exit 1
fi

if [[ ! -f "$ARTIFACT_PATH" ]]; then
  echo "❌ Artifact not found: $ARTIFACT_PATH" >&2
  exit 1
fi

LOCKFILE_PATH="${MANIFEST_PATH}.lock.json"
if [[ ! -f "$LOCKFILE_PATH" ]]; then
  echo "❌ Lockfile not found: $LOCKFILE_PATH" >&2
  echo "   Run 'lastdb app check --sync' to generate it" >&2
  exit 1
fi

if ! command -v "$LASTDB_BIN" &>/dev/null; then
  echo "❌ lastdb binary not found: $LASTDB_BIN" >&2
  echo "   Install from: https://github.com/EdgeVector/lastdb" >&2
  exit 1
fi

echo "=== Publishing $APP_ID release to /v2 registry ($REGISTRY_ENV) ==="
echo "Manifest: $MANIFEST_PATH"
echo "Artifact: $ARTIFACT_PATH ($(stat -f%z "$ARTIFACT_PATH" 2>/dev/null || stat -c%s "$ARTIFACT_PATH") bytes)"
echo "Source: $SOURCE_COMMIT"
echo

# Step 1: Publish release
echo "--- Publishing release ---"
RELEASE_RESULT_JSON="$(mktemp)"
trap "rm -f '$RELEASE_RESULT_JSON'" EXIT

set +e
"$LASTDB_BIN" app release-publish \
  --manifest "$MANIFEST_PATH" \
  --app-uuid "$APP_UUID" \
  --source-commit "$SOURCE_COMMIT" \
  --artifact "$ARTIFACT_PATH" \
  --artifact-url "$ARTIFACT_URL" \
  --env "$REGISTRY_ENV" \
  --key-file "$LASTDB_DEV_KEY_FILE" \
  --api-key "$LASTDB_API_KEY" \
  --json \
  | tee "$RELEASE_RESULT_JSON"
PUBLISH_EXIT=$?
set -e

if [[ $PUBLISH_EXIT -ne 0 ]]; then
  echo >&2
  echo "❌ Release publish failed" >&2
  echo >&2
  echo "Common causes:" >&2
  echo "  1. Publisher key mismatch: dev key does not match registered publisher" >&2
  echo "  2. Missing or unresolved schema in lockfile" >&2
  echo "  3. Invalid or expired API key" >&2
  echo "  4. Network or service error" >&2
  echo >&2
  echo "Debug:" >&2
  echo "  • Check app is registered: GET /v2/apps/$APP_ID" >&2
  echo "  • Verify publisher key matches: compare owner_dev_pubkey" >&2
  echo "  • Ensure lockfile is up to date: lastdb app check --sync" >&2
  exit 1
fi

RELEASE_ID="$(jq -r .release_id "$RELEASE_RESULT_JSON")"

echo
echo "✅ Release published"
echo "   Release ID: $RELEASE_ID"
echo

# Step 2: Update channel (optional)
if [[ -n "$CHANNEL" ]]; then
  echo "--- Updating channel $CHANNEL ---"
  
  set +e
  "$LASTDB_BIN" app release-channel \
    --app-id "$APP_ID" \
    --channel "$CHANNEL" \
    --release-id "$RELEASE_ID" \
    --env "$REGISTRY_ENV" \
    --key-file "$LASTDB_DEV_KEY_FILE" \
    --api-key "$LASTDB_API_KEY" \
    --json
  CHANNEL_EXIT=$?
  set -e
  
  if [[ $CHANNEL_EXIT -ne 0 ]]; then
    echo >&2
    echo "❌ Channel update failed" >&2
    echo "   Release was published but channel was not updated" >&2
    exit 1
  fi
  
  echo
  echo "✅ Channel updated"
  echo "   Channel: $APP_ID/$CHANNEL -> $RELEASE_ID"
fi

echo
echo "=== Success ==="
echo "Release ID: $RELEASE_ID"
echo "App: $APP_ID"
echo "Environment: $REGISTRY_ENV"
if [[ -n "$CHANNEL" ]]; then
  echo "Channel: $CHANNEL"
fi

# Output for GitHub Actions
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  echo "release_id=$RELEASE_ID" >> "$GITHUB_OUTPUT"
  echo "app_id=$APP_ID" >> "$GITHUB_OUTPUT"
fi
