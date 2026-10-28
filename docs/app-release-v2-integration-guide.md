# App Release /v2 Registry Integration Guide

This guide explains how to integrate /v2 registry publishing into app release workflows for Brain, LastSecrets, lastseek, and other LastDB apps.

## Overview

The /v2 app registry enables verifiable, signed app releases with immutable release identities. Apps publish releases to /v2, and the verifier imports and signs compatibility rows that installers can verify.

**Repository-specific changes**: Each app repo (EdgeVector/last-stack, EdgeVector/lastsecrets, EdgeVector/lastseek) needs to add /v2 publishing to its release workflow.

**lastdb repository**: Contains the tools (`lastdb app release-publish`, helper scripts) that app workflows call. No app-specific code lives here.

## Prerequisites

### 1. Publisher Secrets

Each app repo needs these GitHub secrets:

#### `LASTDB_API_KEY`
- Developer API key from https://exemem.com
- Requires paid plan or `developer_access` grant
- Authorizes minting short-TTL DevCerts
- Same key works for all apps owned by the same developer

#### `LASTDB_DEV_KEY_FILE`
- Ed25519 private key (64 bytes, base64-encoded)
- Generate: `dd if=/dev/urandom bs=32 count=1 2>/dev/null | base64`
- The verifying key **must** match the registered publisher for the app
- Check with: `curl https://y0q3m6vk75.execute-api.us-west-2.amazonaws.com/v2/apps/{app_id} | jq .owner_dev_pubkey`
- Store the base64-encoded private key directly in the secret (NOT a file path)

#### `{APP}_APP_UUID` (optional)
- Stable app UUID from the manifest
- Can be read from manifest at runtime instead: `jq -r .app_uuid manifest.json`

### 2. Manifest Lockfile

The app repo must include `{manifest}.lock.json` containing locked schema identities:

```json
{
  "schemas": {
    "brain/Concept": "sha256:abc123...",
    "brain/Decision": "sha256:def456...",
    "brain/Situation": "sha256:789abc..."
  }
}
```

**Generate with:**
```bash
lastdb app check --sync --manifest brain/lastdb-app.json
```

**When to update:**
- After adding or modifying schemas in the manifest
- Before cutting a new release
- Commit the lockfile to the repo

### 3. lastdb CLI

The release workflow needs the `lastdb` binary to publish releases.

**Option A: Install from release**
```yaml
- name: Install lastdb
  run: |
    curl -L https://github.com/EdgeVector/lastdb/releases/download/nightly-YYYYMMDD-SHA9/lastdb-aarch64-apple-darwin.tar.gz | tar xz
    sudo mv lastdb /usr/local/bin/
```

**Option B: Build from source**
```yaml
- name: Build lastdb
  run: |
    git clone https://github.com/EdgeVector/lastdb.git /tmp/lastdb
    cd /tmp/lastdb
    cargo build --release -p lastdb_node --bin lastdb
    sudo cp target/release/lastdb /usr/local/bin/
```

**Option C: Use existing installation** (if the app repo includes lastdb as a dev dependency)

## Integration Steps

### Step 1: Add Publisher Secrets

In the app repo's GitHub settings:

1. Navigate to **Settings → Secrets and variables → Actions**
2. Add **Repository secrets**:
   - `LASTDB_API_KEY`: Your developer API key
   - `LASTDB_DEV_KEY_FILE`: Base64-encoded Ed25519 private key
   - `BRAIN_APP_UUID` (or `LASTSECRETS_APP_UUID`, etc.): Stable app UUID

**Security note**: These secrets grant write access to your app's /v2 registry. Protect them accordingly.

### Step 2: Update Release Workflow

Add /v2 publishing to your `.github/workflows/release.yml` (or similar):

#### Example for Brain (EdgeVector/last-stack)

```yaml
name: Release Brain

on:
  push:
    tags:
      - 'brain-v*'
  workflow_dispatch:

jobs:
  release:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Build artifact
        run: |
          cd brain
          bun install --frozen-lockfile
          bun run build
          tar czf ../brain.tar.gz -C . .
      
      - name: Upload to R2
        env:
          R2_ACCESS_KEY_ID: ${{ secrets.R2_ACCESS_KEY_ID }}
          R2_SECRET_ACCESS_KEY: ${{ secrets.R2_SECRET_ACCESS_KEY }}
        run: |
          VERSION="${GITHUB_REF#refs/tags/brain-v}"
          aws s3 cp brain.tar.gz \
            s3://folddb-apps/brain/releases/brain-${VERSION}.tar.gz \
            --endpoint-url https://r2.folddb.com
      
      - name: Install lastdb
        run: |
          # Use latest nightly or pin to a specific version
          curl -L https://github.com/EdgeVector/lastdb/releases/latest/download/lastdb-x86_64-unknown-linux-gnu.tar.gz | tar xz
          sudo mv lastdb /usr/local/bin/
      
      - name: Publish to /v2 registry (DEV)
        env:
          LASTDB_API_KEY: ${{ secrets.LASTDB_API_KEY }}
          LASTDB_DEV_KEY_FILE: ${{ secrets.LASTDB_DEV_KEY_FILE }}
        run: |
          VERSION="${GITHUB_REF#refs/tags/brain-v}"
          
          # Write dev key to temp file
          echo "$LASTDB_DEV_KEY_FILE" > /tmp/dev-key.txt
          
          # Publish release
          lastdb app release-publish \
            --manifest brain/lastdb-app.json \
            --app-uuid "${{ secrets.BRAIN_APP_UUID }}" \
            --source-commit "$GITHUB_SHA" \
            --artifact brain.tar.gz \
            --artifact-url "https://r2.folddb.com/brain/releases/brain-${VERSION}.tar.gz" \
            --env dev \
            --key-file /tmp/dev-key.txt \
            --api-key "$LASTDB_API_KEY" \
            --json | tee release-result.json
          
          # Extract release ID
          RELEASE_ID=$(jq -r .release_id release-result.json)
          echo "release_id=$RELEASE_ID" >> $GITHUB_ENV
          
          # Update nightly channel
          lastdb app release-channel \
            --app-id brain \
            --channel nightly \
            --release-id "$RELEASE_ID" \
            --env dev \
            --key-file /tmp/dev-key.txt \
            --api-key "$LASTDB_API_KEY"
          
          # Clean up key
          rm /tmp/dev-key.txt
      
      - name: Verify release
        run: |
          # Check release is readable
          curl -sf "https://y0q3m6vk75.execute-api.us-west-2.amazonaws.com/v2/releases/${{ env.release_id }}" | jq .
          
          # Check channel points to release
          curl -sf "https://y0q3m6vk75.execute-api.us-west-2.amazonaws.com/v2/apps/brain/channels/nightly" | jq .
```

#### Using the Helper Script

Alternatively, use the provided helper script:

```yaml
      - name: Publish to /v2 registry (DEV)
        env:
          LASTDB_API_KEY: ${{ secrets.LASTDB_API_KEY }}
          LASTDB_DEV_KEY_FILE: ${{ secrets.LASTDB_DEV_KEY_FILE }}
          APP_ID: brain
          APP_UUID: ${{ secrets.BRAIN_APP_UUID }}
          MANIFEST_PATH: brain/lastdb-app.json
          ARTIFACT_PATH: brain.tar.gz
          ARTIFACT_URL: https://r2.folddb.com/brain/releases/brain-${{ env.VERSION }}.tar.gz
          SOURCE_COMMIT: ${{ github.sha }}
          REGISTRY_ENV: dev
          CHANNEL: nightly
        run: |
          # Write dev key to temp file
          echo "$LASTDB_DEV_KEY_FILE" > /tmp/dev-key.txt
          export LASTDB_DEV_KEY_FILE=/tmp/dev-key.txt
          
          # Download and run helper script
          curl -sL https://raw.githubusercontent.com/EdgeVector/lastdb/main/scripts/release/publish-app-release-v2.sh | bash
          
          # Clean up
          rm /tmp/dev-key.txt
```

### Step 3: Update Lockfile Generation

Ensure the lockfile is generated and committed before releases:

#### Option A: Manual update before release
```bash
cd brain
lastdb app check --sync --manifest lastdb-app.json
git add lastdb-app.json.lock.json
git commit -m "Update schema lockfile"
```

#### Option B: CI check (fails if lockfile is stale)
```yaml
      - name: Verify lockfile is current
        run: |
          lastdb app check --manifest brain/lastdb-app.json
          git diff --exit-code brain/lastdb-app.json.lock.json || {
            echo "❌ Lockfile is stale. Run 'lastdb app check --sync'"
            exit 1
          }
```

#### Option C: Auto-update in release workflow (risky)
```yaml
      - name: Generate lockfile
        run: |
          lastdb app check --sync --manifest brain/lastdb-app.json
```

**Recommendation**: Use Option A or B. Option C can silently publish with wrong schemas.

## Environment Strategy

### DEV-first rollout

1. **Phase 1: DEV only** (current)
   - All apps publish to DEV /v2
   - Verifier runs against DEV
   - No production impact

2. **Phase 2: PROD after validation**
   - After DEV validation passes (verifier can import compat rows)
   - Add PROD publisher secrets separately
   - Add `--env prod` flag or environment-conditional logic

### Conditional publishing

```yaml
      - name: Publish to /v2 registry
        env:
          LASTDB_API_KEY: ${{ secrets.LASTDB_API_KEY }}
          LASTDB_DEV_KEY_FILE: ${{ secrets.LASTDB_DEV_KEY_FILE }}
          # Use PROD secrets for tagged releases
          # LASTDB_API_KEY_PROD: ${{ secrets.LASTDB_API_KEY_PROD }}
          # LASTDB_DEV_KEY_FILE_PROD: ${{ secrets.LASTDB_DEV_KEY_FILE_PROD }}
        run: |
          if [[ "$GITHUB_REF" == refs/tags/* ]]; then
            ENV=prod
            API_KEY="$LASTDB_API_KEY_PROD"
            KEY_FILE="$LASTDB_DEV_KEY_FILE_PROD"
          else
            ENV=dev
            API_KEY="$LASTDB_API_KEY"
            KEY_FILE="$LASTDB_DEV_KEY_FILE"
          fi
          
          echo "$KEY_FILE" > /tmp/dev-key.txt
          
          lastdb app release-publish \
            --env "$ENV" \
            --api-key "$API_KEY" \
            --key-file /tmp/dev-key.txt \
            ...
```

## Troubleshooting

### Publisher key mismatch

**Error**: `403 Forbidden` or `unauthorized_publisher`

**Cause**: The dev key's verifying pubkey does not match the registered publisher

**Fix**:
1. Check registered publisher: `curl https://y0q3m6vk75.execute-api.us-west-2.amazonaws.com/v2/apps/brain | jq .owner_dev_pubkey`
2. Verify your key: Compute verifying key from `LASTDB_DEV_KEY_FILE`
3. If mismatch:
   - Use the correct dev key that was used to register the app, OR
   - Re-register the app with `POST /v2/apps` using the current dev key

### Missing or stale lockfile

**Error**: `unresolved_schema: schema 'brain/Concept' has no locked identity`

**Fix**: Run `lastdb app check --sync --manifest brain/lastdb-app.json` and commit the lockfile

### Invalid API key

**Error**: `401 Unauthorized` from DevCert mint

**Fix**: Obtain fresh key from https://exemem.com (paid plan or `developer_access` grant)

### Network errors

**Error**: Connection timeout or API Gateway 404

**Fix**:
- Verify DEV service is reachable: `curl https://y0q3m6vk75.execute-api.us-west-2.amazonaws.com/v2/apps/brain`
- Check `--env dev` flag is set
- Ensure workflow has internet access

## Next Steps

After integrating /v2 publishing:

1. **Run the workflow** to publish a test release to DEV
2. **Verify the release** is readable:
   ```bash
   curl https://y0q3m6vk75.execute-api.us-west-2.amazonaws.com/v2/releases/{release_id} | jq .
   ```
3. **Check the channel** points to the release:
   ```bash
   curl https://y0q3m6vk75.execute-api.us-west-2.amazonaws.com/v2/apps/{app_id}/channels/nightly | jq .
   ```
4. **Notify the verifier** that releases are available for compat row import
5. **Monitor verifier imports** in lastdb-verifier workflow runs

## Repository-Specific Checklists

### Brain (EdgeVector/last-stack)

- [ ] Add `LASTDB_API_KEY`, `LASTDB_DEV_KEY_FILE`, `BRAIN_APP_UUID` secrets
- [ ] Generate and commit `brain/lastdb-app.json.lock.json`
- [ ] Update `.github/workflows/release.yml` with /v2 publishing step
- [ ] Run workflow, verify release in DEV
- [ ] Confirm verifier can import compat rows

### LastSecrets (EdgeVector/lastsecrets)

- [ ] Add `LASTDB_API_KEY`, `LASTDB_DEV_KEY_FILE`, `LASTSECRETS_APP_UUID` secrets
- [ ] Generate and commit `lastsecrets.app.json.lock.json`
- [ ] Update release workflow with /v2 publishing step
- [ ] Run workflow, verify release in DEV
- [ ] Confirm verifier can import compat rows

### lastseek (EdgeVector/lastseek)

- [ ] Add `LASTDB_API_KEY`, `LASTDB_DEV_KEY_FILE`, `LASTSEEK_APP_UUID` secrets
- [ ] Generate and commit `lastseek.app.json.lock.json`
- [ ] Update release workflow with /v2 publishing step
- [ ] Run workflow, verify release in DEV
- [ ] Confirm verifier can import compat rows

## References

- [/v2 Registry Design](./designs/exemem-app-registry-simple.md)
- [App Install Sources Design](./design/app-install-sources.md)
- [Nightly Releases](./nightly-releases.md)
- [Proof Script](../scripts/proofs/app-release-v2-publish-dev-proof.sh)
- [Helper Script](../scripts/release/publish-app-release-v2.sh)
