# App Release /v2 Publishing Proof (DEV)

**Status**: Proof script is complete and validated (bash -n, shellcheck) but has NOT been executed against DEV. Execution requires credentials and a registered test app that are not available in the development environment.

**Required to run**:
- `LASTDB_API_KEY`: Developer API key from https://exemem.com
- `LASTDB_DEV_KEY_B64`: Base64-encoded Ed25519 private key (32-byte secret) that matches a registered app's publisher
- `TEST_APP_ID`: A registered test app in DEV /v2 (not brain, lastsecrets, or lastseek)
- `TEST_APP_UUID`: The registered UUID for the test app
- The test app must be registered in DEV with `POST /v2/apps` using the same dev key

**Goal**: Demonstrate publishing an app release to the /v2 app registry in DEV, enabling the verifier to import and sign compat rows.

**Context**: 
- The /v2 registry is live in DEV (schema-infra PR #477, #479)
- Apps are registered in /v2 but have ZERO releases published
- lastdb-verifier PR #4 found 9 apps registered but 0/54 compat rows importable
- This proof establishes the "publisher switch" for normal app release workflows

## Prerequisites

Each app release workflow needs:

1. **Developer API Key** (`LASTDB_API_KEY` or `EXEMEM_API_KEY` secret)
   - Obtain from: https://exemem.com (paid plan or developer_access grant)
   - Authorizes minting a short-TTL DevCert

2. **Developer Ed25519 Signing Key** (`LASTDB_DEV_KEY_B64` secret)
   - 32-byte base64-encoded Ed25519 private key
   - Generate: `dd if=/dev/urandom bs=32 count=1 2>/dev/null | base64`
   - The verifying key must match the registered publisher for the app

3. **App manifest with locked schemas** (`<manifest>.lock.json`)
   - Contains schema identities from `lastdb app check --sync`
   - Release manifest references these locked identities
   - Publish fails with `unresolved_schema` if any identity is missing

4. **Built artifact** (e.g. `brain.tar.gz`, `lastsecrets.wasm`)
   - Release manifest includes SHA-256 digest and signature
   - Installer verifies digest matches downloaded bytes

## DEV Environment URLs

```bash
export EXEMEM_ENV=dev  # or ENVIRONMENT=dev
# DEV schema service: https://y0q3m6vk75.execute-api.us-west-2.amazonaws.com
# DEV exemem API: https://ygyu7ritx8.execute-api.us-west-2.amazonaws.com
```

## Publishing Flow

### Step 1: Build the release manifest

```bash
# Load manifest, read artifact bytes, build release manifest
lastdb app release-publish \
  --manifest brain/lastdb-app.json \
  --app-uuid "01234567-89ab-cdef-0123-456789abcdef" \
  --source-commit f7e9c5aa3610de3d96e2c401afd84865cddbd857 \
  --artifact brain.tar.gz \
  --artifact-url "https://r2.folddb.com/apps/brain/releases/brain-0.23.3-2378.tar.gz" \
  --env dev \
  --key-file ~/.lastdb-dev-key \
  --api-key "$LASTDB_API_KEY" \
  --json
```

**What this does:**
1. Loads `brain/lastdb-app.json` manifest
2. Reads locked schema identities from `brain/lastdb-app.json.lock.json`
3. Computes SHA-256 digest of `brain.tar.gz` artifact bytes
4. Signs the digest with the dev key
5. Mints a short-TTL DevCert from DEV exemem API
6. Signs the release manifest with purpose `app_release_publish`
7. POSTs to `POST /v2/apps/brain/releases` in DEV
8. Returns the immutable `release_id` (SHA-256 of JCS canonical manifest)

**Output:**
```json
{
  "release_id": "abc123def456...",
  "app_id": "brain",
  "publisher_dev_pubkey": "...",
  "published_at": "2026-10-28T17:00:00Z",
  "manifest": {
    "app_id": "brain",
    "app_uuid": "01234567-89ab-cdef-0123-456789abcdef",
    "schemas": {
      "brain/Concept": "sha256:...",
      "brain/Decision": "sha256:...",
      "brain/Situation": "sha256:..."
    },
    "source_commit": "f7e9c5aa3610de3d96e2c401afd84865cddbd857",
    "artifact_digest": "sha256:...",
    "artifact_url": "https://r2.folddb.com/apps/brain/releases/brain-0.23.3-2378.tar.gz",
    "artifact_signature": "base64..."
  }
}
```

### Step 2: Point a channel at the release

```bash
# Set the 'nightly' channel to point at the published release
lastdb app release-channel \
  --app-id brain \
  --channel nightly \
  --release-id abc123def456... \
  --generation 0 \
  --env dev \
  --key-file ~/.lastdb-dev-key \
  --api-key "$LASTDB_API_KEY" \
  --json
```

**What this does:**
1. Reads the current channel (if it exists) to get the generation
2. Mints a DevCert
3. Signs the channel update with purpose `app_channel_set`
4. PUTs to `PUT /v2/apps/brain/channels/nightly` with generation check
5. Returns the new channel record with `generation + 1`

**Output:**
```json
{
  "app_id": "brain",
  "channel": "nightly",
  "release_id": "abc123def456...",
  "generation": 1,
  "updated_at": "2026-10-28T17:01:00Z"
}
```

### Step 3: Verify the release is readable

```bash
# Anonymous read: GET /v2/releases/{release_id}
curl https://y0q3m6vk75.execute-api.us-west-2.amazonaws.com/v2/releases/abc123def456... | jq .

# Anonymous read: GET /v2/apps/brain/channels/nightly
curl https://y0q3m6vk75.execute-api.us-west-2.amazonaws.com/v2/apps/brain/channels/nightly | jq .
```

## Integration with App Release Workflows

### Brain (EdgeVector/last-stack)

Add to Brain's release workflow (e.g. `.github/workflows/release.yml`):

```yaml
- name: Publish to /v2 registry (DEV)
  env:
    LASTDB_API_KEY: ${{ secrets.LASTDB_API_KEY }}
    LASTDB_DEV_KEY_B64: ${{ secrets.LASTDB_DEV_KEY_B64 }}
  run: |
    # Publish release
    lastdb app release-publish \
      --manifest brain/lastdb-app.json \
      --app-uuid "$(jq -r .app_uuid brain/lastdb-app.json)" \
      --source-commit "$GITHUB_SHA" \
      --artifact brain.tar.gz \
      --artifact-url "https://r2.folddb.com/apps/brain/releases/brain-$VERSION.tar.gz" \
      --env dev \
      --key-file <(echo "$LASTDB_DEV_KEY_B64") \
      --api-key "$LASTDB_API_KEY" \
      --json | tee release-result.json
    
    # Extract release_id
    RELEASE_ID=$(jq -r .release_id release-result.json)
    
    # Update nightly channel
    lastdb app release-channel \
      brain \
      --channel nightly \
      --release-id "$RELEASE_ID" \
      --env dev \
      --key-file <(echo "$LASTDB_DEV_KEY_B64") \
      --api-key "$LASTDB_API_KEY"
```

### LastSecrets (EdgeVector/lastsecrets)

Similar workflow, replacing:
- `brain` → `lastsecrets`
- Manifest path: `lastsecrets.app.json`
- Artifact: `lastsecrets.wasm` or similar

### lastseek (EdgeVector/lastseek)

Similar workflow, replacing:
- `brain` → `lastseek`
- Manifest path: `lastseek.app.json`
- Artifact: `lastseek` binary or archive

## Known Blockers

### 1. Publisher Key Mismatch

**Symptom**: POST /v2/apps/{app_id}/releases returns 403 or `unauthorized_publisher`

**Cause**: The dev key's verifying pubkey does NOT match the registered publisher for the app

**Fix**: 
- Check registered publisher: `GET /v2/apps/{app_id}` and read `owner_dev_pubkey`
- Ensure `LASTDB_DEV_KEY_FILE` secret derives the same verifying key
- Or re-register the app with the correct publisher key

### 2. Missing Lockfile

**Symptom**: Publish fails with `unresolved_schema`

**Cause**: Manifest schema has no locked identity in `<manifest>.lock.json`

**Fix**:
- Run `lastdb app check --sync` during development
- Commit the generated `<manifest>.lock.json` to the app repo
- Release workflow must include the lockfile

### 3. Missing API Key

**Symptom**: DevCert mint fails with 401 or "is this developer enrolled?"

**Cause**: `LASTDB_API_KEY` is missing or invalid

**Fix**:
- Obtain from https://exemem.com (paid plan or `developer_access` grant)
- Add as GitHub secret `LASTDB_API_KEY` or `EXEMEM_API_KEY`

### 4. Wrong Environment

**Symptom**: POST returns 404 or "Not Found" from API Gateway

**Cause**: Workflow is hitting PROD instead of DEV

**Fix**:
- Explicitly pass `--env dev` to all registry commands
- Or set `EXEMEM_ENV=dev` before calling lastdb

## Verification

After publishing, the verifier can:

1. **List releases**: `GET /v2/apps/brain/releases` (not yet implemented; use exact release_id)
2. **Read release**: `GET /v2/releases/{release_id}`
3. **Import compat rows**: POST /v2/releases/{release_id}/compat with verifier signature
4. **Re-verify**: Installer reads compat row, verifies verifier signature

## Next Steps

1. **Add publisher secrets to app repos**:
   - `LASTDB_API_KEY` (from https://exemem.com)
   - `LASTDB_DEV_KEY_FILE` (Ed25519 private key, base64)

2. **Update app release workflows**:
   - Add `/v2` publish step after building artifact
   - Set channel after successful publish
   - DEV only (use `--env dev`)

3. **Verify in DEV**:
   - Run modified workflow
   - Check `GET /v2/apps/{app_id}/channels/nightly` returns release_id
   - Confirm lastdb-verifier can import compat rows

4. **Extend to PROD** (separate decision):
   - After DEV validation passes
   - Requires separate PROD publisher secrets
   - Add `--env prod` flag or environment-conditional logic
