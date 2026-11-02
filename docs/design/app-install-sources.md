# App-LastDB contract and local install sources

**Written 2026-10-24 · Status: proposal, corrected (Tom verified lastseek exists)**

## 0. The goal: a firm contract between apps and LastDB

The app registry must be something Tom can rely on. This proposal establishes an **explicit CONTRACT between how apps access the database and the database itself**, so we can pin versions, upgrade LastDB, and know ahead of time which apps stay backward compatible.

The contract has **two surfaces**:
1. **HTTP API** (@lastdb/app-sdk for TypeScript): `POST /api/mutation`, `POST /api/query`, etc.
2. **Outbox/IndexChangeBatch format**: JSON files written to `{LASTDB_HOME}/apps/search/inbox/` for vector indexing apps

Each surface has its own version identifier, and apps declare which surface(s) they depend on.

**Secondary goal**: A first-class local/dev install source (`--from <path> --rev <sha> --dev`) that goes through the same install code as the registry path, so proofs and dogfooding run the real code.

## 1. How apps talk to LastDB today (inventory)

### 1.1 Brain (`EdgeVector/last-stack`, TypeScript, HTTP API)

**Location**: Brain lives in `EdgeVector/last-stack` (monorepo, alongside last-stack publisher). The manifest is `brain/lastdb-app.json`.

**Access surface**: **HTTP API**

**Routes**:
- **HTTP API over UDS**: Primary path is `~/.lastdb/data/folddb.sock` (Unix domain socket)
- **Schemas**: Declared via `lastdb app check --sync`, e.g. `brain/Concept`, `brain/Decision`, `brain/Situation`
- **CLI commands**: `brain init` (schema sync), `brain put concepts <id>`, `brain get concepts <id>`, `brain list`
- **Runtime**: Bun + TypeScript, uses a vendored subset of `@lastdb/app-sdk` primitives (see `lastdb_app_sdk/CONTRACT.md` convergence gap)
- **Routes**: `POST /api/mutation`, `POST /api/query`, `POST /api/app/search`, `GET /api/schemas`, `GET /api/system/auto-identity`

**Evidence**: `.github/workflows/app-install-smoke.yml:134-193` show Brain's full integration: `bun install`, `brain init`, `brain put/get`, schema list.

### 1.2 LastSecrets (`EdgeVector/lastsecrets`, TypeScript, HTTP API)

**Location**: `https://github.com/EdgeVector/lastsecrets` (public repo)

**Access surface**: **HTTP API**

**Routes**:
- **HTTP API over UDS**: Uses `@lastdb/app-sdk` (`src/lastdb.ts:11-13`)
- **Schemas**: Hash schema for secrets (`src/schema.ts:28-80`), plus search index schema
- **Manifest**: `lastsecrets.app.json` (root)
- **CLI**: `lastsecrets init`, `lastsecrets put <slug>`, `lastsecrets get <slug>`, `lastsecrets list`, `lastsecrets search`
- **Runtime**: Bun + TypeScript
- **Routes**: Same HTTP API as Brain, plus schema distribution registration (`POST /api/apps/register-for-distribution`, `POST /api/apps/verify-distribution-ready`)

**Evidence**:
- `lastsecrets/src/lastdb.ts` imports `@lastdb/app-sdk` (line 11)
- `lastsecrets/src/schema.ts` defines `lastSecretSchema` and `lastSecretIndexSchema` (Hash layout)
- `lastsecrets/src/cli.ts:72-78` calls `client.declareAppSchema()`

**Schemas**:
- `lastsecrets/LastSecret` (Hash, fields: `slug`, `label`, `provider`, `purpose`, `environment`, `secret_value`, timestamps)
- `lastsecrets/LastSecretIndex` (HashRange, for search without exposing raw secret values)

### 1.3 Lastseek (`EdgeVector/lastseek`, Rust, outbox contract)

**Location**: `https://github.com/EdgeVector/lastseek` (private repo, verified by Tom)

**Access surface**: **Outbox/IndexChangeBatch format** (NOT HTTP API)

**How it works**:
- **Rust, version 0.1.1** (`Cargo.toml`)
- **NO LastDB HTTP client dependency by design**
- **Consumes outbox sink**: Reads `IndexChangeBatch` JSON files from `{LASTDB_HOME}/apps/search/inbox/` (default `~/.lastdb/apps/search/inbox/`)
- **Per `src/ingest.rs`**: Advances a watermark over batch filenames, only `--consume` deletes files
- **Shares inbox with Search app** (both consume the same outbox)
- **Backfill**: Owning apps pipe JSONL into `lastseek add`
- **Manifest**: `lastseek.app.json` (lastseek PR #7)

**Evidence**:
- `lastseek/src/ingest.rs`: Consumes `IndexChangeBatch` JSON files from inbox
- `lastseek/Cargo.toml`: Version 0.1.1, Rust app
- `lastseek/lastseek.app.json`: Manifest

**Contract dependency**: Outbox format (IndexChangeBatch JSON schema), NOT HTTP API.

### 1.4 Search (`EdgeVector/search`, TypeScript, outbox contract)

**Location**: `https://github.com/EdgeVector/search` (public repo)

**Access surface**: **Outbox/IndexChangeBatch format** (NOT HTTP API)

**How it works**:
- **Semantic MiniLM vector index** for Brain, F-Kanban, and other EdgeVector apps
- **Processes `IndexChangeBatch` files** from `{LASTDB_HOME}/apps/search/inbox/` (same inbox as lastseek)
- **No direct LastDB HTTP client**: Does NOT use `@lastdb/app-sdk` or HTTP API routes
- **Runtime**: Bun + TypeScript + `@xenova/transformers`
- **CLI**: `search init`, `search query`, `search drain`, `search online-backfill`

**Evidence**:
- `search/README.md:6-17`: "Search owns a local regenerable MiniLM vector index for Brain, F-Kanban, and other EdgeVector apps. The LastDB kernel keeps thin contracts for durable records and index-change outbox; it does not ship embeddings in default `lastdbd`."
- `search/src/semantic.ts:1-80`: No `@lastdb/app-sdk` import; processes `IndexChangeBatch` types from inbox

**Contract dependency**: Outbox format (IndexChangeBatch JSON schema), NOT HTTP API.

### 1.5 Two contract surfaces

Apps depend on one or both surfaces:

#### Surface 1: HTTP API

**Apps**: Brain, LastSecrets

**Routes** (served by `lastdb_node`, implemented in `lastdb_host/src/route.rs`):
- `POST /api/mutation` (write one row)
- `POST /api/query` (read with filter)
- `POST /api/queries/batch` (multi-read, PR #1527+)
- `POST /api/app/search` (scoped search)
- `GET /api/schemas` (list loaded schemas)
- `GET /api/system/auto-identity` (owner identity)
- `GET /api/version` (node version + `api_version` handshake)

**Request/response grammar**: deny_unknown_fields, discriminated errors, row envelope with `{key, keyValue, fields, metadata}`

**Schema declaration**: `lastdb app check --sync` or programmatic `/api/apps/declare-schema`

**Capability/consent surface**: production nodes, gated by `X-App-Capability` header

**SDK**: `@lastdb/app-sdk` (TypeScript, `lastdb_app_sdk/` in-tree), codified in `CONTRACT.md`

**Version identifier**: `lastdb_api_version` (unsigned integer, already in `CompatRow` and `GET /api/version`)

#### Surface 2: Outbox/IndexChangeBatch format

**Apps**: lastseek, Search

**How it works**:
- LastDB writes `IndexChangeBatch` JSON files to `{LASTDB_HOME}/apps/search/inbox/` (one file per mutation)
- Apps consume files from the inbox (lastseek and Search share the same inbox)
- Watermark-based consumption (only `--consume` deletes files)

**IndexChangeBatch JSON schema** (from `fold_db/crates/core/src/db_operations/search_index/sink.rs`):

```json
{
  "format_version": 1,
  "schema_name": "brain/Concept",
  "searchable_fields": ["title", "content"],
  "changes": [
    {
      "mutation_id": "01JB3...",
      "kind": "upsert",
      "key_value": {
        "hash": null,
        "range": "concept-001"
      },
      "fields_and_values": {
        "id": "concept-001",
        "title": "Example Concept",
        "content": "..."
      }
    }
  ]
}
```

**Contract elements**:
- `format_version` field (bumped on breaking changes)
- Top-level keys: `schema_name`, `searchable_fields`, `changes`
- Each change: `mutation_id` (string), `kind` (enum: "upsert" | "tombstone"), `key_value` (`{hash: Option<String>, range: Option<String>}`), `fields_and_values` (HashMap<String, Value>)

**Inbox path**: `{LASTDB_HOME}/apps/search/inbox/` (configurable via `SEARCH_INBOX` env var)

**Version identifier**: `outbox_format_version` (unsigned integer, new field in `CompatRow` and manifest)

## 2. The contract identifiers: `lastdb_api_version` and `outbox_format_version`

### 2.1 Two version identifiers

**`lastdb_api_version`** (HTTP API surface):
- Governs HTTP request/response schemas, routes, error discriminators
- Unsigned integer
- Already in `CompatRow.lastdb_api_version` and `GET /api/version`

**`outbox_format_version`** (outbox surface):
- Governs IndexChangeBatch JSON schema, top-level keys, change types
- Unsigned integer
- New field: `CompatRow.outbox_format_version` and `GET /api/version`

**Apps declare which surface(s) they use**:
- Brain, LastSecrets: HTTP API only (`manifest.contract_surfaces = ["http"]`)
- lastseek, Search: Outbox only (`manifest.contract_surfaces = ["outbox"]`)
- Future apps could use both (`manifest.contract_surfaces = ["http", "outbox"]`)

### 2.2 Breaking versus additive changes

#### HTTP API surface

**Breaking changes** (require `lastdb_api_version` bump):
- Remove a route or an HTTP API key
- Change the type of a request or response field (e.g., string → number)
- Change error discriminators or status codes that apps branch on
- Change the semantics of an existing key (e.g., `filter.HashKey` now requires a prefix)
- Tighten validation (e.g., a manifest field that was optional is now required)

**Additive changes** (no version bump):
- Add a new optional request key (old clients omit it; the node defaults)
- Add a new response field (old clients ignore it)
- Add a new route (old clients don't call it)
- Add a new error kind (old clients treat it as `UnexpectedResponseError`)
- Relax validation (accept more inputs)

#### Outbox surface

**Breaking changes** (require `outbox_format_version` bump):
- Remove a top-level key from `IndexChangeBatch` (e.g., remove `schema_name`)
- Change the type of a key (e.g., `searchable_fields` Option<HashSet<String>> → Vec<String>)
- Remove a `kind` value (e.g., no more `"tombstone"`)
- Change the structure of `changes[]` items or `key_value`
- Change the inbox path convention

**Additive changes** (no version bump):
- Add a new optional top-level key (old consumers ignore it via serde default)
- Add a new `kind` value (old consumers that match exhaustively on "upsert" | "tombstone" would break, but consumers using serde typically handle unknown variants gracefully if they use a catch-all)
- Add new keys to `fields_and_values` (old consumers ignore them)

**Examples**:
- **Breaking (HTTP)**: Removing `GET /api/schemas` would break Brain's `brain init`. Bump `lastdb_api_version`.
- **Breaking (outbox)**: Renaming `changes` to `mutations` would break lastseek and Search. Bump `outbox_format_version`.
- **Additive (HTTP)**: Adding `POST /api/queries/batch` (PR #1527). No version bump.
- **Additive (outbox)**: Adding `"source_app_id"` field to `IndexChangeBatch`. No version bump.

### 2.3 How LastDB declares supported contract versions

The node declares **two supported ranges** with deprecation windows:

```rust
// lastdb_node/src/version.rs (or similar)
pub const CURRENT_API_VERSION: u32 = 2;
pub const MIN_SUPPORTED_API_VERSION: u32 = 1;

pub const CURRENT_OUTBOX_FORMAT_VERSION: u32 = 1;
pub const MIN_SUPPORTED_OUTBOX_FORMAT_VERSION: u32 = 1;
```

**Deprecation window**: Same rule for both surfaces. When a breaking change lands, bump `CURRENT_*_VERSION` but leave `MIN_SUPPORTED_*_VERSION` unchanged for at least two releases (e.g., 6 weeks).

## 3. SDK and manifest declaration

### 3.1 HTTP API surface: SDK with pinned contract version

**Use `@lastdb/app-sdk` as the contract carrier** for TypeScript apps (Brain, LastSecrets).

**SDK major version tracks `lastdb_api_version`**:
- `@lastdb/app-sdk@1.x.x` → `api_version: 1`
- `@lastdb/app-sdk@2.x.x` → `api_version: 2`

**Registry publisher** reads SDK version from `package-lock.json` and writes `lastdb_api_version` to `CompatRow`.

### 3.2 Outbox surface: manifest declaration

**Outbox apps** (lastseek, Search) declare `outbox_format_version` in their manifest:

```json
{
  "app_id": "lastseek",
  "version": "0.1.1",
  "contract_surfaces": ["outbox"],
  "outbox_format_version": 1,
  "schemas": []
}
```

**Registry publisher** reads `outbox_format_version` from manifest and writes it to `CompatRow.outbox_format_version`.

### 3.3 Manifest `contract_surfaces` field

New manifest field to declare which surface(s) an app uses:

```json
{
  "app_id": "brain",
  "contract_surfaces": ["http"],
  ...
}
```

```json
{
  "app_id": "lastseek",
  "contract_surfaces": ["outbox"],
  "outbox_format_version": 1,
  ...
}
```

**Values**:
- `["http"]`: HTTP API only (Brain, LastSecrets)
- `["outbox"]`: Outbox only (lastseek, Search)
- `["http", "outbox"]`: Both surfaces (future apps)

### 3.4 No Rust HTTP client crate (lastseek doesn't need it)

**Drop `lastdb_app_client`** as the solution for lastseek. Lastseek uses the outbox contract by design, not the HTTP API.

**If a future Rust app needs the HTTP API**, add `lastdb_app_client` then (wrapping `lastdb_host::FoldDbHttpClient` with contract version guards). Not part of this proposal.

## 4. Compatibility at install and upgrade time

### 4.1 Install: check before download

The index has `CompatRow.lastdb_api_version: Option<u32>` and `CompatRow.outbox_format_version: Option<u32>` (new). When resolving an app, the node checks:

```rust
// lastdb_node/src/app_registry_index/resolve.rs::pick_row()
pub fn pick_row<'a>(
    index: &'a RegistryIndex,
    app_id: &str,
    node_api_version: u32,
    node_min_supported_api_version: u32,
    node_outbox_format_version: u32,
    node_min_supported_outbox_format_version: u32,
) -> Result<(&'a IndexApp, &'a CompatRow), String> {
    let app = index.apps.iter().find(|a| a.app_id == app_id)
        .ok_or_else(|| format!("app '{app_id}' not in {} index", index.channel))?;

    let mut compatible: Vec<&CompatRow> = app
        .compat
        .iter()
        .filter(|row| {
            // Check HTTP API compatibility if row declares it
            if let Some(required) = row.lastdb_api_version {
                if required < node_min_supported_api_version || required > node_api_version {
                    return false;
                }
            }
            
            // Check outbox compatibility if row declares it
            if let Some(required) = row.outbox_format_version {
                if required < node_min_supported_outbox_format_version || required > node_outbox_format_version {
                    return false;
                }
            }
            
            // A row with no version fields is assumed compatible (old index)
            true
        })
        .collect();

    if compatible.is_empty() {
        let proved: Vec<String> = app.compat.iter()
            .filter_map(|r| {
                let mut parts = vec![];
                if let Some(v) = r.lastdb_api_version {
                    parts.push(format!("api_version {}", v));
                }
                if let Some(v) = r.outbox_format_version {
                    parts.push(format!("outbox_format_version {}", v));
                }
                if parts.is_empty() { None } else { Some(parts.join(", ")) }
            })
            .collect();
        return Err(format!(
            "no {} row for app '{}' is compatible with this node (api_version {}, outbox_format_version {}, supports api {}-{}, outbox {}-{}); proved with: {}",
            index.channel, app_id, node_api_version, node_outbox_format_version,
            node_min_supported_api_version, node_api_version,
            node_min_supported_outbox_format_version, node_outbox_format_version,
            if proved.is_empty() { "none".to_string() } else { proved.join("; ") }
        ));
    }

    // Prefer the newest proved_at, then app_version
    compatible.sort_by(|a, b| {
        a.proved_at.cmp(&b.proved_at)
            .then_with(|| a.app_version.cmp(&b.app_version))
    });

    let row = compatible.pop().unwrap();
    Ok((app, row))
}
```

### 4.2 Pre-upgrade check for installed apps (BLOCKS by default)

`lastdb app check-compat --preview-version <version>` checks **both** HTTP API and outbox compatibility:

```bash
# Before brew upgrade lastdb
lastdb app check-compat --preview-version 0.25.0

# Output when incompatible apps exist (exit code 1):
# Checking installed apps against lastdb 0.25.0 (api_version 2, supports 2-2; outbox_format_version 2, supports 1-2)...
# ✓ brain (http: api_version 2) - compatible
# ✓ lastsecrets (http: api_version 2) - compatible
# ✗ lastseek (outbox: outbox_format_version 1) - INCOMPATIBLE (requires outbox_format_version >= 2, but lastseek needs 1)
# ✗ search (outbox: outbox_format_version 1) - INCOMPATIBLE (requires outbox_format_version >= 2, but search needs 1)
# 
# ERROR: 2 installed app(s) would break with lastdb 0.25.0.
# 
# Options:
# 1. Upgrade incompatible apps: lastdb app upgrade lastseek search
# 2. Remove them: lastdb app uninstall lastseek search
# 3. Force the upgrade (apps will break): lastdb app check-compat --preview-version 0.25.0 --force
```

**Homebrew `pre_install` hook**: Same as before (§4.3 in previous draft), but the check now covers both surfaces.

## 5. How CI enforces the contract

### 5.1 Contract-diff check (FAILS CI on breaking changes without version bump or explicit additive marker)

Add `.github/workflows/contract-diff.yml` (runs on every PR):

**Purpose**: Detects breaking changes to **BOTH** contract surfaces (HTTP API and outbox) and **FAILS CI** unless:
1. Relevant `CURRENT_*_VERSION` was bumped, OR
2. The change is explicitly marked additive (via PR label `contract:additive` or contract changelog entry)

**Contract snapshot** (extended to include outbox):

```json
{
  "http_api": {
    "version": 1,
    "routes": [
      {
        "method": "POST",
        "path": "/api/mutation",
        "required_keys": ["schema_name", "mutation_type", "fields", "key"],
        "optional_keys": ["expected"]
      },
      {
        "method": "POST",
        "path": "/api/query",
        "required_keys": ["schema_name", "fields"],
        "optional_keys": ["filter", "limit", "offset"]
      }
    ],
    "error_kinds": [
      "missing_required_key",
      "unknown_key",
      "cas_conflict",
      "permission_denied",
      "capability_denied"
    ],
    "sdk_exports": [
      "connect",
      "ownerClient",
      "NodeTooOldError",
      "CasConflictError"
    ]
  },
  "outbox": {
    "version": 1,
    "schema": {
      "format_version": "integer",
      "batch_id": "string",
      "timestamp": "string (RFC3339)",
      "changes": {
        "type": "array",
        "items": {
          "operation": "string (create|update|delete)",
          "schema_name": "string",
          "key": "object",
          "fields": "object",
          "metadata": "object"
        }
      }
    },
    "inbox_path": "{LASTDB_HOME}/apps/search/inbox/"
  }
}
```

**Snapshot generation**:
- Parse `lastdb_host/src/route.rs` for HTTP routes
- Parse `lastdb_host/src/error.rs` for error kinds
- Parse `lastdb_app_sdk/src/client.ts` for SDK exports
- Parse outbox writer code (e.g., `lastdb_host/src/outbox.rs`) for `IndexChangeBatch` schema
- Generate combined JSON snapshot

**Snapshot storage**: CI artifact, NOT committed (per AGENTS.md generated-evidence rule).

**CI workflow** (extended):

```yaml
- name: Diff contract snapshots
  id: diff
  run: |
    python3 scripts/lints/diff-contract-snapshots.py \
      /tmp/contract-base.json \
      /tmp/contract-head.json \
      > /tmp/contract-diff.txt
    
    cat /tmp/contract-diff.txt
    
    # Read versions from base and head
    BASE_API_VERSION=$(jq -r '.http_api.version' /tmp/contract-base.json)
    HEAD_API_VERSION=$(jq -r '.http_api.version' /tmp/contract-head.json)
    BASE_OUTBOX_VERSION=$(jq -r '.outbox.version' /tmp/contract-base.json)
    HEAD_OUTBOX_VERSION=$(jq -r '.outbox.version' /tmp/contract-head.json)
    
    echo "base_api_version=$BASE_API_VERSION" >> $GITHUB_OUTPUT
    echo "head_api_version=$HEAD_API_VERSION" >> $GITHUB_OUTPUT
    echo "base_outbox_version=$BASE_OUTBOX_VERSION" >> $GITHUB_OUTPUT
    echo "head_outbox_version=$HEAD_OUTBOX_VERSION" >> $GITHUB_OUTPUT
    
    # Check if there are breaking changes (HTTP or outbox)
    if grep -q "BREAKING:" /tmp/contract-diff.txt; then
      echo "has_breaking=true" >> $GITHUB_OUTPUT
      
      # Determine which surface(s) have breaking changes
      if grep -q "BREAKING:.*http_api" /tmp/contract-diff.txt; then
        echo "breaking_http=true" >> $GITHUB_OUTPUT
      else
        echo "breaking_http=false" >> $GITHUB_OUTPUT
      fi
      
      if grep -q "BREAKING:.*outbox" /tmp/contract-diff.txt; then
        echo "breaking_outbox=true" >> $GITHUB_OUTPUT
      else
        echo "breaking_outbox=false" >> $GITHUB_OUTPUT
      fi
    else
      echo "has_breaking=false" >> $GITHUB_OUTPUT
      echo "breaking_http=false" >> $GITHUB_OUTPUT
      echo "breaking_outbox=false" >> $GITHUB_OUTPUT
    fi

- name: Check version bump or additive marker
  if: steps.diff.outputs.has_breaking == 'true'
  run: |
    BREAKING_HTTP=${{ steps.diff.outputs.breaking_http }}
    BREAKING_OUTBOX=${{ steps.diff.outputs.breaking_outbox }}
    
    BASE_API_VERSION=${{ steps.diff.outputs.base_api_version }}
    HEAD_API_VERSION=${{ steps.diff.outputs.head_api_version }}
    BASE_OUTBOX_VERSION=${{ steps.diff.outputs.base_outbox_version }}
    HEAD_OUTBOX_VERSION=${{ steps.diff.outputs.head_outbox_version }}
    
    FAIL=false
    
    # Check HTTP API version bump if HTTP breaking changes
    if [ "$BREAKING_HTTP" = "true" ]; then
      if [ "$HEAD_API_VERSION" -le "$BASE_API_VERSION" ]; then
        echo "❌ Breaking HTTP API changes without CURRENT_API_VERSION bump"
        FAIL=true
      else
        echo "✓ CURRENT_API_VERSION bumped: $BASE_API_VERSION → $HEAD_API_VERSION"
      fi
    fi
    
    # Check outbox version bump if outbox breaking changes
    if [ "$BREAKING_OUTBOX" = "true" ]; then
      if [ "$HEAD_OUTBOX_VERSION" -le "$BASE_OUTBOX_VERSION" ]; then
        echo "❌ Breaking outbox changes without CURRENT_OUTBOX_FORMAT_VERSION bump"
        FAIL=true
      else
        echo "✓ CURRENT_OUTBOX_FORMAT_VERSION bumped: $BASE_OUTBOX_VERSION → $HEAD_OUTBOX_VERSION"
      fi
    fi
    
    # If still failing, check for contract:additive label
    if [ "$FAIL" = "true" ]; then
      HAS_LABEL=$(gh pr view ${{ github.event.pull_request.number }} \
        --json labels --jq '.labels[].name' | grep -c "^contract:additive$" || true)
      
      if [ "$HAS_LABEL" -gt 0 ]; then
        echo "✓ PR labeled as contract:additive"
        exit 0
      fi
      
      # FAIL: Breaking changes without version bump or additive marker
      echo ""
      cat /tmp/contract-diff.txt
      echo ""
      echo "Options:"
      echo "1. If HTTP API is breaking: bump CURRENT_API_VERSION in lastdb_node/src/version.rs"
      echo "2. If outbox format is breaking: bump CURRENT_OUTBOX_FORMAT_VERSION in lastdb_node/src/version.rs"
      echo "3. If this is additive: add the 'contract:additive' label to this PR"
      exit 1
    fi
  env:
    GH_TOKEN: ${{ github.token }}
```

**New scripts**:
- `scripts/lints/generate-contract-snapshot.py`: Extended to parse outbox schema
- `scripts/lints/diff-contract-snapshots.py`: Extended to diff both surfaces

### 5.2 App install smoke test (extend with lastseek and Search)

**Changes to `app-install-smoke.yml`**:

1. Add lastseek and Search to the matrix:

```yaml
strategy:
  fail-fast: false
  matrix:
    app: [brain, lastsecrets, lastseek, search]
    lastdb_version: [current, v0.23.0]
```

2. Add lastseek-specific setup:

```yaml
- name: Set up ${{ matrix.app }} dependencies (lastseek)
  if: matrix.app == 'lastseek'
  run: |
    cd "$LASTDB_HOME/apps/${{ matrix.app }}/source"
    cargo build --locked --release
    cp target/release/lastseek "$HOME/.local/bin/" || cp target/release/lastseek /usr/local/bin/

- name: Initialize ${{ matrix.app }} (lastseek)
  if: matrix.app == 'lastseek'
  run: |
    # lastseek consumes outbox files; seed the inbox
    mkdir -p "$LASTDB_HOME/apps/search/inbox"
    
    # Generate a test IndexChangeBatch file
    cat > "$LASTDB_HOME/apps/search/inbox/test-batch-001.json" <<'EOF'
    {
      "format_version": 1,
      "batch_id": "test-${{ github.run_number }}",
      "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
      "changes": [
        {
          "operation": "create",
          "schema_name": "brain/Concept",
          "key": {"hash": null, "range": "test-concept-001"},
          "fields": {"id": "test-concept-001", "title": "Test Concept", "content": "Smoke test"},
          "metadata": {}
        }
      ]
    }
    EOF
    
    lastseek --version 2>&1 | tee "$PROOF_DIR/${{ matrix.app }}-version.txt"

- name: Run ${{ matrix.app }} health checks (lastseek)
  if: matrix.app == 'lastseek'
  run: |
    # Test consuming the inbox
    lastseek ingest --inbox "$LASTDB_HOME/apps/search/inbox" 2>&1 | tee "$PROOF_DIR/${{ matrix.app }}-ingest-test.txt"
    
    if [ $? -eq 0 ]; then
      echo "✓ lastseek ingest test passed"
    else
      echo "✗ lastseek ingest test failed"
      exit 1
    fi
```

3. Add Search-specific setup:

```yaml
- name: Set up ${{ matrix.app }} dependencies (search)
  if: matrix.app == 'search'
  run: |
    cd "$LASTDB_HOME/apps/${{ matrix.app }}/source"
    bun install --frozen-lockfile 2>&1 | tee "$PROOF_DIR/${{ matrix.app }}-deps-install.log"
    bun run build 2>&1 | tee "$PROOF_DIR/${{ matrix.app }}-build.log"

- name: Initialize ${{ matrix.app }} (search)
  if: matrix.app == 'search'
  run: |
    cd "$LASTDB_HOME/apps/${{ matrix.app }}/source"
    
    # Seed the inbox (same as lastseek test)
    mkdir -p "$LASTDB_HOME/apps/search/inbox"
    cat > "$LASTDB_HOME/apps/search/inbox/test-batch-search-001.json" <<'EOF'
    {
      "format_version": 1,
      "batch_id": "test-search-${{ github.run_number }}",
      "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
      "changes": [
        {
          "operation": "create",
          "schema_name": "brain/Concept",
          "key": {"hash": null, "range": "test-concept-search-001"},
          "fields": {"id": "test-concept-search-001", "title": "Search Test", "content": "Semantic search test"},
          "metadata": {}
        }
      ]
    }
    EOF
    
    search init 2>&1 | tee "$PROOF_DIR/${{ matrix.app }}-init.log"
    search --version 2>&1 | tee "$PROOF_DIR/${{ matrix.app }}-version.txt"

- name: Run ${{ matrix.app }} health checks (search)
  if: matrix.app == 'search'
  run: |
    cd "$LASTDB_HOME/apps/${{ matrix.app }}/source"
    
    # Test draining the inbox
    search drain 2>&1 | tee "$PROOF_DIR/${{ matrix.app }}-drain-test.txt"
    
    # Test semantic query
    search query "semantic search" --json 2>&1 | tee "$PROOF_DIR/${{ matrix.app }}-query-test.txt"
    
    if [ $? -eq 0 ]; then
      echo "✓ search test passed"
    else
      echo "✗ search test failed"
      exit 1
    fi
```

4. Record contract surface for each app:

```yaml
- name: Record app contract surfaces
  run: |
    cd "$PROOF_DIR"
    
    # Detect contract surfaces from manifest
    MANIFEST_PATH="$LASTDB_HOME/apps/${{ matrix.app }}/source/*/lastdb-app.json"
    if [ ! -f "$MANIFEST_PATH" ]; then
      MANIFEST_PATH="$LASTDB_HOME/apps/${{ matrix.app }}/source/lastdb-app.json"
    fi
    if [ ! -f "$MANIFEST_PATH" ]; then
      MANIFEST_PATH="$LASTDB_HOME/apps/${{ matrix.app }}/source/*.app.json"
    fi
    
    if [ -f "$MANIFEST_PATH" ]; then
      CONTRACT_SURFACES=$(cat "$MANIFEST_PATH" | jq -r '.contract_surfaces // ["unknown"]' | jq -r 'join(", ")')
      echo "App ${{ matrix.app }} uses contract surfaces: $CONTRACT_SURFACES" | tee app-contract-surfaces.txt
      
      # Extract version requirements
      if echo "$CONTRACT_SURFACES" | grep -q "http"; then
        API_VERSION=$(cat "$MANIFEST_PATH" | jq -r '.api_version // "from SDK"')
        echo "HTTP API version: $API_VERSION" | tee -a app-contract-surfaces.txt
      fi
      
      if echo "$CONTRACT_SURFACES" | grep -q "outbox"; then
        OUTBOX_VERSION=$(cat "$MANIFEST_PATH" | jq -r '.outbox_format_version // "unknown"')
        echo "Outbox format version: $OUTBOX_VERSION" | tee -a app-contract-surfaces.txt
      fi
    fi
    
    # Node's versions
    NODE_API_VERSION=$(lastdb --data-dir "$LASTDB_HOME" version --json 2>/dev/null | jq -r '.api_version' || echo "unknown")
    NODE_OUTBOX_VERSION=$(lastdb --data-dir "$LASTDB_HOME" version --json 2>/dev/null | jq -r '.outbox_format_version' || echo "unknown")
    echo "Node provides api_version: $NODE_API_VERSION" | tee node-api-version.txt
    echo "Node provides outbox_format_version: $NODE_OUTBOX_VERSION" | tee node-outbox-version.txt
```

**Purpose**: Proves that apps using HTTP API (Brain, LastSecrets) and apps using outbox (lastseek, Search) both work with the current `lastdb` build.

## 6. Local install source (`--from` / `--rev` / `--dev`)

Same as previous draft (§6 in corrected version), with one addition:

**Manifest location convention** now includes `lastseek.app.json`:

```rust
fn find_manifest(source: &Path, app_id: &str) -> Result<PathBuf, String> {
    let candidates = [
        source.join(app_id).join("lastdb-app.json"),       // brain/lastdb-app.json
        source.join("lastdb-app.json"),                    // lastdb-app.json
        source.join(format!("{}.app.json", app_id)),       // lastsecrets.app.json, lastseek.app.json
    ];
    
    for path in &candidates {
        if path.exists() {
            return Ok(path.clone());
        }
    }
    
    Err(format!(
        "manifest not found: tried {}/lastdb-app.json, {}, and {}.app.json",
        app_id,
        source.join("lastdb-app.json").display(),
        app_id
    ))
}
```

## 7. Migration (PR sequence)

### 7.1 What to do with PR #357

**Merge as-is** after this design is approved. (Same rationale as before)

### 7.2 PR-sized sequence (updated)

Implement in this order:

#### PR 1: `EdgeVector/lastdb` — Add contract version infrastructure

- Add `lastdb_node/src/version.rs` with:
  - `CURRENT_API_VERSION: u32` and `MIN_SUPPORTED_API_VERSION: u32`
  - `CURRENT_OUTBOX_FORMAT_VERSION: u32` and `MIN_SUPPORTED_OUTBOX_FORMAT_VERSION: u32`
- Extend `GET /api/version` to report all four constants
- Add to `AppManifest`:
  - `pub contract_surfaces: Vec<String>` (e.g., `["http"]`, `["outbox"]`, `["http", "outbox"]`)
  - `pub api_version: Option<u32>` (fallback for no-SDK HTTP apps)
  - `pub outbox_format_version: Option<u32>` (for outbox apps)
- Add to `CompatRow`:
  - `pub outbox_format_version: Option<u32>` (new field)
- Update `CompatRow` doc comment to note both version identifiers
- No breaking change; old indexes still work
- Open the PR, reference this design, merge

**Title**: "Add contract version infrastructure (api_version + outbox_format_version)"

#### PR 2: `EdgeVector/lastdb` — Use contract versions in `pick_row()`

- Update `resolve.rs::pick_row()` to check both `CompatRow.lastdb_api_version` and `CompatRow.outbox_format_version` compatibility
- Pass all four version constants to `pick_row()`
- Update error messages to show both version mismatches
- Defense-in-depth: keep the post-clone manifest check from PR #357 (extend it to check fallback `manifest.api_version` and `manifest.outbox_format_version` if present)
- Open the PR, reference this design, merge

**Title**: "Use contract versions for app compatibility (api + outbox)"

#### PR 3: `EdgeVector/lastdb` — Local install source (`--from` / `--rev` / `--dev`)

- Add `IndexLocation::Local` to `location.rs`
- Add `resolve_local()` to `resolve.rs` (reads manifest at `--rev` via `git show`, checks both contract versions)
- Add `--from`, `--rev`, and `--dev` flags to CLI
- Guard `--from` behind `--dev`
- Update install receipt to record `"source": "dev"`, `"channel": "dev"`, `"trust": "dev"`
- Add manifest location convention: try `<app-id>/lastdb-app.json`, `lastdb-app.json`, `<app-id>.app.json`
- Open the PR, reference this design, merge

**Title**: "Add local install source (--from/--rev/--dev)"

#### PR 4: `EdgeVector/lastdb` — CI: contract-diff check

- Add `.github/workflows/contract-diff.yml` (FAILS on breaking changes without version bump or `contract:additive` label)
- Add `scripts/lints/generate-contract-snapshot.py` (parses HTTP API + outbox schema to generate JSON snapshot)
- Add `scripts/lints/diff-contract-snapshots.py` (compares snapshots for both surfaces, outputs `BREAKING:` or `ADDITIVE:` lines)
- Snapshot is CI artifact, not committed
- Open the PR, reference this design, merge

**Title**: "CI: contract-diff check (HTTP API + outbox, fails on breaking changes)"

#### PR 5: `EdgeVector/lastdb` — CI: extend app-install-smoke test

- Extend `.github/workflows/app-install-smoke.yml` to test multiple `lastdb` versions (current + v0.23.0)
- Add LastSecrets, lastseek, and Search to the app matrix (alongside Brain)
- Add contract surface evidence to smoke test artifacts and summary
- Add app-specific setup and health checks for each app
- Open the PR, reference this design, merge

**Title**: "CI: extend app-install-smoke with all apps (Brain, LastSecrets, lastseek, Search)"

#### PR 6: `EdgeVector/lastdb` — Pre-upgrade compatibility check

- Add `lastdb app check-compat --preview-version <version>` command
- Reads install receipts, resolves each app's contract surface(s) and version(s), checks against preview version's `MIN_SUPPORTED_*`
- **BLOCKS by default (exit 1)** when incompatible apps exist (HTTP or outbox)
- `--force` flag to bypass (exit 0, warning only)
- Open the PR, reference this design, merge

**Title**: "Add pre-upgrade app compatibility check (blocks for HTTP + outbox)"

#### PR 7: `EdgeVector/homebrew-lastdb` — Homebrew pre-install hook

- Add `pre_install` hook to `Formula/lastdb.rb` that runs `lastdb app check-compat --preview-version <new_version>`
- Hook aborts upgrade if check exits 1 (incompatible apps on either surface)
- User can override with `brew install --force lastdb`
- Open the PR, reference this design, merge

**Title**: "Add pre-install compatibility check to Homebrew formula"

#### PR 8: `EdgeVector/last-stack` — Publisher plumbs contract versions to compat rows

- Update the registry publisher (in `last-stack`) to:
  - Read `contract_surfaces` from manifest
  - For HTTP apps: read `@lastdb/app-sdk` version from `package-lock.json`, map to `api_version`
  - For outbox apps: read `outbox_format_version` from manifest
  - Write `"lastdb_api_version": <int>` and/or `"outbox_format_version": <int>` to `CompatRow` based on surfaces
- Publish the next index with contract versions
- Open the PR, reference this design, merge

**Title**: "Registry publisher: plumb contract versions (api + outbox) to compat rows"

#### PR 9: `EdgeVector/homebrew-lastdb` — Registry update (automatic, from publisher)

- The publisher (PR 8) writes the next index with contract versions
- Commit and push the new `registry/stable.json` and `registry/stable.json.sig`
- At this point, all new installs check contract compatibility before download

#### PR 10: `EdgeVector/last-stack` — Use local install in Brain CI (optional)

- Add `.github/workflows/brain-lastdb-compat.yml` (tests Brain against multiple `lastdb` versions using `--from` / `--dev`)
- Open the PR, reference this design, merge

**Title**: "CI: Brain compatibility matrix using local install"

#### PR 11: `EdgeVector/lastsecrets` — Use local install in LastSecrets CI (optional)

- Add `.github/workflows/lastsecrets-lastdb-compat.yml` (tests LastSecrets against multiple `lastdb` versions using `--from` / `--dev`)
- Open the PR, reference this design, merge

**Title**: "CI: LastSecrets compatibility matrix using local install"

#### PR 12: `EdgeVector/lastseek` — Use local install in lastseek CI (optional)

- Add `.github/workflows/lastseek-lastdb-compat.yml` (tests lastseek against multiple `lastdb` versions using `--from` / `--dev`)
- Tests outbox contract compatibility
- Open the PR, reference this design, merge

**Title**: "CI: lastseek compatibility matrix using local install (outbox contract)"

#### PR 13: `EdgeVector/search` — Use local install in search CI (optional)

- Add `.github/workflows/search-lastdb-compat.yml` (tests Search against multiple `lastdb` versions using `--from` / `--dev`)
- Tests outbox contract compatibility
- Open the PR, reference this design, merge

**Title**: "CI: Search compatibility matrix using local install (outbox contract)"

### 7.3 Rollout

**No feature flag needed.** The changes are additive:

- `CompatRow.lastdb_api_version` and `CompatRow.outbox_format_version` are optional. Old indexes work.
- `--from` / `--dev` is new surface. Existing installs are unaffected.
- The contract checks in `pick_row()` only reject when a row has version fields set and the node is outside the supported range.

**No migration for existing installs.** Installed apps keep working. The next `lastdb app upgrade` uses the new resolve logic if the registry has contract versions.

## 8. Summary

This proposal establishes a **firm contract between apps and LastDB with two surfaces**:

1. **HTTP API surface**: `lastdb_api_version`, for apps like Brain and LastSecrets that use `@lastdb/app-sdk`
2. **Outbox surface**: `outbox_format_version`, for apps like lastseek and Search that consume `IndexChangeBatch` files

**Contract identifiers**:
- `lastdb_api_version` (HTTP API)
- `outbox_format_version` (outbox format)
- Apps declare which surface(s) they use via `manifest.contract_surfaces`

**Breaking vs additive**: Defined for both surfaces (§2.2). **CI FAILS** when either surface changes without a version bump or explicit `contract:additive` label. Contract surface captured as a generated JSON snapshot (both HTTP and outbox), stored as CI artifact.

**Install/upgrade checks contract compatibility** in `pick_row()` BEFORE download (both surfaces). Remove exact `lastdb_version` string match.

**Pre-upgrade check** (`lastdb app check-compat --preview-version`) **BLOCKS by default** (exit 1) if installed apps would break on either surface (HTTP or outbox). Homebrew `pre_install` hook runs the check and aborts upgrade unless `brew install --force lastdb`.

**Local install source** (`--from <path> --rev <sha> --dev`) for dev, CI, and proofs. Always require `--rev`. Read manifest at `--rev` using `git show`. Production defaults stay signed-only.

**CI enforcement**:
- Contract-diff check (FAILS on breaking changes to HTTP API or outbox format)
- Extended app-install-smoke test (Brain, LastSecrets, lastseek, Search; multiple `lastdb` versions; contract surface evidence)
- Per-app compat matrix (app repos test against supported `lastdb` versions using local install)

All proof-based, no unit test suites.

**Inventory**:
- **Brain** (TypeScript, HTTP API, `@lastdb/app-sdk` vendored subset, `brain/lastdb-app.json`)
- **LastSecrets** (TypeScript, HTTP API, `@lastdb/app-sdk`, `lastsecrets.app.json`)
- **lastseek** (Rust, outbox, consumes `IndexChangeBatch` from inbox, `lastseek.app.json`)
- **Search** (TypeScript, outbox, consumes `IndexChangeBatch` from inbox)

**Migration**: Merge PR #357 as-is (defense-in-depth). Implement this in 13 PRs across `lastdb`, `last-stack`, `homebrew-lastdb`, `lastsecrets`, `lastseek`, and `search`.

**Result**: Tom can rely on the app registry. Apps declare the contract surface(s) and version(s) they need (via SDK or manifest), the node checks both surfaces before download, pre-upgrade check blocks incompatible upgrades on both surfaces, and CI proves both contracts hold.
