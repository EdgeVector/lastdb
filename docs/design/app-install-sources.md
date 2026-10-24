# App-LastDB contract and local install sources

**Written 2026-10-24 · Status: proposal, third pass (approved)**

## 0. The goal: a firm contract between apps and LastDB

The app registry must be something Tom can rely on. This proposal establishes an **explicit CONTRACT between how apps access the database and the database itself**, so we can pin versions, upgrade LastDB, and know ahead of time which apps stay backward compatible.

The contract is:
- An SDK (@lastdb/app-sdk for TypeScript, lastdb_app_client for Rust) or HTTP API surface that apps depend on
- A versioned contract identifier (`lastdb_api_version`) that governs compatibility
- Rules for what counts as a breaking versus an additive change
- A pre-upgrade check that blocks (with override) if installed apps would break
- CI proof that the contract holds, with generated snapshots and evidence in job summaries

**Secondary goal**: A first-class local/dev install source (`--from <path> --rev <sha> --dev`) that goes through the same install code as the registry path, so proofs and dogfooding run the real code.

## 1. How apps talk to LastDB today (inventory)

### 1.1 Brain (`EdgeVector/last-stack`, TypeScript)

**Location**: Brain lives in `EdgeVector/last-stack` (monorepo, alongside last-stack publisher). The manifest is `brain/lastdb-app.json`.

**Access surface**:
- **HTTP API over UDS**: Primary path is `~/.lastdb/data/folddb.sock` (Unix domain socket)
- **Schemas**: Declared via `lastdb app check --sync`, e.g. `brain/Concept`, `brain/Decision`, `brain/Situation`
- **CLI commands**: `brain init` (schema sync), `brain put concepts <id>`, `brain get concepts <id>`, `brain list`
- **Runtime**: Bun + TypeScript, uses a vendored subset of `@lastdb/app-sdk` primitives (see `lastdb_app_sdk/CONTRACT.md` convergence gap)
- **Routes**: `POST /api/mutation`, `POST /api/query`, `POST /api/app/search`, `GET /api/schemas`, `GET /api/system/auto-identity`

**Evidence**: `.github/workflows/app-install-smoke.yml:134-193` show Brain's full integration: `bun install`, `brain init`, `brain put/get`, schema list.

### 1.2 LastSecrets (`EdgeVector/lastsecrets`, TypeScript)

**Location**: `https://github.com/EdgeVector/lastsecrets` (public repo)

**Access surface**:
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

### 1.3 Search (`EdgeVector/search`, TypeScript)

**Location**: `https://github.com/EdgeVector/search` (public repo)

**Access surface**:
- **Vector plane, NOT LastDB API**: Search is a semantic MiniLM vector index app that processes `IndexChangeBatch` files from `{LASTDB_HOME}/apps/search/inbox/` (see `search/README.md:6-17`)
- **No direct LastDB client**: Does NOT use `@lastdb/app-sdk` or HTTP API routes
- **Runtime**: Bun + TypeScript + `@xenova/transformers`
- **CLI**: `search init`, `search query`, `search drain`, `search online-backfill`

**Evidence**:
- `search/README.md:1-25`: "Search owns a local regenerable MiniLM vector index for Brain, F-Kanban, and other EdgeVector apps. The LastDB kernel keeps thin contracts for durable records and index-change outbox; it does not ship embeddings in default `lastdbd`."
- `search/src/semantic.ts:1-80`: No `@lastdb/app-sdk` import; processes `IndexChangeBatch` types from inbox

**Status**: Search is NOT part of the app-LastDB contract. It's a vector plane companion to LastDB, not a client of the HTTP API. It will NOT be in the smoke canary matrix.

### 1.4 Common contract surface (Brain and LastSecrets)

Both Brain and LastSecrets depend on:

1. **HTTP API routes** (served by `lastdb_node`, implemented in `lastdb_host/src/route.rs`):
   - `POST /api/mutation` (write one row)
   - `POST /api/query` (read with filter)
   - `POST /api/queries/batch` (multi-read, PR #1527+)
   - `POST /api/app/search` (scoped search)
   - `GET /api/schemas` (list loaded schemas)
   - `GET /api/system/auto-identity` (owner identity)
   - `GET /api/version` (node version + `api_version` handshake)

2. **Request/response grammar** (deny_unknown_fields, discriminated errors, row envelope with `{key, keyValue, fields, metadata, authorPubKey}`)

3. **Schema declaration** (`lastdb app check --sync` or programmatic `/api/apps/declare-schema`)

4. **Capability/consent surface** (production nodes, gated by `X-App-Capability` header)

5. **@lastdb/app-sdk** (TypeScript, `lastdb_app_sdk/` in-tree):
   - Primary contract document: `lastdb_app_sdk/CONTRACT.md`
   - Types: `connect()`, `query()`, `mutate()`, `search()`, `queryBatch()`
   - Error taxonomy: `UnknownAppError`, `PermissionDeniedError`, `CapabilityDeniedError`, `CasConflictError`, `NodeTooOldError`
   - Version handshake: `requireApiVersion` parameter, throws `NodeTooOldError` when node is older

**The contract is already partially codified** in `lastdb_app_sdk/CONTRACT.md` (lines 1-220): the app-facing primitives, machine-actionable error taxonomy, and why route changes break in one place.

### 1.5 Future Rust apps

**Note**: Lastseek was mentioned in earlier drafts but does not exist as `EdgeVector/lastseek`. Future Rust apps (if any) will use a Rust client crate (`lastdb_app_client`, see §3.4).

## 2. The contract identifier: `lastdb_api_version`

### 2.1 What it is

`lastdb_api_version` is an unsigned integer that identifies the node's **request grammar version**. It governs:
- HTTP API request/response schemas (new keys, removed keys, type changes)
- Route availability (new routes, removed routes, moved endpoints)
- Error discriminators (new error kinds, changed error shapes)

**Current state**:
- The node serves `GET /api/version → {ok, api_version, build, capabilities, instance_id}`
- `@lastdb/app-sdk` reads it in `connect({ requireApiVersion })` and throws `NodeTooOldError` when the node is older than the SDK needs
- `CompatRow` already has an optional `lastdb_api_version` field (`lastdb_node/src/app_registry_index/signing.rs:32`)

**This proposal**: Build on `lastdb_api_version` as the **primary contract identifier**, not a parallel system. The index carries it, apps declare it, and the node enforces it.

### 2.2 Breaking versus additive changes

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

**Examples**:
- **Breaking**: Removing `GET /api/schemas` would break Brain's `brain init`. Bump `lastdb_api_version`.
- **Breaking**: Changing `POST /api/query` to require a new `version: <int>` key. Bump `lastdb_api_version`.
- **Additive**: Adding `POST /api/queries/batch` (PR #1527). No version bump; old clients keep using `POST /api/query`.
- **Additive**: Adding `QueryRow.schemaDisplayName: string | null`. No version bump; old clients ignore it.

### 2.3 How LastDB declares supported contract versions

The node declares a **supported range** with a deprecation window:

```rust
// lastdb_node/src/version.rs (or similar)
pub const CURRENT_API_VERSION: u32 = 2;
pub const MIN_SUPPORTED_API_VERSION: u32 = 1;
```

- `CURRENT_API_VERSION`: The grammar this build implements.
- `MIN_SUPPORTED_API_VERSION`: The oldest grammar version still accepted. Clients needing `< MIN_SUPPORTED_API_VERSION` are refused with `410 Gone` or `NodeTooNewError`.

**Deprecation window**: When a breaking change lands, bump `CURRENT_API_VERSION` but leave `MIN_SUPPORTED_API_VERSION` unchanged for at least two releases (e.g., 6 weeks). This gives apps time to update. After the window, bump `MIN_SUPPORTED_API_VERSION` to retire the old grammar.

**Example timeline**:
- **v0.23.0**: `CURRENT_API_VERSION = 1`, `MIN_SUPPORTED = 1` (only v1)
- **v0.24.0**: `CURRENT_API_VERSION = 2`, `MIN_SUPPORTED = 1` (v1 and v2 both work)
- **v0.25.0**: `CURRENT_API_VERSION = 2`, `MIN_SUPPORTED = 2` (v1 is retired)

Apps using v1 can upgrade to SDK v2 anytime between v0.24.0 and v0.25.0.

## 3. SDK crate/package versus manifest declaration

### 3.1 The question

Should apps:
1. Depend on a thin SDK that pins the contract version (e.g., `@lastdb/app-sdk@2.0.0` → `requireApiVersion: 2`), or
2. Declare the contract version in the manifest (`lastdb-app.json` `"api_version": 2`) with no SDK dependency?

### 3.2 Recommendation: SDK with pinned contract version

**Use the SDK as the contract carrier.**

**Reasons**:
1. **The SDK already does this.** `@lastdb/app-sdk` has `requireApiVersion` and `LASTDB_SDK_API_VERSION` (pinned to the node's Rust constant by a test, per `CONTRACT.md:107`). Brain and LastSecrets already depend on it (or should).
2. **Type safety.** TypeScript apps get compile-time errors when they use a removed API. Manifest-only apps discover breakage at runtime.
3. **Error handling.** The SDK maps every HTTP failure to a typed error (`NodeTooOldError`, `CapabilityDeniedError`, `CasConflictError`). Apps branch on discriminators, not status codes or free text.
4. **One update.** When the contract changes, apps `npm install @lastdb/app-sdk@3.0.0` and fix compilation errors. No separate manifest edit.
5. **Rust apps.** Future Rust apps depend on a Rust SDK crate (see §3.4).

**What the manifest carries**:
- `"app_id"`, `"version"`, `"schemas"`, `"run"`, `"uses"` (existing fields)
- **No `"api_version"` key**. The SDK version implies it.

**What the index carries**:
- `CompatRow.lastdb_api_version: Option<u32>` (already present). The registry publisher reads the SDK version from the app's lockfile (`package-lock.json` `@lastdb/app-sdk` version → `api_version` via a version map) and writes it to the compat row.

**Fallback for no-SDK apps**: If an app uses raw HTTP with no SDK, it can declare `"api_version": <int>` in the manifest. The publisher reads it from there. This is the escape hatch for non-TypeScript/Rust apps (e.g., Python, Go).

### 3.3 SDK versioning convention

**Major version tracks `lastdb_api_version`**:
- `@lastdb/app-sdk@1.x.x` → `api_version: 1`
- `@lastdb/app-sdk@2.x.x` → `api_version: 2`

**Minor/patch versions** are additive fixes or new helpers (no contract change).

### 3.4 Rust client crate (part of the plan, not future work)

**Rust apps** (if any emerge) will depend on `lastdb_app_client`, a thin Rust crate that:
- Wraps `lastdb_host::FoldDbHttpClient` (the existing Rust HTTP client over UDS)
- Pins `api_version` (const `CURRENT_API_VERSION: u32`)
- Provides typed errors (`NodeTooOldError`, `PermissionDeniedError`, etc.)
- Follows the same major-version-tracks-api-version convention (`lastdb_app_client 2.x.x` → `api_version: 2`)

**Implementation**:
- Create `lastdb_app_client/` crate in `lastdb` monorepo
- Re-export `lastdb_host::FoldDbHttpClient` with contract version guards
- Add version handshake wrapper (`check_api_version()` calls `GET /api/version` and returns `Result<(), NodeTooOldError>`)
- Document in `lastdb_app_client/CONTRACT.md` (Rust equivalent of `lastdb_app_sdk/CONTRACT.md`)

**Migration PR**: See §7.2, PR 9.

**Note**: Lastseek does not exist as `EdgeVector/lastseek`. If a Rust app emerges, it will use `lastdb_app_client`.

## 4. Compatibility at install and upgrade time

### 4.1 Install: check before download

The index already has `CompatRow.lastdb_api_version: Option<u32>`. When resolving an app, the node checks:

```rust
// lastdb_node/src/app_registry_index/resolve.rs::pick_row()
pub fn pick_row<'a>(
    index: &'a RegistryIndex,
    app_id: &str,
    node_api_version: u32,
    node_min_supported_api_version: u32,
) -> Result<(&'a IndexApp, &'a CompatRow), String> {
    let app = index.apps.iter().find(|a| a.app_id == app_id)
        .ok_or_else(|| format!("app '{app_id}' not in {} index", index.channel))?;

    let mut compatible: Vec<&CompatRow> = app
        .compat
        .iter()
        .filter(|row| {
            // Check api_version compatibility BEFORE selection
            if let Some(required) = row.lastdb_api_version {
                if required < node_min_supported_api_version || required > node_api_version {
                    return false; // Too old or too new
                }
            }
            // A row with no lastdb_api_version is assumed compatible (old index)
            true
        })
        .collect();

    if compatible.is_empty() {
        let proved: Vec<String> = app.compat.iter()
            .filter_map(|r| r.lastdb_api_version.map(|v| format!("api_version {}", v)))
            .collect();
        return Err(format!(
            "no {} row for app '{}' is compatible with this node (api_version {}, supports {}-{}); proved with: {}",
            index.channel, app_id, node_api_version, node_min_supported_api_version, node_api_version,
            if proved.is_empty() { "none".to_string() } else { proved.join(", ") }
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

**Key change from earlier draft**: Remove the exact `lastdb_version` string match. A node on `0.23.2` can install an app proved on `0.23.0` if the `api_version` is compatible. The node's build string (`lastdb --version`) is metadata, not a gate.

### 4.2 Upgrade: check before download, defense-in-depth after clone

`upgrade_pinned()` calls `pick_row()`, which rejects incompatible rows before any git operation.

**Defense-in-depth**: After checkout, read the manifest's fallback `"api_version"` field (if present) and check it against the node's supported range. This catches:
- An index publisher bug (wrong `lastdb_api_version` in the compat row)
- A local `--from` install where the manifest was edited after the commit

The post-clone check is PR #357's version constraint guard, extended to `api_version`.

### 4.3 Pre-upgrade check for installed apps (BLOCKS by default)

**Problem**: When LastDB upgrades (e.g., `brew upgrade lastdb`), installed apps might break if the new node's `MIN_SUPPORTED_API_VERSION` is higher than what they need.

**Solution**: A pre-upgrade check that **BLOCKS by default** with an explicit `--force` override.

**Implementation**: Add `lastdb app check-compat --preview-version <version>`:

```bash
# Before brew upgrade lastdb
lastdb app check-compat --preview-version 0.25.0

# Output when incompatible apps exist (exit code 1):
# Checking installed apps against lastdb 0.25.0 (api_version 2, supports 2-2)...
# ✓ brain (api_version 2) - compatible
# ✗ old-app (api_version 1) - INCOMPATIBLE (requires api_version >= 2, but old-app needs 1)
# 
# ERROR: 1 installed app(s) would break with lastdb 0.25.0.
# 
# Options:
# 1. Upgrade incompatible apps: lastdb app upgrade old-app
# 2. Remove them: lastdb app uninstall old-app
# 3. Force the upgrade (apps will break): lastdb app check-compat --preview-version 0.25.0 --force

# With --force (exit code 0, warning only):
# WARNING: 1 installed app(s) would break with lastdb 0.25.0, but --force was passed.
```

**Homebrew integration**:

The `lastdb` brew formula runs the check in a **pre-install hook**:

```ruby
# homebrew-lastdb/Formula/lastdb.rb
class Lastdb < Formula
  # ...

  def install
    # Build binaries
    system "cargo", "build", "--locked", "--release", "-p", "lastdb_node"
    bin.install "target/release/lastdb"
    bin.install "target/release/lastdbd"
  end

  def pre_install
    # Only run if upgrading (not fresh install)
    return unless File.exist?("#{ENV['HOME']}/.lastdb/apps")
    
    # Get the version we're about to install
    new_version = version.to_s
    
    # Run the check with the old lastdb binary (still in PATH)
    system "#{HOMEBREW_PREFIX}/bin/lastdb", "app", "check-compat", "--preview-version", new_version
    
    if $?.exitstatus != 0
      odie <<~EOS
        One or more installed LastDB apps are incompatible with lastdb #{new_version}.
        
        Options:
        1. Upgrade incompatible apps first: lastdb app upgrade <app_id>
        2. Remove them: lastdb app uninstall <app_id>
        3. Force the upgrade (apps will break): brew install --force lastdb
      EOS
    end
  end
end
```

**When the check runs**:
1. User runs `brew upgrade lastdb`
2. Homebrew downloads the new formula
3. `pre_install` runs **before** the new binaries are installed
4. The check uses the **old** `lastdb` binary (still in PATH) to query installed apps
5. If incompatible apps exist, the check **exits 1** and Homebrew aborts with the error message
6. User upgrades or removes incompatible apps, then retries `brew upgrade lastdb`
7. User can override with `brew install --force lastdb` (Homebrew's `--force` bypasses the check)

**Detail**: The check reads `~/.lastdb/apps/*/lastdb-app-install.json` (the install receipts) and resolves each app's `api_version` (from the compat row's `lastdb_api_version`, or by reading the installed SDK version from `node_modules/@lastdb/app-sdk/package.json` if present). It compares against the preview version's `MIN_SUPPORTED_API_VERSION`.

## 5. How CI enforces the contract

All enforcement is **canary or proof-based**, with evidence in job summaries or artifacts. **No unit test suites** (AGENTS.md policy, 2026-10-09).

### 5.1 Contract-diff check (FAILS CI on breaking changes without version bump or explicit additive marker)

Add `.github/workflows/contract-diff.yml` (runs on every PR):

**Purpose**: Detects breaking changes to the HTTP/API contract surface and **FAILS CI** unless:
1. `CURRENT_API_VERSION` was bumped, OR
2. The change is explicitly marked additive (via PR label `contract:additive` or contract changelog entry)

**How the contract surface is captured**:

Generate a **snapshot** of the contract surface from the codebase:
- Parse `lastdb_host/src/route.rs` for route definitions (`POST /api/mutation`, `GET /api/query`, etc.)
- Parse `lastdb_host/src/error.rs` and `lastdb_host/src/reject.rs` for error discriminators
- Parse `lastdb_app_sdk/src/client.ts` and `lastdb_app_sdk/src/errors.ts` for SDK types and error classes
- Generate a JSON snapshot with route signatures, error kinds, and key SDK types

**Snapshot format** (example):

```json
{
  "api_version": 1,
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
}
```

**Snapshot storage**: The snapshot is a **CI artifact**, NOT committed to the repo (per AGENTS.md generated-evidence rule). The base snapshot is generated from `origin/main` at PR time and compared against the head snapshot.

**Breaking vs additive decision**:

The diff script compares base and head snapshots:
- **Breaking**: Removed route, removed required key, removed error kind, removed SDK export, changed key type
- **Additive**: Added route, added optional key, added error kind, added SDK export, added response field

**CI workflow**:

```yaml
name: contract-diff

on: [pull_request]

jobs:
  detect-breaking-changes:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v7
        with:
          fetch-depth: 0  # Need base for diff
      
      - name: Install dependencies
        run: |
          sudo apt-get update
          sudo apt-get install -y jq
          pip install tree-sitter tree-sitter-rust tree-sitter-typescript
      
      - name: Generate base contract snapshot
        run: |
          git checkout origin/main
          python3 scripts/lints/generate-contract-snapshot.py > /tmp/contract-base.json
          cat /tmp/contract-base.json
      
      - name: Generate head contract snapshot
        run: |
          git checkout ${{ github.event.pull_request.head.sha }}
          python3 scripts/lints/generate-contract-snapshot.py > /tmp/contract-head.json
          cat /tmp/contract-head.json
      
      - name: Diff contract snapshots
        id: diff
        run: |
          python3 scripts/lints/diff-contract-snapshots.py \
            /tmp/contract-base.json \
            /tmp/contract-head.json \
            > /tmp/contract-diff.txt
          
          cat /tmp/contract-diff.txt
          
          # Read api_version from base and head
          BASE_VERSION=$(jq -r '.api_version' /tmp/contract-base.json)
          HEAD_VERSION=$(jq -r '.api_version' /tmp/contract-head.json)
          
          echo "base_version=$BASE_VERSION" >> $GITHUB_OUTPUT
          echo "head_version=$HEAD_VERSION" >> $GITHUB_OUTPUT
          
          # Check if there are breaking changes
          if grep -q "BREAKING:" /tmp/contract-diff.txt; then
            echo "has_breaking=true" >> $GITHUB_OUTPUT
          else
            echo "has_breaking=false" >> $GITHUB_OUTPUT
          fi
      
      - name: Check version bump or additive marker
        if: steps.diff.outputs.has_breaking == 'true'
        run: |
          BASE_VERSION=${{ steps.diff.outputs.base_version }}
          HEAD_VERSION=${{ steps.diff.outputs.head_version }}
          
          # Check if CURRENT_API_VERSION was bumped
          if [ "$HEAD_VERSION" -gt "$BASE_VERSION" ]; then
            echo "✓ CURRENT_API_VERSION bumped: $BASE_VERSION → $HEAD_VERSION"
            exit 0
          fi
          
          # Check for contract:additive label
          HAS_LABEL=$(gh pr view ${{ github.event.pull_request.number }} \
            --json labels --jq '.labels[].name' | grep -c "^contract:additive$" || true)
          
          if [ "$HAS_LABEL" -gt 0 ]; then
            echo "✓ PR labeled as contract:additive"
            exit 0
          fi
          
          # Check for contract changelog entry (optional alternative to label)
          if grep -q "^### Contract: additive$" CHANGELOG.md 2>/dev/null; then
            echo "✓ Contract changelog entry found (additive)"
            exit 0
          fi
          
          # FAIL: Breaking changes without version bump or additive marker
          echo "❌ BREAKING contract changes detected without version bump or additive marker"
          echo ""
          cat /tmp/contract-diff.txt
          echo ""
          echo "Options:"
          echo "1. If this is a breaking change: bump CURRENT_API_VERSION in lastdb_node/src/version.rs"
          echo "2. If this is additive: add the 'contract:additive' label to this PR"
          echo "3. If the diff is wrong: fix scripts/lints/generate-contract-snapshot.py"
          exit 1
        env:
          GH_TOKEN: ${{ github.token }}
      
      - name: Upload contract snapshots
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: contract-snapshots
          path: |
            /tmp/contract-base.json
            /tmp/contract-head.json
            /tmp/contract-diff.txt
      
      - name: Add summary
        if: always()
        run: |
          echo "## Contract Diff Check" >> $GITHUB_STEP_SUMMARY
          echo "" >> $GITHUB_STEP_SUMMARY
          echo "**Base api_version**: ${{ steps.diff.outputs.base_version }}" >> $GITHUB_STEP_SUMMARY
          echo "**Head api_version**: ${{ steps.diff.outputs.head_version }}" >> $GITHUB_STEP_SUMMARY
          echo "" >> $GITHUB_STEP_SUMMARY
          echo '```' >> $GITHUB_STEP_SUMMARY
          cat /tmp/contract-diff.txt >> $GITHUB_STEP_SUMMARY 2>/dev/null || echo "No changes" >> $GITHUB_STEP_SUMMARY
          echo '```' >> $GITHUB_STEP_SUMMARY
```

**New scripts**:
- `scripts/lints/generate-contract-snapshot.py`: Parses Rust and TypeScript to generate JSON snapshot
- `scripts/lints/diff-contract-snapshots.py`: Compares two snapshots and outputs `BREAKING:` or `ADDITIVE:` lines

**Result**: Breaking changes without a version bump **FAIL CI**. The reviewer must either bump the version or apply the `contract:additive` label.

### 5.2 App install smoke test (extend existing `.github/workflows/app-install-smoke.yml`)

**Current state**: PR #352 added `.github/workflows/app-install-smoke.yml`. It installs Brain from the registry and runs `brain put/get` as proof.

**Extension**: Add LastSecrets to the matrix and test multiple `lastdb` versions.

**Changes to `app-install-smoke.yml`**:

1. Add LastSecrets to the app matrix:

```yaml
strategy:
  fail-fast: false
  matrix:
    app: [brain, lastsecrets]  # Add lastsecrets
    lastdb_version: [current, v0.23.0]  # Test current main + stable release
```

2. For `lastdb_version: v0.23.0`, check out and build that tag instead of `HEAD`.

3. Add LastSecrets-specific setup steps (analogous to Brain's Bun install):

```yaml
- name: Set up ${{ matrix.app }} dependencies (LastSecrets)
  if: matrix.app == 'lastsecrets'
  run: |
    cd "$LASTDB_HOME/apps/${{ matrix.app }}/source"
    bun install --frozen-lockfile 2>&1 | tee "$PROOF_DIR/${{ matrix.app }}-deps-install.log"
    bun link 2>&1 | tee "$PROOF_DIR/${{ matrix.app }}-link.log"

- name: Initialize ${{ matrix.app }} schemas (LastSecrets)
  if: matrix.app == 'lastsecrets'
  run: |
    cd "$LASTDB_HOME/apps/${{ matrix.app }}/source"
    lastsecrets init 2>&1 | tee "$PROOF_DIR/${{ matrix.app }}-init.log"
    
    # Check schema registration
    lastdb --data-dir "$LASTDB_HOME" schemas list | grep -E "lastsecrets" | tee "$PROOF_DIR/${{ matrix.app }}-schemas.txt" || echo "No lastsecrets schemas found"

- name: Run ${{ matrix.app }} health checks (LastSecrets)
  if: matrix.app == 'lastsecrets'
  run: |
    lastsecrets --version 2>&1 | tee "$PROOF_DIR/${{ matrix.app }}-version.txt"
    
    # Try storing and retrieving a test secret
    TEST_SLUG="test-secret-ci-$$"
    echo "Testing ${{ matrix.app }} with slug: $TEST_SLUG"
    
    lastsecrets put "$TEST_SLUG" \
      --label "Test Secret" \
      --provider "CI" \
      --purpose "Smoke test" \
      --environment "test" \
      --value "test-value-${{ github.run_number }}" \
      2>&1 | tee "$PROOF_DIR/${{ matrix.app }}-put-test.txt" || {
      echo "LastSecrets put failed"
      exit 1
    }
    
    # Retrieve it
    lastsecrets get "$TEST_SLUG" 2>&1 | tee "$PROOF_DIR/${{ matrix.app }}-get-test.txt" || {
      echo "LastSecrets get failed"
      exit 1
    }
    
    # Verify we can list it
    lastsecrets list 2>&1 | tee "$PROOF_DIR/${{ matrix.app }}-list-test.txt"
    if grep -q "$TEST_SLUG" "$PROOF_DIR/${{ matrix.app }}-list-test.txt"; then
      echo "✓ LastSecrets read/write test passed"
    else
      echo "✗ LastSecrets read/write test failed"
      exit 1
    fi
```

4. After install, record the app's `api_version`:

```yaml
- name: Record app api_version
  run: |
    cd "$PROOF_DIR"
    
    # Extract api_version from compat row or SDK version
    API_VERSION=$(cat ${{ matrix.app }}-resolve.json | grep -o '"lastdb_api_version":[0-9]*' | grep -oE '[0-9]+' || echo "unknown")
    
    # If not in compat row, try reading from installed SDK
    if [ "$API_VERSION" = "unknown" ]; then
      SDK_VERSION=$(cat "$LASTDB_HOME/apps/${{ matrix.app }}/source/node_modules/@lastdb/app-sdk/package.json" 2>/dev/null | jq -r '.version' || echo "unknown")
      if [[ "$SDK_VERSION" =~ ^([0-9]+)\. ]]; then
        API_VERSION="${BASH_REMATCH[1]}"
      fi
    fi
    
    echo "App ${{ matrix.app }} requires api_version: $API_VERSION" | tee app-api-version.txt
    
    # Node's api_version
    NODE_API_VERSION=$(lastdb --data-dir "$LASTDB_HOME" version --json 2>/dev/null | jq -r '.api_version' || echo "unknown")
    echo "Node provides api_version: $NODE_API_VERSION" | tee node-api-version.txt
```

5. Update the summary to show compatibility:

```yaml
- name: Add summary to job output
  if: always()
  run: |
    echo "## App Install Smoke: ${{ matrix.app }} @ lastdb ${{ matrix.lastdb_version }}" >> $GITHUB_STEP_SUMMARY
    echo "" >> $GITHUB_STEP_SUMMARY
    echo "- Node api_version: $(cat "$PROOF_DIR/node-api-version.txt" 2>/dev/null || echo unknown)" >> $GITHUB_STEP_SUMMARY
    echo "- App api_version: $(cat "$PROOF_DIR/app-api-version.txt" 2>/dev/null || echo unknown)" >> $GITHUB_STEP_SUMMARY
    echo "" >> $GITHUB_STEP_SUMMARY
    echo '```' >> $GITHUB_STEP_SUMMARY
    cat "$PROOF_DIR/smoke-summary.txt" >> $GITHUB_STEP_SUMMARY 2>/dev/null || echo "Summary not available" >> $GITHUB_STEP_SUMMARY
    echo '```' >> $GITHUB_STEP_SUMMARY
```

**Purpose**: Proves that the current `lastdb` build can install and run each registry app (Brain and LastSecrets), and that older `lastdb` releases still work with the latest apps (within the deprecation window).

**Note**: Search is NOT included in the smoke test matrix because it does not use the LastDB HTTP API contract (it's a vector plane).

### 5.3 Per-app compat matrix (app repos test against multiple `lastdb` versions)

Each app repo (e.g., `last-stack` for Brain, `lastsecrets` for LastSecrets) can add a CI job that tests against multiple `lastdb` versions using **local install** (`--from` / `--dev`, see §6).

**Example** (Brain in `last-stack`):

```yaml
name: brain-lastdb-compat

on: [pull_request, workflow_dispatch]

strategy:
  matrix:
    lastdb_version: [v0.23.0, v0.23.1, main]

steps:
  - uses: actions/checkout@v7
    with:
      repository: EdgeVector/lastdb
      ref: ${{ matrix.lastdb_version }}
      path: lastdb-build
  
  - name: Build lastdb
    run: |
      cd lastdb-build
      cargo build --locked -p lastdb_node --bin lastdb --bin lastdbd
  
  - uses: actions/checkout@v7
    with:
      path: brain-source
  
  - name: Start lastdbd
    run: |
      cd brain-source
      ../lastdb-build/target/debug/lastdbd --data-dir ~/.lastdb-test &
      sleep 5
      ../lastdb-build/target/debug/lastdb --data-dir ~/.lastdb-test status
  
  - name: Install brain locally
    run: |
      ../lastdb-build/target/debug/lastdb app install brain \
        --from $(pwd)/brain \
        --rev ${{ github.sha }} \
        --dev
  
  - name: Run brain health checks
    run: |
      cd ~/.lastdb-test/apps/brain/source
      bun install --frozen-lockfile
      brain init
      brain put concepts test-${{ github.run_number }} --content "Compat test"
      brain get concepts test-${{ github.run_number }}
```

**Purpose**: Proves Brain works against supported `lastdb` versions before the registry publisher runs.

## 6. Local install source (`--from` / `--rev` / `--dev`)

### 6.1 Command surface

Add a local install mode that takes a git source (directory or `file://` URL) and an exact revision:

```bash
lastdb app install <app_id> --from <path|file://url> --rev <sha> --dev
lastdb app upgrade <app_id> --from <path|file://url> --rev <sha> --dev
```

**Examples**:

```bash
# Local checkout
lastdb app install brain --from ~/EdgeVector/last-stack --rev abc123def --dev

# file:// URL
lastdb app install brain --from file:///opt/repos/last-stack --rev abc123def --dev
```

**The `--dev` flag** is required for local sources. It gates the feature behind an explicit opt-in, and prevents production defaults from silently accepting unsigned sources. The flag records `"source": "dev"`, `"channel": "dev"`, `"trust": "dev"` in the install receipt.

**Always require `--rev`**. Every install must name a commit, so the receipt and the proof are reproducible.

### 6.2 Manifest location convention

Apps have different manifest naming:
- Brain: `brain/lastdb-app.json` (inside `last-stack` monorepo)
- LastSecrets: `lastsecrets.app.json` (repo root)

**Convention**: The manifest is `<source-root>/<app-id>/lastdb-app.json` or `<source-root>/lastdb-app.json` (fallback) or `<source-root>/<app-id>.app.json` (LastSecrets convention). The installer tries all three:

```rust
fn find_manifest(source: &Path, app_id: &str) -> Result<PathBuf, String> {
    let candidates = [
        source.join(app_id).join("lastdb-app.json"),       // brain/lastdb-app.json
        source.join("lastdb-app.json"),                    // lastdb-app.json
        source.join(format!("{}.app.json", app_id)),       // lastsecrets.app.json
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

**Or**: Add `--manifest <path>` flag (optional, explicit override). If present, use it. Otherwise, try the convention.

### 6.3 Implementation: `IndexLocation::Local`

Add a `Local` variant to `location.rs::IndexLocation`:

```rust
pub enum IndexLocation {
    Url(String),
    Directory(PathBuf),
    Local { source: PathBuf, rev: String }, // NEW
}
```

**Resolution with `Local`**: A `Local` source has no index file. `resolve.rs::resolve()` needs a new code path:

```rust
pub async fn resolve(
    location: &IndexLocation,
    channel: &str,
    trust: &VerifyingKey,
    trust_source: &TrustSource,
    app_id: &str,
    node_api_version: u32,
    node_min_supported_api_version: u32,
) -> Result<Resolution, String> {
    match location {
        IndexLocation::Local { source, rev } => {
            resolve_local(source, rev, app_id, node_api_version, node_min_supported_api_version)
        }
        _ => {
            // Existing path: fetch index, verify, pick row
            let (index_bytes, sig_bytes) = location.fetch(channel).await?;
            let index = load_verified_index(trust, &index_bytes, &sig_bytes)?;
            let (app, row) = pick_row(&index, app_id, node_api_version, node_min_supported_api_version)?;
            // ... build Resolution from row
        }
    }
}
```

**New function `resolve_local()` in `resolve.rs`**:

```rust
fn resolve_local(
    source: &Path,
    rev: &str,
    app_id: &str,
    node_api_version: u32,
    node_min_supported_api_version: u32,
) -> Result<Resolution, String> {
    // 1. Validate rev is a plausible git sha (7-40 hex digits)
    if !rev.chars().all(|c| c.is_ascii_hexdigit()) || rev.len() < 7 || rev.len() > 40 {
        return Err(format!("--rev '{rev}' is not a git commit sha"));
    }

    // 2. Read manifest at --rev using `git show <rev>:<path>`
    // Try <app-id>/lastdb-app.json first, then lastdb-app.json, then <app-id>.app.json
    let manifest_paths = [
        format!("{}/lastdb-app.json", app_id),
        "lastdb-app.json".to_string(),
        format!("{}.app.json", app_id),
    ];
    
    let manifest_json = manifest_paths.iter()
        .find_map(|path| {
            let output = std::process::Command::new("git")
                .arg("-C")
                .arg(source)
                .arg("show")
                .arg(format!("{}:{}", rev, path))
                .output()
                .ok()?;
            if output.status.success() {
                Some(String::from_utf8_lossy(&output.stdout).to_string())
            } else {
                None
            }
        })
        .ok_or_else(|| format!(
            "manifest not found at rev {}: tried {}/lastdb-app.json, lastdb-app.json, {}.app.json",
            rev, app_id, app_id
        ))?;
    
    let manifest: AppManifest = serde_json::from_str(&manifest_json)
        .map_err(|e| format!("invalid manifest at rev {}: {}", rev, e))?;
    
    // 3. Check app_id matches
    if manifest.app_id != app_id {
        return Err(format!(
            "manifest app_id is '{}' but install requested '{}'",
            manifest.app_id, app_id
        ));
    }

    // 4. Check api_version compatibility if manifest declares it (fallback for no-SDK apps)
    if let Some(required_api_version) = manifest.api_version {
        if required_api_version < node_min_supported_api_version {
            return Err(format!(
                "app '{}' requires api_version {} but this node only supports {} and above",
                app_id, required_api_version, node_min_supported_api_version
            ));
        }
        if required_api_version > node_api_version {
            return Err(format!(
                "app '{}' requires api_version {} but this node only provides {}; run `brew upgrade lastdb`",
                app_id, required_api_version, node_api_version
            ));
        }
    }

    // 5. Build Resolution
    Ok(Resolution {
        app_id: app_id.to_string(),
        channel: "dev".to_string(),
        source: format!("file://{}", source.display()),
        lastdb_version: format!("dev-{}", node_api_version),
        lastdb_version_source: "local-dev".to_string(),
        app_version: manifest.version,
        sha: rev.to_string(),
        artifact: None,
        proved_at: chrono::Utc::now().to_rfc3339(),
        proof_run: "local-dev".to_string(),
        index: format!("dev:{}", source.display()),
        trust: "dev".to_string(),
    })
}
```

**Key requirement**: `resolve_local()` reads the manifest at `--rev` using `git show <rev>:<path>`, not from the working tree. This ensures the check uses the manifest that was committed at `--rev`, not a possibly-modified working copy.

**Install with `Local`**: The existing `install.rs::install_pinned()` already clones and checks out a commit. It needs no changes for local sources: `resolution.source` is a `file://` URL, which git accepts.

### 6.4 Security boundary

**Production defaults stay signed-only.**

- `--from` without `--dev` is refused.
- `--dev` without `--from` is refused (use the registry).
- The `--dev` flag and the `"source": "dev"` receipt field make dev installs visible.
- `lastdb app list` shows the trust source: `"trust": "pinned"` vs `"trust": "dev"`.

**No environment variable override** for the `--dev` flag, unless nightly CI needs it (add `LASTDB_APP_DEV_MODE=1` as an escape hatch, but explicit `--dev` is preferred).

## 7. Migration (PR sequence)

### 7.1 What to do with PR #357

**Merge as-is** after this design is approved. Reasons:

1. The manifest `lastdb` field and the post-clone check are useful now, before the index has `lastdb_api_version` checks.
2. The staging logic is sound and should be kept (atomic swap).
3. PR #357's post-clone check becomes defense-in-depth after this proposal lands.

Once this proposal is implemented, the post-clone check only fires when:
- An index publisher bug (wrong `lastdb_api_version` in the compat row)
- A local `--from` install (the manifest check in `resolve_local()` is first-line defense, but post-clone catches manifest changes between commits)

Both are legitimate guard cases.

### 7.2 PR-sized sequence

Implement in this order:

#### PR 1: `EdgeVector/lastdb` — Add contract version infrastructure

- Add `lastdb_node/src/version.rs` with `CURRENT_API_VERSION: u32` and `MIN_SUPPORTED_API_VERSION: u32`
- Extend `GET /api/version` to report both (or confirm it already does)
- Add `pub api_version: Option<u32>` to `AppManifest` (fallback field for no-SDK apps)
- Update `CompatRow` doc comment to note `lastdb_api_version` is the primary contract identifier
- No breaking change; old indexes still work
- Open the PR, reference this design, merge

**Title**: "Add contract version infrastructure (api_version range)"

#### PR 2: `EdgeVector/lastdb` — Use `api_version` in `pick_row()` instead of exact version match

- Update `resolve.rs::pick_row()` to check `CompatRow.lastdb_api_version` compatibility instead of exact `lastdb_version` string match
- Pass `node_api_version` and `node_min_supported_api_version` to `pick_row()`
- Update error messages to show `api_version` mismatch, not string mismatch
- Defense-in-depth: keep the post-clone manifest check from PR #357 (extend it to check fallback `manifest.api_version` if present)
- Open the PR, reference this design, merge

**Title**: "Use api_version for app compatibility (replaces exact lastdb version match)"

#### PR 3: `EdgeVector/lastdb` — Local install source (`--from` / `--rev` / `--dev`)

- Add `IndexLocation::Local` to `location.rs`
- Add `resolve_local()` to `resolve.rs` (reads manifest at `--rev` via `git show`, checks `api_version`)
- Add `--from`, `--rev`, and `--dev` flags to `lastdb app install` and `lastdb app upgrade` CLI
- Guard `--from` behind `--dev`
- Update install receipt to record `"source": "dev"`, `"channel": "dev"`, `"trust": "dev"`
- Add manifest location convention: try `<app-id>/lastdb-app.json`, `lastdb-app.json`, `<app-id>.app.json`
- Open the PR, reference this design, merge

**Title**: "Add local install source (--from/--rev/--dev) for dev and CI proofs"

#### PR 4: `EdgeVector/lastdb` — CI: contract-diff check

- Add `.github/workflows/contract-diff.yml` (FAILS on breaking changes without version bump or `contract:additive` label)
- Add `scripts/lints/generate-contract-snapshot.py` (parses Rust + TypeScript to generate JSON snapshot)
- Add `scripts/lints/diff-contract-snapshots.py` (compares snapshots, outputs `BREAKING:` or `ADDITIVE:` lines)
- Snapshot is CI artifact, not committed
- Open the PR, reference this design, merge

**Title**: "CI: contract-diff check (fails on breaking changes without version bump)"

#### PR 5: `EdgeVector/lastdb` — CI: extend app-install-smoke test

- Extend `.github/workflows/app-install-smoke.yml` to test multiple `lastdb` versions (current + v0.23.0)
- Add LastSecrets to the app matrix (alongside Brain)
- Add `api_version` evidence to smoke test artifacts and summary
- Add LastSecrets-specific setup and health checks
- Open the PR, reference this design, merge

**Title**: "CI: extend app-install-smoke with LastSecrets and multi-version testing"

#### PR 6: `EdgeVector/lastdb` — Pre-upgrade compatibility check

- Add `lastdb app check-compat --preview-version <version>` command
- Reads install receipts, resolves each app's `api_version`, checks against preview version's `MIN_SUPPORTED`
- **BLOCKS by default (exit 1)** when incompatible apps exist
- `--force` flag to bypass (exit 0, warning only)
- Open the PR, reference this design, merge

**Title**: "Add pre-upgrade app compatibility check (blocks by default)"

#### PR 7: `EdgeVector/homebrew-lastdb` — Homebrew pre-install hook

- Add `pre_install` hook to `Formula/lastdb.rb` that runs `lastdb app check-compat --preview-version <new_version>`
- Hook aborts upgrade if check exits 1 (incompatible apps)
- User can override with `brew install --force lastdb`
- Open the PR, reference this design, merge

**Title**: "Add pre-install compatibility check to Homebrew formula"

#### PR 8: `EdgeVector/last-stack` — Publisher plumbs `api_version` to compat rows

- Update the registry publisher (in `last-stack`) to:
  - Read `@lastdb/app-sdk` version from `package-lock.json`
  - Map SDK version to `api_version` (e.g., `2.0.0` → `2`)
  - Write `"lastdb_api_version": <int>` to `CompatRow` in the index
- Publish the next index with `api_version` ranges
- Open the PR, reference this design, merge

**Title**: "Registry publisher: plumb api_version from SDK version to compat rows"

#### PR 9: `EdgeVector/lastdb` — Rust client crate (`lastdb_app_client`)

- Create `lastdb_app_client/` crate (wraps `lastdb_host::FoldDbHttpClient`)
- Add `pub const CURRENT_API_VERSION: u32` (tracks contract version)
- Add version handshake wrapper (`check_api_version()` calls `GET /api/version`, returns `Result<(), NodeTooOldError>`)
- Add typed errors (`NodeTooOldError`, `PermissionDeniedError`, etc.)
- Document in `lastdb_app_client/CONTRACT.md` (Rust equivalent of `@lastdb/app-sdk/CONTRACT.md`)
- Major version tracks `api_version` (`lastdb_app_client 2.x.x` → `api_version: 2`)
- Open the PR, reference this design, merge

**Title**: "Add Rust client crate (lastdb_app_client) with contract version pinning"

#### PR 10: `EdgeVector/homebrew-lastdb` — Registry update (automatic, from publisher)

- The publisher (PR 8) writes the next index with `lastdb_api_version` ranges
- Commit and push the new `registry/stable.json` and `registry/stable.json.sig`
- At this point, all new installs check `api_version` compatibility before download

#### PR 11: `EdgeVector/last-stack` — Use local install in Brain CI (optional)

- Add `.github/workflows/brain-lastdb-compat.yml` (tests Brain against multiple `lastdb` versions using `--from` / `--dev`)
- Open the PR, reference this design, merge

**Title**: "CI: Brain compatibility matrix using local install"

#### PR 12: `EdgeVector/lastsecrets` — Use local install in LastSecrets CI (optional)

- Add `.github/workflows/lastsecrets-lastdb-compat.yml` (tests LastSecrets against multiple `lastdb` versions using `--from` / `--dev`)
- Open the PR, reference this design, merge

**Title**: "CI: LastSecrets compatibility matrix using local install"

### 7.3 Rollout

**No feature flag needed.** The changes are additive:

- `CompatRow.lastdb_api_version` is optional. Old indexes work (rows with no `api_version` are assumed compatible).
- `--from` / `--dev` is new surface. Existing installs are unaffected.
- The `api_version` check in `pick_row()` only rejects when a row has `lastdb_api_version` set and the node is outside the supported range.

**No migration for existing installs.** Installed apps keep working. The next `lastdb app upgrade` uses the new resolve logic if the registry has `api_version` ranges.

## 8. What could not be verified

**Lastseek / EdgeVector/lastseek**: This repo does not exist. Earlier drafts referenced "Lastseek" as a Rust app. There is no such repo in the EdgeVector org. If a Rust app emerges in the future, it will use the `lastdb_app_client` crate (PR 9).

**Search app contract status**: The `EdgeVector/search` app is a vector plane companion to LastDB, not a client of the HTTP API contract. It processes `IndexChangeBatch` files from an inbox and does NOT use `@lastdb/app-sdk` or any HTTP routes. It is NOT part of the app-LastDB contract and will NOT be in the smoke test matrix.

## 9. Summary

This proposal establishes a **firm contract between apps and LastDB**:

1. **Contract identifier**: `lastdb_api_version` (unsigned integer), already present in `CompatRow` and `GET /api/version`.
2. **Apps depend on the SDK** (`@lastdb/app-sdk` for TypeScript, `lastdb_app_client` for Rust), which pins the contract version. The registry publisher reads the SDK version and writes `api_version` to compat rows.
3. **Breaking vs additive**: Defined (§2.2). **CI FAILS** when the HTTP API changes without a version bump or explicit `contract:additive` label. Contract surface is captured as a generated JSON snapshot (CI artifact, not committed).
4. **Install/upgrade checks `api_version`** in `pick_row()` BEFORE download. A node on `0.23.2` can install an app proved on `0.23.0` if `api_version` is compatible. Remove exact `lastdb_version` string match.
5. **Pre-upgrade check** (`lastdb app check-compat --preview-version`) **BLOCKS by default** (exit 1) if installed apps would break after upgrading LastDB. Homebrew `pre_install` hook runs the check and aborts upgrade unless `--force` is passed.
6. **Local install source** (`--from <path> --rev <sha> --dev`) for dev, CI, and proofs. Always require `--rev`. Read manifest at `--rev` using `git show`. Production defaults stay signed-only.
7. **CI enforcement**: contract-diff check (FAILS on breaking changes), extended app-install-smoke test (Brain + LastSecrets, multiple `lastdb` versions, `api_version` evidence), per-app compat matrix (app repos test against supported `lastdb` versions using local install). All proof-based, no unit test suites.
8. **Rust client crate** (`lastdb_app_client`) wraps `lastdb_host::FoldDbHttpClient`, pins `api_version`, provides typed errors. Part of the plan (PR 9), not future work.
9. **Migration**: Merge PR #357 as-is (defense-in-depth). Implement this in 12 PRs across `lastdb`, `last-stack`, `homebrew-lastdb`, and `lastsecrets`.

**Inventory**:
- **Brain** (TypeScript, `@lastdb/app-sdk` vendored subset, `brain/lastdb-app.json`)
- **LastSecrets** (TypeScript, `@lastdb/app-sdk`, `lastsecrets.app.json`)
- **Search** (TypeScript, vector plane, NOT part of the contract)

**Result**: Tom can rely on the app registry. Apps declare the contract version they need (via SDK), the node checks it before download, pre-upgrade check blocks incompatible upgrades, and CI proves the contract holds.
