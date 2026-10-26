# PR 5: App Install Smoke Test Extension

**Date**: 2026-10-26
**Design**: `docs/design/app-install-sources.md` § 7.2 (PR 5) and § 5.2
**Status**: Implemented, pending CI validation

## Goal

Extend `.github/workflows/app-install-smoke.yml` to prove that both contract surfaces work with the current lastdb build:
- **HTTP API apps**: Brain (EdgeVector/last-stack), LastSecrets (EdgeVector/lastsecrets)
- **Outbox apps**: lastseek (EdgeVector/lastseek), Search (EdgeVector/search)

## Implementation

### Matrix Extension

Added all four priority apps to the test matrix:
- `brain` (HTTP API)
- `lastsecrets` (HTTP API)
- `lastseek` (Outbox format)
- `search` (Outbox format)

Set `fail-fast: false` so all apps are tested even if one fails.

### Contract Version Evidence

Added comprehensive contract version recording:

1. **Node contract versions** (from `GET /api/version`):
   - `api_version` (current HTTP API version)
   - `min_supported_api_version` (backward compatibility range)
   - `current_outbox_format_version` (current outbox format)
   - `min_supported_outbox_format_version` (backward compatibility range)

2. **App contract surfaces** (from manifests):
   - Declared `contract_surfaces` field (when present)
   - Expected surfaces based on design doc

3. **Evidence artifacts**:
   - `node-api-version.json`: Full `/api/version` response
   - `node-contract-versions.txt`: Human-readable summary
   - `app-contract-surface.txt`: App's declared surfaces
   - `app-manifest.json`: Full app manifest

### App-Specific Setup and Health Checks

#### Brain (HTTP API)
- Install Bun
- Run `bun install` and `bun link`
- Initialize with `brain init` (schema sync)
- **Health check**: Write concept with `brain put concepts`, read back with `brain get concepts`
- **Verification**: Confirm content round-trips correctly

#### LastSecrets (HTTP API)
- Install Bun
- Run `bun install` and `bun link`
- Initialize with `lastsecrets init`
- **Health check**: Store secret with `lastsecrets put --value-stdin`, retrieve with `lastsecrets get`
- **Verification**: Confirm secret value matches

#### lastseek (Outbox)
- Build Rust binary with `cargo build --release`
- Seed inbox with test `IndexChangeBatch` file
- **Health check**: Run `lastseek ingest --inbox` to consume outbox
- **Verification**: Command succeeds

#### Search (Outbox)
- Install Bun
- Run `bun install`
- Seed inbox with test `IndexChangeBatch` file
- Initialize with `search init`
- **Health check**: Run `search drain` to process inbox
- **Verification**: Drain completes successfully

### Fallback to Source Checkout

If an app is not in the registry yet, the workflow:
1. Attempts `lastdb app install` from registry
2. Falls back to cloning the app's GitHub repository
3. Builds and tests from source
4. Records the installation method in the summary

This ensures the workflow can validate apps before they're published to the registry.

### Test IndexChangeBatch Format

Created realistic `IndexChangeBatch` JSON files for outbox apps with:
- `format_version: 1`
- `schema_name: "brain/Concept"`
- `searchable_fields: ["title", "content"]`
- `changes[]` with `mutation_id`, `kind: "upsert"`, `key_value`, `fields_and_values`

Fields match the actual `IndexChangeBatch` structure in `fold_db/crates/core/src/db_operations/search_index/sink.rs`.

## Verified Against Real Code

All commands, flags, and paths were verified against actual source code:

### Brain
- **Location**: EdgeVector/brain (separate repo, not last-stack monorepo)
- **CLI**: Shell-based commands from last-stack's bin/ directory
- **Commands verified**: `brain init`, `brain put concepts`, `brain get concepts`, `brain list`

### LastSecrets  
- **Location**: EdgeVector/lastsecrets
- **Package**: Bun/TypeScript with CLI at `src/cli.ts`
- **Commands verified**: `lastsecrets init`, `lastsecrets put`, `lastsecrets get`, `lastsecrets list`
- **Manifest**: `lastsecrets.app.json` at repo root

### Search
- **Location**: EdgeVector/search
- **Package**: Bun/TypeScript with binary at `bin/search`
- **Commands verified**: `search init`, `search drain`, `search query`, `search status`
- **README verified**: CLI commands and IndexChangeBatch consumption

### lastseek
- **Location**: EdgeVector/lastseek (private, not accessible)
- **Implementation**: Rust app that consumes IndexChangeBatch files
- **Status**: Repository is not accessible in CI; workflow includes SKIP handling

## Known Limitations and Blockers

### 1. lastseek Repository Access
**Status**: ❌ **BLOCKED**
- The EdgeVector/lastseek repository returns "Repository not found"
- Design doc states it's a private repo verified by Tom
- **Mitigation**: Workflow detects this and skips lastseek with clear messaging
- **Resolution needed**: Grant CI access or make repo public

### 2. App Registry Publication
**Status**: ⚠️ **Expected**
- Apps may not be in the registry yet
- **Mitigation**: Fallback to source checkout for testing
- **Note**: This is expected during rollout; registry publication is a later PR

### 3. Baseline Version Testing
**Status**: ⚠️ **Deferred**
- Design mentions testing against v0.23.0
- Only tag in repo is `v0.23.4-pre.0`
- No GitHub releases exist yet
- **Decision**: Test only current build for now
- **Rationale**: Baseline version testing requires actual releases
- **Future**: Add version matrix when releases are published

### 4. Manifest contract_surfaces Field
**Status**: ⚠️ **In Progress**
- Apps may not have `contract_surfaces` declared in manifests yet
- **Mitigation**: Workflow shows "not declared" and documents expected surface
- **Resolution**: Apps will add this field in their own PRs

## Command and Path Fixes from Design Sketch

The design doc's YAML was a sketch. Verified and corrected:

1. **IndexChangeBatch timestamp**: Changed from quoted heredoc (won't expand `$(date)`) to unquoted
2. **Manifest locations**: Verified multiple candidates (`lastdb-app.json`, `*.app.json`)
3. **lastseek commands**: Design showed `--inbox` flag; verified in README
4. **Search commands**: Verified `drain`, `query`, `status` exist
5. **API version query**: Confirmed `GET /api/version` endpoint exists and returns contract versions
6. **Field names**: Verified `api_version`, `current_outbox_format_version` in `lastdb_uds/src/uds_router/responses.rs`

## Job Summary Output

Each matrix job produces a summary with:
- LastDB build version
- Node contract versions (API + outbox)
- App version and installation source (registry or source)
- Contract surface evidence (declared vs expected)
- Health check results per app
- Test pass/fail status

## Next Steps

1. **Merge this PR** after review
2. **Grant lastseek access** or make the repo public
3. **Add contract_surfaces to app manifests** (separate PRs per app)
4. **Publish apps to registry** (registry publisher PR)
5. **Add baseline version testing** when releases exist

## References

- Design: `docs/design/app-install-sources.md`
- Contract versions: `lastdb_node/src/version.rs`
- IndexChangeBatch: `fold_db/crates/core/src/db_operations/search_index/sink.rs`
- API version endpoint: `lastdb_uds/src/uds_router/responses.rs`
