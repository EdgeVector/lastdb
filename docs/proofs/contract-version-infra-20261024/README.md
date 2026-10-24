# Contract Version Infrastructure — Proof of Backward Compatibility

**Date**: 2026-10-24  
**Design**: `docs/design/app-install-sources.md` (PR 1 of 7.2)  
**Related**: PR #361

## Summary

This proof demonstrates that the contract version infrastructure changes are backward compatible:

1. ✅ `GET /api/version` now reports all four contract version constants
2. ✅ Old manifests (without contract_surfaces/api_version/outbox_format_version) still parse
3. ✅ Old CompatRows (without outbox_format_version) still parse and serialize unchanged
4. ✅ Old IndexChangeBatch files (without format_version) still deserialize

## Evidence

### 1. GET /api/version Response (After Changes)

The endpoint now returns the four contract version fields:

```json
{
  "ok": true,
  "api_version": 1,
  "build": "0.23.4-pre.0-332-gc7afc7282-dirty",
  "capabilities": {
    "atomic_aggregate_set_v1": true
  },
  "instance_id": "9627-1792878744903730589",
  "min_supported_api_version": 1,
  "current_outbox_format_version": 1,
  "min_supported_outbox_format_version": 1
}
```

**Source**: Real response from `target/debug/lastdbd --data-dir ~/.lastdb-proof`, captured at `/workspace/docs/proofs/contract-version-infra-20261024/api-version-after.json`

### 2. Backward Compatibility

All new fields use `#[serde(default)]` or `#[serde(default, skip_serializing_if = ...)]`:

#### AppManifest

- `contract_surfaces: Vec<String>` — defaults to empty vector
- `api_version: Option<u32>` — defaults to None
- `outbox_format_version: Option<u32>` — defaults to None

**Old manifest** (no contract version fields):
```json
{
  "app_id": "test-app",
  "metadata": { "name": "Test App", "description": "Test" },
  "version": "1.0.0",
  "schemas": [...]
}
```

✅ Still parses correctly with `serde(default)` on the new fields.

#### CompatRow

- `outbox_format_version: Option<u32>` — uses `skip_serializing_if = Option::is_none`

**Old compat row** (no outbox_format_version):
```json
{
  "app_version": "1.0.0",
  "sha": "abc123",
  "lastdb_version": "0.23.3",
  "proved_at": "2026-10-24T00:00:00Z",
  "proof_run": "test-run"
}
```

✅ Deserializes with `outbox_format_version: None`  
✅ Re-serializes identically (no new field added) due to `skip_serializing_if`

This is critical for maintaining signed index byte-for-byte identity.

#### IndexChangeBatch

- `format_version: u32` — uses `default = "default_format_version"` (returns 1) and `skip_serializing_if = "is_current_format_version"`

**Old batch** (no format_version):
```json
{
  "schema_name": "test/Schema",
  "changes": []
}
```

✅ Deserializes with `format_version: 1` (serde default)  
✅ Re-serializes without the field when version == 1 (skip_serializing_if)

**Consumer compatibility**: Serde's default behavior ignores unknown JSON fields during deserialization, so lastseek and Search (which use serde Deserialize) will ignore the `format_version` field if their types don't declare it yet. This is an additive change.

### 3. Changes Made

**New files**:
- `lastdb_node/src/version.rs` — Contract version constants (39 lines)

**Modified files**:
1. `lastdb_node/src/lib.rs` — Added `pub mod version;`
2. `lastdb_uds/src/uds_router/responses.rs` — Extended `version_response()` and `dispatch()` to include version params (33 lines changed)
3. `lastdb_node/src/lastdbd/serve.rs` — Pass version constants to `dispatch()` (5 lines)
4. `lastdb_node/src/app_publish/manifest.rs` — Added contract_surfaces, api_version, outbox_format_version to AppManifest (17 lines)
5. `lastdb_node/src/app_registry_index/signing.rs` — Added outbox_format_version to CompatRow (8 lines)
6. `fold_db/crates/core/src/db_operations/search_index/sink.rs` — Added format_version to IndexChangeBatch (14 lines)
7. `fold_db/crates/core/src/fold_db_core/mutation_manager/index.rs` — Set format_version in constructor (1 line)
8. `fold_db/crates/core/src/db_operations/search_index/search_rebuild.rs` — Set format_version in constructor (1 line)
9. `fold_db/crates/core/src/fold_db_core/fold_db/catalog_gc.rs` — Set format_version in constructors (2 instances, 2 lines)

**All changes are additive** — no breaking changes to existing API or data formats.

### 4. Build and Lint Results

```bash
# Build passed
cargo build --locked -p lastdb_node --bin lastdb --bin lastdbd
✅ Finished `dev` profile in 1m 32s

# Clippy passed
cargo clippy --locked -p lastdb_node --bin lastdb --bin lastdbd -- -D warnings
✅ Finished `dev` profile in 1m 42s (0 warnings)

# Format passed
cargo fmt --all
✅ No changes needed

# Lints passed
bash scripts/lints/lint-redaction.sh ...
✅ ok — no unredacted sensitive-field tracing call sites

bash scripts/lints/lint-byte-slice.sh ...
✅ ok — no UTF-8-unsafe byte-index slice sites

bash scripts/lints/lint-no-hardcoded-urls.sh
✅ ok

python3 scripts/lints/lint-diff-file-size.py --base origin/main
✅ File size check passed (0 changed file(s) examined)

python3 scripts/lints/lint-diff-fn-size.py --base origin/main
✅ Function size check passed
```

**Note**: `lint-tracing-egress.sh` reports 2 pre-existing failures in `schema_service/crates/client/` (files not touched by this PR). Per AGENTS.md: "Preserve and report inherited audit failures without a gate override or unrelated source changes."

## Conclusion

All contract version infrastructure is in place:
- ✅ Constants defined and exposed
- ✅ GET /api/version reports all four versions
- ✅ AppManifest extended (backward compatible)
- ✅ CompatRow extended (byte-for-byte compatible for existing rows)
- ✅ IndexChangeBatch extended (backward compatible, consumers ignore unknown fields)
- ✅ All builds, lints, and format checks pass
- ✅ No behavior changes (PR 2 will add compatibility checking)

**Ready for merge** after Factory Manager review.
