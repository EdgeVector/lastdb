# App install sources and early compatibility checks

**Written 2026-10-24 · Status: proposal**

## 0. Context

The app registry installer (`lastdb_node/src/app_registry_index/`: `install.rs`, `resolve.rs`, and the signed index format in `EdgeVector/homebrew-lastdb` `registry/*.json`, published by `EdgeVector/last-stack`) only installs what the signed index points to, so a real install or upgrade can't be driven without the full publishing pipeline.

PR #357 adds a manifest `lastdb` semver range, but checks it **after** cloning. This required staging, rollback, and swap logic in `install.rs::upgrade_pinned()` and has produced repeated bugs. The installer also cannot be proved without a one-off example program, because it needs a complete published registry entry.

## 1. Goals

This proposal addresses five needs:

1. **A first-class local/dev install source** that goes through the same install/upgrade code as the registry path, so proofs and dogfooding run the real code.
2. **Carry the lastdb version range in the index's compat rows**, checked by `resolve.rs::pick_row()` BEFORE any download. The post-clone manifest check becomes a defense-in-depth assertion.
3. **Simplify upgrades** given 1 and 2: atomic swap semantics and what's still needed.
4. **E2E CI and docs/proofs use it**: the nightly install canary, the per-app compat matrix, and no unit test suites.
5. **Migration**: what to do with #357 (merge as-is, trim, or supersede), and the PR-sized sequence across `lastdb`, `last-stack`, and `homebrew-lastdb`.

## 2. Local install source (Goal 1)

### 2.1 Command surface

Add a local install mode that takes a git source (directory or `file://` URL) and an exact revision:

```bash
lastdb app install <app_id> --from <path|file://url> --rev <sha>
lastdb app upgrade <app_id> --from <path|file://url> --rev <sha>
```

Examples:

```bash
# Local checkout
lastdb app install brain --from ~/EdgeVector/last-stack --rev abc123def --dev

# file:// URL
lastdb app install brain --from file:///opt/repos/last-stack --rev abc123def --dev

# Unpinned dev mode (HEAD of the given branch or current checkout)
lastdb app install brain --from ~/EdgeVector/last-stack --dev
```

**The `--dev` flag** is required for local sources. It gates the feature behind an explicit opt-in, and prevents production defaults from silently accepting unsigned sources. The flag also records `"source": "dev"` in the install receipt (`lastdb-app-install.json`), so `lastdb app list` can distinguish dev installs.

When `--from` is absent, the installer uses the registry index (the current signed path).

### 2.2 Implementation: `IndexLocation::Local`

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
    lastdb_version: &str,
    lastdb_version_source: &str,
) -> Result<Resolution, String> {
    match location {
        IndexLocation::Local { source, rev } => {
            // No index fetch, no signature check.
            // Read the manifest from <source>/lastdb-app.json to get app_version.
            // Build Resolution directly.
            resolve_local(source, rev, app_id, lastdb_version, lastdb_version_source)
        }
        _ => {
            // Existing path: fetch index, verify, pick row
            let (index_bytes, sig_bytes) = location.fetch(channel).await?;
            let index = load_verified_index(trust, &index_bytes, &sig_bytes)?;
            // ...
        }
    }
}
```

New function `resolve_local()` in `resolve.rs`:

```rust
fn resolve_local(
    source: &Path,
    rev: &str,
    app_id: &str,
    lastdb_version: &str,
    lastdb_version_source: &str,
) -> Result<Resolution, String> {
    // 1. Validate rev is a plausible git sha (7-40 hex digits)
    if !rev.chars().all(|c| c.is_ascii_hexdigit()) || rev.len() < 7 || rev.len() > 40 {
        return Err(format!("--rev '{rev}' is not a git commit sha"));
    }

    // 2. Read lastdb-app.json from source
    let manifest_path = source.join("lastdb-app.json");
    let manifest = load_manifest(&manifest_path)?;
    
    // 3. Check app_id matches
    if manifest.app_id != app_id {
        return Err(format!(
            "manifest app_id is '{}' but install requested '{app_id}'",
            manifest.app_id
        ));
    }

    // 4. Check manifest.lastdb constraint if present (Goal 2, defense-in-depth)
    if let Some(constraint_str) = manifest.lastdb {
        check_version_constraint(lastdb_version, &constraint_str)?;
    }

    // 5. Build Resolution
    Ok(Resolution {
        app_id: app_id.to_string(),
        channel: "dev".to_string(),
        source: format!("file://{}", source.display()),
        lastdb_version: lastdb_version.to_string(),
        lastdb_version_source: lastdb_version_source.to_string(),
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

**Install with `Local`**: The existing `install.rs::install_pinned()` already clones and checks out a commit. It needs no changes for local sources: `resolution.source` is a `file://` URL, which git accepts. The receipt records `"channel": "dev"`, `"trust": "dev"`, and `"source": "file://..."`.

### 2.3 Security boundary

**Production defaults stay signed-only.**

- `--from` without `--dev` is refused.
- `--dev` without `--from` is refused (use the registry).
- The `--dev` flag and the `"source": "dev"` receipt field make dev installs visible.
- `lastdb app list` shows the trust source: `"trust": "pinned"` vs `"trust": "dev"`.

**No environment variable override** for the `--dev` flag: an attacker who controls the environment can already supply `LASTDB_REGISTRY_INDEX` and `LASTDB_REGISTRY_TRUST_KEY`, so a `LASTDB_APP_DEV_MODE` adds no attack surface and only saves typing. Add it if the nightly CI needs it.

## 3. Version range in the index (Goal 2)

### 3.1 The `lastdb` field in `CompatRow`

`last-stack` PR #346 already plumbs a `lastdb` semver range through the index publisher. Add it to the `CompatRow` struct in `signing.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompatRow {
    pub app_version: String,
    pub sha: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact: Option<String>,
    pub lastdb_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lastdb_api_version: Option<u32>,
    pub proved_at: String,
    pub proof_run: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lastdb: Option<String>, // NEW: semver range, e.g. ">=0.23.0, <0.24.0"
}
```

**When absent**: A row with no `lastdb` field accepts any running node. This preserves backward compatibility with old indexes.

### 3.2 Check in `pick_row()`

`resolve.rs::pick_row()` already selects the newest row that names `lastdb_version` exactly. Add a constraint check:

```rust
pub fn pick_row<'a>(
    index: &'a RegistryIndex,
    app_id: &str,
    lastdb_version: &str,
) -> Result<(&'a IndexApp, &'a CompatRow), String> {
    let app = index
        .apps
        .iter()
        .find(|a| a.app_id == app_id)
        .ok_or_else(|| format!("app '{app_id}' is not in the {} index", index.channel))?;

    let mut matches: Vec<&CompatRow> = app
        .compat
        .iter()
        .filter(|row| {
            // Existing exact match filter
            if row.lastdb_version != lastdb_version {
                return false;
            }
            // NEW: Check semver range if present
            if let Some(ref constraint_str) = row.lastdb {
                if let Err(_) = check_version_constraint(lastdb_version, constraint_str) {
                    return false;
                }
            }
            true
        })
        .collect();

    if matches.is_empty() {
        // Existing error with proved versions list
        // ...
    }
    
    // Existing sort and pick newest
    matches.sort_by(|a, b| {
        a.proved_at
            .cmp(&b.proved_at)
            .then_with(|| a.app_version.cmp(&b.app_version))
    });
    
    let row = matches
        .pop()
        .ok_or_else(|| "compat row selection produced no row".to_string())?;
    Ok((app, row))
}
```

**Behavior**: When a `CompatRow` has `"lastdb": ">=0.23.0, <0.24.0"` and the running node is `0.22.5`, the row is filtered out before selection. If no rows remain, the error lists the proved versions (unchanged).

### 3.3 Defense-in-depth: post-clone manifest check

The manifest check in `install.rs::install_pinned()` (added by PR #357) stays as a guard. It runs after checkout:

```rust
pub fn install_pinned(
    resolution: &Resolution,
    install_dir: &Path,
    force: bool,
) -> Result<PinnedInstallOutcome, String> {
    // ... clone and checkout ...
    
    // NEW (from #357): Check manifest constraint as defense-in-depth
    check_version_constraint_at_checkout(&checkout_path, &resolution.lastdb_version)?;

    // ... write receipt ...
}
```

This catches:

- A local `--from` install where the manifest constraint was added after the commit was made (the `resolve_local()` check reads the current manifest, not the one at `--rev`).
- An index publisher bug (wrong `lastdb` in a compat row).
- A git clone that landed on the wrong commit (already checked by comparing HEAD to `resolution.sha`, but belt-and-suspenders).

## 4. Simplified upgrades (Goal 3)

### 4.1 What staging solves

PR #357 adds staging (`<install_dir>.upgrade-stage`) because the manifest check happens after clone. The upgrade path:

1. Clone to `.upgrade-stage`
2. Check version constraint
3. If pass: swap `.upgrade-stage` to `install_dir` with a backup
4. If fail: remove `.upgrade-stage`

**With early checks (Goal 2)**, the version range is checked before `resolve()` returns. `upgrade_pinned()` can fail early, before any git operation.

### 4.2 Atomic swap semantics

**Keep the staging model** for atomicity, even with early checks. The staging directory isolates the clone from the running install. Failures (network, disk full, wrong commit) leave the old install intact.

**The swap**:

1. Clone to `.upgrade-stage`
2. Verify HEAD matches `resolution.sha` (existing)
3. Post-clone manifest check (defense-in-depth, from #357)
4. Atomic rename sequence:
   - `install_dir` → `.upgrade-backup`
   - `.upgrade-stage` → `install_dir`
   - Remove `.upgrade-backup`
5. On failure: restore from `.upgrade-backup`

This is PR #357's swap logic. **Keep it.**

### 4.3 What can be removed

**The post-clone rollback for version constraint failures is dead code** when the index has `lastdb` ranges. The early check in `pick_row()` rejects the row before `upgrade_pinned()` starts. The staging still handles git failures and wrong commits.

The manifest check stays as a guard (§3.3), but it should never fire when the index is correct. Log when it does, so the index publisher can be fixed.

## 5. E2E CI and proofs (Goal 4)

### 5.1 Nightly install canary

Add a nightly CI job (`.github/workflows/app-install-canary.yml`) that:

1. Builds `lastdb` / `lastdbd` from the workflow commit
2. Installs each registry app with `lastdb app install <app_id>`
3. Runs `lastdb app check --sync` for each app
4. Reports success/failure

**Local installs for pre-release testing**:

Before publishing a new app version, the canary can test a local checkout:

```yaml
- name: Test brain pre-release
  run: |
    lastdb app install brain \
      --from ~/last-stack \
      --rev ${{ github.event.inputs.test_sha }} \
      --dev
    lastdb app check --app brain --sync
```

This runs the real install code against a commit that isn't in the registry yet.

### 5.2 Per-app compat matrix

Each app repo (e.g., `last-stack`) can have its own CI job that tests multiple `lastdb` versions. Example for `last-stack`:

```yaml
strategy:
  matrix:
    lastdb_version: ["0.22.5", "0.23.0", "0.23.1"]
steps:
  - uses: actions/checkout@v4
    with:
      repository: EdgeVector/lastdb
      ref: v${{ matrix.lastdb_version }}
      path: lastdb-build
  - name: Build lastdb
    run: |
      cd lastdb-build
      cargo build --locked -p lastdb_node --bin lastdb --bin lastdbd
  - name: Install app locally
    run: |
      lastdb-build/target/debug/lastdb app install brain \
        --from ${{ github.workspace }} \
        --rev ${{ github.sha }} \
        --dev
  - name: Verify schema check
    run: lastdb-build/target/debug/lastdb app check --app brain --sync
```

This proves compatibility before the registry publisher runs.

### 5.3 No unit tests

The repo policy (`AGENTS.md`, 2026-10-09: "No tests") applies. The install code is proved by:

- The nightly canary (§5.1)
- Per-app compat CI (§5.2)
- Manual dogfooding with `--from` and `--dev`

**No `version_constraint.rs` unit tests** (from PR #357). The constraint parser is proved by the canary failing when the manifest has a bad constraint, and by the compat matrix testing real version pairs.

## 6. Migration (Goal 5)

### 6.1 What to do with PR #357

**Merge as-is** after this design is approved. Reasons:

1. The manifest `lastdb` field and the post-clone check are useful now, before the index has `lastdb` ranges.
2. The staging logic is sound and should be kept (§4.2).
3. PR #357's code is defense-in-depth after Goal 2 lands (§3.3).

Once this proposal is implemented, the post-clone check only fires when:

- A local `--from` install (the manifest check in `resolve_local()` is the first-line defense, but the post-clone check catches manifest changes between commits).
- An index publisher bug (the index `lastdb` range is wrong).

Both are legitimate guard cases.

### 6.2 PR-sized sequence

Implement in this order across three repos:

#### PR 1: `EdgeVector/lastdb` — Add `lastdb` field to `CompatRow` (read-side)

- Add `pub lastdb: Option<String>` to `signing.rs::CompatRow`
- Add the constraint check to `resolve.rs::pick_row()`
- Update `INDEX_VERSION` to 2 (optional: only if the grammar change is required to be explicit)
- Open the PR, reference this design, merge

**No breaking change**: Old indexes (no `lastdb` field) still work. New indexes with `lastdb` ranges work with this build.

#### PR 2: `EdgeVector/last-stack` — Publisher plumbs `lastdb` range

- `last-stack` PR #346 already plumbs the field. Merge it after `lastdb` PR 1.
- Update the registry publisher to read `lastdb` from app manifests and write it to compat rows.

#### PR 3: `EdgeVector/homebrew-lastdb` — Registry update

- The publisher (in `last-stack`) writes the next index with `lastdb` ranges.
- Commit and push the new `registry/stable.json` and `registry/stable.json.sig`.

At this point, all new installs check the version range before download.

#### PR 4: `EdgeVector/lastdb` — Local install source (`--from` / `--dev`)

- Add `IndexLocation::Local` to `location.rs`
- Add `resolve_local()` to `resolve.rs`
- Add `--from`, `--rev`, and `--dev` flags to `lastdb app install` and `lastdb app upgrade`
- Guard `--from` behind `--dev` (§2.3)
- Update install receipt to record `"source": "dev"`, `"channel": "dev"`, `"trust": "dev"`
- Open the PR, reference this design, merge

#### PR 5: `EdgeVector/lastdb` — CI canary and compat matrix (optional)

- Add `.github/workflows/app-install-canary.yml` (§5.1)
- Add per-app compat matrix CI (§5.2) or document it for app repos to adopt

#### PR 6: `EdgeVector/last-stack` — Use local install in CI

- Update `last-stack` CI to use `lastdb app install brain --from . --rev $GITHUB_SHA --dev` (§5.2)

### 6.3 Rollout

**No feature flag needed.** The changes are additive:

- `CompatRow.lastdb` is optional. Old indexes work.
- `--from` / `--dev` is new surface. Existing installs are unaffected.
- The manifest check (from #357) runs post-clone for all installs. It only rejects when the manifest has a `lastdb` constraint that the running node violates.

**No migration for existing installs.** Installed apps keep working. The next `lastdb app upgrade` uses the new resolve logic if the registry has `lastdb` ranges.

## 7. Alternatives considered

### 7.1 Environment variable for `--dev` mode

**Rejected.** An attacker who controls environment variables can already supply `LASTDB_REGISTRY_INDEX` and `LASTDB_REGISTRY_TRUST_KEY` to point the installer at an unsigned index. A `LASTDB_APP_DEV_MODE=1` env var adds no security boundary and only saves typing. Add it if the nightly CI needs it, but the explicit `--dev` flag is clearer for human operators.

### 7.2 No staging, just early checks

**Rejected.** Even with early version checks, the clone and checkout can fail (network, disk full, wrong commit). Staging isolates the old install from the new one. The atomic swap (§4.2) is worth keeping.

### 7.3 Bump `INDEX_VERSION` to 2

**Optional.** Adding an optional field (`CompatRow.lastdb`) does not break parsers. Old clients (those without PR 1) ignore the field. New clients (with PR 1) accept rows with or without it. A version bump is only needed if the grammar change is required to be explicit, for example to refuse old clients that cannot enforce the version range. Since old clients already refuse rows whose `lastdb_version` does not match exactly, the risk is low: they pick a row without `lastdb` ranges (or none), which is the same as the current behavior.

**Recommendation**: Do not bump `INDEX_VERSION`. Add a note to the `CompatRow` doc comment that `lastdb` was added in `lastdb` version `X.Y.Z` (the version of PR 1).

### 7.4 Manifest `lastdb` as the only check (no index field)

**Rejected.** The post-clone check (PR #357) alone requires staging and rollback logic, which has produced bugs. The early check (index `lastdb` field) rejects incompatible rows before any download. The manifest check stays as defense-in-depth (§3.3), but the primary check is in `pick_row()`.

### 7.5 Local installs without signature bypass (`--from` accepts signed indexes)

**Considered and deferred.** A local index file (`file:///opt/registry/stable.json`) could go through the same signature verification as a URL-based index. This is useful for airgapped installs. However:

- The `--from` flag as proposed (§2.1) takes a git source, not an index file.
- Airgapped installs can use `LASTDB_REGISTRY_INDEX=file:///...` (existing).
- `--from` is for dev/CI, where unsigned sources are acceptable with `--dev`.

**Future work**: Add `lastdb app install --index file:///path/to/stable.json` for signed local indexes, without `--dev`. This is orthogonal to `--from`.

## 8. Implementation notes

### 8.1 File changes

**New files**: None. All changes are in existing modules.

**Changed files**:

- `lastdb_node/src/app_registry_index/location.rs`: Add `IndexLocation::Local { source, rev }`
- `lastdb_node/src/app_registry_index/resolve.rs`: Add `resolve_local()`, update `resolve()`, add constraint check to `pick_row()`
- `lastdb_node/src/app_registry_index/signing.rs`: Add `pub lastdb: Option<String>` to `CompatRow`
- `lastdb_node/src/app_registry_index/install.rs`: Keep staging logic from #357, add defense-in-depth manifest check
- `lastdb_node/src/app_registry_index/mod.rs`: Export new types if needed
- `lastdb_node/src/main.rs` (or the CLI parser): Add `--from`, `--rev`, `--dev` flags to `app install` and `app upgrade` commands

### 8.2 Function size limits

All new functions should stay under 100 lines (repo policy, `AGENTS.md`). `resolve_local()` is ~50 lines. The constraint check in `pick_row()` adds ~10 lines. No function crosses the limit.

### 8.3 Error messages

**When `pick_row()` finds no matching rows** because all were filtered by the `lastdb` constraint:

```
no stable row for app 'brain' was proved with lastdb 0.22.5 (proved with: 0.23.0, 0.23.1); 
the app requires lastdb >=0.23.0, <0.24.0; run `brew upgrade lastdb` or wait for the next proved set
```

**When `--from` is used without `--dev`**:

```
--from requires --dev: local installs are not signed and must be explicitly opted into with --dev
```

**When `--dev` is used without `--from`**:

```
--dev requires --from: use --from <path> to install from a local source, or omit --dev to install from the registry
```

## 9. Open questions

1. **Should `resolve_local()` check the manifest at the current HEAD, or at the `--rev` commit?**
   - **Current proposal**: Reads the manifest from the current checkout (`source/lastdb-app.json`), not from the commit at `--rev`. This is simpler (no git checkout in `resolve()`), but the post-clone check in `install_pinned()` catches manifest changes.
   - **Alternative**: Check out `--rev` in a temp directory, read the manifest, then discard it. More robust, but duplicates the git checkout logic.

2. **Should the nightly canary (§5.1) live in `EdgeVector/lastdb` or in each app repo?**
   - **Current proposal**: One canary in `lastdb` tests all registry apps. Each app repo has its own compat matrix (§5.2).
   - **Alternative**: No central canary. Each app repo is responsible for its own testing. This is more decentralized but risks gaps.

3. **Should `lastdb app list` show the `trust` source by default, or only with `--verbose`?**
   - **Current proposal**: Always show `trust` (`"pinned"`, `"dev"`, `"flag"`, `"env:LASTDB_REGISTRY_TRUST_KEY"`) so dev installs are visible.
   - **Alternative**: Hide it unless `--verbose`, since most installs are `"pinned"`.

## 10. Summary

This proposal adds two capabilities:

1. **Local install source** (`--from <path> --rev <sha> --dev`) for proofs, dogfooding, and pre-release testing. It goes through the same install code as the registry path. Production defaults stay signed-only.
2. **Early version checks** in the index (`CompatRow.lastdb`), checked by `pick_row()` before any download. The manifest check from PR #357 stays as defense-in-depth.

Together, these simplify the install flow, remove the need for post-clone version rollback in the common case, and enable real end-to-end testing without a published registry entry.

**Migration**: Merge PR #357 as-is. Implement this proposal in 4-6 PRs across `lastdb`, `last-stack`, and `homebrew-lastdb` (§6.2). No feature flags. No breaking changes.
