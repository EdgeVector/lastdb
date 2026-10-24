# App Manifest Version Constraints

Apps can declare which LastDB versions they're compatible with using semver-style constraints in their manifest.

## Manifest Field

Add a `lastdb` field to your app manifest (JSON or TOML):

**JSON** (`lastdb-app.json`, `<app>.app.json`):
```json
{
  "app_id": "brain",
  "version": "0.8.0",
  "lastdb": ">=0.23.0, <0.24.0",
  "metadata": { ... },
  "schemas": [ ... ]
}
```

**TOML** (`folddb.toml`):
```toml
app_id = "brain"
lastdb = ">=0.23.0, <0.24.0"

[metadata]
display_name = "Brain"
...
```

## Constraint Syntax

Constraints are comma-separated predicates. All predicates must match for install to succeed.

| Syntax | Meaning | Example |
|--------|---------|---------|
| `>=X.Y.Z` | Greater than or equal | `>=0.23.0` |
| `<X.Y.Z` | Less than | `<0.24.0` |
| `=X.Y.Z` or `X.Y.Z` | Exact match | `=0.23.4` or `0.23.4` |

**Combined:**
```json
"lastdb": ">=0.23.0, <0.24.0"   // 0.23.x only
"lastdb": ">=0.23.5"             // 0.23.5 or newer
"lastdb": "0.23.4"               // exactly 0.23.4
```

## Version Matching

LastDB build strings (e.g. `0.23.4-pre.0-324-g0ef44ca87`) are parsed as SemVer:
- **Core version:** `major.minor.patch` (e.g. `0.23.4`)
- **Pre-release / build metadata:** ignored for constraint matching

Constraints match against the **core version only**.

## When Constraints Are Checked

1. **Install time:** `lastdb app install <app>` checks after cloning but before completing install
2. **Upgrade time:** `lastdb app upgrade <app>` checks the new version
3. **Registry resolution:** Future work - may filter compat rows by constraint

## Error Messages

If the running LastDB doesn't satisfy the constraint, install fails with a clear message:

```
Error: app 'brain' requires lastdb >=0.23.0, <0.24.0, but you have 0.22.9 (core version 0.22.9)
Remedy: brew upgrade lastdb
```

## Optional vs Required

- **No `lastdb` field:** App installs on any LastDB version (backward compatible default)
- **With `lastdb` field:** Install enforced, but only for manifests that declare it

## Registry Integration

The static registry (`stable.json`, `next.json`) **does not include constraints**. Constraints live in app source repos and are checked at install time after the source is cloned.

A future enhancement may:
- Include `lastdb_constraint` in compat rows
- Let `lastdb app resolve` pre-filter incompatible rows
- Show constraints in `lastdb app info`

## Recommendations

### For App Developers

1. **Test before constraining:** Verify your app works with a LastDB version before adding a constraint
2. **Use ranges, not exact versions:** `>=0.23.0, <0.24.0` is more flexible than `0.23.4`
3. **Update constraints in lockstep:** Bump the constraint when you use new LastDB APIs
4. **Document breaking changes:** Note in your changelog when constraint changes

### Common Patterns

**Minor version compatibility:**
```json
"lastdb": ">=0.23.0, <0.24.0"
```
Works with any 0.23.x, breaks on 0.24.0 (assumes semantic versioning).

**Open-ended minimum:**
```json
"lastdb": ">=0.23.5"
```
Works with 0.23.5 or any newer version. Use when you need a specific feature but expect forward compatibility.

**Exact pinning (rare):**
```json
"lastdb": "0.23.4"
```
Only use when you've proved the app breaks on other versions. Blocks upgrades.

## CI Integration

Test your app against multiple LastDB versions in CI:

```yaml
strategy:
  matrix:
    lastdb: [stable, canary, main]
```

See `.github/workflows/app-install-smoke.yml` in the lastdb repo for a full example.

## See Also

- `docs/lastdb-app-registry-index.md` — registry compat rows
- `docs/designs/exemem-app-registry-simple.md` — registry design
- `lastdb_node/src/app_registry_index/version_constraint.rs` — implementation
