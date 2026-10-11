//! Snapshot assembly and on-disk output for the seed generator.

use super::*;
use std::collections::HashSet;
use std::path::Path;

pub(crate) type CanonicalSnapshot =
    ValidatedSnapshot<BTreeMap<String, schema_service_core::types::CanonicalField>>;
pub(crate) type SchemaSnapshot = ValidatedSnapshot<BTreeMap<String, Schema>>;

pub(crate) fn build_snapshots(
    state: &SchemaServiceState,
) -> Result<(CanonicalSnapshot, SchemaSnapshot), Box<dyn std::error::Error>> {
    let canonical_map: BTreeMap<String, _> = {
        let read = state
            .canonical_fields
            .read()
            .map_err(|e| format!("canonical_fields read lock: {e}"))?;
        read.clone().into_iter().collect()
    };

    // Narrow the schema snapshot to StarterSeed only — SystemSeed schemas
    // are owned by the fingerprint subsystem and live in a different
    // pipeline; we don't want to freeze their definitions here.
    let starter_schemas: BTreeMap<String, Schema> = {
        let read = state
            .schemas
            .read()
            .map_err(|e| format!("schemas read lock: {e}"))?;
        read.clone()
            .into_iter()
            .filter(|(_, s)| matches!(s.source, SchemaSource::StarterSeed))
            .collect()
    };

    let now = Utc::now().to_rfc3339();
    let canonical_count = canonical_map.len();
    let schema_count = starter_schemas.len();
    Ok((
        ValidatedSnapshot::new(now.clone(), canonical_count, canonical_map),
        ValidatedSnapshot::new(now, schema_count, starter_schemas),
    ))
}

/// Canonical fields stay in a single aggregate file. Each entry is a
/// small structural record; per-file would mean 1,600+ tiny files
/// with no reviewability win.
///
/// Serialize via `serde_json::to_value` so map keys come out sorted
/// (serde_json::Map is BTreeMap-backed without the `preserve_order`
/// feature). Direct struct serialization would emit `HashMap` fields
/// (field_types, field_classifications, …) in arbitrary order, making
/// every regeneration an unreviewable full-file diff.
pub(crate) fn write_canonical_snapshot(
    data_dir: &Path,
    snapshot: &CanonicalSnapshot,
) -> Result<(), Box<dyn std::error::Error>> {
    let canonical_path = data_dir.join("validated_canonical_fields.json");
    fs::write(
        &canonical_path,
        serde_json::to_string_pretty(&serde_json::to_value(snapshot)?)?,
    )?;
    eprintln!(
        "==> Wrote {} canonical_fields → {}",
        snapshot.entry_count,
        canonical_path.display()
    );
    Ok(())
}

/// Schemas go one-per-file under `schemas/`. Each schema is an
/// intentional entity with ~50+ fields and distinct metadata —
/// per-file gives clean PR diffs (change Person, only Person.json
/// shows up), mirrors Sled's per-key storage, and makes selective
/// regeneration trivial.
pub(crate) fn write_schema_files(
    data_dir: &Path,
    snapshot: &SchemaSnapshot,
) -> Result<(), Box<dyn std::error::Error>> {
    // Blow away the entire directory first so deleted Schema.org types
    // (e.g. if Schema.org drops something in a future version) don't
    // leave stale files around.
    let schemas_dir = data_dir.join("schemas");
    if schemas_dir.exists() {
        fs::remove_dir_all(&schemas_dir)?;
    }
    fs::create_dir_all(&schemas_dir)?;

    let colliding = colliding_descriptive_names(snapshot);
    if !colliding.is_empty() {
        eprintln!(
            "==> Note: {} descriptive_name collisions will be disambiguated with \
             short identity_hash suffixes: {:?}",
            colliding.len(),
            colliding
        );
    }

    let mut written = 0usize;
    for schema in snapshot.entries.values() {
        // File-safe name — Schema.org types are CamelCase ASCII with a
        // couple of leading-digit types (`3DModel`). Fall back to the
        // identity_hash if descriptive_name is missing.
        let descriptive = schema
            .descriptive_name
            .as_deref()
            .unwrap_or(schema.name.as_str());
        let filename = if colliding.contains(descriptive) {
            // Identity_hash is the schema.name post-validation. Take an
            // 8-char prefix for a filesystem-friendly suffix.
            let short_hash = &schema.name[..schema.name.len().min(8)];
            format!("{descriptive}_{short_hash}.json")
        } else {
            format!("{descriptive}.json")
        };
        fs::write(
            schemas_dir.join(filename),
            serde_json::to_string_pretty(&serde_json::to_value(schema)?)?,
        )?;
        written += 1;
    }
    eprintln!(
        "==> Wrote {} per-schema files under {}",
        written,
        schemas_dir.display()
    );
    Ok(())
}

/// Detect descriptive_name collisions ahead of write — the generator
/// must never silently overwrite one schema with another that happens
/// to share a descriptive_name. If collisions exist, a short
/// identity_hash slice disambiguates just the colliding set, so
/// unique names still produce clean `Person.json`-style filenames.
pub(crate) fn colliding_descriptive_names(snapshot: &SchemaSnapshot) -> HashSet<String> {
    let mut name_counts: BTreeMap<String, usize> = BTreeMap::new();
    for schema in snapshot.entries.values() {
        let name = schema
            .descriptive_name
            .as_deref()
            .unwrap_or(schema.name.as_str())
            .to_string();
        *name_counts.entry(name).or_default() += 1;
    }
    name_counts
        .into_iter()
        .filter_map(|(n, c)| (c > 1).then_some(n))
        .collect()
}
