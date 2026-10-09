//! Generator: validated Schema.org seed snapshot.
//!
//! **Run manually.** Do not invoke at service startup.
//!
//! ```bash
//! cargo run --release --bin generate-validated-schema-org-seeds
//! ```
//!
//! What this script does, in order:
//!
//! 1. Spins up a real [`SchemaServiceState`] in a temporary Sled directory,
//!    backed by [`FoldDbFastEmbedder`] — the same fastembed-backed
//!    embedding model the dev binary and Lambda use in production.
//! 2. Seeds the curated canonical fields ([`builtin_canonical_fields::seed`])
//!    and the Schema.org canonical fields with the classification overlay
//!    ([`schema_org_seeds::seed_canonical_fields`]).
//! 3. Iterates every Schema.org class **serially** (one at a time), calling
//!    [`SchemaServiceState::add_schema`] per class. Each add runs the full
//!    validation + semantic-matching + classification pipeline. Because
//!    classes are processed in order, later classes benefit from the
//!    canonical fields the earlier ones added, and the semantic similarity
//!    matcher collapses Schema.org properties that embed close to an
//!    existing canonical name.
//! 4. When every class is done, the final registry (`canonical_fields`) and
//!    schema map (`schemas`) are serialized to JSON and written to the repo:
//!    - `crates/core/data/schema_org/validated_canonical_fields.json`
//!    - `crates/core/data/schema_org/validated_schemas.json`
//!
//! At runtime, the service loads those JSON files at cold start instead of
//! re-running this pipeline. That moves the expensive validation and
//! embedding-based canonicalization to dev time, making cold starts a
//! deterministic, byte-identical restore from committed data.
//!
//! ## Regeneration policy
//!
//! Run this binary whenever:
//! - The Schema.org dump (`schemaorg-current-https.jsonld`) is refreshed.
//! - The classification overlay (`classification_overlay.json`) is
//!   regenerated.
//! - The curated canonical fields in `builtin_canonical_fields.rs` change.
//! - The embedder version or the semantic-matching threshold changes.
//!
//! Commit the resulting JSON diffs alongside whichever source change
//! triggered regeneration so PR review shows the downstream blast radius.
//!
//! ## Cost
//!
//! One run touches ~1000 classes × add_schema pipeline. Expect minutes of
//! wall-clock time. No Anthropic calls as long as canonical fields stay
//! pre-seeded via the overlay (the add_schema path only hits the LLM for
//! fields it cannot match to an existing canonical).

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::Utc;
use schema_service_core::{
    builtin_canonical_fields, schema_org_seeds, Embedder, SchemaServiceState,
};
use schema_service_server_shared::FoldDbFastEmbedder;
use schema_types::{FieldValueType, KeyConfig, Schema, SchemaSource, SchemaType};
use serde::Serialize;
use tempfile::tempdir;

const HASH_FIELD: &str = "identifier";

#[derive(Serialize)]
struct ValidatedSnapshot<T: Serialize> {
    version: u32,
    generated_at: String,
    generator: &'static str,
    source_overlay: &'static str,
    source_jsonld: &'static str,
    embedder: &'static str,
    entry_count: usize,
    entries: T,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("==> Bootstrapping schema service state with real embedder …");
    let dir = tempdir()?;
    let db_path = dir
        .path()
        .join("validated_generator_db")
        .to_string_lossy()
        .to_string();
    let embedder: Arc<dyn Embedder> = Arc::new(FoldDbFastEmbedder::new());
    let state = SchemaServiceState::new(&db_path, embedder)?;

    eprintln!("==> Seeding curated canonical fields (the 151 hand-reviewed entries) …");
    builtin_canonical_fields::seed(&state).await?;

    eprintln!("==> Seeding Schema.org canonical fields via the classification overlay …");
    let dump = schema_org_seeds::parse_dump()?;
    schema_org_seeds::seed_canonical_fields(&state, &dump).await?;

    eprintln!(
        "==> Ingesting {} Schema.org classes serially through add_schema …",
        dump.classes.len()
    );

    let mut added = 0usize;
    let mut skipped = 0usize;
    let mut failed: Vec<(String, String)> = Vec::new();

    for (i, class) in dump.classes.iter().enumerate() {
        let label = class.label.as_str();

        let mut field_names: Vec<String> = dump
            .properties_by_domain
            .get(label)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|p| schema_org_seeds::property_name_to_snake_case(&p))
            .collect();
        if !field_names.iter().any(|f| f == HASH_FIELD) {
            field_names.insert(0, HASH_FIELD.to_string());
        }
        field_names.sort();
        field_names.dedup();

        let mut schema = Schema::new(
            label.to_string(),
            SchemaType::Hash,
            Some(KeyConfig::new(Some(HASH_FIELD.to_string()), None)),
            Some(field_names.clone()),
            None,
            None,
        );
        schema.descriptive_name = Some(label.to_string());
        schema.source = SchemaSource::StarterSeed;

        for field in &field_names {
            let description = dump
                .property_by_snake_name
                .get(field)
                .and_then(|p| p.comment.clone())
                .unwrap_or_else(|| format!("Schema.org property '{field}'"));
            schema
                .field_descriptions
                .insert(field.clone(), description.trim().to_string());

            // Pull the field's type and data classification from the
            // canonical registry seeded above — `seed_canonical_fields`
            // already inferred real types from Schema.org `rangeIncludes`
            // and pulled classifications from the curated overlay. Fields
            // the registry doesn't cover fall back to the pre-registry
            // placeholders (String / "word").
            //
            // Everything must be baked in here rather than left to
            // `state.add_schema`: it runs `apply_canonical_classifications`
            // internally, but the copy it writes to `state.schemas` is
            // taken BEFORE that call, so anything not set on the schema
            // up front is missing from the persisted/snapshot version we
            // dump to JSON.
            let canonical = state
                .canonical_fields
                .read()
                .ok()
                .and_then(|registry| registry.get(field).cloned());

            let field_type = canonical
                .as_ref()
                .map_or(FieldValueType::String, |c| c.field_type.clone());
            let classification_tags = if matches!(
                field_type,
                FieldValueType::Integer | FieldValueType::Float | FieldValueType::Number
            ) {
                vec!["number".to_string()]
            } else {
                vec!["word".to_string()]
            };
            schema.field_types.insert(field.clone(), field_type);
            schema
                .field_classifications
                .insert(field.clone(), classification_tags);

            if let Some(classification) = canonical.and_then(|c| c.classification) {
                schema
                    .field_data_classifications
                    .insert(field.clone(), classification);
            }
        }
        schema.compute_identity_hash();

        match state
            .add_schema(schema, std::collections::HashMap::new())
            .await
        {
            Ok(schema_service_core::types::SchemaAddOutcome::AlreadyExists(_, _)) => skipped += 1,
            Ok(
                schema_service_core::types::SchemaAddOutcome::Added(_, _)
                | schema_service_core::types::SchemaAddOutcome::Expanded(_, _, _)
                | schema_service_core::types::SchemaAddOutcome::Composed(_, _, _),
            ) => added += 1,
            Ok(schema_service_core::types::SchemaAddOutcome::DescriptiveNameConflict(c)) => {
                failed.push((
                    label.to_string(),
                    format!("conflict with {}: {}", c.existing_canonical, c.reason),
                ));
            }
            Err(e) => {
                failed.push((label.to_string(), e.to_string()));
            }
        }

        if (i + 1) % 100 == 0 || i + 1 == dump.classes.len() {
            eprintln!(
                "    processed {}/{} (added={}, skipped={}, failed={})",
                i + 1,
                dump.classes.len(),
                added,
                skipped,
                failed.len()
            );
        }
    }

    if !failed.is_empty() {
        eprintln!(
            "==> WARNING: {} classes failed add_schema. First 10 failures:",
            failed.len()
        );
        for (label, err) in failed.iter().take(10) {
            eprintln!("      {label} — {err}");
        }
    }

    eprintln!("==> Serializing final canonical_fields + schemas to JSON …");

    let canonical_map: BTreeMap<String, _> = {
        let read = state
            .canonical_fields
            .read()
            .map_err(|e| format!("canonical_fields read lock: {e}"))?;
        read.clone().into_iter().collect()
    };

    let schemas_map: BTreeMap<String, Schema> = {
        let read = state
            .schemas
            .read()
            .map_err(|e| format!("schemas read lock: {e}"))?;
        read.clone().into_iter().collect()
    };

    // Narrow the schema snapshot to StarterSeed only — SystemSeed schemas
    // are owned by the fingerprint subsystem and live in a different
    // pipeline; we don't want to freeze their definitions here.
    let starter_schemas: BTreeMap<String, Schema> = schemas_map
        .into_iter()
        .filter(|(_, s)| matches!(s.source, SchemaSource::StarterSeed))
        .collect();

    let now = Utc::now().to_rfc3339();

    let canonical_snapshot = ValidatedSnapshot {
        version: 1,
        generated_at: now.clone(),
        generator: "scripts/generate_validated_schema_org_seeds",
        source_overlay: "classification_overlay.json",
        source_jsonld: "schemaorg-current-https.jsonld",
        embedder: "FoldDbFastEmbedder (fastembed all-MiniLM-L6-v2)",
        entry_count: canonical_map.len(),
        entries: canonical_map,
    };

    let schema_snapshot = ValidatedSnapshot {
        version: 1,
        generated_at: now,
        generator: "scripts/generate_validated_schema_org_seeds",
        source_overlay: "classification_overlay.json",
        source_jsonld: "schemaorg-current-https.jsonld",
        embedder: "FoldDbFastEmbedder (fastembed all-MiniLM-L6-v2)",
        entry_count: starter_schemas.len(),
        entries: starter_schemas,
    };

    let data_dir: PathBuf = [
        env!("CARGO_MANIFEST_DIR"),
        "..",
        "..",
        "crates",
        "core",
        "data",
        "schema_org",
    ]
    .iter()
    .collect();

    // Canonical fields stay in a single aggregate file. Each entry is a
    // small structural record; per-file would mean 1,600+ tiny files
    // with no reviewability win.
    // Serialize via `serde_json::to_value` so map keys come out sorted
    // (serde_json::Map is BTreeMap-backed without the `preserve_order`
    // feature). Direct struct serialization would emit `HashMap` fields
    // (field_types, field_classifications, …) in arbitrary order, making
    // every regeneration an unreviewable full-file diff.
    let canonical_path = data_dir.join("validated_canonical_fields.json");
    fs::write(
        &canonical_path,
        serde_json::to_string_pretty(&serde_json::to_value(&canonical_snapshot)?)?,
    )?;
    eprintln!(
        "==> Wrote {} canonical_fields → {}",
        canonical_snapshot.entry_count,
        canonical_path.display()
    );

    // Schemas go one-per-file under `schemas/`. Each schema is an
    // intentional entity with ~50+ fields and distinct metadata —
    // per-file gives clean PR diffs (change Person, only Person.json
    // shows up), mirrors Sled's per-key storage, and makes selective
    // regeneration trivial.
    //
    // Blow away the entire directory first so deleted Schema.org types
    // (e.g. if Schema.org drops something in a future version) don't
    // leave stale files around.
    let schemas_dir = data_dir.join("schemas");
    if schemas_dir.exists() {
        fs::remove_dir_all(&schemas_dir)?;
    }
    fs::create_dir_all(&schemas_dir)?;

    // Detect descriptive_name collisions ahead of write — the generator
    // must never silently overwrite one schema with another that happens
    // to share a descriptive_name. If collisions exist, append a short
    // identity_hash slice to disambiguate just the colliding set, so
    // unique names still produce clean `Person.json`-style filenames.
    let mut name_counts: BTreeMap<String, usize> = BTreeMap::new();
    for schema in schema_snapshot.entries.values() {
        let name = schema
            .descriptive_name
            .as_deref()
            .unwrap_or(schema.name.as_str())
            .to_string();
        *name_counts.entry(name).or_default() += 1;
    }
    let colliding: std::collections::HashSet<String> = name_counts
        .iter()
        .filter_map(|(n, c)| if *c > 1 { Some(n.clone()) } else { None })
        .collect();
    if !colliding.is_empty() {
        eprintln!(
            "==> Note: {} descriptive_name collisions will be disambiguated with \
             short identity_hash suffixes: {:?}",
            colliding.len(),
            colliding
        );
    }

    let mut written = 0usize;
    for schema in schema_snapshot.entries.values() {
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
        let out_path = schemas_dir.join(filename);
        fs::write(
            &out_path,
            serde_json::to_string_pretty(&serde_json::to_value(schema)?)?,
        )?;
        written += 1;
    }
    eprintln!(
        "==> Wrote {} per-schema files under {}",
        written,
        schemas_dir.display()
    );

    if !failed.is_empty() {
        eprintln!(
            "==> NOTE: {} Schema.org classes failed to add_schema. See stderr above.",
            failed.len()
        );
    }

    Ok(())
}
