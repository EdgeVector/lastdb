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
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::Utc;
use schema_service_core::schema_org_seeds::{ParsedDump, SchemaOrgClass};
use schema_service_core::types::SchemaAddOutcome;
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

const GENERATOR: &str = "scripts/generate_validated_schema_org_seeds";
const SOURCE_OVERLAY: &str = "classification_overlay.json";
const SOURCE_JSONLD: &str = "schemaorg-current-https.jsonld";
const EMBEDDER: &str = "FoldDbFastEmbedder (fastembed all-MiniLM-L6-v2)";

impl<T: Serialize> ValidatedSnapshot<T> {
    fn new(generated_at: String, entry_count: usize, entries: T) -> Self {
        Self {
            version: 1,
            generated_at,
            generator: GENERATOR,
            source_overlay: SOURCE_OVERLAY,
            source_jsonld: SOURCE_JSONLD,
            embedder: EMBEDDER,
            entry_count,
            entries,
        }
    }
}

/// Counters and failures from the serial `add_schema` pass.
#[derive(Default)]
struct IngestReport {
    added: usize,
    skipped: usize,
    failed: Vec<(String, String)>,
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
    let report = ingest_classes(&state, &dump).await;
    report_failures(&report);

    eprintln!("==> Serializing final canonical_fields + schemas to JSON …");
    let (canonical_snapshot, schema_snapshot) = build_snapshots(&state)?;

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
    write_canonical_snapshot(&data_dir, &canonical_snapshot)?;
    write_schema_files(&data_dir, &schema_snapshot)?;

    if !report.failed.is_empty() {
        eprintln!(
            "==> NOTE: {} Schema.org classes failed to add_schema. See stderr above.",
            report.failed.len()
        );
    }

    Ok(())
}

async fn ingest_classes(state: &SchemaServiceState, dump: &ParsedDump) -> IngestReport {
    let mut report = IngestReport::default();
    for (i, class) in dump.classes.iter().enumerate() {
        let label = class.label.as_str();
        let schema = build_class_schema(state, dump, class);
        match state
            .add_schema(schema, std::collections::HashMap::new())
            .await
        {
            Ok(SchemaAddOutcome::AlreadyExists(_, _)) => report.skipped += 1,
            Ok(
                SchemaAddOutcome::Added(_, _)
                | SchemaAddOutcome::Expanded(_, _, _)
                | SchemaAddOutcome::Composed(_, _, _),
            ) => report.added += 1,
            Ok(SchemaAddOutcome::DescriptiveNameConflict(c)) => {
                report.failed.push((
                    label.to_string(),
                    format!("conflict with {}: {}", c.existing_canonical, c.reason),
                ));
            }
            Err(e) => {
                report.failed.push((label.to_string(), e.to_string()));
            }
        }

        if (i + 1) % 100 == 0 || i + 1 == dump.classes.len() {
            eprintln!(
                "    processed {}/{} (added={}, skipped={}, failed={})",
                i + 1,
                dump.classes.len(),
                report.added,
                report.skipped,
                report.failed.len()
            );
        }
    }
    report
}

fn report_failures(report: &IngestReport) {
    if report.failed.is_empty() {
        return;
    }
    eprintln!(
        "==> WARNING: {} classes failed add_schema. First 10 failures:",
        report.failed.len()
    );
    for (label, err) in report.failed.iter().take(10) {
        eprintln!("      {label} — {err}");
    }
}

/// Build the schema for one Schema.org class, with every field's type,
/// description, and classification baked in.
fn build_class_schema(
    state: &SchemaServiceState,
    dump: &ParsedDump,
    class: &SchemaOrgClass,
) -> Schema {
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
        apply_canonical_field_metadata(state, &mut schema, field);
    }
    schema.compute_identity_hash();
    schema
}

/// Pull the field's type and data classification from the canonical registry
/// seeded earlier — `seed_canonical_fields` already inferred real types from
/// Schema.org `rangeIncludes` and pulled classifications from the curated
/// overlay. Fields the registry doesn't cover fall back to the pre-registry
/// placeholders (String / "word").
///
/// Everything must be baked in here rather than left to `state.add_schema`:
/// it runs `apply_canonical_classifications` internally, but the copy it
/// writes to `state.schemas` is taken BEFORE that call, so anything not set
/// on the schema up front is missing from the persisted/snapshot version we
/// dump to JSON.
fn apply_canonical_field_metadata(state: &SchemaServiceState, schema: &mut Schema, field: &str) {
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
    schema.field_types.insert(field.to_string(), field_type);
    schema
        .field_classifications
        .insert(field.to_string(), classification_tags);

    if let Some(classification) = canonical.and_then(|c| c.classification) {
        schema
            .field_data_classifications
            .insert(field.to_string(), classification);
    }
}

type CanonicalSnapshot =
    ValidatedSnapshot<BTreeMap<String, schema_service_core::types::CanonicalField>>;
type SchemaSnapshot = ValidatedSnapshot<BTreeMap<String, Schema>>;

fn build_snapshots(
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
fn write_canonical_snapshot(
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
fn write_schema_files(
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
fn colliding_descriptive_names(snapshot: &SchemaSnapshot) -> HashSet<String> {
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
