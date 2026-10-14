//! App manifest loading.

use super::*;
use fold_db::schema::ManifestRetention;

/// App manifest consumed by `lastdb app …` (JSON file, conventionally
/// `lastdb-app.json`). `schemas` entries are declarative schema definitions
/// (`{ "name", "descriptive_name"?, "schema_type", "fields": [...] }`).
///
/// A schema entry may also carry a TTL for its records:
///
/// ```json
/// { "name": "myapp/DayLog", "schema_type": "HashRange",
///   "key": { "hash_field": "day", "range_field": "at" },
///   "fields": ["day", "at", "text"],
///   "retention": { "ttl_days": 90 } }
/// ```
///
/// `retention` takes `ttl_days` or `ttl_seconds` (not both; above zero and at
/// most 3650 days). It is node-local policy: `lastdb app check` sends it beside
/// the schema, never inside it, so it cannot change the schema identity hash.
/// An entry with no `retention` gets no policy, and nothing is deleted.
///
/// The node applies the TTL when the schema is declared (`lastdb app check
/// --sync`), before the first write, and it covers both installed names of the
/// schema. There is no backfill: a record written before the declare is never
/// expired. A synced second device declares the same manifest on its own node,
/// because the policy is not synced; a TTL delete syncs as an ordinary delete.
///
/// What the app author must do:
///
/// - Range and HashRange: begin every range key with the entry time as unix
///   seconds zero-padded to 20 digits, for example
///   `00000000001791863226-0007`. A key that does not begin with 20 digits is
///   never expired. A manifest cannot show the values, so `lastdb app check`
///   warns only when `field_types` makes the range field a non-String type.
/// - Use a day slice (or coarser) as the HashRange hash. The node keeps one
///   registry entry for each hash it has ever seen, and every sweep queries
///   them all.
/// - Hash and Single: nothing. A record expires by the time since its last
///   write.
/// - Write one record for each entry and never edit it: a rewrite of a
///   HashRange or Range record does not reset its age.
#[derive(Debug, Clone, Deserialize)]
pub struct AppManifest {
    pub app_id: String,
    pub metadata: AppMetadata,
    pub version: String,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub artifact: Option<AppArtifact>,
    #[serde(default)]
    pub run: Option<AppRunConfig>,
    #[serde(default)]
    pub uses: Vec<String>,
    #[serde(default)]
    pub schemas: Vec<Value>,
}

/// Local app runner convention for source-installed apps.
///
/// The entrypoint is always relative to the installed source checkout. Runtime
/// support is deliberately small until artifact verification lands.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct AppRunConfig {
    pub runtime: String,
    pub entrypoint: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
}

pub fn load_manifest(path: &Path) -> Result<AppManifest, String> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read manifest {}: {e}", path.display()))?;
    let manifest: AppManifest = serde_json::from_str(&raw)
        .map_err(|e| format!("invalid manifest {}: {e}", path.display()))?;
    if manifest.app_id.trim().is_empty() {
        return Err("manifest app_id must be non-empty".into());
    }
    validate_app_version(&manifest.version)?;
    if manifest.schemas.is_empty() {
        return Err(
            "manifest declares no schemas — a LastDB app manifest must list the schemas it uses"
                .into(),
        );
    }
    for schema in &manifest.schemas {
        manifest_schema_retention(schema)?;
    }
    Ok(manifest)
}

/// The validated `retention` of one manifest schema entry, if it has one.
pub fn manifest_schema_retention(schema: &Value) -> Result<Option<ManifestRetention>, String> {
    let name = schema
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("<unnamed>");
    schema
        .get("retention")
        .map(ManifestRetention::from_value)
        .transpose()
        .map_err(|e| format!("manifest schema '{name}': {e}"))
}

// ─── Step 1: check (declare on the local Mini) ────────────────────────────
