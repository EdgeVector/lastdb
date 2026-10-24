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
/// A TTL deletes records for good. `retention` takes `ttl_days` or
/// `ttl_seconds` (not both; at least one hour by default, at most 3650 days),
/// and `confirm_expire_existing`. It is node-local policy: `lastdb app check`
/// sends it beside the schema, never inside it, so it cannot change the schema
/// identity hash. An entry with no `retention` gets no policy.
///
/// The node applies the TTL when the schema is declared (`lastdb app check
/// --sync`), before the first write, and it covers both installed names of the
/// schema. It refuses the TTL when another schema is bound to the same catalog
/// schema (`shared_with`) or another app owns it. It also refuses a new TTL on
/// a Range or HashRange schema that already holds data, and a shorter TTL than
/// the one running, unless `--confirm-expire-existing` says the records may
/// expire. On a Range schema every old record expires at the next sweep, those
/// written before the declare included. Hash and Single schemas track only
/// later writes. A synced second device must declare the same manifest: the
/// policy is not synced, and a TTL erase syncs to the peers as an erase of the key. The
/// full contract is in `docs/app-manifest-retention.md`.
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
/// - Give schemas with different lifecycles different shapes: two schemas of
///   one shape can resolve to one catalog schema and then share one dataset.
#[derive(Debug, Clone, Deserialize)]
pub struct AppManifest {
    /// Optional LastDB version constraint (semver-style, e.g. `>=0.23.0, <0.24.0`).
    /// Checked at install time against the running `lastdb --version`.
    #[serde(default, rename = "lastdb")]
    pub lastdb_constraint: Option<String>,
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
    /// Contract surfaces this app depends on: "http" for HTTP API routes,
    /// "outbox" for IndexChangeBatch format. Apps using the HTTP API (e.g.,
    /// Brain, LastSecrets) declare `["http"]`. Apps consuming the outbox
    /// (e.g., lastseek, Search) declare `["outbox"]`. Apps using both declare
    /// `["http", "outbox"]`. When omitted, defaults to an empty list for
    /// backward compatibility with old manifests.
    #[serde(default)]
    pub contract_surfaces: Vec<String>,
    /// HTTP API version this app requires. For TypeScript apps using
    /// `@lastdb/app-sdk`, this is inferred from the SDK version by the
    /// registry publisher. For other HTTP API apps, declare it explicitly.
    /// Only relevant when `contract_surfaces` includes "http".
    #[serde(default)]
    pub api_version: Option<u32>,
    /// Outbox format version this app requires. Apps consuming
    /// IndexChangeBatch files (lastseek, Search) declare the format version
    /// they understand. Only relevant when `contract_surfaces` includes "outbox".
    #[serde(default)]
    pub outbox_format_version: Option<u32>,
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
