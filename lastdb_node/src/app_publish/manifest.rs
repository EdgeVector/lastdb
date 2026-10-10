//! App manifest loading.

use super::*;

/// App manifest consumed by `lastdb app …` (JSON file, conventionally
/// `lastdb-app.json`). `schemas` entries are declarative schema definitions
/// (`{ "name", "descriptive_name"?, "schema_type", "fields": [...] }`).
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
    Ok(manifest)
}

// ─── Step 1: check (declare on the local Mini) ────────────────────────────
