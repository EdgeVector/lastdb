//! Resolve: pick the one proved compat row for this node.

use app_identity_crypto::VerifyingKey;
use serde::Serialize;

use super::location::IndexLocation;
use super::signing::{load_verified_index, CompatRow, IndexApp, RegistryIndex, TrustSource};
use super::TRUST_KEY_ENV;

/// Contract version ranges this node supports.
#[derive(Debug, Clone, Copy)]
pub struct ContractVersions {
    pub api_version: u32,
    pub min_supported_api_version: u32,
    pub outbox_format_version: u32,
    pub min_supported_outbox_format_version: u32,
}

/// The running LastDB version and how it was determined.
#[derive(Debug, Clone)]
pub struct NodeVersion<'a> {
    pub version: &'a str,
    pub source: &'a str,
}

/// What `lastdb app resolve` answers: the one proved row for this node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Resolution {
    pub app_id: String,
    pub channel: String,
    pub source: String,
    pub lastdb_version: String,
    pub lastdb_version_source: String,
    pub app_version: String,
    pub sha: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact: Option<String>,
    pub proved_at: String,
    pub proof_run: String,
    pub index: String,
    pub trust: String,
}

/// Pick the newest compatible compat row of `app_id`. A row is compatible when:
/// - Its `lastdb_api_version` (if present) is within the node's supported range
/// - Its `outbox_format_version` (if present) is within the node's supported range
/// - Its `lastdb` semver constraint (from manifest, if present) admits the running version
///
/// Rows with no contract info (old indexes) keep exact `lastdb_version` matching.
/// Among compatible rows, prefer exact `lastdb_version` match first, then newest
/// by `proved_at`, then `app_version` as a tie-break.
///
/// # Errors
/// Names the app when absent, and shows required vs supported versions when none is compatible.
pub fn pick_row<'a>(
    index: &'a RegistryIndex,
    app_id: &str,
    lastdb_version: &str,
    versions: &ContractVersions,
) -> Result<(&'a IndexApp, &'a CompatRow), String> {
    let app = index
        .apps
        .iter()
        .find(|a| a.app_id == app_id)
        .ok_or_else(|| format!("app '{app_id}' is not in the {} index", index.channel))?;

    let mut compatible = Vec::new();
    let mut exact_version_rows = Vec::new();

    for row in &app.compat {
        let has_contract_info =
            row.lastdb_api_version.is_some() || row.outbox_format_version.is_some();

        if has_contract_info {
            if let Some(api_version) = row.lastdb_api_version {
                if api_version < versions.min_supported_api_version
                    || api_version > versions.api_version
                {
                    continue;
                }
            }
            if let Some(outbox_version) = row.outbox_format_version {
                if outbox_version < versions.min_supported_outbox_format_version
                    || outbox_version > versions.outbox_format_version
                {
                    continue;
                }
            }
            compatible.push(row);
        } else if row.lastdb_version == lastdb_version {
            exact_version_rows.push(row);
        }
    }

    if compatible.is_empty() && exact_version_rows.is_empty() {
        let mut version_details: Vec<String> = Vec::new();
        for row in &app.compat {
            let mut parts = Vec::new();
            parts.push(format!("lastdb {}", row.lastdb_version));
            if let Some(v) = row.lastdb_api_version {
                parts.push(format!("api_version {v}"));
            }
            if let Some(v) = row.outbox_format_version {
                parts.push(format!("outbox_format_version {v}"));
            }
            version_details.push(format!("({})", parts.join(", ")));
        }
        version_details.sort_unstable();
        version_details.dedup();

        return Err(format!(
            "no {} row for app '{app_id}' is compatible with this node (lastdb {lastdb_version}, api_version {}, outbox_format_version {}, supports api {}-{}, outbox {}-{}); proved with: {}",
            index.channel,
            versions.api_version,
            versions.outbox_format_version,
            versions.min_supported_api_version,
            versions.api_version,
            versions.min_supported_outbox_format_version,
            versions.outbox_format_version,
            if version_details.is_empty() {
                "none".to_string()
            } else {
                version_details.join("; ")
            }
        ));
    }

    let mut final_candidates = Vec::new();
    for row in &compatible {
        if row.lastdb_version == lastdb_version {
            final_candidates.push((row, true));
        } else {
            final_candidates.push((row, false));
        }
    }
    for row in &exact_version_rows {
        final_candidates.push((row, true));
    }

    final_candidates.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| a.0.proved_at.cmp(&b.0.proved_at).reverse())
            .then_with(|| a.0.app_version.cmp(&b.0.app_version).reverse())
    });

    let row = final_candidates
        .first()
        .map(|(row, _)| *row)
        .ok_or_else(|| "compat row selection produced no row".to_string())?;
    Ok((app, row))
}

/// Fetch, verify, and pick: the whole anonymous resolve in one call.
///
/// # Errors
/// Any fetch, signature, parse, or selection failure, with its cause.
pub async fn resolve(
    location: &IndexLocation,
    channel: &str,
    trust: &VerifyingKey,
    trust_source: &TrustSource,
    app_id: &str,
    node_version: &NodeVersion<'_>,
    versions: &ContractVersions,
) -> Result<Resolution, String> {
    let (index_bytes, sig_bytes) = location.fetch(channel).await?;
    let index = load_verified_index(trust, &index_bytes, &sig_bytes)?;
    if index.channel != channel {
        return Err(format!(
            "index at {} says channel '{}' but '{channel}' was requested",
            location.describe(channel),
            index.channel
        ));
    }
    let (app, row) = pick_row(&index, app_id, node_version.version, versions)?;
    Ok(Resolution {
        app_id: app.app_id.clone(),
        channel: channel.to_string(),
        source: app.source.clone(),
        lastdb_version: node_version.version.to_string(),
        lastdb_version_source: node_version.source.to_string(),
        app_version: row.app_version.clone(),
        sha: row.sha.clone(),
        artifact: row.artifact.clone(),
        proved_at: row.proved_at.clone(),
        proof_run: row.proof_run.clone(),
        index: location.describe(channel),
        trust: match trust_source {
            TrustSource::Pinned => "pinned".to_string(),
            TrustSource::Flag => "flag".to_string(),
            TrustSource::Env => format!("env:{TRUST_KEY_ENV}"),
        },
    })
}

/// Fetch and verify a whole channel for `list` / `info`.
///
/// # Errors
/// Any fetch, signature, or parse failure.
pub async fn fetch_verified(
    location: &IndexLocation,
    channel: &str,
    trust: &VerifyingKey,
) -> Result<RegistryIndex, String> {
    let (index_bytes, sig_bytes) = location.fetch(channel).await?;
    load_verified_index(trust, &index_bytes, &sig_bytes)
}

/// Find a manifest at the given path and its common variants.
fn find_manifest(source: &std::path::Path, app_id: &str) -> Result<std::path::PathBuf, String> {
    let candidates = [
        source.join(app_id).join("lastdb-app.json"),
        source.join("lastdb-app.json"),
        source.join(format!("{app_id}.app.json")),
    ];

    for path in &candidates {
        if path.exists() {
            return Ok(path.clone());
        }
    }

    Err(format!(
        "manifest not found: tried {app_id}/lastdb-app.json, lastdb-app.json, and {app_id}.app.json"
    ))
}

fn resolve_full_sha(path: &std::path::Path, rev: &str) -> Result<String, String> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(path)
        .arg("rev-parse")
        .arg("--verify")
        .arg(format!("{rev}^{{commit}}"))
        .output()
        .map_err(|e| format!("failed to run git rev-parse: {e}"))?;

    if !output.status.success() {
        return Err(format!(
            "git rev-parse --verify {rev}^{{commit}} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn read_manifest_at_rev(
    path: &std::path::Path,
    rev: &str,
    relative_manifest: &std::path::Path,
) -> Result<serde_json::Value, String> {
    let git_show_arg = format!("{}:{}", rev, relative_manifest.display());
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(path)
        .arg("show")
        .arg(&git_show_arg)
        .output()
        .map_err(|e| format!("failed to run git show: {e}"))?;

    if !output.status.success() {
        return Err(format!(
            "git show {}:{} failed (is the rev valid?): {}",
            rev,
            relative_manifest.display(),
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    serde_json::from_slice(&output.stdout).map_err(|e| {
        format!(
            "failed to parse manifest at {}:{}: {e}",
            rev,
            relative_manifest.display()
        )
    })
}

fn check_local_manifest_versions(
    manifest_json: &serde_json::Value,
    app_id: &str,
    versions: &ContractVersions,
) -> Result<(), String> {
    if let Some(api_version_u64) = manifest_json
        .get("api_version")
        .and_then(serde_json::Value::as_u64)
    {
        let api_version = u32::try_from(api_version_u64).map_err(|_| {
            format!("app '{app_id}' api_version {api_version_u64} is not a valid u32")
        })?;
        if api_version < versions.min_supported_api_version || api_version > versions.api_version {
            return Err(format!(
                "app '{app_id}' requires api_version {api_version}, but this node supports {}-{} (current: {})",
                versions.min_supported_api_version,
                versions.api_version,
                versions.api_version
            ));
        }
    }

    if let Some(outbox_version_u64) = manifest_json
        .get("outbox_format_version")
        .and_then(serde_json::Value::as_u64)
    {
        let outbox_version = u32::try_from(outbox_version_u64).map_err(|_| {
            format!("app '{app_id}' outbox_format_version {outbox_version_u64} is not a valid u32")
        })?;
        if outbox_version < versions.min_supported_outbox_format_version
            || outbox_version > versions.outbox_format_version
        {
            return Err(format!(
                "app '{app_id}' requires outbox_format_version {outbox_version}, but this node supports {}-{} (current: {})",
                versions.min_supported_outbox_format_version,
                versions.outbox_format_version,
                versions.outbox_format_version
            ));
        }
    }

    Ok(())
}

fn resolve_source_url(path: &std::path::Path) -> String {
    if let Ok(output) = std::process::Command::new("git")
        .arg("-C")
        .arg(path)
        .arg("remote")
        .arg("get-url")
        .arg("origin")
        .output()
    {
        if output.status.success() {
            return String::from_utf8_lossy(&output.stdout).trim().to_string();
        }
    }
    format!("file://{}", path.display())
}

/// Resolve from a local git repository or path at a specific revision.
///
/// # Errors
/// Git operations, manifest read, or contract version check failures.
pub fn resolve_local(
    location: &IndexLocation,
    app_id: &str,
    node_version: &NodeVersion<'_>,
    versions: &ContractVersions,
    is_dev: bool,
) -> Result<Resolution, String> {
    let IndexLocation::Local { path, rev } = location else {
        return Err("resolve_local requires IndexLocation::Local".to_string());
    };

    let manifest_path = if path.is_dir() {
        find_manifest(path, app_id)?
    } else {
        return Err(format!(
            "--from path must be a directory: {}",
            path.display()
        ));
    };

    let full_sha = resolve_full_sha(path, rev)?;

    let relative_manifest = manifest_path
        .strip_prefix(path)
        .map_err(|_| "manifest is not inside the source directory".to_string())?;

    let manifest_json = read_manifest_at_rev(path, rev, relative_manifest)?;

    check_local_manifest_versions(&manifest_json, app_id, versions)?;

    let app_version = manifest_json
        .get("version")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("dev")
        .to_string();

    let source_url = resolve_source_url(path);

    Ok(Resolution {
        app_id: app_id.to_string(),
        channel: if is_dev {
            "dev".to_string()
        } else {
            "local".to_string()
        },
        source: source_url,
        lastdb_version: node_version.version.to_string(),
        lastdb_version_source: node_version.source.to_string(),
        app_version,
        sha: full_sha,
        artifact: None,
        proved_at: chrono::Utc::now().to_rfc3339(),
        proof_run: "local-install".to_string(),
        index: location.describe("local"),
        trust: if is_dev {
            "dev".to_string()
        } else {
            "local".to_string()
        },
    })
}
