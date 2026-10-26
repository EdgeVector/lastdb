//! Local install source resolution: read app manifest from a git repository at a specific revision.

use super::{ContractVersions, IndexLocation, NodeVersion, Resolution};

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
    sha: &str,
    relative_manifest: &std::path::Path,
) -> Result<serde_json::Value, String> {
    let git_show_arg = format!("{}:{}", sha, relative_manifest.display());
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
            sha,
            relative_manifest.display(),
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    serde_json::from_slice(&output.stdout).map_err(|e| {
        format!(
            "failed to parse manifest at {}:{}: {e}",
            sha,
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

    let manifest_json = read_manifest_at_rev(path, &full_sha, relative_manifest)?;

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
