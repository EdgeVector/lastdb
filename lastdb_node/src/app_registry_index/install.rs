//! Install by proof and running-node contract inspection.

use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use std::time::Duration;

use serde_json::{json, Value};

use super::{ContractVersions, Resolution};

/// The receipt `lastdb app install` leaves in the install directory.
pub const RECEIPT_FILE: &str = "lastdb-app-install.json";

/// Clone `resolution.source` and check out `resolution.sha` under
/// `install_dir/source`, then write the receipt.
pub fn install_pinned(resolution: &Resolution, install_dir: &Path) -> Result<(), String> {
    if !resolution.sha.chars().all(|c| c.is_ascii_hexdigit()) || resolution.sha.len() != 40 {
        return Err(format!(
            "compat row sha '{}' is not a git commit",
            resolution.sha
        ));
    }
    let checkout_path = install_dir.join("source");
    prepare_install_dir(install_dir)?;

    let cleanup = |why: String| -> String {
        let _ = std::fs::remove_dir_all(install_dir);
        why
    };

    let head = clone_and_checkout(
        &checkout_path,
        &resolution.source,
        &resolution.sha,
        &resolution.app_id,
        &cleanup,
    )?;

    let receipt = json!({
        "app_id": resolution.app_id,
        "version": resolution.app_version,
        "sha": head,
        "lastdb_version": resolution.lastdb_version,
        "channel": resolution.channel,
        "source": resolution.source,
        "proved_at": resolution.proved_at,
        "proof_run": resolution.proof_run,
        "index": resolution.index,
        "trust": resolution.trust,
        "listing": resolution.list_entry,
        "installed_at": chrono::Utc::now().to_rfc3339(),
    });
    let receipt_path = install_dir.join(RECEIPT_FILE);
    let bytes = serde_json::to_vec_pretty(&receipt)
        .map_err(|e| cleanup(format!("failed to encode install receipt: {e}")))?;
    std::fs::write(&receipt_path, bytes)
        .map_err(|e| cleanup(format!("failed to write {}: {e}", receipt_path.display())))?;

    Ok(())
}

fn clone_and_checkout<F>(
    checkout_path: &Path,
    source: &str,
    sha: &str,
    app_id: &str,
    cleanup: &F,
) -> Result<String, String>
where
    F: Fn(String) -> String,
{
    let clone = ProcessCommand::new("git")
        .arg("clone")
        .arg("--quiet")
        .arg("--no-checkout")
        .arg(source)
        .arg(checkout_path)
        .status()
        .map_err(|e| cleanup(format!("failed to run git clone: {e}")))?;
    if !clone.success() {
        return Err(cleanup(format!(
            "git clone failed for app '{app_id}' from {source}"
        )));
    }
    let checkout = ProcessCommand::new("git")
        .arg("-C")
        .arg(checkout_path)
        .arg("checkout")
        .arg("--quiet")
        .arg("--detach")
        .arg(sha)
        .status()
        .map_err(|e| cleanup(format!("failed to run git checkout: {e}")))?;
    if !checkout.success() {
        return Err(cleanup(format!(
            "git checkout {sha} failed for app '{app_id}': the proved commit is not in {source}"
        )));
    }
    let head = ProcessCommand::new("git")
        .arg("-C")
        .arg(checkout_path)
        .arg("rev-parse")
        .arg("HEAD")
        .output()
        .map_err(|e| cleanup(format!("failed to run git rev-parse: {e}")))?;
    let head = String::from_utf8_lossy(&head.stdout).trim().to_string();
    if head != sha {
        return Err(cleanup(format!(
            "checkout HEAD is {head} but the proved commit is {sha}"
        )));
    }
    Ok(head)
}

fn prepare_install_dir(install_dir: &Path) -> Result<(), String> {
    if install_dir.exists() {
        return Err(format!(
            "install directory already exists: {}",
            install_dir.display()
        ));
    }
    std::fs::create_dir_all(install_dir).map_err(|e| {
        format!(
            "failed to create install directory {}: {e}",
            install_dir.display()
        )
    })?;
    Ok(())
}

fn sibling_lastdbd_version() -> Option<String> {
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            candidates.push(dir.join("lastdbd"));
        }
    }
    candidates.push(PathBuf::from("lastdbd"));
    for candidate in candidates {
        let output = ProcessCommand::new(&candidate)
            .arg("--version")
            .env("RUST_LOG", "off")
            .output();
        let Ok(output) = output else { continue };
        if !output.status.success() {
            continue;
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        for line in stdout.lines() {
            if let Some(rest) = line.trim().strip_prefix("lastdbd ") {
                let build = rest.trim();
                if !build.is_empty() {
                    return Some(build.to_string());
                }
            }
        }
    }
    None
}

/// Read the node's build and exact contract versions from one socket request.
/// Falls back to the paired daemon build and this release's contract constants.
#[must_use]
pub fn running_node(socket: &Path) -> (String, &'static str, ContractVersions) {
    let mut versions = ContractVersions {
        api_version: crate::version::CURRENT_API_VERSION,
        min_supported_api_version: crate::version::MIN_SUPPORTED_API_VERSION,
        outbox_format_version: crate::version::CURRENT_OUTBOX_FORMAT_VERSION,
        min_supported_outbox_format_version: crate::version::MIN_SUPPORTED_OUTBOX_FORMAT_VERSION,
    };
    if let Some(value) = socket_version(socket) {
        let api = value
            .get("api_version")
            .and_then(Value::as_u64)
            .and_then(|v| u32::try_from(v).ok());
        let outbox = value
            .get("current_outbox_format_version")
            .and_then(Value::as_u64)
            .and_then(|v| u32::try_from(v).ok());
        if let (Some(build), Some(api), Some(outbox)) =
            (value.get("build").and_then(Value::as_str), api, outbox)
        {
            versions.api_version = api;
            versions.outbox_format_version = outbox;
            return (build.to_string(), "socket", versions);
        }
    }
    let build = sibling_lastdbd_version();
    (
        build
            .clone()
            .unwrap_or_else(|| crate::version::build_version().to_string()),
        if build.is_some() { "lastdbd" } else { "cli" },
        versions,
    )
}

fn socket_version(socket: &Path) -> Option<Value> {
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;
    let mut stream = UnixStream::connect(socket).ok()?;
    let timeout = Some(Duration::from_secs(5));
    stream.set_read_timeout(timeout).ok()?;
    stream.set_write_timeout(timeout).ok()?;
    stream
        .write_all(
            b"GET /api/version HTTP/1.1\r\nHost: localhost\r\nX-LastDB-Client: lastdb\r\nConnection: close\r\n\r\n",
        )
        .ok()?;
    let mut raw = String::new();
    stream.read_to_string(&mut raw).ok()?;
    let status: u16 = raw.split_whitespace().nth(1)?.parse().ok()?;
    if status != 200 {
        return None;
    }
    let body = raw.split_once("\r\n\r\n")?.1.trim();
    let start = body.find('{')?;
    let end = body.rfind('}')?;
    serde_json::from_str(&body[start..=end]).ok()
}
