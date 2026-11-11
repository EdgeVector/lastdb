//! Install by proof and running-node contract inspection.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use std::time::Duration;

use serde_json::{json, Value};

use super::{ContractVersions, Resolution};

pub const RECEIPT_FILE: &str = "lastdb-app-install.json";

pub fn install_pinned(resolution: &Resolution, install_dir: &Path) -> Result<(), String> {
    if !resolution.sha.chars().all(|c| c.is_ascii_hexdigit()) || resolution.sha.len() != 40 {
        return Err(format!(
            "compat row sha '{}' is not a git commit",
            resolution.sha
        ));
    }
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

    let cleanup = |why: String| -> String {
        let _ = std::fs::remove_dir_all(install_dir);
        why
    };
    let checkout_path = install_dir.join("source");
    let head = clone_and_checkout(
        &checkout_path,
        &resolution.source,
        &resolution.sha,
        &resolution.app_id,
    )
    .map_err(&cleanup)?;

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
        .map_err(|e| cleanup(format!("failed to write {}: {e}", receipt_path.display())))
}

fn clone_and_checkout(
    checkout_path: &Path,
    source: &str,
    sha: &str,
    app_id: &str,
) -> Result<String, String> {
    let clone = ProcessCommand::new("git")
        .args(["clone", "--quiet", "--no-checkout", source])
        .arg(checkout_path)
        .status()
        .map_err(|e| format!("failed to run git clone: {e}"))?;
    if !clone.success() {
        return Err(format!("git clone failed for app '{app_id}' from {source}"));
    }
    let checkout = ProcessCommand::new("git")
        .arg("-C")
        .arg(checkout_path)
        .args(["checkout", "--quiet", "--detach", sha])
        .status()
        .map_err(|e| format!("failed to run git checkout: {e}"))?;
    if !checkout.success() {
        return Err(format!(
            "git checkout {sha} failed for app '{app_id}': the proved commit is not in {source}"
        ));
    }
    let head = ProcessCommand::new("git")
        .arg("-C")
        .arg(checkout_path)
        .args(["rev-parse", "HEAD"])
        .output()
        .map_err(|e| format!("failed to run git rev-parse: {e}"))?;
    let head = String::from_utf8_lossy(&head.stdout).trim().to_string();
    if head != sha {
        return Err(format!(
            "checkout HEAD is {head} but the proved commit is {sha}"
        ));
    }
    Ok(head)
}

fn sibling_lastdbd_version() -> Option<String> {
    let mut candidates = Vec::new();
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            candidates.push(dir.join("lastdbd"));
        }
    }
    candidates.push(PathBuf::from("lastdbd"));
    for candidate in candidates {
        let Ok(output) = ProcessCommand::new(&candidate)
            .arg("--version")
            .env("RUST_LOG", "off")
            .output()
        else {
            continue;
        };
        if !output.status.success() {
            continue;
        }
        for line in String::from_utf8_lossy(&output.stdout).lines() {
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
    match sibling_lastdbd_version() {
        Some(build) => (build, "lastdbd", versions),
        None => (crate::version::build_version().to_string(), "cli", versions),
    }
}

fn socket_version(socket: &Path) -> Option<Value> {
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
    if raw.split_whitespace().nth(1)?.parse::<u16>().ok()? != 200 {
        return None;
    }
    let body = raw.split_once("\r\n\r\n")?.1.trim();
    serde_json::from_str(&body[body.find('{')?..=body.rfind('}')?]).ok()
}
