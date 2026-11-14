//! Live create and access bodies plus log-sync wakeup signal.
//!
//! `NativeDatabase` is `state`, `version`, `environment`, and `database_id`.
//! Nothing signs this object. A saved file is not one.

use std::path::{Path, PathBuf};

use cloud_backup_protocol::v2::Version;
use serde::Deserialize;
use serde_json::Value;

const BODY_LIMIT: usize = 65_536;

fn wake_path(home: &Path) -> PathBuf {
    home.join("log-sync").join("wake")
}

/// Touch `<home>/log-sync/wake` so a sleeping `lastdbd` wakes on its next 1s check.
pub fn request_wake(home: &Path) {
    if !home.is_dir() {
        return;
    }
    let _ = super::disk::write_private(&wake_path(home), b"wake\n");
}

pub(super) fn wake_requested(home: &Path) -> bool {
    wake_path(home).exists()
}

pub(super) fn consume_wake(home: &Path) -> bool {
    let path = wake_path(home);
    if !path.exists() {
        return false;
    }
    let _ = std::fs::remove_file(path);
    true
}

/// What the latest signed create or access body said about one database.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NativeView {
    /// The TLS body is `NativeDatabase` for this id and realm.
    Live,
    /// Transport, server error, or any other body.
    Unavailable,
}

/// `NativeDatabase` for this id and realm. Any other body is not live.
pub(crate) fn native_body(body: &[u8], database_id: &str, environment: &str) -> NativeView {
    let Some(value) = read_value(body) else {
        return NativeView::Unavailable;
    };
    match bound(&value, database_id, environment) {
        Some(()) => NativeView::Live,
        None => NativeView::Unavailable,
    }
}

fn read_value(body: &[u8]) -> Option<Value> {
    if body.is_empty() || body.len() > BODY_LIMIT {
        return None;
    }
    serde_json::from_slice(body).ok()
}

fn bound(value: &Value, database_id: &str, environment: &str) -> Option<()> {
    let wire: BoundWire = serde_json::from_value(value.clone()).ok()?;
    (wire.state == "native"
        && wire.version == Version::V2
        && wire.environment == environment
        && wire.database_id == database_id)
        .then_some(())
}

/// `NativeDatabase`. Extra fields, including a signature, fail.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BoundWire {
    state: String,
    version: Version,
    environment: String,
    database_id: String,
}
