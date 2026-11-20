//! Prefix durability marker for received/shared databases.
//!
//! Unsigned snapshot replay is retired. Materialization for signed history
//! lives in `log_sync::materialize`.

use std::fs;
use std::os::unix::fs::PermissionsExt;

/// True when sync stored `log-sync/<id>/prefix.json` after a durable pull.
pub(crate) fn prefix_durable(home: &std::path::Path, database_id: &str) -> bool {
    if database_id.is_empty() || database_id.contains(['/', '\\']) {
        return false;
    }
    let path = home.join("log-sync").join(database_id).join("prefix.json");
    let Ok(metadata) = fs::symlink_metadata(&path) else {
        return false;
    };
    if !metadata.is_file() || metadata.permissions().mode() & 0o022 != 0 || metadata.len() > 4096 {
        return false;
    }
    let Ok(bytes) = fs::read(&path) else {
        return false;
    };
    serde_json::from_slice::<LogPrefix>(&bytes)
        .ok()
        .is_some_and(|marker| marker.database_id == database_id)
}

#[derive(serde::Deserialize)]
struct LogPrefix {
    database_id: String,
}
