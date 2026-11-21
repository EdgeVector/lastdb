//! Prefix durability marker for received/shared databases.
//!
//! Unsigned snapshot replay is retired. Materialization for signed history
//! lives in `log_sync::materialize`.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use crate::profile::private_fs as disk;

/// True when sync stored `log-sync/<id>/prefix.json` after a durable pull.
pub(crate) fn prefix_durable(home: &Path, database_id: &str) -> bool {
    if database_id.is_empty() || database_id.contains(['/', '\\']) {
        return false;
    }
    let path = prefix_path(home, database_id);
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

/// Persist `log-sync/<id>/prefix.json` after a durable received materialize.
pub(crate) fn mark_prefix(home: &Path, database_id: &str, through: u64) -> Result<(), String> {
    if database_id.is_empty() || database_id.contains(['/', '\\']) {
        return Err("database id is not a log-sync label".into());
    }
    let bytes = serde_json::to_vec(&LogPrefixFile {
        database_id,
        through,
    })
    .map_err(|_| "log prefix encoding failed")?;
    disk::write_private(&prefix_path(home, database_id), &bytes)
        .map_err(|_| "cannot write log prefix".into())
}

fn prefix_path(home: &Path, database_id: &str) -> std::path::PathBuf {
    home.join("log-sync").join(database_id).join("prefix.json")
}

#[derive(serde::Deserialize)]
struct LogPrefix {
    database_id: String,
}

#[derive(serde::Serialize)]
struct LogPrefixFile<'a> {
    database_id: &'a str,
    through: u64,
}
