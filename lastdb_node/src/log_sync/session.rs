//! Device id and delivery cursor for one home. Unenrolled homes get no files.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::disk;

const LIMIT: usize = 4096;

#[derive(Serialize, Deserialize)]
struct SessionFile {
    cursor: u64,
}

#[derive(Serialize)]
struct PrefixFile<'a> {
    database_id: &'a str,
    through: u64,
}

pub(crate) fn device_id(home: &Path) -> Result<String, String> {
    let path = device_path(home);
    if let Some(bytes) = disk::read_limited(&path, LIMIT).map_err(|_| "cannot read device id")? {
        let text = std::str::from_utf8(&bytes)
            .map_err(|_| "device id is not text")?
            .trim();
        if super::cutover::label_ok(text) {
            return Ok(text.to_owned());
        }
        return Err("device id is not a log label".into());
    }
    let fresh = uuid::Uuid::new_v4().to_string();
    disk::write_private(&path, format!("{fresh}\n").as_bytes())
        .map_err(|_| "cannot write device id")?;
    Ok(fresh)
}

pub(crate) fn load_cursor(home: &Path, database_id: &str) -> Result<u64, String> {
    let Some(bytes) = disk::read_limited(&session_path(home, database_id), LIMIT)
        .map_err(|_| "cannot read log sync session")?
    else {
        return Ok(0);
    };
    let file: SessionFile =
        serde_json::from_slice(&bytes).map_err(|_| "log sync session is invalid")?;
    Ok(file.cursor)
}

pub(crate) fn store_cursor(home: &Path, database_id: &str, cursor: u64) -> Result<(), String> {
    let bytes = serde_json::to_vec(&SessionFile { cursor })
        .map_err(|_| "log sync session encoding failed")?;
    disk::write_private(&session_path(home, database_id), &bytes)
        .map_err(|_| "cannot write log sync session".to_owned())
}

/// Record a durable first prefix, including an empty log with no snapshot.
pub(crate) fn mark_prefix(home: &Path, database_id: &str, through: u64) -> Result<(), String> {
    let bytes = serde_json::to_vec(&PrefixFile {
        database_id,
        through,
    })
    .map_err(|_| "log prefix encoding failed")?;
    disk::write_private(&prefix_path(home, database_id), &bytes)
        .map_err(|_| "cannot write log prefix".to_owned())
}

fn device_path(home: &Path) -> PathBuf {
    home.join("log-sync").join("device-id")
}

fn session_path(home: &Path, database_id: &str) -> PathBuf {
    home.join("log-sync").join(database_id).join("session.json")
}

fn prefix_path(home: &Path, database_id: &str) -> PathBuf {
    home.join("log-sync").join(database_id).join("prefix.json")
}
