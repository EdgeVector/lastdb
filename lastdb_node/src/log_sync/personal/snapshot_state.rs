//! What a snapshot leaves on disk beside the sync state, and the requests for one.
//!
//! `snapshot-request` holds the nonce of a request that waits, `snapshot-result`
//! the answer to the last request, `snapshot-error` why it failed, and
//! `snapshot-last.json` the manifest and positions of the last snapshot this
//! device published. None of them is part of the store, so a restored device
//! starts with none.

use std::path::{Path, PathBuf};

use mutation_log::SnapshotThrough;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::snapshot_manifest::Manifest;
use super::Ctx;
use crate::host::Host;
use crate::profile::private_fs as disk;

pub(super) const LAST_LIMIT: usize = 8 * 1024 * 1024;

/// What a published snapshot looked like. It is also the answer to a request.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Report {
    /// The sequence number of the `snapshot` entry on the snapshot stream.
    pub(super) snapshot_seq: u64,
    /// Files in the manifest.
    pub(super) files: usize,
    /// Total length of the files.
    pub(super) snapshot_bytes: u64,
    /// Files uploaded for this snapshot (the others were already on the relay).
    pub(super) uploaded_files: usize,
    /// Length of the files uploaded for this snapshot.
    pub(super) uploaded_bytes: u64,
    /// Streams the snapshot covers.
    pub(super) streams: usize,
    /// Milliseconds from the freeze to the end of the hashing of the files.
    pub(super) capture_ms: u64,
    /// Milliseconds to upload the files, the manifest and the entry.
    pub(super) upload_ms: u64,
}

/// The last snapshot this device published, kept beside the request files. It is
/// not part of the store, so a restored device starts with none.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Last {
    /// The sequence number of this device's own stream at the cut.
    pub(super) own_seq: u64,
    pub(super) through: Vec<SnapshotThrough>,
    pub(super) manifest: Manifest,
    pub(super) report: Report,
}

fn dir(home: &Path, database_id: &str) -> std::path::PathBuf {
    home.join("log-sync").join(database_id)
}

pub(super) fn file_of(home: &Path, database_id: &str, name: &str) -> std::path::PathBuf {
    dir(home, database_id).join(name)
}

/// Ask for a snapshot now. Returns the database id and the nonce that names the request.
pub(crate) fn request(host: &Host, selector: Option<&str>) -> Result<(String, String), String> {
    let owner = host
        .owned
        .owner()
        .map_err(|_| "the personal database is not registered".to_owned())?;
    if selector.is_some_and(|name| name != "personal" && name != owner.id) {
        return Err("database is not owned by this node".into());
    }
    let nonce = uuid::Uuid::new_v4().simple().to_string();
    let _ = disk::remove_private(&file_of(&host.home, &owner.id, "snapshot-result"));
    let _ = disk::remove_private(&file_of(&host.home, &owner.id, "snapshot-error"));
    disk::write_private(
        &file_of(&host.home, &owner.id, "snapshot-request"),
        nonce.as_bytes(),
    )
    .map_err(|_| "cannot save the snapshot request".to_owned())?;
    Ok((owner.id, nonce))
}

/// True while a request waits for its snapshot.
pub(super) fn requested(home: &Path, database_id: &str) -> bool {
    file_of(home, database_id, "snapshot-request").is_file()
}

/// The state of the request `nonce`: ready with its report, failed, or neither yet.
pub(crate) fn status(host: &Host, nonce: &str) -> Result<Value, String> {
    let owner = host
        .owned
        .owner()
        .map_err(|_| "the personal database is not registered".to_owned())?;
    let read = |name: &str| {
        disk::read_limited(&file_of(&host.home, &owner.id, name), LAST_LIMIT)
            .ok()
            .flatten()
    };
    if let Some(result) = read("snapshot-result") {
        let value: Value = serde_json::from_slice(&result).map_err(|_| "bad snapshot result")?;
        if value["request"].as_str() == Some(nonce) {
            return Ok(json!({ "ready": true, "database_id": owner.id, "report": value }));
        }
    }
    let error = read("snapshot-error").and_then(|bytes| String::from_utf8(bytes).ok());
    Ok(match error {
        Some(error) => json!({ "ready": false, "error": error }),
        None => json!({ "ready": false }),
    })
}

pub(super) fn load_last(home: &Path, database_id: &str) -> Result<Option<Last>, String> {
    let Some(bytes) = disk::read_limited(
        &file_of(home, database_id, "snapshot-last.json"),
        LAST_LIMIT,
    )
    .map_err(|_| "cannot read the last snapshot record".to_owned())?
    else {
        return Ok(None);
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| "the last snapshot record is invalid".to_owned())
}

/// Record the answer of a request, then retire the request if it is still the same one.
pub(super) fn finish_request(ctx: &Ctx<'_>, nonce: &str, report: &Report) -> Result<(), String> {
    let home = &ctx.host.home;
    let mut answer = serde_json::to_value(report).map_err(|_| "cannot encode the report")?;
    answer["request"] = Value::String(nonce.to_owned());
    let bytes = serde_json::to_vec(&answer).map_err(|_| "cannot encode the report")?;
    disk::write_private(&file_of(home, &ctx.database_id, "snapshot-result"), &bytes)
        .map_err(|_| "cannot record the snapshot result".to_owned())?;
    let path = file_of(home, &ctx.database_id, "snapshot-request");
    let same = disk::read_limited(&path, 64).ok().flatten().as_deref() == Some(nonce.as_bytes());
    if same {
        disk::remove_private(&path).map_err(|_| "cannot retire the snapshot request".to_owned())?;
    }
    Ok(())
}
