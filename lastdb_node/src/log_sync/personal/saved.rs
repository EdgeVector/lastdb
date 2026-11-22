//! The signed entries that wait for the relay's acknowledgement.
//!
//! Before a batch is sent, its exact signed entries are saved here, with the
//! sealed file blobs that the entries name. A retry sends the same bytes. It
//! never signs again, because sealing uses a fresh nonce and a second signing
//! would put a different entry at the same sequence number.
//!
//! This is a file beside the spool, not rows in the store. The entries are
//! needed only until the relay acknowledges them, they are never read by key,
//! and they must not be carried by a snapshot of the store: a restored home has
//! its own device stream and no entries of the old one to send. The file is
//! written whole with a rename, as the old retry store was, so a crash leaves
//! either the old file or the new one.

use std::path::{Path, PathBuf};

use mutation_log::SignedEntry;
use serde::{Deserialize, Serialize};

use crate::profile::private_fs as disk;

/// The saved file is at most this large. A batch stops at a few MiB of payload.
const FILE_LIMIT: usize = 64 * 1024 * 1024;

/// One signed batch and the spool range it covers.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Saved {
    /// The spool position before the batch.
    pub(super) after: u64,
    /// The spool position after the batch.
    pub(super) through: u64,
    /// The signed entries, in stream order.
    pub(super) entries: Vec<SignedEntry>,
}

fn dir(home: &Path, database_id: &str) -> PathBuf {
    home.join("log-sync").join(database_id)
}

fn file(home: &Path, database_id: &str) -> PathBuf {
    dir(home, database_id).join("outgoing.json")
}

/// Where the sealed blob with this digest is kept while its entry waits.
pub(super) fn blob_path(home: &Path, database_id: &str, sha256: &str) -> PathBuf {
    dir(home, database_id).join("outgoing-blobs").join(sha256)
}

/// Keep a sealed blob. It must be on disk before the entry that names it is saved.
pub(super) fn save_blob(
    home: &Path,
    database_id: &str,
    sha256: &str,
    sealed: &[u8],
) -> Result<(), String> {
    disk::write_private(&blob_path(home, database_id, sha256), sealed)
        .map_err(|_| "cannot save a sealed file blob".to_owned())
}

/// The batch that waits, if one does.
pub(super) fn load(home: &Path, database_id: &str) -> Result<Option<Saved>, String> {
    let Some(bytes) = disk::read_limited(&file(home, database_id), FILE_LIMIT)
        .map_err(|_| "cannot read the saved entries".to_owned())?
    else {
        return Ok(None);
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| "the saved entries are invalid".to_owned())
}

/// Save a batch. The sealed blobs it names are already saved.
pub(super) fn store(home: &Path, database_id: &str, saved: &Saved) -> Result<(), String> {
    let bytes = serde_json::to_vec(saved).map_err(|_| "cannot encode the saved entries")?;
    if bytes.len() > FILE_LIMIT {
        return Err("a batch of entries is too large to save".into());
    }
    disk::write_private(&file(home, database_id), &bytes)
        .map_err(|_| "cannot save the signed entries".to_owned())
}

/// Delete the batch and its blobs after the relay acknowledged it and the spool
/// retired it. The file goes first, so a crash leaves only blobs that nothing
/// names, which the next batch overwrites or the next clear removes.
pub(super) fn clear(home: &Path, database_id: &str, blobs: &[String]) -> Result<(), String> {
    disk::remove_private(&file(home, database_id))
        .map_err(|_| "cannot delete the saved entries".to_owned())?;
    for sha256 in blobs {
        disk::remove_private(&blob_path(home, database_id, sha256))
            .map_err(|_| "cannot delete a saved file blob".to_owned())?;
    }
    Ok(())
}
