//! Personal record blobs in a backup job (design
//! `design-lastdb-blob-files-on-demand-20261107`).
//!
//! A record file version is one B2 object at the path derived from its
//! record, not a member of the job's inventory. The device that created the
//! version uploads it in the job that publishes its write: the commit that
//! adds the `fb:` row queues it (`fp:`), and the job plans its uploads after
//! the capture, so every version that a captured record names is queued. A
//! snapshot first queues every local version without an uploaded marker.
//! After the service accepts the job, the node writes the `fu:` markers.
//!
//! The job state keeps a bounded plan: an index and pages of 64 entries in
//! the job's store, so a restart resumes each frozen transfer exactly.

use super::{model::Saved, source::Source, store::Store, BackupError, Phase, Result};
use cloud_backup_protocol::v2::blobs::{BlobPath, MAX_BLOB_BATCH};
use cloud_backup_protocol::v2::transfer::{ObjectReceipt, TransferInput};
use serde::{Deserialize, Serialize};

mod upload;
pub(super) use upload::upload;

const INDEX: &str = "blobs.json";
const VERSION: u16 = 1;
/// Upload pages per job. Versions past this bound wait for the next job.
const MAX_UPLOAD_PAGES: u32 = 64;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Index {
    version: u16,
    /// A snapshot queued the local versions without markers.
    queued: bool,
    upload_pages: u32,
    /// Every upload page is planned.
    uploads_planned: bool,
    /// Pages whose entries all have a receipt or vanished.
    uploads_done: u32,
    /// The `fu:` markers are written for an accepted job.
    marked: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Upload {
    key: String,
    path: BlobPath,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    input: Option<TransferInput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    receipt: Option<ObjectReceipt>,
    /// The version left the device before its upload: no record names it.
    #[serde(default)]
    vanished: bool,
}

fn upload_page(index: u32) -> String {
    format!("blob-upload-{index:04}.json")
}

fn load(store: &Store) -> Result<Index> {
    let index = store.read::<Index>(INDEX)?.unwrap_or(Index {
        version: VERSION,
        ..Index::default()
    });
    if index.version != VERSION
        || index.uploads_done > index.upload_pages
        || index.upload_pages > MAX_UPLOAD_PAGES
    {
        return Err(BackupError::Invalid);
    }
    Ok(index)
}

fn read_page<T: serde::de::DeserializeOwned>(store: &Store, name: &str) -> Result<Vec<T>> {
    let page: Vec<T> = store.read(name)?.ok_or(BackupError::Missing)?;
    if page.len() > MAX_BLOB_BATCH {
        return Err(BackupError::Invalid);
    }
    Ok(page)
}

/// After the service accepted the job: mark its uploaded versions. Repeating
/// this is harmless, so a crash before the index records it only repeats it.
pub(super) async fn accepted(source: &Source, store: &Store, saved: &Saved) -> Result<()> {
    if saved.phase != Phase::Published {
        return Ok(());
    }
    let mut index = load(store)?;
    if index.marked {
        return Ok(());
    }
    let mut uploaded = Vec::new();
    for page in 0..index.upload_pages {
        let entries: Vec<Upload> = read_page(store, &upload_page(page))?;
        uploaded.extend(
            entries
                .into_iter()
                .filter(|entry| entry.receipt.is_some())
                .map(|entry| entry.key),
        );
    }
    source.blobs_accepted(&uploaded, &[]).await?;
    index.marked = true;
    store.write(INDEX, &index)
}
