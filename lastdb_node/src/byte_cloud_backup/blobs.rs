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
//! Releases are derived from record writes, not from file rows: the commit
//! that retires an `fb:` row after the main publication that dropped its
//! version queues an `fr:` row. The job plans its releases before its first
//! capture, so each planned release belongs to a write that the job's
//! backup holds, and sends them (`blobs/release`) before it publishes. The
//! service deletes the objects when it accepts the job; the node then drops
//! the `fr:` rows. A release of a path that was never uploaded is harmless.

use super::{model::Saved, retry, source::Source, store::Store, BackupError, Phase, Result};
use crate::cloud_backup_client::byte_backup::ByteBackupClient;
use cloud_backup_protocol::v2::blobs::{BlobPath, BlobReleaseRequest, MAX_BLOB_BATCH};
use cloud_backup_protocol::v2::transfer::{ObjectReceipt, TransferInput};
use cloud_backup_protocol::v2::Version;
use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicBool;

mod upload;
pub(super) use upload::upload;

const INDEX: &str = "blobs.json";
/// Version 2: the plan has no page limit, keeps its cursors in the index,
/// and shrinks a finished upload page to its uploaded keys.
const VERSION: u16 = 2;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Index {
    version: u16,
    queued: bool,
    upload_pages: u32,
    upload_cursor: Option<String>,
    uploads_planned: bool,
    uploads_done: u32,
    releases_planned: bool,
    release_pages: u32,
    release_cursor: Option<String>,
    releases_sent: u32,
    marked: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Release {
    key: String,
    path: BlobPath,
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
    #[serde(default)]
    vanished: bool,
}

fn upload_page(index: u32) -> String {
    format!("blob-upload-{index:04}.json")
}

fn uploaded_page(index: u32) -> String {
    format!("blob-uploaded-{index:04}.json")
}

fn release_page(index: u32) -> String {
    format!("blob-release-{index:04}.json")
}

fn load(store: &Store) -> Result<Index> {
    let index = store.read::<Index>(INDEX)?.unwrap_or(Index {
        version: VERSION,
        ..Index::default()
    });
    if index.version != VERSION
        || index.uploads_done > index.upload_pages
        || index.releases_sent > index.release_pages
        || (index.upload_pages > 0) != index.upload_cursor.is_some()
        || (index.release_pages > 0) != index.release_cursor.is_some()
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

/// Plan once, before the job's first capture: every queued release, in pages of 64.
pub(super) async fn plan_releases(source: &Source, store: &Store, saved: &Saved) -> Result<()> {
    let mut index = load(store)?;
    if index.releases_planned {
        return Ok(());
    }
    if !saved.begin_submitted {
        loop {
            let rows = source
                .pending_blob_releases(index.release_cursor.as_deref(), MAX_BLOB_BATCH)
                .await?;
            let Some(last) = rows.last() else {
                break;
            };
            let full = rows.len() == MAX_BLOB_BATCH;
            index.release_cursor = Some(last.key.clone());
            let entries: Vec<Release> = rows
                .into_iter()
                .map(|row| Release {
                    key: row.key,
                    path: row.path,
                })
                .collect();
            store.write(&release_page(index.release_pages), &entries)?;
            index.release_pages += 1;
            store.write(INDEX, &index)?;
            if !full {
                break;
            }
        }
    }
    index.releases_planned = true;
    store.write(INDEX, &index)
}

/// Record the planned releases on the open job before it publishes.
pub(super) async fn release(
    store: &Store,
    saved: &Saved,
    client: &ByteBackupClient,
    pause: &AtomicBool,
) -> Result<()> {
    let begin = saved.begin.as_ref().ok_or(BackupError::Invalid)?;
    let mut index = load(store)?;
    while index.releases_sent < index.release_pages {
        let entries: Vec<Release> = read_page(store, &release_page(index.releases_sent))?;
        let request = BlobReleaseRequest {
            version: Version::V2,
            job: begin.job.clone(),
            paths: entries.into_iter().map(|entry| entry.path).collect(),
        };
        retry::control("blob_release", pause, || async {
            Ok(client.release_blobs(&request).await?)
        })
        .await?;
        index.releases_sent += 1;
        store.write(INDEX, &index)?;
    }
    Ok(())
}

/// After the service accepted the job: mark its uploaded versions and drop its released `fr:` rows.
pub(super) async fn accepted(source: &Source, store: &Store, saved: &Saved) -> Result<()> {
    if saved.phase != Phase::Published {
        return Ok(());
    }
    let mut index = load(store)?;
    if index.marked {
        return Ok(());
    }
    for page in 0..index.upload_pages {
        let uploaded: Vec<String> = if page < index.uploads_done {
            read_page(store, &uploaded_page(page))?
        } else {
            uploaded_keys(store, page)?
        };
        source.blobs_accepted(&uploaded, &[]).await?;
    }
    for page in 0..index.releases_sent {
        let entries: Vec<Release> = read_page(store, &release_page(page))?;
        let released: Vec<String> = entries.into_iter().map(|entry| entry.key).collect();
        source.blobs_accepted(&[], &released).await?;
    }
    index.marked = true;
    store.write(INDEX, &index)
}

fn uploaded_keys(store: &Store, page: u32) -> Result<Vec<String>> {
    let entries: Vec<Upload> = read_page(store, &upload_page(page))?;
    Ok(entries
        .into_iter()
        .filter(|entry| entry.receipt.is_some())
        .map(|entry| entry.key)
        .collect())
}

#[derive(Deserialize)]
struct Pages {
    #[serde(default)]
    upload_pages: u32,
    #[serde(default)]
    release_pages: u32,
}

/// Remove the blob plan of a finished job by exact names.
pub(super) fn remove_plan(root: &std::path::Path) -> Result<()> {
    let Some(pages) = super::store::read::<Pages>(&root.join(INDEX))? else {
        return Ok(());
    };
    let mut names = Vec::new();
    for page in 0..=pages.upload_pages {
        names.push(upload_page(page));
        names.push(uploaded_page(page));
    }
    for page in 0..=pages.release_pages {
        names.push(release_page(page));
    }
    names.push(INDEX.to_owned());
    for name in names {
        let path = root.join(&name);
        super::store::remove_owned(&path.with_extension("pending"))?;
        super::store::remove_owned(&path)?;
    }
    Ok(())
}
