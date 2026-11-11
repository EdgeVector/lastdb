//! Personal record blobs in a backup job: uploads and releases by path.
use super::{
    model::Saved,
    source::Source,
    store::Store,
    transfer,
    work::{self, Work},
    BackupError, Phase, Result,
};
use crate::cloud_backup_client::byte_backup::ByteBackupClient;
use cloud_backup_protocol::v2::blobs::{BlobPath, BlobReleaseRequest, MAX_BLOB_BATCH};
use cloud_backup_protocol::v2::limits::MAX_BATCH_ITEMS;
use cloud_backup_protocol::v2::transfer::{ObjectReceipt, TransferInput};
use cloud_backup_protocol::v2::{Destination, Object, Version};
use cloud_backup_protocol::{bytes_sha256, DecimalU64, Id};
use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicBool;

const INDEX: &str = "blobs.json";
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

fn page_name(kind: &str, index: u32) -> String {
    format!("blob-{kind}-{index:04}.json")
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
                .runtime()
                .pending_blob_releases(
                    &source.source_home,
                    index.release_cursor.as_deref(),
                    MAX_BLOB_BATCH,
                )
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
            store.write(&page_name("release", index.release_pages), &entries)?;
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

/// Plan and upload every queued version, page by page, after capture is prepared.
pub(super) async fn upload(
    source: &Source,
    store: &Store,
    saved: &Saved,
    client: &ByteBackupClient,
    pause: &AtomicBool,
) -> Result<()> {
    let mut index = load(store)?;
    if !index.queued {
        if matches!(saved.work, Work::Snapshot { .. }) {
            source.runtime().queue_unuploaded_blobs().await?;
        }
        index.queued = true;
        store.write(INDEX, &index)?;
    }
    loop {
        if index.uploads_done < index.upload_pages {
            let page = index.uploads_done;
            upload_one_page(source, store, saved, client, pause, page).await?;
            store.write(&page_name("uploaded", page), &uploaded_keys(store, page)?)?;
            index.uploads_done += 1;
            store.write(INDEX, &index)?;
            store.remove(&page_name("upload", page))?;
            continue;
        }
        if index.uploads_planned {
            return Ok(());
        }
        let rows = source
            .runtime()
            .pending_blob_uploads(
                &source.source_home,
                index.upload_cursor.as_deref(),
                MAX_BLOB_BATCH,
            )
            .await?;
        let full = rows.len() == MAX_BLOB_BATCH;
        if let Some(last) = rows.last() {
            index.upload_cursor = Some(last.key.clone());
            let entries: Vec<Upload> = rows
                .into_iter()
                .map(|row| Upload {
                    key: row.key,
                    path: row.path,
                    input: None,
                    receipt: None,
                    vanished: false,
                })
                .collect();
            store.write(&page_name("upload", index.upload_pages), &entries)?;
            index.upload_pages += 1;
        }
        index.uploads_planned = !full;
        store.write(INDEX, &index)?;
    }
}

async fn upload_one_page(
    source: &Source,
    store: &Store,
    saved: &Saved,
    client: &ByteBackupClient,
    pause: &AtomicBool,
    page: u32,
) -> Result<()> {
    let name = page_name("upload", page);
    let mut entries: Vec<Upload> = read_page(store, &name)?;
    let open: Vec<usize> = (0..entries.len())
        .filter(|&i| entries[i].receipt.is_none() && !entries[i].vanished)
        .collect();
    for chunk in open.chunks(MAX_BATCH_ITEMS) {
        transfer::check_pause(pause)?;
        let batch = freeze_batch(source, saved, &mut entries, chunk).await?;
        store.write(&name, &entries)?;
        if batch.is_empty() {
            continue;
        }
        let inputs: Vec<_> = batch.iter().map(|(_, input, _)| input.clone()).collect();
        let context = work::Context::new(store, saved, pause);
        let failure = transfer::upload_batch(
            client,
            &inputs,
            &context,
            |pos| Ok(Box::new(std::io::Cursor::new(batch[pos].2.clone()))),
            |pos, receipt| {
                entries[batch[pos].0].receipt = Some(receipt);
                Ok(())
            },
        )
        .await?;
        store.write(&name, &entries)?;
        if let Some(error) = failure {
            return Err(error);
        }
    }
    Ok(())
}

async fn freeze_batch(
    source: &Source,
    saved: &Saved,
    entries: &mut [Upload],
    chunk: &[usize],
) -> Result<Vec<(usize, TransferInput, Vec<u8>)>> {
    let mut batch = Vec::new();
    for &i in chunk {
        let entry = &mut entries[i];
        let Some(bytes) = source.runtime().record_blob_bytes(&entry.key).await? else {
            if entry.input.is_some() {
                return Err(BackupError::Source);
            }
            entry.vanished = true;
            continue;
        };
        let input = match &entry.input {
            Some(input) => input.clone(),
            None => freeze(saved, &entry.path, &bytes)?,
        };
        if input.object != object(&bytes) {
            return Err(BackupError::Identity);
        }
        entry.input = Some(input.clone());
        batch.push((i, input, bytes));
    }
    Ok(batch)
}

fn object(bytes: &[u8]) -> Object {
    Object {
        sha256: bytes_sha256(bytes),
        bytes: DecimalU64::new(bytes.len() as u64),
        destination: Destination::B2,
    }
}

fn freeze(saved: &Saved, path: &BlobPath, bytes: &[u8]) -> Result<TransferInput> {
    let begin = saved.begin.as_ref().ok_or(BackupError::Invalid)?;
    let input = TransferInput {
        version: Version::V2,
        request_id: Id::try_from(uuid::Uuid::new_v4().to_string())?,
        job: begin.job.clone(),
        object: object(bytes),
        blob: Some(path.clone()),
    };
    input.validate()?;
    Ok(input)
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
        let entries: Vec<Release> = read_page(store, &page_name("release", index.releases_sent))?;
        let request = BlobReleaseRequest {
            version: Version::V2,
            job: begin.job.clone(),
            paths: entries.into_iter().map(|entry| entry.path).collect(),
        };
        work::control("blob_release", pause, || async {
            Ok(client.release_blobs(&request).await?)
        })
        .await?;
        index.releases_sent += 1;
        store.write(INDEX, &index)?;
    }
    Ok(())
}

/// Mark uploaded versions and drop released `fr:` rows after job acceptance.
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
            read_page(store, &page_name("uploaded", page))?
        } else {
            uploaded_keys(store, page)?
        };
        source.runtime().blobs_accepted(&uploaded, &[]).await?;
    }
    for page in 0..index.releases_sent {
        let entries: Vec<Release> = read_page(store, &page_name("release", page))?;
        let released: Vec<String> = entries.into_iter().map(|entry| entry.key).collect();
        source.runtime().blobs_accepted(&[], &released).await?;
    }
    index.marked = true;
    store.write(INDEX, &index)
}

fn uploaded_keys(store: &Store, page: u32) -> Result<Vec<String>> {
    let entries: Vec<Upload> = read_page(store, &page_name("upload", page))?;
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

pub(super) fn remove_plan(root: &std::path::Path) -> Result<()> {
    let Some(pages) = super::store::read::<Pages>(&root.join(INDEX))? else {
        return Ok(());
    };
    let names = (0..=pages.upload_pages)
        .flat_map(|p| [page_name("upload", p), page_name("uploaded", p)])
        .chain((0..=pages.release_pages).map(|p| page_name("release", p)))
        .chain(std::iter::once(INDEX.to_owned()));
    for name in names {
        super::store::remove_pair(&root.join(&name))?;
    }
    Ok(())
}
