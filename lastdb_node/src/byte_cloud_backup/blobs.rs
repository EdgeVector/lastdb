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
            source.queue_unuploaded_blobs().await?;
        }
        index.queued = true;
        store.write(INDEX, &index)?;
    }
    loop {
        if index.uploads_done < index.upload_pages {
            let page = index.uploads_done;
            upload_one_page(source, store, saved, client, pause, page).await?;
            store.write(&uploaded_page(page), &uploaded_keys(store, page)?)?;
            index.uploads_done += 1;
            store.write(INDEX, &index)?;
            store.remove(&upload_page(page))?;
            continue;
        }
        if index.uploads_planned {
            return Ok(());
        }
        let rows = source
            .pending_blob_uploads(index.upload_cursor.as_deref(), MAX_BLOB_BATCH)
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
            store.write(&upload_page(index.upload_pages), &entries)?;
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
    let name = upload_page(page);
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
        let prepared = transfer::prepare(client, &inputs, pause).await?;
        if let Some(error) = prepared.error {
            for ((i, _, _), item) in batch.iter().zip(prepared.items) {
                if let Ok(Some(receipt)) = transfer::verified(item) {
                    entries[*i].receipt = Some(receipt);
                }
            }
            store.write(&name, &entries)?;
            return Err(error);
        }
        let context = work::Context::new(store, saved, pause);
        let futures = batch
            .iter()
            .zip(prepared.items)
            .map(|((_, input, bytes), item)| {
                let reader: Box<dyn std::io::Read + Send> =
                    Box::new(std::io::Cursor::new(bytes.clone()));
                transfer::complete(client, input, item, reader, &context)
            });
        let results = futures::future::join_all(futures).await;
        let mut failure = None;
        for ((i, input, _), result) in batch.iter().zip(results) {
            match result {
                Ok(receipt) if receipt.object == input.object => {
                    entries[*i].receipt = Some(receipt)
                }
                Ok(_) => work::record_error(&mut failure, BackupError::Identity),
                Err(error) => work::record_error(&mut failure, error),
            }
        }
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
        let Some(bytes) = source.blob_bytes(&entry.key).await? else {
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
        let entries: Vec<Release> = read_page(store, &release_page(index.releases_sent))?;
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

pub(super) fn remove_plan(root: &std::path::Path) -> Result<()> {
    let Some(pages) = super::store::read::<Pages>(&root.join(INDEX))? else {
        return Ok(());
    };
    let names = (0..=pages.upload_pages)
        .flat_map(|p| [upload_page(p), uploaded_page(p)])
        .chain((0..=pages.release_pages).map(release_page))
        .chain(std::iter::once(INDEX.to_owned()));
    for name in names {
        let path = root.join(&name);
        super::store::remove_owned(&path.with_extension("pending"))?;
        super::store::remove_owned(&path)?;
    }
    Ok(())
}
