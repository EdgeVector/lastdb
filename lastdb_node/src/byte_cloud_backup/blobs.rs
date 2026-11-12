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
use cloud_backup_protocol::v2::{
    blobs::{BlobPath, BlobReleaseRequest, MAX_BLOB_BATCH},
    limits::MAX_BATCH_ITEMS,
    transfer::TransferInput,
    Destination, Object, Version,
};
use cloud_backup_protocol::{bytes_sha256, DecimalU64};
use fold_db::cloud_backup_plan::new_transfer_input;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::sync::atomic::AtomicBool;

const INDEX: &str = "blobs.json";
const VERSION: u16 = 3;

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
struct Upload {
    key: String,
    path: BlobPath,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    input: Option<TransferInput>,
    #[serde(default)]
    done: bool,
    #[serde(default)]
    vanished: bool,
}

impl Upload {
    fn pending(key: String, path: BlobPath) -> Self {
        Self {
            key,
            path,
            input: None,
            done: false,
            vanished: false,
        }
    }
}

fn page_name(kind: &str, index: u32) -> String {
    format!("blob-{kind}-{index:04}.json")
}

fn load(store: &Store) -> Result<Index> {
    let idx = store.read::<Index>(INDEX)?.unwrap_or(Index {
        version: VERSION,
        ..Index::default()
    });
    let ok = idx.version == VERSION
        && idx.uploads_done <= idx.upload_pages
        && idx.releases_sent <= idx.release_pages
        && (idx.upload_pages > 0) == idx.upload_cursor.is_some()
        && (idx.release_pages > 0) == idx.release_cursor.is_some();
    ok.then_some(idx).ok_or(BackupError::Invalid)
}

fn read_page<T: DeserializeOwned>(store: &Store, kind: &str, page: u32) -> Result<Vec<T>> {
    let entries: Vec<T> = store
        .read(&page_name(kind, page))?
        .ok_or(BackupError::Missing)?;
    (entries.len() <= MAX_BLOB_BATCH)
        .then_some(entries)
        .ok_or(BackupError::Invalid)
}

pub(super) async fn plan_releases(source: &Source, store: &Store, saved: &Saved) -> Result<()> {
    let mut index = load(store)?;
    if index.releases_planned {
        return Ok(());
    }
    if !saved.begin_submitted {
        loop {
            let cur = index.release_cursor.as_deref();
            let rows = source
                .runtime
                .pending_blob_releases(&source.source_home, cur, MAX_BLOB_BATCH)
                .await?;
            let Some(last) = rows.last() else { break };
            let full = rows.len() == MAX_BLOB_BATCH;
            index.release_cursor = Some(last.key.clone());
            let entries: Vec<_> = rows.into_iter().map(|r| (r.key, r.path)).collect();
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
            source.runtime.queue_unuploaded_blobs().await?;
        }
        index.queued = true;
        store.write(INDEX, &index)?;
    }
    loop {
        if index.uploads_done < index.upload_pages {
            upload_one_page(source, store, saved, client, pause, index.uploads_done).await?;
            index.uploads_done += 1;
            store.write(INDEX, &index)?;
            continue;
        }
        if index.uploads_planned {
            return Ok(());
        }
        let cur = index.upload_cursor.as_deref();
        let rows = source
            .runtime
            .pending_blob_uploads(&source.source_home, cur, MAX_BLOB_BATCH)
            .await?;
        let full = rows.len() == MAX_BLOB_BATCH;
        if let Some(last) = rows.last() {
            index.upload_cursor = Some(last.key.clone());
            let entries: Vec<_> = rows
                .into_iter()
                .map(|r| Upload::pending(r.key, r.path))
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
    let mut entries: Vec<Upload> = read_page(store, "upload", page)?;
    let open: Vec<usize> = (0..entries.len())
        .filter(|&i| !entries[i].done && !entries[i].vanished)
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
            |pos, _| {
                let entry = &mut entries[batch[pos].0];
                (entry.done, entry.input) = (true, None);
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
    let begin = saved.begin.as_ref().ok_or(BackupError::Invalid)?;
    let mut batch = Vec::new();
    for &i in chunk {
        let entry = &mut entries[i];
        let Some(bytes) = source.runtime.record_blob_bytes(&entry.key).await? else {
            if entry.input.is_some() {
                return Err(BackupError::Source);
            }
            entry.vanished = true;
            continue;
        };
        let obj = Object {
            sha256: bytes_sha256(&bytes),
            bytes: DecimalU64::new(bytes.len() as u64),
            destination: Destination::B2,
        };
        let input = match &entry.input {
            Some(input) => input.clone(),
            None => new_transfer_input(begin.job.clone(), obj.clone(), Some(entry.path.clone()))?,
        };
        if input.object != obj {
            return Err(BackupError::Identity);
        }
        entry.input = Some(input.clone());
        batch.push((i, input, bytes));
    }
    Ok(batch)
}

pub(super) async fn release(
    store: &Store,
    saved: &Saved,
    client: &ByteBackupClient,
    pause: &AtomicBool,
) -> Result<()> {
    let job = &saved.begin.as_ref().ok_or(BackupError::Invalid)?.job;
    let mut index = load(store)?;
    while index.releases_sent < index.release_pages {
        let entries: Vec<(String, BlobPath)> = read_page(store, "release", index.releases_sent)?;
        let req = BlobReleaseRequest {
            version: Version::V2,
            job: job.clone(),
            paths: entries.into_iter().map(|(_, path)| path).collect(),
        };
        work::control("blob_release", pause, || async {
            Ok(client.release_blobs(&req).await?)
        })
        .await?;
        index.releases_sent += 1;
        store.write(INDEX, &index)?;
    }
    Ok(())
}

pub(super) async fn accepted(source: &Source, store: &Store, saved: &Saved) -> Result<()> {
    let mut index = load(store)?;
    if saved.phase != Phase::Published || index.marked {
        return Ok(());
    }
    for page in 0..index.upload_pages {
        let uploaded: Vec<_> = read_page::<Upload>(store, "upload", page)?
            .into_iter()
            .filter(|e| e.done)
            .map(|e| e.key)
            .collect();
        source.runtime.blobs_accepted(&uploaded, &[]).await?;
    }
    for page in 0..index.releases_sent {
        let released: Vec<_> = read_page::<(String, BlobPath)>(store, "release", page)?
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        source.runtime.blobs_accepted(&[], &released).await?;
    }
    index.marked = true;
    store.write(INDEX, &index)
}
