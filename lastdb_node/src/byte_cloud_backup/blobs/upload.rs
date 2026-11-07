//! Upload the queued record blob versions of one job by path.

use super::*;
use crate::byte_cloud_backup::{transfer, wait, work::Work};
use cloud_backup_client::byte_backup::ByteBackupClient;
use cloud_backup_protocol::v2::{limits::MAX_BATCH_ITEMS, Destination, Object, Version};
use cloud_backup_protocol::{bytes_sha256, DecimalU64, Id};
use std::sync::atomic::AtomicBool;

/// Plan and upload every queued version, page by page. Called after the
/// capture is prepared and before the job publishes.
pub(in crate::byte_cloud_backup) async fn upload(
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
            upload_one_page(source, store, saved, client, pause, index.uploads_done).await?;
            index.uploads_done += 1;
            store.write(INDEX, &index)?;
            continue;
        }
        if index.uploads_planned {
            return Ok(());
        }
        let after = match index.upload_pages {
            0 => None,
            pages => read_page::<Upload>(store, &upload_page(pages - 1))?
                .last()
                .map(|entry| entry.key.clone()),
        };
        let rows = source
            .pending_blob_uploads(after.as_deref(), MAX_BLOB_BATCH)
            .await?;
        let full = rows.len() == MAX_BLOB_BATCH;
        if !rows.is_empty() {
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
        index.uploads_planned = !full || index.upload_pages == MAX_UPLOAD_PAGES;
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
        let mut batch = Vec::new();
        for &i in chunk {
            let entry = &mut entries[i];
            let Some(bytes) = source.blob_bytes(&entry.key).await? else {
                // Retired before its upload: no record of this job names it.
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
        // Frozen inputs are durable before the first request names them.
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
        let context = wait::Context::new(store, saved, pause);
        let mut work = Vec::new();
        for ((_, input, bytes), prepared) in batch.iter().zip(prepared.items) {
            let reader: Box<dyn std::io::Read + Send> =
                Box::new(std::io::Cursor::new(bytes.clone()));
            work.push(transfer::complete(
                client, input, prepared, reader, &context,
            ));
        }
        let results = futures::future::join_all(work).await;
        let mut failure = None;
        for ((i, input, _), result) in batch.iter().zip(results) {
            match result {
                Ok(receipt) if receipt.object == input.object => {
                    entries[*i].receipt = Some(receipt)
                }
                Ok(_) => wait::record_error(&mut failure, BackupError::Identity),
                Err(error) => wait::record_error(&mut failure, error),
            }
        }
        store.write(&name, &entries)?;
        if let Some(error) = failure {
            return Err(error);
        }
    }
    Ok(())
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
