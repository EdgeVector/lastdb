//! Release the record blob versions that this job's writes dropped.

use super::*;
use crate::byte_cloud_backup::retry;
use cloud_backup_client::byte_backup::ByteBackupClient;
use cloud_backup_protocol::v2::blobs::BlobReleaseRequest;
use cloud_backup_protocol::v2::Version;
use std::sync::atomic::AtomicBool;

/// Plan once, before the job's first capture: every queued release, in
/// pages of 64, with no limit on the number of pages. Every planned release
/// then belongs to a main publication that the capture holds. The index
/// records each page, so a restart continues the plan after its cursor. A
/// job that already sent its begin without a plan releases nothing; the next
/// job does.
pub(in crate::byte_cloud_backup) async fn plan_releases(
    source: &Source,
    store: &Store,
    saved: &Saved,
) -> Result<()> {
    let mut index = load(store)?;
    if index.releases_planned {
        return Ok(());
    }
    while !saved.begin_submitted {
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
    index.releases_planned = true;
    store.write(INDEX, &index)
}

/// Record the planned releases on the open job before it publishes.
pub(in crate::byte_cloud_backup) async fn release(
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
