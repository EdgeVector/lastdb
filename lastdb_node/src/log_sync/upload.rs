//! Upload one sealed file through the existing byte backup client.
//!
//! The begin value, transfer input, and sealed bytes are durable before the
//! first request. A verified receipt is durable before the job closes. The
//! cloud body then names `file.object` from that receipt and omits the local
//! sealed-row hash. This module does not read a stream head, release a blob,
//! or prepare a backup commit.

use std::path::{Path, PathBuf};

use cloud_backup_protocol::v2::blobs::BlobPath;
use cloud_backup_protocol::v2::jobs::{CloseReason, JobBeginRequest, JobCloseRequest};
use cloud_backup_protocol::v2::transfer::{ObjectReceipt, TransferInput};
use cloud_backup_protocol::v2::{ClientDescriptor, Destination, Job, Object, Stream, Version};
use cloud_backup_protocol::{bytes_sha256, canonical_bytes, CloudScope, DecimalU64, Id};
use serde::{Deserialize, Serialize};

use crate::byte_cloud_backup;
use crate::cloud_backup_client::byte_backup::{ByteBackupClient, ExactUpload};
use crate::host::Host;
use crate::named_snapshot_restore::{self, FileObject};

use super::cloud::CloudLog;
use super::disk;
use super::files;
use super::retry;

const JOB_LIMIT: usize = 65_536;

#[derive(Serialize, Deserialize)]
struct FileJob {
    begin: JobBeginRequest,
    input: TransferInput,
    #[serde(default)]
    receipt: Option<ObjectReceipt>,
    closed: bool,
}

pub(super) async fn cloud_body(
    host: &Host,
    log: &CloudLog,
    mutation_id: &str,
    canonical: &[u8],
    ciphertext: &[u8],
) -> Option<Vec<u8>> {
    if retry::store_file(&host.home, log.database_id(), mutation_id, ciphertext).is_err() {
        return None;
    }
    let path = files::path_for(log.database_id(), mutation_id, canonical).ok()?;
    let mut saved = load_or_create(host, log, mutation_id, &path, ciphertext)?;
    if saved.receipt.is_none() || !saved.closed {
        let client = byte_cloud_backup::client_from_home(&host.home).ok()?;
        if saved.receipt.is_none() {
            saved.receipt = Some(verified(&client, &saved, ciphertext.to_vec()).await?);
            store(&host.home, log.database_id(), mutation_id, &saved)?;
        }
        if !saved.closed {
            close_job(&client, &saved).await?;
            saved.closed = true;
            store(&host.home, log.database_id(), mutation_id, &saved)?;
        }
    }
    let scope = saved.receipt.as_ref()?.scope.clone();
    let blob = saved.input.blob.clone()?;
    typed_body(canonical, &scope, &blob)
}

fn load_or_create(
    host: &Host,
    log: &CloudLog,
    mutation_id: &str,
    path: &BlobPath,
    ciphertext: &[u8],
) -> Option<FileJob> {
    match load(&host.home, log.database_id(), mutation_id) {
        Ok(Some(saved)) if same_saved(&saved, log, path, ciphertext) => Some(saved),
        Ok(Some(_)) | Err(()) => None,
        Ok(None) => fresh(host, log, mutation_id, path, ciphertext),
    }
}

fn fresh(
    host: &Host,
    log: &CloudLog,
    mutation_id: &str,
    path: &BlobPath,
    ciphertext: &[u8],
) -> Option<FileJob> {
    let stream_id = Id::try_from(log.database_id().to_owned()).ok()?;
    let commit_id = Id::try_from(uuid::Uuid::new_v4().to_string()).ok()?;
    let request_id = Id::try_from(uuid::Uuid::new_v4().to_string()).ok()?;
    let len = u64::try_from(ciphertext.len()).ok()?;
    let begin = JobBeginRequest {
        version: Version::V2,
        job: Job {
            stream: Stream {
                scope: log.scope.clone(),
                stream_id,
            },
            commit_id,
        },
        descriptor: ClientDescriptor::empty(),
        required_destinations: vec![Destination::B2],
        r2_multipart: None,
        predecessor: None,
        recovery_dependencies: Vec::new(),
        covered_logs: Vec::new(),
    };
    let input = TransferInput {
        version: Version::V2,
        request_id,
        job: begin.job.clone(),
        object: Object {
            sha256: bytes_sha256(ciphertext),
            bytes: DecimalU64::new(len),
            destination: Destination::B2,
        },
        blob: Some(path.clone()),
    };
    begin.validate().ok()?;
    input.validate().ok()?;
    let saved = FileJob {
        begin,
        input,
        receipt: None,
        closed: false,
    };
    store(&host.home, log.database_id(), mutation_id, &saved)?;
    Some(saved)
}

fn same_saved(saved: &FileJob, log: &CloudLog, path: &BlobPath, ciphertext: &[u8]) -> bool {
    let Ok(len) = u64::try_from(ciphertext.len()) else {
        return false;
    };
    saved.begin.predecessor.is_none()
        && saved.begin.job == saved.input.job
        && saved.begin.job.stream.scope == log.scope
        && saved.begin.job.stream.stream_id.as_str() == log.database_id()
        && saved.input.blob.as_ref() == Some(path)
        && saved.input.object.destination == Destination::B2
        && saved.input.object.bytes.get() == len
        && saved.input.object.sha256 == bytes_sha256(ciphertext)
}

async fn verified(
    client: &ByteBackupClient,
    saved: &FileJob,
    sealed: Vec<u8>,
) -> Option<ObjectReceipt> {
    match client
        .upload_exact(&saved.begin, &saved.input, sealed)
        .await
    {
        Ok(ExactUpload::Verified(receipt)) => Some(receipt),
        Ok(ExactUpload::Retryable(step)) => {
            tracing::warn!(
                target: "lastdbd::log_sync",
                step,
                "named file upload can be retried"
            );
            None
        }
        Err(error) => {
            tracing::warn!(
                target: "lastdbd::log_sync",
                error = %error,
                "named file upload stopped"
            );
            None
        }
    }
}

async fn close_job(client: &ByteBackupClient, saved: &FileJob) -> Option<()> {
    let request = JobCloseRequest {
        version: Version::V2,
        job: saved.begin.job.clone(),
        begin_sha256: saved.begin.input_sha256().ok()?,
        reason: CloseReason::UserRequested,
    };
    client.close(&request).await.ok()?;
    Some(())
}

fn typed_body(canonical: &[u8], scope: &CloudScope, path: &BlobPath) -> Option<Vec<u8>> {
    let mut value: serde_json::Value = serde_json::from_slice(canonical).ok()?;
    let file = value.get_mut("file")?.as_object_mut()?;
    file.remove("sealed_sha256");
    let mut body: named_snapshot_restore::AcceptedBody = serde_json::from_value(value).ok()?;
    let section = body.file.as_mut()?;
    if section.cleared {
        return None;
    }
    section.object = Some(FileObject {
        scope: scope.clone(),
        path: path.clone(),
    });
    canonical_bytes(&body).ok()
}

fn load(home: &Path, database_id: &str, mutation_id: &str) -> Result<Option<FileJob>, ()> {
    let Some(bytes) =
        disk::read_limited(&job_path(home, database_id, mutation_id), JOB_LIMIT).map_err(|_| ())?
    else {
        return Ok(None);
    };
    serde_json::from_slice(&bytes).map(Some).map_err(|_| ())
}

fn store(home: &Path, database_id: &str, mutation_id: &str, saved: &FileJob) -> Option<()> {
    let bytes = serde_json::to_vec(saved).ok()?;
    if bytes.len() > JOB_LIMIT {
        return None;
    }
    disk::write_private(&job_path(home, database_id, mutation_id), &bytes).ok()
}

fn job_path(home: &Path, database_id: &str, mutation_id: &str) -> PathBuf {
    home.join("log-sync")
        .join(database_id)
        .join("seals")
        .join(format!("{mutation_id}.job.json"))
}
