//! Upload one sealed file for a signed log entry.
//!
//! The begin value, transfer input, and sealed bytes are durable before the
//! first request. A verified receipt is durable before the job closes. The
//! cloud body then names `file.object` from that receipt and omits the local
//! sealed-row hash. This module does not read a stream head, release a blob,
//! or prepare a backup commit.

use std::fs::OpenOptions;
use std::io::Read;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use cloud_backup_protocol::v2::blobs::BlobPath;
use cloud_backup_protocol::v2::jobs::{CloseReason, JobBeginRequest, JobCloseRequest};
use cloud_backup_protocol::v2::transfer::{ObjectReceipt, TransferInput};
use cloud_backup_protocol::v2::{ClientDescriptor, Destination, Job, Object, Stream, Version};
use cloud_backup_protocol::{bytes_sha256, canonical_bytes, DecimalU64, Id};
use fold_db::record_value::FileMarker;
use serde::{Deserialize, Serialize};

use crate::cloud_backup_client::storage_transfer::{ExactUpload, StorageTransferClient};
use crate::cloud_backup_client::BackupTransportError;
use crate::database_replay::{self, FileObject};
use crate::host::Host;
use crate::log_envelope::{self, DatabaseContentKey, LogBinding};

use super::cloud::CloudLog;
use super::disk;
use super::files;
use super::retry::{self, Seal};

const JOB_LIMIT: usize = 65_536;
const PREPARE_FAILED: &str = "the next local write could not be prepared for upload";

#[derive(Serialize, Deserialize)]
struct FileJob {
    seal: Seal,
    begin: JobBeginRequest,
    input: TransferInput,
    #[serde(default)]
    receipt: Option<ObjectReceipt>,
    closed: bool,
}

pub(super) struct FileSource<'a> {
    pub(super) canonical: &'a [u8],
    pub(super) row: Option<&'a [u8]>,
    pub(super) reference: Option<&'a str>,
}

pub(super) async fn cloud_body(
    host: &Host,
    log: &CloudLog,
    key: &DatabaseContentKey,
    mutation_id: &str,
    source: FileSource<'_>,
    proposed: Seal,
) -> Result<(Vec<u8>, Seal), &'static str> {
    let path = files::path_for(log.database_id(), mutation_id, source.canonical)
        .map_err(|_| PREPARE_FAILED)?;
    let mut body = file_body(source.canonical).ok_or(PREPARE_FAILED)?;
    let section = body.file.as_mut().ok_or(PREPARE_FAILED)?;
    let marker = file_marker(section).ok_or(PREPARE_FAILED)?;
    let mut saved =
        match load(&host.home, log.database_id(), mutation_id).map_err(|_| PREPARE_FAILED)? {
            Some(saved) if same_saved(&saved, log, &path, &proposed) => saved,
            Some(_) => return Err(PREPARE_FAILED),
            None => {
                let envelope =
                    seal_file(key, log, &source, &marker, &proposed).ok_or(PREPARE_FAILED)?;
                retry::store_file(&host.home, log.database_id(), mutation_id, &envelope)
                    .map_err(|_| PREPARE_FAILED)?;
                let saved = fresh(log, &path, &envelope, proposed).ok_or(PREPARE_FAILED)?;
                store(&host.home, log.database_id(), mutation_id, &saved).ok_or(PREPARE_FAILED)?;
                saved
            }
        };
    if saved.receipt.is_none() || !saved.closed {
        let client = crate::cloud::client_from_home(&host.home).map_err(|_| PREPARE_FAILED)?;
        if saved.receipt.is_none() {
            let envelope = retry::load_file(&host.home, log.database_id(), mutation_id)
                .map_err(|_| PREPARE_FAILED)?
                .ok_or(PREPARE_FAILED)?;
            saved.receipt = Some(verified(&client, &saved, envelope).await?);
            store(&host.home, log.database_id(), mutation_id, &saved).ok_or(PREPARE_FAILED)?;
        }
        if !saved.closed {
            close_job(&client, &saved).await?;
            saved.closed = true;
            store(&host.home, log.database_id(), mutation_id, &saved).ok_or(PREPARE_FAILED)?;
        }
    }
    let scope = saved.receipt.as_ref().ok_or(PREPARE_FAILED)?.scope.clone();
    section.object = Some(FileObject { scope, path });
    let encoded = canonical_bytes(&body).map_err(|_| PREPARE_FAILED)?;
    Ok((encoded, saved.seal))
}

fn fresh(log: &CloudLog, path: &BlobPath, ciphertext: &[u8], seal: Seal) -> Option<FileJob> {
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
    Some(FileJob {
        seal,
        begin,
        input,
        receipt: None,
        closed: false,
    })
}

fn same_saved(saved: &FileJob, log: &CloudLog, path: &BlobPath, proposed: &Seal) -> bool {
    saved.seal.writer_seq == proposed.writer_seq
        && saved.seal.namespace == proposed.namespace
        && saved.begin.predecessor.is_none()
        && saved.begin.job == saved.input.job
        && saved.begin.job.stream.scope == log.scope
        && saved.begin.job.stream.stream_id.as_str() == log.database_id()
        && saved.input.blob.as_ref() == Some(path)
        && saved.input.object.destination == Destination::B2
        && (!saved.closed || saved.receipt.is_some())
        && saved.receipt.as_ref().is_none_or(|receipt| {
            receipt.scope == saved.input.job.stream.scope && receipt.object == saved.input.object
        })
}

async fn verified(
    client: &StorageTransferClient,
    saved: &FileJob,
    sealed: Vec<u8>,
) -> Result<ObjectReceipt, &'static str> {
    match client
        .upload_exact(&saved.begin, &saved.input, sealed)
        .await
    {
        Ok(ExactUpload::Verified(receipt)) => Ok(receipt),
        Ok(ExactUpload::Retryable(step)) => {
            tracing::warn!(
                target: "lastdbd::log_sync",
                step,
                "named file upload can be retried"
            );
            Err(PREPARE_FAILED)
        }
        Err(BackupTransportError::Protocol(p)) if p.is_payment_subscription() => {
            Err(crate::cloud::PAYMENT_REQUIRED_MESSAGE)
        }
        Err(error) => {
            tracing::warn!(
                target: "lastdbd::log_sync",
                error = %error,
                "named file upload stopped"
            );
            Err(PREPARE_FAILED)
        }
    }
}

async fn close_job(client: &StorageTransferClient, saved: &FileJob) -> Result<(), &'static str> {
    let request = JobCloseRequest {
        version: Version::V2,
        job: saved.begin.job.clone(),
        begin_sha256: saved.begin.input_sha256().map_err(|_| PREPARE_FAILED)?,
        reason: CloseReason::UserRequested,
    };
    client.close(&request).await.map_err(|error| {
        if matches!(&error, BackupTransportError::Protocol(p) if p.is_payment_subscription()) {
            crate::cloud::PAYMENT_REQUIRED_MESSAGE
        } else {
            PREPARE_FAILED
        }
    })?;
    Ok(())
}

fn file_body(canonical: &[u8]) -> Option<database_replay::AcceptedBody> {
    let mut value: serde_json::Value = serde_json::from_slice(canonical).ok()?;
    let file = value.get_mut("file")?.as_object_mut()?;
    file.remove("sealed_sha256");
    serde_json::from_value(value).ok()
}

fn file_marker(section: &database_replay::FileSection) -> Option<FileMarker> {
    let marker = FileMarker {
        name: section.name.clone(),
        media_type: section.media_type.clone(),
        size_bytes: section.size?,
        sha256: section.content_sha256.clone()?,
    };
    if section.cleared
        || section.object.is_some()
        || !marker.has_valid_hash()
        || marker.size_bytes > fold_db::storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES
    {
        return None;
    }
    Some(marker)
}

fn seal_file(
    key: &DatabaseContentKey,
    log: &CloudLog,
    source: &FileSource<'_>,
    marker: &FileMarker,
    seal: &Seal,
) -> Option<Vec<u8>> {
    let plain = match source.row {
        Some(row) => fold_db::storage::disk_backup::accepted::open_file_row(
            key.encryption_bytes(),
            marker,
            row,
        )
        .ok()?,
        None => read_reference(source.reference?, marker)?,
    };
    let binding = LogBinding {
        database_id: log.database_id(),
        writer_key: log.principal().as_str(),
        writer_device: log.device().as_str(),
        writer_seq: seal.writer_seq,
        authored_at_ms: seal.authored_at_ms,
        namespace: &seal.namespace,
        incorporated_from: None,
    };
    log_envelope::seal_blob(key, &binding, &plain).ok()
}

fn read_reference(path: &str, marker: &FileMarker) -> Option<Vec<u8>> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .ok()?;
    let metadata = file.metadata().ok()?;
    if !metadata.is_file() || metadata.len() != marker.size_bytes {
        return None;
    }
    let mut bytes = Vec::new();
    file.take(marker.size_bytes + 1)
        .read_to_end(&mut bytes)
        .ok()?;
    (bytes.len() as u64 == marker.size_bytes && bytes_sha256(&bytes).as_str() == marker.sha256)
        .then_some(bytes)
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
