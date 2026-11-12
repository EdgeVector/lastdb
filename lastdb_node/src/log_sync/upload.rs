//! Upload one sealed file through the existing byte backup client.
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

use crate::byte_cloud_backup;
use crate::cloud_backup_client::byte_backup::{ByteBackupClient, ExactUpload};
use crate::host::Host;
use crate::log_envelope::{self, DatabaseContentKey, LogBinding};
use crate::named_snapshot_restore::{self, FileObject};

use super::cloud::CloudLog;
use super::disk;
use super::files;
use super::retry::{self, Seal};

const JOB_LIMIT: usize = 65_536;

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
    mutation_id: &str,
    source: FileSource<'_>,
    proposed: Seal,
) -> Option<(Vec<u8>, Seal)> {
    let path = files::path_for(log.database_id(), mutation_id, source.canonical).ok()?;
    let mut body = file_body(source.canonical)?;
    let section = body.file.as_mut()?;
    let marker = file_marker(section)?;
    let mut saved = match load(&host.home, log.database_id(), mutation_id).ok()? {
        Some(saved) if same_saved(&saved, log, &path, &proposed) => saved,
        Some(_) => return None,
        None => {
            let envelope = seal_file(host, log, &source, &marker, &proposed)?;
            retry::store_file(&host.home, log.database_id(), mutation_id, &envelope).ok()?;
            let saved = fresh(log, &path, &envelope, proposed)?;
            store(&host.home, log.database_id(), mutation_id, &saved)?;
            saved
        }
    };
    if saved.receipt.is_none() || !saved.closed {
        let client = byte_cloud_backup::client_from_home(&host.home).ok()?;
        if saved.receipt.is_none() {
            let envelope = retry::load_file(&host.home, log.database_id(), mutation_id).ok()??;
            saved.receipt = Some(verified(&client, &saved, envelope).await?);
            store(&host.home, log.database_id(), mutation_id, &saved)?;
        }
        if !saved.closed {
            close_job(&client, &saved).await?;
            saved.closed = true;
            store(&host.home, log.database_id(), mutation_id, &saved)?;
        }
    }
    let scope = saved.receipt.as_ref()?.scope.clone();
    section.object = Some(FileObject { scope, path });
    Some((canonical_bytes(&body).ok()?, saved.seal))
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

fn file_body(canonical: &[u8]) -> Option<named_snapshot_restore::AcceptedBody> {
    let mut value: serde_json::Value = serde_json::from_slice(canonical).ok()?;
    let file = value.get_mut("file")?.as_object_mut()?;
    file.remove("sealed_sha256");
    serde_json::from_value(value).ok()
}

fn file_marker(section: &named_snapshot_restore::FileSection) -> Option<FileMarker> {
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
    host: &Host,
    log: &CloudLog,
    source: &FileSource<'_>,
    marker: &FileMarker,
    seal: &Seal,
) -> Option<Vec<u8>> {
    let key =
        DatabaseContentKey::owned(&host.keypair.secret_key_bytes(), log.database_id()).ok()?;
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
    log_envelope::seal_blob(&key, &binding, &plain).ok()
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
