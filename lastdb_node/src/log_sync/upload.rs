//! Upload one sealed file for a signed log entry.
//!
//! The blob metadata and sealed bytes are durable before the first request. A
//! verified upload is durable before the log entry references `file.object`.

use std::fs::OpenOptions;
use std::io::Read;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cloud_backup_protocol::v2::blobs::BlobPath;
use cloud_backup_protocol::{bytes_sha256, canonical_bytes, CloudScope, DecimalU64, Digest};
use fold_db::record_value::FileMarker;
use serde::{Deserialize, Serialize};

use crate::cloud_backup_client::{BackupTransportError, BlobUploadTarget};
use crate::database_replay::{self, FileObject};
use crate::host::Host;
use crate::log_envelope::{self, DatabaseContentKey, LogBinding};

use super::cloud::CloudLog;
use super::disk;
use super::files;
use super::session::retry::{self, Seal};

const PENDING_LIMIT: usize = 65_536;
const PREPARE_FAILED: &str = "the next local write could not be prepared for upload";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingBlobUpload {
    seal: Seal,
    scope: CloudScope,
    path: BlobPath,
    bytes: DecimalU64,
    sha256: Digest,
    #[serde(default)]
    verified: bool,
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
    if !saved.verified {
        let envelope = retry::load_file(&host.home, log.database_id(), mutation_id)
            .map_err(|_| PREPARE_FAILED)?
            .ok_or(PREPARE_FAILED)?;
        upload_verified(host, log, &saved, envelope).await?;
        saved.verified = true;
        store(&host.home, log.database_id(), mutation_id, &saved).ok_or(PREPARE_FAILED)?;
    }
    section.object = Some(FileObject {
        scope: saved.scope,
        path,
    });
    let encoded = canonical_bytes(&body).map_err(|_| PREPARE_FAILED)?;
    Ok((encoded, saved.seal))
}

fn fresh(
    log: &CloudLog,
    path: &BlobPath,
    ciphertext: &[u8],
    seal: Seal,
) -> Option<PendingBlobUpload> {
    let len = u64::try_from(ciphertext.len()).ok().filter(|&n| n > 0)?;
    log.scope.validate().ok()?;
    path.validate().ok()?;
    if path.database.as_str() != log.database_id() {
        return None;
    }
    Some(PendingBlobUpload {
        seal,
        scope: log.scope.clone(),
        path: path.clone(),
        bytes: DecimalU64::new(len),
        sha256: bytes_sha256(ciphertext),
        verified: false,
    })
}

fn same_saved(saved: &PendingBlobUpload, log: &CloudLog, path: &BlobPath, proposed: &Seal) -> bool {
    saved.seal.writer_seq == proposed.writer_seq
        && saved.seal.namespace == proposed.namespace
        && saved.scope == log.scope
        && saved.path == *path
        && saved.path.database.as_str() == log.database_id()
        && saved.bytes.get() > 0
}

async fn upload_verified(
    host: &Host,
    log: &CloudLog,
    saved: &PendingBlobUpload,
    sealed: Vec<u8>,
) -> Result<(), &'static str> {
    let client = crate::cloud::client_from_home_with_keypair(&host.home, Arc::clone(&host.keypair))
        .map_err(|_| PREPARE_FAILED)?;
    let target = BlobUploadTarget {
        scope: &saved.scope,
        path: &saved.path,
        bytes: saved.bytes,
        sha256: &saved.sha256,
    };
    match client.upload_blob(target, sealed).await {
        Ok(()) => Ok(()),
        Err(BackupTransportError::Protocol(p)) if p.is_payment_subscription() => {
            Err(crate::cloud::PAYMENT_REQUIRED_MESSAGE)
        }
        Err(error) => {
            tracing::warn!(
                target: "lastdbd::log_sync",
                %error,
                "signed file blob upload stopped"
            );
            Err(PREPARE_FAILED)
        }
    }
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
    let plain = if let Some(row) = source.row {
        fold_db::storage::disk_backup::accepted::open_file_row(key.encryption_bytes(), marker, row)
            .ok()?
    } else {
        read_reference(source.reference?, marker)?
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

fn load(
    home: &Path,
    database_id: &str,
    mutation_id: &str,
) -> Result<Option<PendingBlobUpload>, ()> {
    let Some(bytes) =
        disk::read_limited(&pending_path(home, database_id, mutation_id), PENDING_LIMIT)
            .map_err(|_| ())?
    else {
        return Ok(None);
    };
    serde_json::from_slice(&bytes).map(Some).map_err(|_| ())
}

fn store(
    home: &Path,
    database_id: &str,
    mutation_id: &str,
    saved: &PendingBlobUpload,
) -> Option<()> {
    let bytes = serde_json::to_vec(saved).ok()?;
    if bytes.len() > PENDING_LIMIT {
        return None;
    }
    disk::write_private(&pending_path(home, database_id, mutation_id), &bytes).ok()
}

fn pending_path(home: &Path, database_id: &str, mutation_id: &str) -> PathBuf {
    // Keep the #1656 pending-upload filename in seals/.
    home.join("log-sync")
        .join(database_id)
        .join("seals")
        .join(format!("{mutation_id}.job.json"))
}
