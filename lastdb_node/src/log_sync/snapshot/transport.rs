//! Use the existing encrypted-object transport, then commit on the signed route.

use cloud_backup_protocol::v2::blobs::BlobPath;
use cloud_backup_protocol::v2::jobs::{CloseReason, JobBeginRequest, JobCloseRequest};
use cloud_backup_protocol::v2::share_access::MUTATION_LOG_SNAPSHOT_BYTES_PATH;
use cloud_backup_protocol::v2::transfer::TransferInput;
use cloud_backup_protocol::v2::{ClientDescriptor, Destination, Job, Object, Stream, Version};
use cloud_backup_protocol::{bytes_sha256, canonical_bytes, DecimalU64, Id};
use serde::Deserialize;
use serde_json::json;

use crate::cloud_backup_client::byte_backup::ExactUpload;
use crate::database_replay::{SnapshotFileRequest, SnapshotFileSource};
use crate::log_sync::{cloud::CloudLog, http::Proof, restore};

use super::{archive::MAX_BYTES, Manifest};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Read {
    database_id: Id,
    snapshot: Option<Manifest>,
}

pub(super) async fn current(log: &CloudLog) -> Result<Option<Manifest>, String> {
    let body = canonical_bytes(&json!({"action": "read", "database_id": log.database_id()}))
        .map_err(|e| e.to_string())?;
    let (status, raw) = log
        .post_status(MUTATION_LOG_SNAPSHOT_BYTES_PATH, body, Proof::Data)
        .await
        .map_err(|_| "snapshot lookup did not complete")?;
    if status == 404 {
        return Ok(None);
    }
    if status != 200 {
        return Err(format!("signed snapshot lookup returned HTTP {status}"));
    }
    let value: Read = serde_json::from_slice(&raw).map_err(|_| "invalid signed snapshot reply")?;
    if value.database_id.as_str() != log.database_id() {
        return Err("snapshot reply names a different database".into());
    }
    if let Some(manifest) = &value.snapshot {
        validate(log, manifest)?;
    }
    Ok(value.snapshot)
}

pub(super) async fn publish(
    log: &CloudLog,
    previous: Option<&Manifest>,
    through: u64,
    bytes: Vec<u8>,
) -> Result<Manifest, String> {
    let digest = bytes_sha256(&bytes);
    let database = Id::try_from(log.database_id().to_owned()).map_err(|e| e.to_string())?;
    let manifest = Manifest {
        through: DecimalU64::new(through),
        bytes: DecimalU64::new(bytes.len() as u64),
        path: BlobPath {
            database: database.clone(),
            space: "lastdb-snapshot"
                .to_owned()
                .try_into()
                .map_err(|e: cloud_backup_protocol::ProtocolError| e.to_string())?,
            record: digest.clone(),
            write_id: "v1"
                .to_owned()
                .try_into()
                .map_err(|e: cloud_backup_protocol::ProtocolError| e.to_string())?,
        },
        sha256: digest,
    };
    upload(log, &manifest, bytes).await?;
    let body = canonical_bytes(&json!({
        "action": "publish",
        "database_id": database,
        "previous": previous.map(|snapshot| &snapshot.sha256),
        "snapshot": &manifest,
    }))
    .map_err(|e| e.to_string())?;
    let raw = log
        .post(MUTATION_LOG_SNAPSHOT_BYTES_PATH, body)
        .await
        .map_err(|e| e.to_string())?;
    let saved: Manifest =
        serde_json::from_slice(&raw).map_err(|_| "invalid snapshot commit reply")?;
    if saved != manifest {
        return Err("snapshot commit returned different bytes or position".into());
    }
    Ok(saved)
}

async fn upload(log: &CloudLog, manifest: &Manifest, bytes: Vec<u8>) -> Result<(), String> {
    let begin = JobBeginRequest {
        version: Version::V2,
        job: Job {
            stream: Stream {
                scope: log.scope.clone(),
                stream_id: manifest.path.database.clone(),
            },
            commit_id: fresh_id()?,
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
        request_id: fresh_id()?,
        job: begin.job.clone(),
        object: Object {
            sha256: manifest.sha256.clone(),
            bytes: manifest.bytes,
            destination: Destination::B2,
        },
        blob: Some(manifest.path.clone()),
    };
    let host = log.host().map_err(|e| e.to_string())?;
    let client = crate::cloud::client_from_home(&host.home)?;
    let uploaded = client.upload_exact(&begin, &input, bytes).await;
    let close = JobCloseRequest {
        version: Version::V2,
        job: begin.job.clone(),
        begin_sha256: begin.input_sha256().map_err(|e| e.to_string())?,
        reason: CloseReason::UserRequested,
    };
    let closed = client.close(&close).await;
    match uploaded.map_err(|e| e.to_string())? {
        ExactUpload::Verified(_) => closed.map(|_| ()).map_err(|e| e.to_string()),
        ExactUpload::Retryable(step) => Err(format!("snapshot upload awaits {step}")),
    }
}

pub(super) async fn download(log: &CloudLog, manifest: &Manifest) -> Result<Vec<u8>, String> {
    validate(log, manifest)?;
    let request = SnapshotFileRequest {
        database_id: log.database_id().to_owned(),
        scope: log.scope.clone(),
        path: manifest.path.clone(),
    };
    let bytes = restore::file_source(log.clone())
        .read_blob(&request)
        .await
        .map_err(|e| e.to_string())?;
    if bytes.len() as u64 != manifest.bytes.get() || bytes_sha256(&bytes) != manifest.sha256 {
        return Err("snapshot download differs from its saved digest or length".into());
    }
    Ok(bytes)
}

fn validate(log: &CloudLog, manifest: &Manifest) -> Result<(), String> {
    if manifest.path.database.as_str() != log.database_id()
        || manifest.path.space.as_str() != "lastdb-snapshot"
        || manifest.path.record != manifest.sha256
        || !(1..=MAX_BYTES as u64).contains(&manifest.bytes.get())
    {
        return Err("invalid signed snapshot manifest".into());
    }
    manifest.path.validate().map_err(|e| e.to_string())
}

fn fresh_id() -> Result<Id, String> {
    Id::try_from(uuid::Uuid::new_v4().to_string()).map_err(|e| e.to_string())
}
