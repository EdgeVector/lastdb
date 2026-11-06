//! Describe the held generation and its accepted per-writer log positions.

use super::super::{FrozenFile, FrozenSnapshot, SignedSnapshotTransport};
use crate::byte_cloud_restore::{RestoreError, Result};
use crate::shared_restore::recovery::SnapshotSchema;
use cloud_backup_protocol::shared::objects::{SnapshotLogEndsRequest, SnapshotLogPosition};
use cloud_backup_protocol::{DecimalU64, Digest, Id, RelativePath};
use fold_db::fold_db_core::mutation_manager::portable_checkpoint::PortableCheckpoint;
use fold_db::portable_mutation::StreamId;
use fold_db::storage::disk_pack::byte_backup::SharedRunCapture;
use sha2::{Digest as _, Sha256};
use std::io::Read;

pub(super) async fn prepare(
    source: PortableCheckpoint,
    capture: SharedRunCapture,
    snapshot_id: Id,
    deadline_ms: u64,
    transport: &SignedSnapshotTransport,
) -> Result<(FrozenSnapshot, SharedRunCapture)> {
    let database_id = source.writer().database_id.clone();
    let positions = source
        .progress()
        .iter()
        .filter(|(_, cursor)| cursor.applied_through != 0)
        .map(|(stream, cursor)| {
            Ok(SnapshotLogPosition {
                source_stream: Id::try_from(stream.as_str().to_owned())?,
                sequence: DecimalU64::new(cursor.applied_through),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let lookup = SnapshotLogEndsRequest {
        database_id: database_id.clone(),
        positions,
    };
    let (prior, accepted, hashed) = tokio::try_join!(
        transport.current_snapshot(&database_id),
        transport.accepted_log_ends(&lookup),
        hash_inventory(capture, deadline_ms),
    )?;
    let accepted = accepted
        .into_iter()
        .map(|cover| {
            let stream = cover
                .source_stream
                .as_str()
                .parse::<StreamId>()
                .map_err(|_| RestoreError::Invalid("accepted source stream"))?;
            Ok((stream, cover.through))
        })
        .collect::<Result<_>>()?;
    let covers = source
        .covers(&accepted)
        .map_err(|_| RestoreError::Invalid("snapshot accepted log positions"))?;
    let mut schemas: Vec<_> = source
        .definitions()
        .iter()
        .map(|(local_schema, definition)| SnapshotSchema {
            local_schema: local_schema.clone(),
            definition: definition.clone(),
        })
        .collect();
    schemas.sort_unstable_by(|left, right| left.definition.id().cmp(right.definition.id()));
    let (capture, files) = hashed;
    let cut = FrozenSnapshot {
        database_id,
        snapshot_id,
        replaces: prior.map(|receipt| receipt.snapshot_id),
        expires_at_ms: deadline_ms,
        profile: capture.profile().map_err(RestoreError::native_proof)?,
        covers,
        progress: source.progress().clone(),
        delete_reclaimed_through_ms: source.delete_reclaimed_through_ms(),
        files,
        schemas,
    };
    Ok((cut, capture))
}

async fn hash_inventory(
    capture: SharedRunCapture,
    deadline_ms: u64,
) -> Result<(SharedRunCapture, Vec<FrozenFile>)> {
    tokio::task::spawn_blocking(move || {
        let files = inventory(&capture, deadline_ms)?;
        Ok((capture, files))
    })
    .await
    .map_err(|_| RestoreError::Invalid("snapshot file inventory task"))?
}

fn inventory(capture: &SharedRunCapture, deadline_ms: u64) -> Result<Vec<FrozenFile>> {
    if capture.files().count() > cloud_backup_protocol::limits::NATIVE_FILES_MAX {
        return Err(RestoreError::Limit("snapshot file count"));
    }
    let mut buffer = vec![0; 64 * 1024];
    let mut files = Vec::new();
    for (path, bytes) in capture.files() {
        let mut input = capture
            .open_file(path)
            .map_err(RestoreError::native_proof)?;
        let mut sha = Sha256::new();
        let mut length = 0;
        loop {
            if fold_db::clock::unix_millis() >= deadline_ms {
                return Err(RestoreError::Limit("snapshot file inventory deadline"));
            }
            let read = input.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            length += read as u64;
            if length > bytes {
                return Err(RestoreError::Invalid("frozen disk file grew"));
            }
            sha.update(&buffer[..read]);
        }
        if length != bytes {
            return Err(RestoreError::Invalid("frozen disk file length"));
        }
        let destination = capture
            .destination(path)
            .map_err(RestoreError::native_proof)?;
        let path = path
            .to_str()
            .ok_or(RestoreError::Invalid("frozen disk file path"))?;
        files.push(FrozenFile {
            path: RelativePath::try_from(path.to_owned())?,
            destination,
            bytes: DecimalU64::new(bytes),
            sha256: Digest::try_from(fold_db::hex::hex_lower(sha.finalize()))?,
        });
    }
    files.sort_unstable_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

pub(super) fn require_same_inventory(
    capture: &SharedRunCapture,
    cut: &FrozenSnapshot,
) -> Result<()> {
    if capture.profile().map_err(RestoreError::native_proof)? != cut.profile
        || capture.files().count() != cut.files.len()
    {
        return Err(RestoreError::Invalid(
            "saved snapshot disk inventory changed",
        ));
    }
    for (path, bytes) in capture.files() {
        let text = path
            .to_str()
            .ok_or(RestoreError::Invalid("frozen disk file path"))?;
        let held = cut
            .files
            .binary_search_by(|file| file.path.as_str().cmp(text))
            .ok()
            .and_then(|index| cut.files.get(index))
            .ok_or(RestoreError::Invalid("saved snapshot disk file missing"))?;
        if bytes != held.bytes.get()
            || capture
                .destination(path)
                .map_err(RestoreError::native_proof)?
                != held.destination
        {
            return Err(RestoreError::Invalid("saved snapshot disk file changed"));
        }
    }
    Ok(())
}
