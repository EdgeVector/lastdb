//! Parse I's snapshot JSON into the install types.
//!
//! Manifest and part wrappers use decimal strings. Entry objects inside a
//! part use JSON numbers, and `payload` is unpadded URL-safe base64 of the
//! envelope.

use cloud_backup_protocol::v2::limits::{
    CONTROL_JSON_MAX_BYTES, MUTATION_LOG_SNAPSHOT_PART_MAX_BYTES,
};
use cloud_backup_protocol::DecimalU64;
use serde::Deserialize;

use super::types::{IncorporatedFrom, SnapshotEntry, SnapshotManifest, SnapshotPart};
use super::RestoreError;

/// Parse one manifest body within the control JSON limit.
pub fn manifest_from_json(bytes: &[u8]) -> Result<SnapshotManifest, RestoreError> {
    let body: ManifestBody = decode(bytes, CONTROL_JSON_MAX_BYTES)?;
    let parts =
        u32::try_from(body.parts.get()).map_err(|_| RestoreError::Chain("snapshot part"))?;
    Ok(SnapshotManifest {
        snapshot_ref: body.snapshot_ref,
        through_delivery_seq: body.through_delivery_seq.get(),
        at_ms: body.at_ms.get(),
        written_at_ms: body.written_at_ms.get(),
        parts,
        previous_ref: body.previous_ref,
    })
}

/// Parse one part body, including the entry array stored in `part`.
pub fn part_from_json(bytes: &[u8]) -> Result<SnapshotPart, RestoreError> {
    let body: PartBody = decode(bytes, MUTATION_LOG_SNAPSHOT_PART_MAX_BYTES)?;
    let docs: Vec<EntryDoc> =
        serde_json::from_str(&body.part).map_err(|_| RestoreError::Chain("snapshot json"))?;
    let index =
        u32::try_from(body.index.get()).map_err(|_| RestoreError::Chain("snapshot part"))?;
    Ok(SnapshotPart {
        database_id: body.database_id,
        snapshot_ref: body.snapshot_ref,
        index,
        entries: docs.into_iter().map(entry_from).collect(),
    })
}

fn decode<'a, T: Deserialize<'a>>(bytes: &'a [u8], maximum: u64) -> Result<T, RestoreError> {
    if bytes.len() as u64 > maximum {
        return Err(RestoreError::Chain("snapshot json"));
    }
    serde_json::from_slice(bytes).map_err(|_| RestoreError::Chain("snapshot json"))
}

fn entry_from(doc: EntryDoc) -> SnapshotEntry {
    SnapshotEntry {
        accepted_at_ms: doc.accepted_at_ms,
        writer_key: doc.writer_key,
        writer_device: doc.writer_device,
        writer_seq: doc.writer_seq,
        authored_at_ms: doc.authored_at_ms,
        namespace: doc.namespace,
        incorporated_from: doc.incorporated_from.map(incorporated_from),
        payload_b64: doc.payload,
    }
}

fn incorporated_from(from: IncorporatedDoc) -> IncorporatedFrom {
    IncorporatedFrom {
        writer_key: from.writer_key,
        writer_device: from.writer_device,
        writer_seq: from.writer_seq,
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestBody {
    snapshot_ref: String,
    through_delivery_seq: DecimalU64,
    at_ms: DecimalU64,
    written_at_ms: DecimalU64,
    parts: DecimalU64,
    #[serde(default)]
    previous_ref: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PartBody {
    database_id: String,
    snapshot_ref: String,
    index: DecimalU64,
    part: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EntryDoc {
    accepted_at_ms: u64,
    writer_key: String,
    writer_device: String,
    writer_seq: u64,
    authored_at_ms: u64,
    namespace: String,
    #[serde(default)]
    incorporated_from: Option<IncorporatedDoc>,
    payload: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IncorporatedDoc {
    writer_key: String,
    writer_device: String,
    writer_seq: u64,
}
