//! Values and JSON parsers used to install a database snapshot.

use cloud_backup_protocol::v2::blobs::BlobPath;
use cloud_backup_protocol::v2::limits::{
    CONTROL_JSON_MAX_BYTES, MUTATION_LOG_SNAPSHOT_PART_MAX_BYTES,
};
use cloud_backup_protocol::{CloudScope, DecimalU64};
use serde::Deserialize;

use crate::host::{BlockedDelivery, ReplayBlockReason};

/// One snapshot chain for one database.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotInstall {
    pub database_id: String,
    pub snapshot_ref: String,
    pub objects: Vec<SnapshotObject>,
}

/// One snapshot object: its manifest and every part.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotObject {
    pub manifest: SnapshotManifest,
    pub parts: Vec<SnapshotPart>,
}

/// Manifest fields from I's snapshot response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotManifest {
    pub snapshot_ref: String,
    pub through_delivery_seq: u64,
    pub at_ms: u64,
    pub parts: u32,
    pub previous_ref: Option<String>,
}

/// One part of one snapshot object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotPart {
    pub database_id: String,
    pub snapshot_ref: String,
    pub index: u32,
    pub entries: Vec<SnapshotEntry>,
}

/// One log entry inside a part. `payload_b64` is unpadded URL-safe base64.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct SnapshotEntry {
    pub accepted_at_ms: u64,
    pub writer_key: String,
    pub writer_device: String,
    pub writer_seq: u64,
    pub authored_at_ms: u64,
    pub namespace: String,
    pub incorporated_from: Option<IncorporatedFrom>,
    pub payload_b64: String,
}

/// Writer identity an incorporated entry was copied from.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct IncorporatedFrom {
    pub writer_key: String,
    pub writer_device: String,
    pub writer_seq: u64,
}

/// File bytes fetched with `delivery/object`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotFileRequest {
    pub database_id: String,
    pub scope: CloudScope,
    pub path: BlobPath,
}

/// Static install failure. The text is a class, never record bytes or a path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestoreError {
    Scope(&'static str),
    Chain(&'static str),
    Entry(&'static str),
    File(&'static str),
    Conflict(&'static str),
    Durable(&'static str),
    Apply(&'static str),
    ReplayBlocked {
        reason: ReplayBlockReason,
        delivery_sequence: Option<u64>,
    },
}

impl RestoreError {
    pub(super) fn at_delivery(self, sequence: u64) -> Self {
        match self {
            Self::ReplayBlocked { reason, .. } => Self::ReplayBlocked {
                reason,
                delivery_sequence: Some(sequence),
            },
            _ => self,
        }
    }

    pub(crate) fn blocked_delivery(&self) -> Option<BlockedDelivery> {
        match *self {
            Self::ReplayBlocked {
                reason,
                delivery_sequence: Some(delivery_sequence),
            } => Some(BlockedDelivery {
                delivery_sequence,
                reason,
            }),
            _ => None,
        }
    }
}

impl std::fmt::Display for RestoreError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (kind, detail) = match self {
            Self::Scope(detail) => ("scope", *detail),
            Self::Chain(detail) => ("chain", *detail),
            Self::Entry(detail) => ("entry", *detail),
            Self::File(detail) => ("file", *detail),
            Self::Conflict(detail) => ("conflict", *detail),
            Self::Durable(detail) => ("durable", *detail),
            Self::Apply(detail) => ("apply", *detail),
            Self::ReplayBlocked {
                reason,
                delivery_sequence: Some(sequence),
            } => {
                return write!(
                    formatter,
                    "cloud replay blocked at delivery {sequence}: {}",
                    reason.message()
                );
            }
            Self::ReplayBlocked {
                reason,
                delivery_sequence: None,
            } => ("blocked", reason.message()),
        };
        write!(formatter, "snapshot restore {kind}: {detail}")
    }
}

impl std::error::Error for RestoreError {}

/// Parse one manifest body within the control JSON limit.
pub fn manifest_from_json(bytes: &[u8]) -> Result<SnapshotManifest, RestoreError> {
    let body: ManifestBody = decode_json(bytes, CONTROL_JSON_MAX_BYTES)?;
    let parts =
        u32::try_from(body.parts.get()).map_err(|_| RestoreError::Chain("snapshot part"))?;
    Ok(SnapshotManifest {
        snapshot_ref: body.snapshot_ref,
        through_delivery_seq: body.through_delivery_seq.get(),
        at_ms: body.at_ms.get(),
        parts,
        previous_ref: body.previous_ref,
    })
}

/// Parse one part body, including the entry array stored in `part`.
pub fn part_from_json(bytes: &[u8]) -> Result<SnapshotPart, RestoreError> {
    let body: PartBody = decode_json(bytes, MUTATION_LOG_SNAPSHOT_PART_MAX_BYTES)?;
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

fn decode_json<'a, T: Deserialize<'a>>(bytes: &'a [u8], maximum: u64) -> Result<T, RestoreError> {
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
        incorporated_from: doc.incorporated_from,
        payload_b64: doc.payload,
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestBody {
    snapshot_ref: String,
    through_delivery_seq: DecimalU64,
    at_ms: DecimalU64,
    #[serde(rename = "written_at_ms")]
    _written_at_ms: DecimalU64,
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
    incorporated_from: Option<IncorporatedFrom>,
    payload: String,
}
