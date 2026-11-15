//! Values used to install a database snapshot.
//!
//! Parsers live in [`super::wire`]. This module does not open a socket and
//! does not acknowledge a delivery cursor.

use cloud_backup_protocol::v2::blobs::BlobPath;
use cloud_backup_protocol::CloudScope;

use crate::host::{BlockedDelivery, ReplayBlockReason};

/// One snapshot chain for one database.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotInstall {
    /// Stable database id. Every part must carry this same id.
    pub database_id: String,
    /// Tip ref to install.
    pub snapshot_ref: String,
    /// Every manifest in the `previous_ref` chain, oldest or newest order.
    pub objects: Vec<SnapshotObject>,
}

/// One snapshot object: its manifest and every part.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotObject {
    /// Manifest for this ref.
    pub manifest: SnapshotManifest,
    /// Parts `0 .. manifest.parts`. Order does not matter.
    pub parts: Vec<SnapshotPart>,
}

/// Manifest fields from I's snapshot response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotManifest {
    /// Object ref.
    pub snapshot_ref: String,
    /// Delivery sequence covered by this object and its ancestors.
    pub through_delivery_seq: u64,
    /// Snapshot clock. Zero is refused.
    pub at_ms: u64,
    /// Time the service stored the object.
    pub written_at_ms: u64,
    /// Part count. At least one.
    pub parts: u32,
    /// Absent when this object's parts are a complete snapshot.
    pub previous_ref: Option<String>,
}

/// One part of one snapshot object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotPart {
    /// Database id. Must match the install request.
    pub database_id: String,
    /// Snapshot ref. Must match the manifest.
    pub snapshot_ref: String,
    /// Part index, `0 .. parts`.
    pub index: u32,
    /// Entries in stored order.
    pub entries: Vec<SnapshotEntry>,
}

/// One log entry inside a part. `payload_b64` is unpadded URL-safe base64.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct SnapshotEntry {
    /// Service accept time.
    pub accepted_at_ms: u64,
    /// Writer node key.
    pub writer_key: String,
    /// Writer device label.
    pub writer_device: String,
    /// Writer sequence. Zero is refused.
    pub writer_seq: u64,
    /// Author time recorded on the entry.
    pub authored_at_ms: u64,
    /// Namespace. The payload schema must use this same namespace.
    pub namespace: String,
    /// Incorporation source, when the entry was copied forward.
    pub incorporated_from: Option<IncorporatedFrom>,
    /// Envelope bytes, unpadded URL-safe base64. Not canonical JSON.
    pub payload_b64: String,
}

/// Writer identity an incorporated entry was copied from.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct IncorporatedFrom {
    /// Writer node key.
    pub writer_key: String,
    /// Writer device label.
    pub writer_device: String,
    /// Writer sequence. Zero is refused.
    pub writer_seq: u64,
}

/// File bytes E fetches with I's `delivery/object` route.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotFileRequest {
    /// Database that owns the blob.
    pub database_id: String,
    /// Account scope from the record payload.
    pub scope: CloudScope,
    /// Blob path from the record payload.
    pub path: BlobPath,
}

/// Static install failure. The text is a class, never record bytes or a path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestoreError {
    /// The database id is not a registered database with a verified content key.
    Scope(&'static str),
    /// The chain, manifest, or part bytes are not acceptable.
    Chain(&'static str),
    /// One entry's identity or record payload is not acceptable.
    Entry(&'static str),
    /// Record file bytes are missing, were not uploaded, or do not match the marker.
    File(&'static str),
    /// Local journal or marker state disagrees with this chain.
    Conflict(&'static str),
    /// Local state could not be made durable. Nothing was acknowledged.
    Durable(&'static str),
    /// The open store refused the record, schema, or file seal.
    Apply(&'static str),
    /// A local rule holds this replay entry and all later deliveries.
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
