//! Values used to install a database snapshot.
//!
//! Parsers live in [`super::wire`]. This module does not open a socket and
//! does not acknowledge a delivery cursor.

use async_trait::async_trait;
use cloud_backup_protocol::v2::blobs::BlobPath;
use cloud_backup_protocol::CloudScope;

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

/// Reads one uploaded file envelope. E implements this.
///
/// `read_blob` returns bytes only when `request.scope` is the publisher
/// account on the upload receipt for `request.path`. A clear, an inline
/// file, and a content hash with no object do not use this source.
#[async_trait]
pub trait SnapshotFileSource: Send {
    /// Return the stored envelope. The bytes are not plaintext.
    async fn read_blob(&mut self, request: &SnapshotFileRequest) -> Result<Vec<u8>, RestoreError>;
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
        };
        write!(formatter, "snapshot restore {kind}: {detail}")
    }
}

impl std::error::Error for RestoreError {}
