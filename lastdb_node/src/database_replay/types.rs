//! Values used to replay database log entries and fetch file blobs.

use async_trait::async_trait;
use cloud_backup_protocol::v2::blobs::BlobPath;
use cloud_backup_protocol::CloudScope;

/// One log entry. `payload_b64` is unpadded URL-safe base64.
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

/// Static replay failure. The text is a class, never record bytes or a path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestoreError {
    /// The database id is not a registered database with a verified content key.
    Scope(&'static str),
    /// One entry's identity or record payload is not acceptable.
    Entry(&'static str),
    /// Record file bytes are missing, were not uploaded, or do not match the marker.
    File(&'static str),
    /// Local marker state disagrees with this replay.
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
