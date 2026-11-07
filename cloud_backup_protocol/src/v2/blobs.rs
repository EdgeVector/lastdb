//! Record blob objects at client-derived paths (design
//! `design-lastdb-blob-files-on-demand-20261107`).
//!
//! A record blob version is one B2 object at a path that every device derives
//! from the record: database, schema space, SHA-256 of the record key, and the
//! write id. Only the device that created the version uploads it, as a
//! transfer of an open job whose `TransferInput::blob` names the path. A
//! release deletes the object when the service accepts the job (optimistic, no
//! tombstone). A read needs no accepted backup: the path is the authority,
//! scoped to the caller's account. Blob objects are never inventory members.
use super::limits::{encoded, CONTROL_JSON_MAX_BYTES};
use super::{Job, Version};
use crate::{CloudScope, DecimalU64, Digest, Id, ProtocolError, Text};
use serde::{Deserialize, Serialize};

/// Maximum paths in one release or read batch.
pub const MAX_BLOB_BATCH: usize = 64;
/// Maximum bytes of one path segment (space, write id).
pub const MAX_BLOB_SEGMENT_BYTES: usize = 128;

/// The client-derived location of one record blob version.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlobPath {
    /// Database identity that owns the record, stable across devices.
    pub database: Id,
    /// Record space of the schema.
    pub space: Text<MAX_BLOB_SEGMENT_BYTES>,
    /// SHA-256 of the record key. Key text never reaches the provider.
    pub record: Digest,
    /// Write id of the blob version; each version is its own object.
    pub write_id: Text<MAX_BLOB_SEGMENT_BYTES>,
}

impl BlobPath {
    /// Reject a segment that is not a plain path segment.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        for segment in [self.space.as_str(), self.write_id.as_str()] {
            if !segment.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':')
            }) || segment.starts_with('.')
            {
                return Err(ProtocolError::invalid("v2.blob_path.segment"));
            }
        }
        Ok(())
    }

    /// The path below the account prefix:
    /// `<database>/<space>/<sha256(record key)>/<write id>`.
    pub fn key_tail(&self) -> Result<String, ProtocolError> {
        self.validate()?;
        Ok(format!(
            "{}/{}/{}/{}",
            self.database.as_str(),
            self.space.as_str(),
            self.record.as_str(),
            self.write_id.as_str()
        ))
    }
}

fn validate_paths(paths: &[BlobPath], what: &'static str) -> Result<(), ProtocolError> {
    if paths.is_empty() || paths.len() > MAX_BLOB_BATCH {
        return Err(ProtocolError::limit(what));
    }
    for (offset, path) in paths.iter().enumerate() {
        path.validate()?;
        if paths[..offset].contains(path) {
            return Err(ProtocolError::invalid("v2.blob_paths.duplicate"));
        }
    }
    Ok(())
}

/// Release blob versions that this job's records no longer use. The service
/// deletes them when it accepts the job.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlobReleaseRequest {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// The open job that carries the record changes.
    pub job: Job,
    /// Released paths. Duplicates and an empty list are rejected.
    pub paths: Vec<BlobPath>,
}

impl BlobReleaseRequest {
    /// Validate the job and paths, and reject oversized JSON.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        validate_paths(&self.paths, "v2.blob_release.paths")?;
        encoded(self, CONTROL_JSON_MAX_BYTES)
    }
}

/// The service recorded the releases on the job.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlobReleaseResponse {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// The job that holds the releases.
    pub job: Job,
    /// Number of paths recorded by this request.
    pub recorded: DecimalU64,
}

/// Ask for temporary GET URLs for blob versions of the caller's account.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlobReadRequest {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Account id and account generation of the caller.
    pub scope: CloudScope,
    /// Paths to read. Duplicates and an empty list are rejected.
    pub paths: Vec<BlobPath>,
}

impl BlobReadRequest {
    /// Validate the scope and paths, and reject oversized JSON.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.scope.validate()?;
        validate_paths(&self.paths, "v2.blob_read.paths")?;
        encoded(self, CONTROL_JSON_MAX_BYTES)
    }
}

/// One path's read result. An absent object is a normal result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum BlobReadItem {
    /// The object exists; the URL reads exactly `bytes` bytes.
    Grant {
        /// Path this grant reads.
        path: BlobPath,
        /// Stored object length, in bytes.
        bytes: DecimalU64,
        /// Unix epoch milliseconds when this grant expires.
        expires_ms: DecimalU64,
        /// Temporary GET URL. Never store it on a durable record.
        url: Text<8192>,
    },
    /// No object at this path: never uploaded, or released.
    Absent {
        /// Path that has no object.
        path: BlobPath,
    },
}

impl BlobReadItem {
    /// The path this item answers.
    #[must_use]
    pub fn path(&self) -> &BlobPath {
        match self {
            Self::Grant { path, .. } | Self::Absent { path } => path,
        }
    }
}

/// One result per requested path, in the same order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlobReadResponse {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Results paired with the request paths.
    pub items: Vec<BlobReadItem>,
}

impl BlobReadResponse {
    /// Require one result per path, in order, with a valid grant.
    pub fn validate_for(&self, request: &BlobReadRequest) -> Result<(), ProtocolError> {
        request.validate()?;
        if self.items.len() != request.paths.len() {
            return Err(ProtocolError::invalid("v2.blob_read.count"));
        }
        for (item, path) in self.items.iter().zip(&request.paths) {
            if item.path() != path {
                return Err(ProtocolError::invalid("v2.blob_read.order"));
            }
            if let BlobReadItem::Grant {
                bytes, expires_ms, ..
            } = item
            {
                if bytes.get() == 0 || expires_ms.get() == 0 {
                    return Err(ProtocolError::invalid("v2.blob_read.grant"));
                }
            }
        }
        encoded(self, CONTROL_JSON_MAX_BYTES)
    }
}
