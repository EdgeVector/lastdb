//! Record blob path on a byte-backup transfer (design
//! `design-lastdb-blob-files-on-demand-20261107`).
//!
//! A record blob version is one B2 object. `TransferInput::blob` names its
//! client-derived path: database, schema space, SHA-256 of the record key, and
//! the write id. The field is absent for every other object.

use crate::{Digest, Id, ProtocolError, Text};
use serde::{Deserialize, Serialize};

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
}
