//! Local data capabilities. The bound format keeps its issuer private; a
//! payload cannot reveal that issuer or have its marker/bytes replaced.

use crate::record_value::FileMarker;
use std::sync::Arc;

/// One format instance's authority to issue and admit sealed file data.
/// Creating another issuer does not authorize writes through the first one.
/// This is local identity/data ownership, with no I/O, randomness or key state.
pub struct FileWriteIssuer {
    identity: Arc<()>,
}

impl Default for FileWriteIssuer {
    fn default() -> Self {
        Self {
            identity: Arc::new(()),
        }
    }
}

struct Payload {
    issuer: Arc<()>,
    marker: FileMarker,
    row: Vec<u8>,
}

/// A marker and its sealed bytes, bound to the format that issued them.
/// Never serialized; clones share the one immutable payload allocation.
#[derive(Clone)]
pub struct SealedRecordFile {
    payload: Arc<Payload>,
}

impl SealedRecordFile {
    #[must_use]
    pub fn marker(&self) -> &FileMarker {
        &self.payload.marker
    }
}

impl PartialEq for SealedRecordFile {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.payload, &other.payload)
            || (Arc::ptr_eq(&self.payload.issuer, &other.payload.issuer)
                && self.payload.marker == other.payload.marker
                && self.payload.row == other.payload.row)
    }
}

impl FileWriteIssuer {
    /// Bind the format's sealing output to its private issuer.
    #[must_use]
    pub fn issue(&self, marker: FileMarker, row: Vec<u8>) -> FileWrite {
        FileWrite::Put(SealedRecordFile {
            payload: Arc::new(Payload {
                issuer: Arc::clone(&self.identity),
                marker,
                row,
            }),
        })
    }

    /// Only the issuing format can recover the sealed row. Move the vector
    /// when uniquely held; copy only if another payload clone still holds it.
    #[must_use]
    pub fn admit(&self, file: SealedRecordFile) -> Option<Vec<u8>> {
        if !Arc::ptr_eq(&self.identity, &file.payload.issuer) {
            return None;
        }
        Some(
            Arc::try_unwrap(file.payload)
                .map(|payload| payload.row)
                .unwrap_or_else(|payload| payload.row.clone()),
        )
    }
}

/// A local file operation. App/replay serialization never carries it.
#[derive(Clone, PartialEq)]
pub enum FileWrite {
    Put(SealedRecordFile),
    Clear,
}

impl FileWrite {
    #[must_use]
    pub fn row_len(&self) -> usize {
        match self {
            Self::Put(file) => file.payload.row.len(),
            Self::Clear => 0,
        }
    }
}

impl std::fmt::Debug for FileWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Put(file) => f
                .debug_struct("Put")
                .field("marker", file.marker())
                .field("row_bytes", &self.row_len())
                .finish(),
            Self::Clear => f.write_str("Clear"),
        }
    }
}
