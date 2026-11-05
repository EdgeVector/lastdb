//! Shared snapshot and per-writer log metadata, separate from Backup V2.
//!
//! The service supplies authenticated identities, exact verified object facts,
//! and accepted metadata from one transaction. These checks perform no I/O and
//! do not authorize storage access by themselves. The node interprets the sealed
//! disk inventory, portable mutations and file contents.

mod append;
mod checkpoint;
mod recovery;

pub use append::{
    AppendContext, AppendDecision, BatchObject, BatchReceipt, FileDependency, LogBatch,
};
pub use checkpoint::{
    inventory_sha256, PublicationContext, PublicationDecision, SnapshotManifest, SnapshotReceipt,
    StreamCover,
};
pub use recovery::{RecoveryView, StreamCut};

use crate::{DecimalU64, Digest, ErrorCode, Id, ProtocolError};
use serde::{Deserialize, Serialize};

/// Maximum writer entries in one snapshot manifest or recovery view.
pub const MAX_WRITERS: usize = 1_024;
/// Maximum file dependencies in one log batch, also subject to its byte limit.
pub const MAX_BATCH_FILES: usize = 1_024;

/// A log stream within one shared database.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriterStream {
    /// Stable shared database identity.
    pub database_id: Id,
    /// Origin stream identity used in portable mutation positions.
    pub source_stream: Id,
}

/// The end of an accepted log prefix. A stream without a prefix has no head.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogHead {
    /// Inclusive last sequence of the prefix, greater than zero.
    pub sequence: DecimalU64,
    /// Domain-separated commitment to the batch ending at this sequence.
    pub prefix_sha256: Digest,
}

impl LogHead {
    /// Reject the zero sequence. The empty prefix is represented by absence.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.sequence.get() == 0 {
            return Err(ProtocolError::invalid("shared.log_head.sequence"));
        }
        Ok(())
    }
}

/// An exact version of sealed bytes in the database's object namespace.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealedObject {
    /// Opaque immutable version identity. Provider locations stay service-private.
    pub version_id: Id,
    /// Positive length of the sealed bytes.
    pub bytes: DecimalU64,
    /// SHA-256 of the exact sealed bytes, without a domain frame.
    pub sha256: Digest,
}

impl SealedObject {
    /// Require a positive sealed length.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.bytes.get() == 0 {
            return Err(ProtocolError::invalid("shared.object.bytes"));
        }
        Ok(())
    }
}

fn validate_object_list(objects: &[SealedObject]) -> Result<(), ProtocolError> {
    if objects
        .windows(2)
        .any(|pair| pair[0].version_id >= pair[1].version_id)
    {
        return Err(ProtocolError::invalid("shared.objects.order"));
    }
    for object in objects {
        object.validate()?;
    }
    Ok(())
}

fn error(code: ErrorCode, field: &'static str) -> ProtocolError {
    ProtocolError {
        code,
        field: field.into(),
    }
}
