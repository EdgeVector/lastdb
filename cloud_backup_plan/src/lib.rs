//! The one upload planner for native LastDB cloud backups.
//!
//! A backup job freezes an ordered set of native files. The planner cuts them
//! into transfer objects: a file larger than one chunk into fixed chunks, and
//! adjacent small files with one destination into one pack. It hashes the
//! bytes, names the expected receipt of each object, and records the extents
//! of each file. It owns no file, store or job state: the caller opens each
//! frozen file and saves each planned object. The snapshot capture (`fold_db`)
//! and the mutation log capture (`lastdb_node`) both plan here.
use cloud_backup_protocol::v2::transfer::{ObjectReceipt, TransferInput};
use cloud_backup_protocol::v2::{Object, Version};
use cloud_backup_protocol::{CloudScope, DecimalU64, Digest, ProtocolError, RelativePath};
use serde::{Deserialize, Serialize};

mod planner;
pub use planner::{plan, PlanFile, PlanSegment, PlanTotals, PlannedObject};

pub const MAX_FILE_EXTENTS: usize = 16384;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientExtent {
    pub receipt_id: Digest,
    pub offset: DecimalU64,
    pub length: DecimalU64,
}
/// One native file and the object ranges that hold its bytes, in order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientNativeFile {
    pub path: RelativePath,
    pub native_sha256: Digest,
    pub native_bytes: DecimalU64,
    pub extents: Vec<ClientExtent>,
}

impl ClientNativeFile {
    pub fn validate(&self) -> Result<(), PlanError> {
        if self.native_bytes.get() == 0
            || self.extents.is_empty()
            || self.extents.len() > MAX_FILE_EXTENTS
        {
            return Err(PlanError::Invalid("byte native file extent count"));
        }
        let mut bytes = 0u64;
        for extent in &self.extents {
            if extent.length.get() == 0
                || extent
                    .offset
                    .get()
                    .checked_add(extent.length.get())
                    .is_none()
            {
                return Err(PlanError::Invalid("byte native extent bounds"));
            }
            bytes = bytes
                .checked_add(extent.length.get())
                .ok_or(PlanError::Invalid("byte native extent overflow"))?;
        }
        if bytes != self.native_bytes.get() {
            return Err(PlanError::Invalid("byte native file exact length"));
        }
        Ok(())
    }
}

/// A refusal from the planner. Each caller maps it to its own error.
#[derive(Debug)]
pub enum PlanError {
    /// A file record is out of bounds. The text names the bound.
    Invalid(&'static str),
    /// A frozen file holds fewer or more bytes than its frozen length.
    SourceLength,
    /// A file needs more extents than one record holds.
    ExtentBound,
    Protocol(ProtocolError),
    Io(std::io::Error),
}
impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(reason) => f.write_str(reason),
            Self::SourceLength => f.write_str("byte native source length changed"),
            Self::ExtentBound => f.write_str("byte native file extent metadata bound"),
            Self::Protocol(_) => f.write_str("byte protocol validation"),
            Self::Io(error) => write!(f, "byte native source read: {error}"),
        }
    }
}
impl std::error::Error for PlanError {}
impl From<ProtocolError> for PlanError {
    fn from(error: ProtocolError) -> Self {
        Self::Protocol(error)
    }
}
impl From<std::io::Error> for PlanError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// The receipt the service gives for `object` in `scope`. Its id depends on
/// the scope and the object only, never on the request id.
pub fn object_receipt(scope: &CloudScope, object: &Object) -> Result<ObjectReceipt, ProtocolError> {
    let mut receipt = ObjectReceipt {
        version: Version::V2,
        receipt_id: object.sha256.clone(),
        scope: scope.clone(),
        object: object.clone(),
    };
    receipt.receipt_id = receipt.identity_sha256()?;
    receipt.validate()?;
    Ok(receipt)
}
/// The receipt that confirms one transfer input.
pub fn expected_receipt(input: &TransferInput) -> Result<ObjectReceipt, ProtocolError> {
    input.validate()?;
    object_receipt(&input.job.stream.scope, &input.object)
}
