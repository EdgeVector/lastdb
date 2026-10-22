use crate::{AccountId, DecimalU64, Digest, Id, ProtocolError};
use serde::{Deserialize, Serialize};

/// Personal account incarnation issued by cloud authority.
/// Physical provider routing remains private service state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloudScope {
    pub account_id: AccountId,
    pub account_generation: DecimalU64,
}

impl CloudScope {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.account_id.as_str().len() != 32 {
            return Err(ProtocolError::invalid("cloud_scope.account_id"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupStream {
    pub scope: CloudScope,
    pub stream_id: Id,
    pub source_home: Id,
}

impl BackupStream {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.scope.validate()
    }
}

/// Exact durable job identity, before any object transfer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupJob {
    pub stream: BackupStream,
    pub commit_id: Id,
}

impl BackupJob {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.stream.validate()
    }
}

/// Each kind fixes its byte format in protocol v1.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectKind {
    NativeFile,
    MetadataPack,
    ManifestPage,
    ManifestIndex,
    BlobPart,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectIdentity {
    pub sha256: Digest,
    pub bytes: DecimalU64,
    pub kind: ObjectKind,
}

impl ObjectIdentity {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        use crate::limits::*;
        let length = self.bytes.get();
        if length == 0 || length > CLOUD_OBJECT_MAX_BYTES {
            return Err(ProtocolError::limit("object.bytes"));
        }
        let valid = match self.kind {
            ObjectKind::NativeFile | ObjectKind::MetadataPack => length <= NATIVE_PART_MAX_BYTES,
            ObjectKind::ManifestPage | ObjectKind::ManifestIndex => {
                (ENCRYPTION_ENVELOPE_OVERHEAD_BYTES..=MANIFEST_PAGE_ENVELOPE_MAX_BYTES)
                    .contains(&length)
            }
            ObjectKind::BlobPart => true,
        };
        if !valid {
            return Err(ProtocolError::invalid("object.kind_length"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceRoot {
    pub sha256: Digest,
    pub page_count: DecimalU64,
    pub object_count: DecimalU64,
    pub stored_bytes: DecimalU64,
}

impl ReferenceRoot {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        use crate::limits::*;
        if self.page_count.get() == 0
            || self.page_count.get() > MANIFEST_PAGES_MAX as u64
            || self.object_count.get() < self.page_count.get()
            || self.object_count.get() > self.page_count.get() * MANIFEST_PAGE_ENTRIES_MAX as u64
            || self.stored_bytes.get() < self.object_count.get()
            || self.stored_bytes.get() > BACKUP_STORED_BYTES_MAX
        {
            return Err(ProtocolError::limit("reference_root"));
        }
        Ok(())
    }
}
