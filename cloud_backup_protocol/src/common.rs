use crate::{AccountId, DecimalU64, Digest, Id, ProtocolError};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountKind {
    Personal,
    Org,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Account {
    pub kind: AccountKind,
    pub id: AccountId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloudScope {
    pub account: Account,
    pub account_generation: DecimalU64,
    pub physical_generation: DecimalU64,
}

impl CloudScope {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if (self.account.kind == AccountKind::Org && self.account.id.as_str().len() != 64)
            || self.physical_generation.get() > self.account_generation.get()
        {
            return Err(ProtocolError::invalid("cloud_scope"));
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
        self.scope.validate()?;
        if self.scope.account.kind != AccountKind::Personal {
            return Err(ProtocolError::invalid("backup_stream.account.kind"));
        }
        if self.scope.physical_generation.get() > self.scope.account_generation.get() {
            return Err(ProtocolError::invalid("backup_stream.physical_generation"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Publisher {
    pub principal: AccountId,
    pub lease_id: Id,
    pub epoch: DecimalU64,
}

impl Publisher {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.epoch.get() == 0 {
            return Err(ProtocolError::invalid("publisher.epoch"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    R2,
    B2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectKind {
    NativeFile,
    MetadataPack,
    ManifestPage,
    ManifestIndex,
    ReferencePage,
    BlobPart,
    LogicalPayload,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectEncoding {
    NativeStoredV1,
    EnvelopeV1,
    PublicJcsV1,
    BlobStoredV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectIdentity {
    pub sha256: Digest,
    pub bytes: DecimalU64,
    pub kind: ObjectKind,
    pub encoding: ObjectEncoding,
}

impl ObjectIdentity {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        use crate::limits::*;
        let length = self.bytes.get();
        if length == 0 || length > MAX_OBJECT_BYTES {
            return Err(ProtocolError::limit("object.bytes"));
        }
        let valid = match self.kind {
            ObjectKind::NativeFile | ObjectKind::MetadataPack => {
                self.encoding == ObjectEncoding::NativeStoredV1 && length <= MAX_NATIVE_PART_BYTES
            }
            ObjectKind::ManifestPage | ObjectKind::ManifestIndex => {
                self.encoding == ObjectEncoding::EnvelopeV1
                    && (ENCRYPTION_ENVELOPE_OVERHEAD_BYTES..=MAX_MANIFEST_ENVELOPE_BYTES)
                        .contains(&length)
            }
            ObjectKind::ReferencePage => {
                self.encoding == ObjectEncoding::PublicJcsV1 && length <= MAX_REFERENCE_PAGE_BYTES
            }
            ObjectKind::BlobPart => self.encoding == ObjectEncoding::BlobStoredV1,
            ObjectKind::LogicalPayload => {
                self.encoding == ObjectEncoding::EnvelopeV1
                    && (ENCRYPTION_ENVELOPE_OVERHEAD_BYTES
                        ..=MAX_LOGICAL_UNIT_BYTES + ENCRYPTION_ENVELOPE_OVERHEAD_BYTES)
                        .contains(&length)
            }
        };
        if !valid {
            return Err(ProtocolError::invalid("object.kind_encoding_length"));
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
            || self.page_count.get() > MAX_REFERENCE_PAGES
            || self.object_count.get() == 0
            || self.object_count.get() < self.page_count.get()
            || self.object_count.get() > self.page_count.get() * MAX_PAGE_ENTRIES
            || self.stored_bytes.get() < self.object_count.get()
            || self.stored_bytes.get() > MAX_BACKUP_BYTES
        {
            return Err(ProtocolError::limit("reference_root"));
        }
        Ok(())
    }
}
