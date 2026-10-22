//! Public metadata creation and temporary reads. Provider bindings remain private.
//! The service checks the exact job and selected confirmed commit's complete closure.
use crate::limits::{
    encoded_json_bytes, MAX_OBJECT_BYTES, MAX_REFERENCE_PAGE_BYTES, REQUEST_JSON_MAX_BYTES,
};
use crate::publication::AcceptedBackup;
use crate::references::{ReferenceIndex, ReferencePage};
use crate::transfer::ObjectReceipt;
use crate::{
    BackupJob, CloudScope, DecimalU64, Digest, ObjectIdentity, ObjectKind, ProtocolError, Text,
    Version,
};
use serde::{Deserialize, Serialize};

pub const READ_GRANT_BATCH_MAX: usize = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceKind {
    Page,
    Index,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReferenceMetadata {
    Page { page: ReferencePage },
    Index { index: ReferenceIndex },
}

impl ReferenceMetadata {
    pub fn scope(&self) -> &CloudScope {
        match self {
            Self::Page { page } => &page.scope,
            Self::Index { index } => &index.scope,
        }
    }

    pub fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            Self::Page { page } => page.validate(),
            Self::Index { index } => index.validate(),
        }
    }

    /// Identity covers the metadata value, without the request or enum envelope.
    pub fn identity(&self) -> Result<(ReferenceKind, Digest, u64), ProtocolError> {
        self.validate()?;
        let (kind, bytes) = match self {
            Self::Page { page } => (ReferenceKind::Page, crate::canonical_bytes(page)?),
            Self::Index { index } => (ReferenceKind::Index, crate::canonical_bytes(index)?),
        };
        Ok((kind, crate::bytes_sha256(&bytes), bytes.len() as u64))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReferencePutRequest {
    pub version: Version,
    pub job: BackupJob,
    pub metadata: ReferenceMetadata,
}

impl ReferencePutRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        self.metadata.validate()?;
        if self.metadata.scope() != &self.job.stream.scope {
            return Err(ProtocolError::invalid("reference_put.scope"));
        }
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

/// The service verifies exact canonical bytes before it returns this value.
/// Reference metadata is excluded from the reference root's object totals.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReferencePutReceipt {
    pub version: Version,
    pub scope: CloudScope,
    pub kind: ReferenceKind,
    pub sha256: Digest,
    pub bytes: DecimalU64,
}

impl ReferencePutReceipt {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.scope.validate()?;
        if self.bytes.get() == 0 || self.bytes.get() > MAX_REFERENCE_PAGE_BYTES {
            return Err(ProtocolError::limit("reference_put.bytes"));
        }
        encoded_json_bytes(self, crate::limits::RECEIPT_JSON_MAX_BYTES).map(|_| ())
    }

    pub fn validate_for(&self, request: &ReferencePutRequest) -> Result<(), ProtocolError> {
        request.validate()?;
        self.validate()?;
        let (kind, sha256, bytes) = request.metadata.identity()?;
        if self.scope != request.job.stream.scope
            || self.kind != kind
            || self.sha256 != sha256
            || self.bytes.get() != bytes
        {
            return Err(ProtocolError::invalid("reference_put.receipt"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReferencePutResult {
    Ok { receipt: ReferencePutReceipt },
    Error { error: ProtocolError },
}

impl ReferencePutResult {
    pub fn validate_for(&self, request: &ReferencePutRequest) -> Result<(), ProtocolError> {
        request.validate()?;
        if let Self::Ok { receipt } = self {
            receipt.validate_for(request)?;
        }
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

/// Select only identities in one confirmed commit. No client storage key is accepted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReadSelector {
    Receipt { receipt_id: Digest },
    ReferenceIndex { sha256: Digest },
    ReferencePage { sha256: Digest },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadGrantRequest {
    pub version: Version,
    pub job: BackupJob,
    pub objects: Vec<ReadSelector>,
}

impl ReadGrantRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        if self.objects.is_empty() || self.objects.len() > READ_GRANT_BATCH_MAX {
            return Err(ProtocolError::limit("read_grant.objects"));
        }
        for (offset, selector) in self.objects.iter().enumerate() {
            if self.objects[..offset].contains(selector) {
                return Err(ProtocolError::invalid("read_grant.objects.duplicate"));
            }
        }
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

/// Temporary GET authority only. Never save this URL in a durable job or receipt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadGrant {
    pub sha256: Digest,
    pub bytes: DecimalU64,
    pub expires_ms: DecimalU64,
    pub url: Text<8192>,
}

impl ReadGrant {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.bytes.get() == 0
            || self.bytes.get() > MAX_OBJECT_BYTES
            || self.expires_ms.get() == 0
        {
            return Err(ProtocolError::invalid("read_grant.bounds"));
        }
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES).map(|_| ())
    }

    pub fn validate_for_receipt(&self, receipt: &ObjectReceipt) -> Result<(), ProtocolError> {
        self.validate()?;
        receipt.validate()?;
        if self.sha256 != receipt.object.sha256 || self.bytes != receipt.object.bytes {
            return Err(ProtocolError::invalid("read_grant.receipt"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReadGrantItemResult {
    Ok {
        selector: ReadSelector,
        grant: ReadGrant,
    },
    Error {
        selector: ReadSelector,
        error: ProtocolError,
    },
}

impl ReadGrantItemResult {
    pub fn selector(&self) -> &ReadSelector {
        match self {
            Self::Ok { selector, .. } | Self::Error { selector, .. } => selector,
        }
    }

    pub fn validate(&self) -> Result<(), ProtocolError> {
        if let Self::Ok { selector, grant } = self {
            grant.validate()?;
            if let ReadSelector::ReferenceIndex { sha256 }
            | ReadSelector::ReferencePage { sha256 } = selector
            {
                if sha256 != &grant.sha256 || grant.bytes.get() > MAX_REFERENCE_PAGE_BYTES {
                    return Err(ProtocolError::invalid("read_grant.metadata"));
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadGrantResponse {
    pub version: Version,
    pub job: BackupJob,
    pub items: Vec<ReadGrantItemResult>,
}

impl ReadGrantResponse {
    pub fn validate_for(&self, request: &ReadGrantRequest) -> Result<(), ProtocolError> {
        request.validate()?;
        if self.job != request.job || self.items.len() != request.objects.len() {
            return Err(ProtocolError::invalid("read_grant.response.identity"));
        }
        for (item, selector) in self.items.iter().zip(&request.objects) {
            item.validate()?;
            if item.selector() != selector {
                return Err(ProtocolError::invalid("read_grant.response.selector"));
            }
        }
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES).map(|_| ())
    }

    /// Validate the first index grant when its receipt is not yet available locally.
    /// The caller must still verify downloaded bytes and authenticated plaintext context.
    pub fn manifest_index_grant_for(
        &self,
        request: &ReadGrantRequest,
        accepted: &AcceptedBackup,
    ) -> Result<&ReadGrant, ProtocolError> {
        self.validate_for(request)?;
        accepted.validate()?;
        if self.job != accepted.job {
            return Err(ProtocolError::invalid("read_grant.manifest_index.job"));
        }
        let expected = ReadSelector::Receipt {
            receipt_id: accepted.encrypted_manifest_receipt_id.clone(),
        };
        let index = request
            .objects
            .iter()
            .position(|selector| selector == &expected)
            .ok_or_else(|| ProtocolError::invalid("read_grant.manifest_index.selector"))?;
        match &self.items[index] {
            ReadGrantItemResult::Ok { grant, .. } => {
                let receipt = ObjectReceipt {
                    version: accepted.version,
                    receipt_id: accepted.encrypted_manifest_receipt_id.clone(),
                    scope: accepted.job.stream.scope.clone(),
                    object: ObjectIdentity {
                        sha256: grant.sha256.clone(),
                        bytes: grant.bytes,
                        kind: ObjectKind::ManifestIndex,
                    },
                };
                grant.validate_for_receipt(&receipt)?;
                Ok(grant)
            }
            ReadGrantItemResult::Error { error, .. } => Err(error.clone()),
        }
    }
}
