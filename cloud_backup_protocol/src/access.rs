//! Reference metadata creation and temporary reads. These types grant no authority.
//! The service admits metadata under the exact pending commit and fixed provider binding.
//! Read grants require membership in the selected confirmed commit's complete closure.
use crate::limits::{
    encoded_json_bytes, MAX_OBJECT_BYTES, MAX_REFERENCE_PAGE_BYTES, REQUEST_JSON_MAX_BYTES,
};
use crate::references::{ReferenceIndex, ReferencePage};
use crate::{
    BackupStream, CloudScope, DecimalU64, Digest, Id, ProtocolError, Provider, Text, Version,
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
    pub stream: BackupStream,
    pub commit_id: Id,
    pub metadata: ReferenceMetadata,
}

impl ReferencePutRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.stream.validate()?;
        self.metadata.validate()?;
        if self.metadata.scope() != &self.stream.scope {
            return Err(ProtocolError::invalid("reference_put.scope"));
        }
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

/// The service returns this only after an exact conditional PUT or verified existing bytes.
/// It is not an ObjectReceipt and is excluded from the reference root's object totals.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReferencePutReceipt {
    pub version: Version,
    pub scope: CloudScope,
    pub kind: ReferenceKind,
    pub sha256: Digest,
    pub bytes: DecimalU64,
    pub provider: Provider,
    pub bucket: Text<256>,
    pub key: Text<1024>,
}

impl ReferencePutReceipt {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.scope.validate()?;
        if self.bytes.get() == 0 || self.bytes.get() > MAX_REFERENCE_PAGE_BYTES {
            return Err(ProtocolError::limit("reference_put.bytes"));
        }
        let scope = crate::input_digest("lastdb:native-backup:scope:v1", &self.scope)?;
        let directory = match self.kind {
            ReferenceKind::Page => "pages",
            ReferenceKind::Index => "indices",
        };
        let key = format!(
            "native-backup/v1/{}/references/{}/{}.json",
            scope.as_str(),
            directory,
            self.sha256.as_str()
        );
        if self.key.as_str() != key {
            return Err(ProtocolError::invalid("reference_put.key"));
        }
        encoded_json_bytes(self, crate::limits::RECEIPT_JSON_MAX_BYTES).map(|_| ())
    }

    pub fn validate_for(&self, request: &ReferencePutRequest) -> Result<(), ProtocolError> {
        request.validate()?;
        self.validate()?;
        let (kind, sha256, bytes) = request.metadata.identity()?;
        if self.scope != request.stream.scope
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
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReferencePutResult {
    Ok { receipt: Box<ReferencePutReceipt> },
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

/// These identities never permit a client to choose a raw storage key or bucket.
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
    pub stream: BackupStream,
    pub commit_id: Id,
    pub objects: Vec<ReadSelector>,
}

impl ReadGrantRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.stream.validate()?;
        validate_count(self.objects.len())?;
        for (offset, selector) in self.objects.iter().enumerate() {
            if self.objects[..offset].contains(selector) {
                return Err(ProtocolError::invalid("read_grant.objects.duplicate"));
            }
        }
        encoded_json_bytes(self, REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

/// Temporary GET authority only. Never place this value in a durable job or receipt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadGrant {
    pub provider: Provider,
    pub bucket: Text<256>,
    pub key: Text<1024>,
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
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
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
    pub stream: BackupStream,
    pub commit_id: Id,
    pub items: Vec<ReadGrantItemResult>,
}

impl ReadGrantResponse {
    pub fn validate_for(&self, request: &ReadGrantRequest) -> Result<(), ProtocolError> {
        request.validate()?;
        if self.stream != request.stream
            || self.commit_id != request.commit_id
            || self.items.len() != request.objects.len()
        {
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
}

fn validate_count(count: usize) -> Result<(), ProtocolError> {
    if count == 0 || count > READ_GRANT_BATCH_MAX {
        return Err(ProtocolError::limit("read_grant.objects"));
    }
    Ok(())
}
