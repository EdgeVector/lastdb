//! Transport metadata and accepted-member read authority. No provider key is public.
use super::inventory::{InventoryIndex, InventoryPage};
use super::{hash, limits, Job, Version};
use crate::{DecimalU64, Digest, ProtocolError};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InventoryKind {
    Page,
    Index,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum InventoryMetadata {
    Page { page: InventoryPage },
    Index { index: InventoryIndex },
}

impl InventoryMetadata {
    pub fn scope(&self) -> &crate::CloudScope {
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
    /// Exact canonical metadata bytes exclude the request and enum envelopes.
    pub fn identity(&self) -> Result<(InventoryKind, Digest, u64), ProtocolError> {
        self.validate()?;
        let (kind, bytes) = match self {
            Self::Page { page } => (InventoryKind::Page, hash::canonical_bytes(page)?),
            Self::Index { index } => (InventoryKind::Index, hash::canonical_bytes(index)?),
        };
        Ok((kind, hash::stored_bytes(&bytes)?, bytes.len() as u64))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InventoryPutRequest {
    pub version: Version,
    pub job: Job,
    pub metadata: InventoryMetadata,
}

impl InventoryPutRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        self.metadata.validate()?;
        if self.metadata.scope() != &self.job.stream.scope {
            return Err(ProtocolError::invalid("v2.inventory_put.scope"));
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
    pub fn input_sha256(&self) -> Result<Digest, ProtocolError> {
        self.validate()?;
        hash::input("lastdb:cloud-byte-backup:inventory-put:v2", self)
    }
}

/// The service stores exact metadata before return. These bytes receive a separate
/// quota charge; they are not ordinary objects in their own receipt inventory.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InventoryPutReceipt {
    pub version: Version,
    pub job: Job,
    pub input_sha256: Digest,
    pub kind: InventoryKind,
    pub sha256: Digest,
    pub bytes: DecimalU64,
}

impl InventoryPutReceipt {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        if self.bytes.get() == 0 || self.bytes.get() > limits::INVENTORY_JSON_MAX_BYTES {
            return Err(ProtocolError::limit("v2.inventory_put.bytes"));
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
    pub fn validate_for(&self, request: &InventoryPutRequest) -> Result<(), ProtocolError> {
        request.validate()?;
        self.validate()?;
        let (kind, sha256, bytes) = request.metadata.identity()?;
        if self.job != request.job
            || self.input_sha256 != request.input_sha256()?
            || self.kind != kind
            || self.sha256 != sha256
            || self.bytes.get() != bytes
        {
            return Err(ProtocolError::invalid("v2.inventory_put.receipt"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum InventoryPutResult {
    Ok { receipt: InventoryPutReceipt },
    Error { error: ProtocolError },
}
impl InventoryPutResult {
    pub fn validate_for(&self, request: &InventoryPutRequest) -> Result<(), ProtocolError> {
        request.validate()?;
        if let Self::Ok { receipt } = self {
            receipt.validate_for(request)?;
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}

mod read;
pub use read::*;
