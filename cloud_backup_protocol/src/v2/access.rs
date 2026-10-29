//! Transport metadata and accepted-member read authority. No provider key is public.
use super::inventory::{InventoryIndex, InventoryPage};
use super::{hash, limits, Job, Version};
use crate::{DecimalU64, Digest, ProtocolError};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Whether stored inventory metadata is a page or an index.
pub enum InventoryKind {
    /// A page of object receipts.
    Page,
    /// An index of page descriptors.
    Index,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
/// Inventory body to store. The tag selects a page or an index.
pub enum InventoryMetadata {
    /// One inventory page.
    Page {
        /// Page body to store.
        page: InventoryPage,
    },
    /// One inventory index.
    Index {
        /// Index body to store.
        index: InventoryIndex,
    },
}

impl InventoryMetadata {
    /// Account scope of the page or index.
    pub fn scope(&self) -> &crate::CloudScope {
        match self {
            Self::Page { page } => &page.scope,
            Self::Index { index } => &index.scope,
        }
    }
    /// Validate the enclosed page or index.
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
/// Request to store one inventory page or index on a job.
pub struct InventoryPutRequest {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Stream and commit id this value belongs to.
    pub job: Job,
    /// Page or index to store. Its scope must match the job's stream.
    pub metadata: InventoryMetadata,
}

impl InventoryPutRequest {
    /// Reject a bad job, bad metadata, a scope that differs from the job, or oversized canonical JSON.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        self.metadata.validate()?;
        if self.metadata.scope() != &self.job.stream.scope {
            return Err(ProtocolError::invalid("v2.inventory_put.scope"));
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
    /// Domain-separated SHA-256 of this request, after validation.
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
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Stream and commit id this value belongs to.
    pub job: Job,
    /// SHA-256 of the request this receipt stores.
    pub input_sha256: Digest,
    /// Whether `sha256` names a page or an index.
    pub kind: InventoryKind,
    /// SHA-256 of the canonical page or index bytes.
    pub sha256: Digest,
    /// Length of those canonical bytes. Zero and values above the inventory JSON cap are rejected.
    pub bytes: DecimalU64,
}

impl InventoryPutReceipt {
    /// Reject a bad job, a zero or oversized byte count, or oversized canonical JSON.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        if self.bytes.get() == 0 || self.bytes.get() > limits::INVENTORY_JSON_MAX_BYTES {
            return Err(ProtocolError::limit("v2.inventory_put.bytes"));
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
    /// Require this receipt to match the request's job, input digest, kind, content digest, and byte length.
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
/// Stored receipt, or the protocol error for the put.
pub enum InventoryPutResult {
    /// The metadata was stored.
    Ok {
        /// Receipt for the stored page or index.
        receipt: InventoryPutReceipt,
    },
    /// The metadata was not stored.
    Error {
        /// Why the put was refused.
        error: ProtocolError,
    },
}
impl InventoryPutResult {
    /// Validate a success receipt against the request. An error arm only checks encoded size.
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
