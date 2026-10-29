use super::super::transfer::ObjectReceipt;
use super::super::{limits, AcceptedBackup, Destination, Job, Object, Version};
use crate::{DecimalU64, Digest, ProtocolError, Text};
use serde::{Deserialize, Serialize};

/// The service resolves all selectors through one immutable accepted record.
/// Receipt hashes without accepted membership never authorize a read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReadSelector {
    /// Read one accepted object receipt by its receipt id.
    Receipt {
        /// SHA-256 id of the accepted object receipt.
        receipt_id: Digest,
    },
    /// Read an inventory index by the SHA-256 of its canonical bytes.
    InventoryIndex {
        /// SHA-256 of the canonical inventory index.
        sha256: Digest,
    },
    /// Read an inventory page by the SHA-256 of its canonical bytes.
    InventoryPage {
        /// SHA-256 of the canonical inventory page.
        sha256: Digest,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Ask for temporary read URLs for selectors on one job.
pub struct ReadGrantRequest {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Stream and commit id this value belongs to.
    pub job: Job,
    /// Selectors to authorize. Duplicates and an empty list are rejected.
    pub objects: Vec<ReadSelector>,
}
impl ReadGrantRequest {
    /// Reject a bad job, an empty or oversized selector list, a duplicate selector, or oversized canonical JSON.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        if self.objects.is_empty() || self.objects.len() > limits::MAX_BATCH_ITEMS {
            return Err(ProtocolError::limit("v2.read_grant.objects"));
        }
        for (offset, selector) in self.objects.iter().enumerate() {
            if self.objects[..offset].contains(selector) {
                return Err(ProtocolError::invalid("v2.read_grant.duplicate"));
            }
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}

/// Temporary exact-byte GET authority. URLs are never durable receipt fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadGrant {
    /// SHA-256 of the object bytes this grant reads.
    pub sha256: Digest,
    /// Object length, in bytes. Zero is rejected.
    pub bytes: DecimalU64,
    /// Unix epoch milliseconds when this grant expires. Zero is rejected.
    pub expires_ms: DecimalU64,
    /// Temporary GET URL. This is not stored on a durable receipt.
    pub url: Text<8192>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Provider for this object. Absent keeps the original provider binding.
    pub destination: Option<Destination>,
}
impl ReadGrant {
    /// Reject a zero byte length, a zero expiry, or oversized canonical JSON.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.bytes.get() == 0 || self.expires_ms.get() == 0 {
            return Err(ProtocolError::invalid("v2.read_grant.bounds"));
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
    /// Require this grant's digest, length, and destination to match the receipt object.
    pub fn validate_for_receipt(&self, receipt: &ObjectReceipt) -> Result<(), ProtocolError> {
        self.validate()?;
        receipt.validate()?;
        if self.sha256 != receipt.object.sha256
            || self.bytes != receipt.object.bytes
            || self.destination != receipt.object.destination
        {
            return Err(ProtocolError::invalid("v2.read_grant.receipt"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
/// One selector's grant, or the protocol error for that selector.
pub enum ReadGrantItemResult {
    /// The selector was authorized.
    Ok {
        /// Selector this grant answers.
        selector: ReadSelector,
        /// Temporary GET authority for that selector.
        grant: ReadGrant,
    },
    /// The selector was not authorized.
    Error {
        /// Selector that was refused.
        selector: ReadSelector,
        /// Why the selector was not authorized.
        error: ProtocolError,
    },
}
impl ReadGrantItemResult {
    /// Return the selector this result answers.
    pub fn selector(&self) -> &ReadSelector {
        match self {
            Self::Ok { selector, .. } | Self::Error { selector, .. } => selector,
        }
    }
    /// Check a successful grant against the accepted backup: receipt shape, index root, or page bounds.
    pub fn validate_for(&self, accepted: &AcceptedBackup) -> Result<(), ProtocolError> {
        if let Self::Ok { selector, grant } = self {
            grant.validate()?;
            match selector {
                ReadSelector::Receipt { receipt_id } => ObjectReceipt {
                    version: Version::V2,
                    receipt_id: receipt_id.clone(),
                    scope: accepted.job().stream.scope.clone(),
                    object: Object {
                        sha256: grant.sha256.clone(),
                        bytes: grant.bytes,
                        destination: grant.destination,
                    },
                }
                .validate()?,
                ReadSelector::InventoryIndex { sha256 } => {
                    if grant.destination == Some(Destination::B2)
                        || sha256 != &accepted.inventory_root.sha256
                        || sha256 != &grant.sha256
                        || grant.bytes.get() > limits::INVENTORY_JSON_MAX_BYTES
                    {
                        return Err(ProtocolError::invalid("v2.read_grant.index"));
                    }
                }
                ReadSelector::InventoryPage { sha256 } => {
                    if grant.destination == Some(Destination::B2)
                        || sha256 != &grant.sha256
                        || grant.bytes.get() > limits::INVENTORY_JSON_MAX_BYTES
                    {
                        return Err(ProtocolError::invalid("v2.read_grant.page"));
                    }
                }
            }
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Grants for one read request, in the request's selector order.
pub struct ReadGrantResponse {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Stream and commit id this value belongs to.
    pub job: Job,
    /// One result per requested selector, in the same order.
    pub items: Vec<ReadGrantItemResult>,
}
impl ReadGrantResponse {
    /// Require the same job as the request and the accepted backup, and one matching result per selector.
    pub fn validate_for(
        &self,
        request: &ReadGrantRequest,
        accepted: &AcceptedBackup,
    ) -> Result<(), ProtocolError> {
        request.validate()?;
        accepted.validate()?;
        if self.job != request.job
            || &self.job != accepted.job()
            || self.items.len() != request.objects.len()
        {
            return Err(ProtocolError::invalid("v2.read_grant.response"));
        }
        for (item, selector) in self.items.iter().zip(&request.objects) {
            item.validate_for(accepted)?;
            if item.selector() != selector {
                return Err(ProtocolError::invalid("v2.read_grant.selector"));
            }
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}
