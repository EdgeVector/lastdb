use super::super::inventory::InventoryProof;
use super::super::transfer::ObjectReceipt;
use super::super::{limits, AcceptedBackup, Destination, Job, Object, Version};
use crate::{DecimalU64, Digest, ProtocolError, Text};
use serde::{Deserialize, Serialize};

/// The service resolves all selectors through one immutable accepted record.
/// Receipt hashes without accepted membership never authorize a read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReadSelector {
    Receipt { receipt_id: Digest },
    InventoryIndex { sha256: Digest },
    InventoryPage { sha256: Digest },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadGrantRequest {
    pub version: Version,
    pub job: Job,
    pub objects: Vec<ReadSelector>,
}
impl ReadGrantRequest {
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
    pub sha256: Digest,
    pub bytes: DecimalU64,
    pub expires_ms: DecimalU64,
    pub url: Text<8192>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination: Option<Destination>,
}
impl ReadGrant {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.bytes.get() == 0 || self.expires_ms.get() == 0 {
            return Err(ProtocolError::invalid("v2.read_grant.bounds"));
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
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
pub struct ReadGrantResponse {
    pub version: Version,
    pub job: Job,
    pub items: Vec<ReadGrantItemResult>,
}
impl ReadGrantResponse {
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

    /// First root read is possible before the inventory pages arrive. The client
    /// still verifies the complete inventory and exact used set before install.
    pub fn root_grant_for(
        &self,
        request: &ReadGrantRequest,
        accepted: &AcceptedBackup,
    ) -> Result<&ReadGrant, ProtocolError> {
        self.validate_for(request, accepted)?;
        let selector = ReadSelector::Receipt {
            receipt_id: accepted.root_receipt_id.clone(),
        };
        let offset = request
            .objects
            .iter()
            .position(|item| item == &selector)
            .ok_or_else(|| ProtocolError::invalid("v2.read_grant.root_selector"))?;
        match &self.items[offset] {
            ReadGrantItemResult::Ok { grant, .. } => Ok(grant),
            ReadGrantItemResult::Error { error, .. } => Err(error.clone()),
        }
    }

    pub fn validate_root_inventory(
        &self,
        request: &ReadGrantRequest,
        accepted: &AcceptedBackup,
        proof: &InventoryProof,
    ) -> Result<(), ProtocolError> {
        accepted.validate_inventory(proof)?;
        self.root_grant_for(request, accepted)?
            .validate_for_receipt(proof.root_receipt())
    }
}
