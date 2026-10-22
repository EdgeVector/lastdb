//! Durable upload identities. A negative provider read never closes an uncertain effect.
use crate::{
    BackupStream, CloudScope, DecimalU64, Digest, Id, ObjectIdentity, ObjectKind, ProtocolError,
    Provider, Publisher, Text, Version,
};
use serde::{Deserialize, Serialize};

pub const PART_BYTES: u64 = crate::limits::UPLOAD_PART_BYTES;
pub const MAX_PARTS: usize = crate::limits::UPLOAD_PARTS_MAX;
pub const MAX_BATCH_ITEMS: usize = 4;
pub const MAX_REQUEST_BYTES: u64 = crate::limits::REQUEST_JSON_MAX_BYTES;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectReceipt {
    pub version: Version,
    pub receipt_id: Digest,
    pub scope: CloudScope,
    pub object: ObjectIdentity,
    pub provider: Provider,
    pub bucket: Text<256>,
    pub key: Text<1024>,
    pub etag: Text<256>,
    pub verified_ms: DecimalU64,
}

impl ObjectReceipt {
    pub fn identity_sha256(&self) -> Result<Digest, ProtocolError> {
        crate::input_digest(
            "lastdb:native-backup:object-receipt:v1",
            &(
                self.version,
                &self.scope,
                &self.object,
                self.provider,
                &self.bucket,
                &self.key,
                &self.etag,
            ),
        )
    }

    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.scope.validate()?;
        self.object.validate()?;
        if self.receipt_id != self.identity_sha256()? {
            return Err(ProtocolError::invalid("object_receipt.receipt_id"));
        }
        crate::limits::encoded_json_bytes(self, crate::limits::RECEIPT_JSON_MAX_BYTES).map(|_| ())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferInput {
    pub version: Version,
    pub request_id: Id,
    /// Binds early transfers to the same pending backup job as metadata and publication.
    pub commit_id: Id,
    pub stream: BackupStream,
    pub publisher: Publisher,
    pub object: ObjectIdentity,
}

impl TransferInput {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.stream.validate()?;
        self.publisher.validate()?;
        self.object.validate()?;
        if matches!(
            self.object.kind,
            ObjectKind::ReferencePage | ObjectKind::LogicalPayload
        ) {
            return Err(ProtocolError::invalid("transfer.object.kind"));
        }
        Ok(())
    }

    pub fn part_count(&self) -> usize {
        self.object.bytes.get().div_ceil(PART_BYTES) as usize
    }

    pub fn input_sha256(&self) -> Result<Digest, ProtocolError> {
        crate::input_digest("lastdb:native-backup:transfer:v1", self)
    }
}

/// This exact binding survives every retry. Provider selection never falls back.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderBinding {
    pub provider: Provider,
    pub bucket: Text<256>,
    pub key: Text<1024>,
    /// Absent until the Create response or exact owned-effect reconciliation supplies it.
    pub upload_id: Option<Text<2048>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferPhase {
    Prepared,
    Creating,
    CreateUncertain,
    Ready,
    Uploading,
    Completing,
    CompleteUncertain,
    Verifying,
    Verified,
    Reused,
    Rejected,
}

impl TransferPhase {
    /// Adjacency is necessary, never sufficient: provider evidence and revision CAS remain required.
    pub fn can_transition_to(self, next: Self) -> bool {
        self == next
            || (next == Self::Rejected
                && !matches!(self, Self::Verified | Self::Reused | Self::Rejected))
            || matches!(
                (self, next),
                (Self::Prepared, Self::Creating | Self::Reused)
                    | (Self::Creating, Self::CreateUncertain | Self::Ready)
                    | (Self::CreateUncertain, Self::Ready)
                    | (Self::Ready, Self::Uploading)
                    | (Self::Uploading, Self::Completing)
                    | (Self::Completing, Self::CompleteUncertain | Self::Verifying)
                    | (Self::CompleteUncertain, Self::Verifying)
                    | (Self::Verifying, Self::Verified)
            )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartReceipt {
    /// Provider part numbers start at one.
    pub index: u16,
    pub etag: Text<256>,
    pub bytes: DecimalU64,
}

/// Save the identity before Create; save the frozen part set before Complete.
/// No active or uncertain record expires. No cancellation route exists in version one.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferRecord {
    pub input: TransferInput,
    pub input_sha256: Digest,
    pub revision: DecimalU64,
    pub phase: TransferPhase,
    pub creation_effect: Id,
    pub provider_binding: Option<ProviderBinding>,
    pub completion_effect: Option<Id>,
    pub completions: Vec<PartReceipt>,
    pub receipt: Option<ObjectReceipt>,
    /// Present only after conclusive effect closure. Unknown effects cannot be rejected.
    pub failure: Option<ProtocolError>,
}

impl TransferRecord {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.input.validate()?;
        if self.input_sha256 != self.input.input_sha256()? || self.revision.get() == 0 {
            return Err(ProtocolError::invalid("transfer.input_sha256"));
        }
        crate::limits::encoded_json_bytes(self, crate::limits::SESSION_JSON_MAX_BYTES)?;
        if self.phase == TransferPhase::Rejected {
            if self.failure.is_none()
                || self.receipt.is_some()
                || self.completion_effect.is_some() == self.completions.is_empty()
            {
                return Err(ProtocolError::invalid("transfer.rejection"));
            }
            if !self.completions.is_empty() {
                validate_parts(&self.input, &self.completions)?;
            }
            return Ok(());
        }
        if self.phase == TransferPhase::Reused {
            return self.validate_reuse();
        }
        if self.failure.is_some() {
            return Err(ProtocolError::invalid("transfer.failure"));
        }
        if self.phase != TransferPhase::Prepared && self.provider_binding.is_none() {
            return Err(ProtocolError::invalid("transfer.provider_binding"));
        }
        if let Some(binding) = &self.provider_binding {
            let created = !matches!(
                self.phase,
                TransferPhase::Prepared | TransferPhase::Creating | TransferPhase::CreateUncertain
            );
            if created != binding.upload_id.is_some() {
                return Err(ProtocolError::invalid(
                    "transfer.provider_binding.upload_id",
                ));
            }
        }
        let completing = matches!(
            self.phase,
            TransferPhase::Completing
                | TransferPhase::CompleteUncertain
                | TransferPhase::Verifying
                | TransferPhase::Verified
        );
        if completing != self.completion_effect.is_some() {
            return Err(ProtocolError::invalid("transfer.completion_effect"));
        }
        if completing {
            validate_parts(&self.input, &self.completions)?;
        } else if !self.completions.is_empty() {
            return Err(ProtocolError::invalid("transfer.completions"));
        }
        match (&self.receipt, self.phase) {
            (Some(receipt), TransferPhase::Verified) => {
                receipt.validate()?;
                if receipt.scope != self.input.stream.scope || receipt.object != self.input.object {
                    return Err(ProtocolError::invalid("transfer.receipt"));
                }
                let binding = self
                    .provider_binding
                    .as_ref()
                    .ok_or_else(|| ProtocolError::invalid("transfer.provider_binding"))?;
                if receipt.provider != binding.provider
                    || receipt.bucket != binding.bucket
                    || receipt.key != binding.key
                {
                    return Err(ProtocolError::invalid("transfer.receipt.binding"));
                }
            }
            (None, phase) if phase != TransferPhase::Verified => {}
            _ => return Err(ProtocolError::invalid("transfer.receipt")),
        }
        Ok(())
    }

    fn validate_reuse(&self) -> Result<(), ProtocolError> {
        let receipt = self
            .receipt
            .as_ref()
            .ok_or_else(|| ProtocolError::invalid("transfer.reused.receipt"))?;
        receipt.validate()?;
        if receipt.scope != self.input.stream.scope
            || receipt.object != self.input.object
            || self.provider_binding.is_some()
            || self.completion_effect.is_some()
            || !self.completions.is_empty()
            || self.failure.is_some()
        {
            return Err(ProtocolError::invalid("transfer.reused"));
        }
        Ok(())
    }

    /// This only checks record consistency. Workers must also prove the exact provider outcome.
    pub fn validate_transition(&self, next: &Self) -> Result<(), ProtocolError> {
        self.validate()?;
        next.validate()?;
        if self == next {
            return Ok(());
        }
        if self.input != next.input
            || self.input_sha256 != next.input_sha256
            || self.creation_effect != next.creation_effect
            || self.revision.get().checked_add(1) != Some(next.revision.get())
            || !self.phase.can_transition_to(next.phase)
            || matches!(
                self.phase,
                TransferPhase::Verified | TransferPhase::Reused | TransferPhase::Rejected
            )
        {
            return Err(ProtocolError::invalid("transfer.transition"));
        }
        if let Some(binding) = self
            .provider_binding
            .as_ref()
            .filter(|_| next.phase != TransferPhase::Reused)
        {
            let Some(target) = &next.provider_binding else {
                return Err(ProtocolError::invalid("transfer.binding_removed"));
            };
            if binding.provider != target.provider
                || binding.bucket != target.bucket
                || binding.key != target.key
                || (binding.upload_id.is_some() && binding.upload_id != target.upload_id)
            {
                return Err(ProtocolError::invalid("transfer.binding_changed"));
            }
        }
        if (self.completion_effect.is_some() && self.completion_effect != next.completion_effect)
            || (!self.completions.is_empty() && self.completions != next.completions)
            || (self.receipt.is_some() && self.receipt != next.receipt)
        {
            return Err(ProtocolError::invalid("transfer.frozen_completion_changed"));
        }
        Ok(())
    }
}

pub fn validate_parts(input: &TransferInput, parts: &[PartReceipt]) -> Result<(), ProtocolError> {
    input.validate()?;
    if parts.len() != input.part_count() || parts.len() > MAX_PARTS {
        return Err(ProtocolError::invalid("transfer.parts.count"));
    }
    for (offset, part) in parts.iter().enumerate() {
        let expected_bytes =
            (input.object.bytes.get() - offset as u64 * PART_BYTES).min(PART_BYTES);
        if usize::from(part.index) != offset + 1 || part.bytes.get() != expected_bytes {
            return Err(ProtocolError::invalid("transfer.parts.layout"));
        }
    }
    Ok(())
}

mod api;
pub use api::*;
