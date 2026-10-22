//! One authoritative head per native backup stream; provider listings are not publication evidence.
use crate::transfer::ObjectReceipt;
use crate::{
    BackupStream, DecimalU64, Digest, Id, NativeRoot, ObjectKind, ProtocolError, Publisher,
    RecoveryDescriptor, ReferenceRoot, Text, Version,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeadIdentity {
    pub commit_id: Id,
    pub head_sha256: Digest,
    pub etag: Text<256>,
    pub ordinal: DecimalU64,
}

impl HeadIdentity {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.ordinal.get() == 0 {
            return Err(ProtocolError::invalid("head.ordinal"));
        }
        Ok(())
    }
}

/// The exact client request survives retries. A changed predecessor requires a new commit identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitInput {
    pub version: Version,
    pub commit_id: Id,
    pub stream: BackupStream,
    pub publisher: Publisher,
    pub native_root: NativeRoot,
    pub recovery: RecoveryDescriptor,
    pub reference_root: ReferenceRoot,
    pub encrypted_manifest: ObjectReceipt,
    pub predecessor: Option<HeadIdentity>,
}

impl CommitInput {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.stream.validate()?;
        self.publisher.validate()?;
        self.native_root.validate()?;
        self.recovery.validate()?;
        self.reference_root.validate()?;
        self.encrypted_manifest.validate()?;
        if self.encrypted_manifest.scope != self.stream.scope
            || self.encrypted_manifest.object.kind != ObjectKind::ManifestIndex
            || self.recovery.scope != self.stream.scope
            || self.native_root.home_id != self.stream.source_home
            || self.recovery.source_home != self.stream.source_home
            || self.recovery.required_native_features != self.native_root.required_features
        {
            return Err(ProtocolError::invalid("commit.scope_root_recovery"));
        }
        if let Some(predecessor) = &self.predecessor {
            predecessor.validate()?;
            if predecessor.commit_id == self.commit_id || predecessor.ordinal.get() == u64::MAX {
                return Err(ProtocolError::invalid("commit.predecessor"));
            }
        }
        Ok(())
    }

    pub fn ordinal(&self) -> Result<u64, ProtocolError> {
        self.predecessor.as_ref().map_or(Ok(1), |head| {
            head.ordinal
                .get()
                .checked_add(1)
                .ok_or_else(|| ProtocolError::invalid("commit.ordinal"))
        })
    }

    pub fn input_sha256(&self) -> Result<Digest, ProtocolError> {
        crate::input_digest("lastdb:native-backup:commit:v1", self)
    }

    pub fn validate_initial(&self) -> Result<(), ProtocolError> {
        self.validate()?;
        if self.predecessor.is_some() {
            return Err(ProtocolError::invalid("commit.initial.predecessor"));
        }
        Ok(())
    }

    pub fn validate_successor(&self, predecessor: &PublishedHead) -> Result<(), ProtocolError> {
        self.validate()?;
        predecessor.validate()?;
        let prior = &predecessor.candidate.input;
        if self.predecessor.as_ref() != Some(&predecessor.identity)
            || self.stream != prior.stream
            || self.native_root.home_digest != prior.native_root.home_digest
            || self.native_root.generation < prior.native_root.generation
            || self.native_root.frontier < prior.native_root.frontier
            || (self.native_root.generation == prior.native_root.generation
                && self.native_root != prior.native_root)
        {
            return Err(ProtocolError::invalid("commit.successor"));
        }
        Ok(())
    }
}

/// Hash the RFC 8785 bytes of this complete value. There is no volatile timestamp in the head.
/// The service stores these exact candidate bytes before the conditional head PUT.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeadCandidate {
    pub version: Version,
    pub input: CommitInput,
    pub input_sha256: Digest,
    pub ordinal: DecimalU64,
}

impl HeadCandidate {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.input.validate()?;
        if self.ordinal.get() != self.input.ordinal()?
            || self.input_sha256 != self.input.input_sha256()?
        {
            return Err(ProtocolError::invalid("head.candidate.ordinal"));
        }
        crate::limits::encoded_json_bytes(self, crate::limits::HEAD_JSON_MAX_BYTES).map(|_| ())
    }

    pub fn head_sha256(&self) -> Result<Digest, ProtocolError> {
        Ok(crate::bytes_sha256(&crate::canonical_bytes(self)?))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitPhase {
    Prepared,
    ReferencesVerified,
    WritingHead,
    HeadUncertain,
    HeadConfirmed,
    Confirmed,
    Rejected,
}

impl CommitPhase {
    /// A head observation must match the complete saved candidate, never just a higher ordinal.
    pub fn can_transition_to(self, next: Self) -> bool {
        self == next
            || (next == Self::Rejected && !matches!(self, Self::Confirmed | Self::Rejected))
            || matches!(
                (self, next),
                (Self::Prepared, Self::ReferencesVerified)
                    | (Self::ReferencesVerified, Self::WritingHead)
                    | (Self::WritingHead, Self::HeadUncertain | Self::HeadConfirmed)
                    | (Self::HeadUncertain, Self::HeadConfirmed)
                    | (Self::HeadConfirmed, Self::Confirmed)
            )
    }
}

/// Confirmation includes the separately admitted encrypted manifest index receipt.
/// The reference root covers content and manifest pages, and excludes that index itself.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitReceipt {
    pub version: Version,
    pub stream: BackupStream,
    pub input_sha256: Digest,
    pub head: HeadIdentity,
    pub reference_root: ReferenceRoot,
    pub encrypted_manifest_receipt_id: Digest,
    pub confirmed_ms: DecimalU64,
}

impl CommitReceipt {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.stream.validate()?;
        self.head.validate()?;
        self.reference_root.validate()?;
        crate::limits::encoded_json_bytes(self, crate::limits::RECEIPT_JSON_MAX_BYTES).map(|_| ())
    }

    pub fn validate_for(
        &self,
        candidate: &HeadCandidate,
        head: &HeadIdentity,
    ) -> Result<(), ProtocolError> {
        self.validate()?;
        candidate.validate()?;
        head.validate()?;
        if self.stream != candidate.input.stream
            || self.input_sha256 != candidate.input_sha256
            || &self.head != head
            || head.commit_id != candidate.input.commit_id
            || head.ordinal != candidate.ordinal
            || head.head_sha256 != candidate.head_sha256()?
            || self.reference_root != candidate.input.reference_root
            || self.encrypted_manifest_receipt_id != candidate.input.encrypted_manifest.receipt_id
        {
            return Err(ProtocolError::invalid("commit.receipt.identity"));
        }
        Ok(())
    }
}

/// Save the candidate and the exact head-effect identity before the provider request.
/// Final confirmation and exact admission cleanup share one control-store transaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitRecord {
    pub candidate: HeadCandidate,
    pub head_sha256: Digest,
    pub revision: DecimalU64,
    pub phase: CommitPhase,
    pub head_effect: Id,
    pub confirmed_head: Option<HeadIdentity>,
    pub receipt: Option<CommitReceipt>,
    /// Only a conclusive no-publication outcome permits rejection; timeout is not evidence.
    pub failure: Option<ProtocolError>,
}

impl CommitRecord {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.candidate.validate()?;
        if self.revision.get() == 0 || self.head_sha256 != self.candidate.head_sha256()? {
            return Err(ProtocolError::invalid("commit.head_sha256"));
        }
        crate::limits::encoded_json_bytes(self, crate::limits::SESSION_JSON_MAX_BYTES)?;
        if self.phase == CommitPhase::Rejected {
            if self.failure.is_none() || self.confirmed_head.is_some() || self.receipt.is_some() {
                return Err(ProtocolError::invalid("commit.rejection"));
            }
            return Ok(());
        }
        if self.failure.is_some() {
            return Err(ProtocolError::invalid("commit.failure"));
        }
        let confirmed = matches!(
            self.phase,
            CommitPhase::HeadConfirmed | CommitPhase::Confirmed
        );
        if confirmed != self.confirmed_head.is_some() {
            return Err(ProtocolError::invalid("commit.confirmed_head"));
        }
        if let Some(head) = &self.confirmed_head {
            head.validate()?;
            if head.commit_id != self.candidate.input.commit_id
                || head.head_sha256 != self.head_sha256
                || head.ordinal != self.candidate.ordinal
            {
                return Err(ProtocolError::invalid("commit.confirmed_head.identity"));
            }
        }
        match (&self.receipt, self.phase) {
            (Some(receipt), CommitPhase::Confirmed) => self.validate_receipt(receipt)?,
            (None, phase) if phase != CommitPhase::Confirmed => {}
            _ => return Err(ProtocolError::invalid("commit.receipt")),
        }
        Ok(())
    }

    /// A provider timeout does not permit a new predecessor, candidate, or effect identity.
    pub fn validate_transition(&self, next: &Self) -> Result<(), ProtocolError> {
        self.validate()?;
        next.validate()?;
        if self == next {
            return Ok(());
        }
        if self.candidate != next.candidate
            || self.head_sha256 != next.head_sha256
            || self.head_effect != next.head_effect
            || self.revision.get().checked_add(1) != Some(next.revision.get())
            || !self.phase.can_transition_to(next.phase)
            || matches!(self.phase, CommitPhase::Confirmed | CommitPhase::Rejected)
            || (self.confirmed_head.is_some() && self.confirmed_head != next.confirmed_head)
            || (self.receipt.is_some() && self.receipt != next.receipt)
        {
            return Err(ProtocolError::invalid("commit.transition"));
        }
        Ok(())
    }

    fn validate_receipt(&self, receipt: &CommitReceipt) -> Result<(), ProtocolError> {
        let head = self
            .confirmed_head
            .as_ref()
            .ok_or_else(|| ProtocolError::invalid("commit.confirmed_head"))?;
        receipt.validate_for(&self.candidate, head)
    }
}

mod api;
pub use api::*;
