use super::{error, LogHead, SealedObject, WriterStream, MAX_BATCH_FILES};
use crate::{input_digest, limits, DecimalU64, ErrorCode, Id, ProtocolError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Bind a file version in a portable record to its exact sealed object.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileDependency {
    /// Stream that authored the file version.
    pub source_stream: Id,
    /// Sequence of the mutation that authored the file version.
    pub sequence: DecimalU64,
    /// Exact immutable object containing this version's bytes.
    pub object: SealedObject,
}

impl FileDependency {
    fn position(&self) -> (&Id, DecimalU64) {
        (&self.source_stream, self.sequence)
    }
}

/// One object that the service must confirm before accepting a new batch.
pub enum BatchObject<'a> {
    /// The encrypted log range itself.
    Segment(&'a SealedObject),
    /// A logical file version and its exact stored object.
    File(&'a FileDependency),
}

/// Append metadata for one immutable encrypted log range.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogBatch {
    /// Database and origin stream.
    pub stream: WriterStream,
    /// Exclusive start with its accepted prefix commitment, absent at genesis.
    pub after: Option<LogHead>,
    /// Inclusive end, strictly greater than the exclusive start.
    pub through: DecimalU64,
    /// Sealed portable log file for this range.
    pub segment: SealedObject,
    /// Exact sealed file versions referenced, sorted by unique origin positions.
    pub file_versions: Vec<FileDependency>,
}

impl LogBatch {
    /// Validate the range and object metadata under the control-message bound.
    /// The transport must bound raw input before decoding.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if let Some(after) = &self.after {
            after.validate()?;
        }
        if self.through.get() <= self.after.as_ref().map_or(0, |head| head.sequence.get()) {
            return Err(ProtocolError::invalid("shared.batch.range"));
        }
        self.segment.validate()?;
        if self.file_versions.len() > MAX_BATCH_FILES {
            return Err(ProtocolError::limit("shared.batch.file_versions"));
        }
        self.validate_file_versions()?;
        limits::encoded_json_bytes(self, limits::REQUEST_JSON_MAX_BYTES)?;
        Ok(())
    }

    fn validate_file_versions(&self) -> Result<(), ProtocolError> {
        if self
            .file_versions
            .windows(2)
            .any(|pair| pair[0].position() >= pair[1].position())
        {
            return Err(ProtocolError::invalid("shared.batch.file_order"));
        }
        let mut objects = BTreeSet::from([&self.segment.version_id]);
        for file in &self.file_versions {
            if file.sequence.get() == 0
                || (file.source_stream == self.stream.source_stream && file.sequence > self.through)
                || !objects.insert(&file.object.version_id)
            {
                return Err(ProtocolError::invalid("shared.batch.file_version"));
            }
            file.object.validate()?;
        }
        Ok(())
    }

    /// Compute the candidate next head from the exact batch input and predecessor.
    pub fn head(&self) -> Result<LogHead, ProtocolError> {
        self.validate()?;
        Ok(LogHead {
            sequence: self.through,
            prefix_sha256: input_digest("lastdb:shared-log:batch:v1", self)?,
        })
    }
}

/// Durable receipt for one immutable accepted batch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchReceipt {
    /// Scope of the accepted batch.
    pub stream: WriterStream,
    /// Position and commitment of the accepted batch.
    pub head: LogHead,
}

impl BatchReceipt {
    /// Match a service receipt to a client's saved input after an uncertain reply.
    pub fn validate_for(&self, batch: &LogBatch) -> Result<(), ProtocolError> {
        if self.stream != batch.stream || self.head != batch.head()? {
            return Err(error(ErrorCode::IdentityConflict, "shared.batch.receipt"));
        }
        Ok(())
    }
}

/// Append checks use service-owned facts read in the transaction that moves the head.
pub struct AppendContext<'a> {
    /// Stream from the service's authorized writer directory.
    pub stream: &'a WriterStream,
    /// Device recorded as this stream's current owner.
    pub owner_device: &'a Id,
    /// Device derived from the request's authenticated principal.
    pub authenticated_device: &'a Id,
    /// Durable current head, absent before the first accepted batch.
    pub current: Option<&'a LogHead>,
    /// Greatest reclaimed sequence, zero before cleanup.
    pub reclaimed_through: DecimalU64,
    /// Durable receipt lookup for this stream at the requested inclusive end.
    pub accepted_at_end: Option<&'a BatchReceipt>,
}

/// Append outcomes before the enclosing service transaction commits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AppendDecision {
    /// Write this receipt, pin exact object versions and advance the head atomically.
    Commit(BatchReceipt),
    /// Return the prior durable receipt for the identical request.
    AlreadyAccepted(BatchReceipt),
    /// The request is at or below the reclaimed floor and its receipt is absent.
    /// Return the durable head for reconciliation without accepting the request.
    Reconcile(LogHead),
}

impl AppendContext<'_> {
    /// Check authorization, receipt idempotency, chain continuation and durable objects.
    /// `confirm_object` reads trusted verified facts about exact versions in this database.
    /// For file versions, the verified registration must also match their origin position.
    pub fn decide(
        &self,
        batch: &LogBatch,
        mut confirm_object: impl FnMut(&WriterStream, BatchObject<'_>) -> Result<(), ProtocolError>,
    ) -> Result<AppendDecision, ProtocolError> {
        if self.stream != &batch.stream || self.authenticated_device != self.owner_device {
            return Err(error(ErrorCode::Unauthorized, "shared.batch.writer"));
        }
        let head = batch.head()?;
        let current_sequence = self.current.map_or(0, |current| current.sequence.get());
        if let Some(current) = self.current {
            current.validate()?;
        }
        if self.reclaimed_through.get() > current_sequence {
            return Err(error(ErrorCode::IntegrityMismatch, "shared.batch.floor"));
        }
        if let Some(accepted) = self.accepted_at_end {
            if accepted.stream != batch.stream
                || accepted.head.sequence != batch.through
                || batch.through.get() > current_sequence
                || (batch.through.get() == current_sequence && self.current != Some(&accepted.head))
            {
                return Err(error(ErrorCode::IntegrityMismatch, "shared.batch.accepted"));
            }
            if accepted.head != head {
                return Err(error(ErrorCode::IdentityConflict, "shared.batch.receipt"));
            }
            return Ok(AppendDecision::AlreadyAccepted(accepted.clone()));
        }
        if batch.through <= self.reclaimed_through {
            let current = self.current.ok_or_else(|| {
                error(ErrorCode::IntegrityMismatch, "shared.batch.reclaimed_head")
            })?;
            return Ok(AppendDecision::Reconcile(current.clone()));
        }
        if batch.after.as_ref() != self.current {
            return Err(error(ErrorCode::PredecessorConflict, "shared.batch.after"));
        }
        confirm_object(self.stream, BatchObject::Segment(&batch.segment))?;
        for file in &batch.file_versions {
            confirm_object(self.stream, BatchObject::File(file))?;
        }
        Ok(AppendDecision::Commit(BatchReceipt {
            stream: batch.stream.clone(),
            head,
        }))
    }
}
