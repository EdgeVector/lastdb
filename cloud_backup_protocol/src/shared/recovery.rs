use super::{error, LogBatch, LogHead, SnapshotManifest, SnapshotReceipt, MAX_WRITERS};
use crate::{limits, DecimalU64, ErrorCode, Id, ProtocolError};
use serde::{Deserialize, Serialize};

/// The retained and published boundaries of a stream at a recovery cut.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamCut {
    /// Origin stream inside the view's database.
    pub source_stream: Id,
    /// Greatest reclaimed sequence, zero before cleanup.
    pub reclaimed_through: DecimalU64,
    /// Captured inclusive head, absent for a stream without an accepted batch.
    pub head: Option<LogHead>,
}

impl StreamCut {
    fn validate_start(&self, cover: Option<&LogHead>) -> Result<(), ProtocolError> {
        if let Some(head) = &self.head {
            head.validate()?;
        }
        let through = self.head.as_ref().map_or(0, |head| head.sequence.get());
        let from = cover.map_or(0, |head| head.sequence.get());
        if from > through || (from == through && cover != self.head.as_ref()) {
            return Err(error(ErrorCode::IntegrityMismatch, "shared.recovery.head"));
        }
        if self.reclaimed_through.get() > from {
            return Err(error(
                ErrorCode::SourceUnavailable,
                "shared.recovery.reclaimed",
            ));
        }
        Ok(())
    }

    /// Verify one continuation from the last verified prefix of this held tail.
    /// Start at the matching manifest cover, or at genesis for an uncovered stream.
    /// The caller separately verifies stored bytes and decoded log entries.
    pub fn validate_next(
        &self,
        database_id: &Id,
        previous: Option<&LogHead>,
        batch: &LogBatch,
    ) -> Result<LogHead, ProtocolError> {
        self.validate_start(previous)?;
        if &batch.stream.database_id != database_id
            || batch.stream.source_stream != self.source_stream
            || batch.after.as_ref() != previous
        {
            return Err(error(ErrorCode::IntegrityMismatch, "shared.recovery.batch"));
        }
        let captured = self
            .head
            .as_ref()
            .ok_or_else(|| error(ErrorCode::IntegrityMismatch, "shared.recovery.empty_stream"))?;
        let next = batch.head()?;
        if next.sequence > captured.sequence
            || (next.sequence == captured.sequence && &next != captured)
        {
            return Err(error(ErrorCode::IntegrityMismatch, "shared.recovery.head"));
        }
        Ok(next)
    }
}

/// Snapshot and writer directory captured together under a service recovery hold.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryView {
    /// Service-issued id for the hold on this exact recovery view.
    pub hold_id: Id,
    /// Hold expiration in UTC milliseconds.
    pub expires_at_ms: DecimalU64,
    /// Accepted snapshot identity and manifest commitment.
    pub snapshot: SnapshotReceipt,
    /// Captured writer cuts, sorted by unique source stream id.
    pub streams: Vec<StreamCut>,
}

impl RecoveryView {
    /// Verify the held snapshot, captured writer directory and recoverable starts.
    /// A live hold and complete tail coverage must also be enforced by the service.
    pub fn validate_for(
        &self,
        manifest: &SnapshotManifest,
        now_ms: DecimalU64,
    ) -> Result<(), ProtocolError> {
        if self.expires_at_ms <= now_ms {
            return Err(error(
                ErrorCode::SourceUnavailable,
                "shared.recovery.expired",
            ));
        }
        if self.streams.len() > MAX_WRITERS {
            return Err(ProtocolError::limit("shared.recovery.streams"));
        }
        if self
            .streams
            .windows(2)
            .any(|pair| pair[0].source_stream >= pair[1].source_stream)
            || self.snapshot != manifest.receipt()?
        {
            return Err(error(ErrorCode::IntegrityMismatch, "shared.recovery.view"));
        }
        for cover in &manifest.covers {
            if self
                .streams
                .binary_search_by(|stream| stream.source_stream.cmp(&cover.source_stream))
                .is_err()
            {
                return Err(error(
                    ErrorCode::IntegrityMismatch,
                    "shared.recovery.missing_stream",
                ));
            }
        }
        for stream in &self.streams {
            stream.validate_start(manifest.cover(&stream.source_stream))?;
        }
        limits::encoded_json_bytes(self, limits::REQUEST_JSON_MAX_BYTES)?;
        Ok(())
    }
}
