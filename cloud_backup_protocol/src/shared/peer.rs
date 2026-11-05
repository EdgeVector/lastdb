//! Signed reads of retained accepted logs by a participating writer device.

use super::{error, LogBatch, LogHead, SharedReadGrant, StreamCut, WriterStream, MAX_WRITERS};
use crate::{input_digest, AccountId, DecimalU64, Digest, ErrorCode, Id, ProtocolError};
use serde::{Deserialize, Serialize, Serializer};

/// Capture the peer stream directory and its current accepted boundaries.
pub const HEADS_PATH: &str = "/cloud/shared/v1/peers/heads";
/// Read one continuation from a retained accepted stream.
pub const NEXT_PATH: &str = "/cloud/shared/v1/peers/next";
/// Get a temporary exact read for an accepted batch's file dependency.
pub const FILE_PATH: &str = "/cloud/shared/v1/peers/file";
/// Get a temporary exact read for one accepted encrypted log segment.
pub const SEGMENT_PATH: &str = "/cloud/shared/v1/peers/segment";

/// Capture peer stream heads for an admitted reader stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerHeadsRequest {
    /// Participating stream owned by the authenticated device.
    pub reader: WriterStream,
}

/// The accepted peer directory captured by the shared service.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerHeads {
    /// Exact scope used for the selection.
    pub reader: WriterStream,
    /// Sorted distinct peer streams, excluding the reader.
    pub streams: Vec<StreamCut>,
}

impl PeerHeads {
    /// Verify reader identity and every peer head and retention boundary.
    pub fn validate_for(&self, request: &PeerHeadsRequest) -> Result<(), ProtocolError> {
        if self.reader != request.reader
            || self.streams.len() >= MAX_WRITERS
            || self
                .streams
                .windows(2)
                .any(|pair| pair[0].source_stream >= pair[1].source_stream)
        {
            return Err(error(ErrorCode::IntegrityMismatch, "shared.peer.directory"));
        }
        for stream in &self.streams {
            let through = stream
                .head
                .as_ref()
                .map_or_else(DecimalU64::default, |head| head.sequence);
            if stream.source_stream == self.reader.source_stream
                || stream.reclaimed_through > through
            {
                return Err(error(ErrorCode::IntegrityMismatch, "shared.peer.stream"));
            }
            if let Some(head) = &stream.head {
                head.validate()?;
            }
        }
        Ok(())
    }
}

/// Read the first retained batch after a saved applied sequence.
/// The service resolves the accepted predecessor at that exact boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerNextRequest {
    /// Participating stream owned by the authenticated device.
    pub reader: WriterStream,
    /// Peer stream whose accepted range is requested.
    pub source_stream: Id,
    /// Previously applied sequence, or zero at genesis.
    pub after_sequence: DecimalU64,
    /// Inclusive upper bound from the accepted directory capture.
    pub through: LogHead,
}

impl PeerNextRequest {
    /// Verify a reply starts at the saved boundary and fits the captured head.
    pub fn validate_batch(&self, batch: &LogBatch) -> Result<(), ProtocolError> {
        self.validate()?;
        let after = batch
            .after
            .as_ref()
            .map_or_else(DecimalU64::default, |head| head.sequence);
        if after != self.after_sequence {
            return Err(error(
                ErrorCode::IntegrityMismatch,
                "shared.peer.predecessor",
            ));
        }
        let cut = StreamCut {
            source_stream: self.source_stream.clone(),
            reclaimed_through: DecimalU64::default(),
            head: Some(self.through.clone()),
        };
        cut.validate_next(&self.reader.database_id, batch.after.as_ref(), batch)?;
        Ok(())
    }

    fn validate(&self) -> Result<(), ProtocolError> {
        other_stream(&self.reader, &self.source_stream)?;
        self.through.validate()?;
        if self.after_sequence >= self.through.sequence {
            return Err(ProtocolError::invalid("shared.peer.sequence"));
        }
        Ok(())
    }
}

/// Request one immutable segment from its exact retained accepted batch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerSegmentRequest {
    /// Participating stream owned by the authenticated device.
    pub reader: WriterStream,
    /// Peer stream whose accepted batch declares the segment.
    pub source_stream: Id,
    /// Exact accepted end and prefix commitment.
    pub head: LogHead,
}

/// Resolve a winning record's retained file through the batch that contains
/// the referring mutation. The file itself may originate in another stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerFileRequest {
    /// Participating stream owned by the authenticated device.
    pub reader: WriterStream,
    /// Peer stream with the referring mutation.
    pub source_stream: Id,
    /// Sequence of the referring mutation.
    pub sequence: DecimalU64,
    /// Origin of the exact file version.
    pub file_stream: Id,
    /// Sequence of the exact file version.
    pub file_sequence: DecimalU64,
}

/// Bind each peer read to its exact device authority domain.
#[derive(Clone, Copy)]
pub enum PeerAuthorityRequest<'a> {
    /// Capture the peer stream directory.
    Heads(&'a PeerHeadsRequest),
    /// Get one exact accepted continuation.
    Next(&'a PeerNextRequest),
    /// Get a temporary read of a retained file.
    File(&'a PeerFileRequest),
    /// Get a temporary read of a retained segment.
    Segment(&'a PeerSegmentRequest),
}

impl PeerAuthorityRequest<'_> {
    /// The protected service route for this operation.
    #[must_use]
    pub fn path(self) -> &'static str {
        match self {
            Self::Heads(_) => HEADS_PATH,
            Self::Next(_) => NEXT_PATH,
            Self::File(_) => FILE_PATH,
            Self::Segment(_) => SEGMENT_PATH,
        }
    }

    /// Bind the operation, exact request, account, device, and proof expiry.
    pub fn authority_digest(
        self,
        account: &AccountId,
        device: &Id,
        expires_at_ms: DecimalU64,
    ) -> Result<Digest, ProtocolError> {
        let domain = match self {
            Self::Heads(_) => "lastdb:shared-log:peer-heads-authority:v1",
            Self::Next(request) => {
                request.validate()?;
                "lastdb:shared-log:peer-next-authority:v1"
            }
            Self::File(request) => {
                other_stream(&request.reader, &request.source_stream)?;
                if request.sequence.get() == 0 || request.file_sequence.get() == 0 {
                    return Err(ProtocolError::invalid("shared.peer.file_position"));
                }
                "lastdb:shared-log:peer-file-authority:v1"
            }
            Self::Segment(request) => {
                other_stream(&request.reader, &request.source_stream)?;
                request.head.validate()?;
                "lastdb:shared-log:peer-segment-authority:v1"
            }
        };
        input_digest(domain, &(account, device, expires_at_ms, self))
    }
}

impl Serialize for PeerAuthorityRequest<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Heads(request) => request.serialize(serializer),
            Self::Next(request) => request.serialize(serializer),
            Self::File(request) => request.serialize(serializer),
            Self::Segment(request) => request.serialize(serializer),
        }
    }
}

/// Check the temporary GET before admitting any response bytes.
pub fn validate_peer_grant(
    grant: &SharedReadGrant,
    now_ms: DecimalU64,
    proof_expires: DecimalU64,
) -> Result<(), ProtocolError> {
    grant.object.validate()?;
    if grant.expires_at_ms <= now_ms || grant.expires_at_ms > proof_expires {
        return Err(error(
            ErrorCode::SourceUnavailable,
            "shared.peer.grant_expired",
        ));
    }
    Ok(())
}

fn other_stream(reader: &WriterStream, source_stream: &Id) -> Result<(), ProtocolError> {
    if &reader.source_stream == source_stream {
        return Err(ProtocolError::invalid("shared.peer.source_stream"));
    }
    Ok(())
}
