//! Shared recovery control requests and signed exact-object grants.

use super::{error, LogHead, RecoveryView, SealedObject};
use crate::{input_digest, AccountId, DecimalU64, Digest, ErrorCode, Id, ProtocolError, Text};
use serde::{Deserialize, Serialize, Serializer};

/// Open a bounded recovery hold at the current shared snapshot and writer heads.
pub const RECOVERY_OPEN_PATH: &str = "/cloud/shared/v1/recovery/open";
/// Read the exact public manifest retained by a recovery hold.
pub const RECOVERY_MANIFEST_PATH: &str = "/cloud/shared/v1/recovery/manifest";
/// Read one exact continuation from a retained stream in a recovery hold.
pub const RECOVERY_LOG_PATH: &str = "/cloud/shared/v1/recovery/logs";
/// Issue a temporary signed GET for one exact held object version.
pub const RECOVERY_OBJECT_PATH: &str = "/cloud/shared/v1/recovery/objects";

/// Open a new recovery hold, or fetch the manifest for an existing hold.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryHoldRequest {
    /// Shared database of the selection.
    pub database_id: Id,
    /// Stable caller-chosen recovery hold identity.
    pub hold_id: Id,
}

/// Read the batch immediately after a verified prefix of one held stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryLogRequest {
    /// Shared database of the selection.
    pub database_id: Id,
    /// Exact retained recovery view.
    pub hold_id: Id,
    /// Stream whose next batch is requested.
    pub source_stream: Id,
    /// Previous verified batch end, absent at genesis.
    pub after: Option<LogHead>,
}

/// An exact parent that the service can verify without a reverse object scan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RecoveryObjectParent {
    /// This recovery view's accepted snapshot.
    Snapshot {
        /// Exact accepted publication.
        snapshot_id: Id,
    },
    /// One retained accepted batch in a captured writer's suffix.
    LogBatch {
        /// Origin of the accepted batch.
        source_stream: Id,
        /// Exact accepted end and prefix commitment.
        head: LogHead,
    },
}

/// Read one exact sealed object from a held snapshot or uncovered log.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryObjectRequest {
    /// Shared database of the selection.
    pub database_id: Id,
    /// Exact retained recovery view.
    pub hold_id: Id,
    /// Exact expected sealed object metadata.
    pub object: SealedObject,
    /// Held snapshot or accepted batch declaring this object.
    pub parent: RecoveryObjectParent,
}

impl RecoveryObjectRequest {
    /// Check that the claimed parent lies in this recovery view's capture.
    /// The service checks the retained parent and its actual object references.
    pub fn validate_for(&self, view: &RecoveryView) -> Result<(), ProtocolError> {
        self.object.validate()?;
        let mismatch = || {
            error(
                ErrorCode::IntegrityMismatch,
                "shared.recovery.object_parent",
            )
        };
        if self.database_id != view.snapshot.database_id || self.hold_id != view.hold_id {
            return Err(mismatch());
        }
        match &self.parent {
            RecoveryObjectParent::Snapshot { snapshot_id } => {
                if snapshot_id != &view.snapshot.snapshot_id {
                    return Err(mismatch());
                }
            }
            RecoveryObjectParent::LogBatch {
                source_stream,
                head,
            } => {
                head.validate()?;
                let index = view
                    .streams
                    .binary_search_by(|cut| cut.source_stream.cmp(source_stream))
                    .map_err(|_| mismatch())?;
                let cut = &view.streams[index];
                let captured = cut.head.as_ref().ok_or_else(mismatch)?;
                if head.sequence <= cut.reclaimed_through
                    || head.sequence > captured.sequence
                    || (head.sequence == captured.sequence && head != captured)
                {
                    return Err(mismatch());
                }
            }
        }
        Ok(())
    }
}

/// A provider GET authorized for an exact object version until a fixed time.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SharedReadGrant {
    /// Object metadata, checked before use and again against the response body.
    pub object: SealedObject,
    /// Grant expiry in UTC milliseconds.
    pub expires_at_ms: DecimalU64,
    /// Opaque HTTPS GET URL. Keep query credentials out of logs.
    pub url: Text<8192>,
}

impl SharedReadGrant {
    /// Check exact identity and expiry against the authenticated held selection.
    pub fn validate_for(
        &self,
        request: &RecoveryObjectRequest,
        view: &RecoveryView,
        now: DecimalU64,
    ) -> Result<(), ProtocolError> {
        request.validate_for(view)?;
        if self.object != request.object || self.expires_at_ms > view.expires_at_ms {
            return Err(error(ErrorCode::IntegrityMismatch, "shared.recovery.grant"));
        }
        if self.expires_at_ms <= now {
            return Err(error(
                ErrorCode::SourceUnavailable,
                "shared.recovery.expired",
            ));
        }
        Ok(())
    }
}

/// Bind each recovery HTTP operation to a distinct device signature domain.
#[derive(Clone, Copy)]
pub enum RecoveryAuthorityRequest<'a> {
    /// Capture the current snapshot and accepted heads.
    Open(&'a RecoveryHoldRequest),
    /// Read the held public manifest.
    Manifest(&'a RecoveryHoldRequest),
    /// Read one uncovered batch.
    Log(&'a RecoveryLogRequest),
    /// Read one held object version.
    Object(&'a RecoveryObjectRequest),
}

impl RecoveryAuthorityRequest<'_> {
    /// The authenticated service route for this request.
    #[must_use]
    pub fn path(self) -> &'static str {
        match self {
            Self::Open(_) => RECOVERY_OPEN_PATH,
            Self::Manifest(_) => RECOVERY_MANIFEST_PATH,
            Self::Log(_) => RECOVERY_LOG_PATH,
            Self::Object(_) => RECOVERY_OBJECT_PATH,
        }
    }

    /// Bind the exact operation, body, account, device, and proof expiry.
    pub fn authority_digest(
        self,
        account: &AccountId,
        device: &Id,
        expires_at_ms: DecimalU64,
    ) -> Result<Digest, ProtocolError> {
        let domain = match self {
            Self::Open(_) => "lastdb:shared-log:recovery-open-authority:v1",
            Self::Manifest(_) => "lastdb:shared-log:recovery-manifest-authority:v1",
            Self::Log(request) => {
                if let Some(after) = &request.after {
                    after.validate()?;
                }
                "lastdb:shared-log:recovery-log-authority:v1"
            }
            Self::Object(request) => {
                request.object.validate()?;
                if let RecoveryObjectParent::LogBatch { head, .. } = &request.parent {
                    head.validate()?;
                }
                "lastdb:shared-log:recovery-object-authority:v1"
            }
        };
        input_digest(domain, &(account, device, expires_at_ms, self))
    }
}

impl Serialize for RecoveryAuthorityRequest<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Open(request) | Self::Manifest(request) => request.serialize(serializer),
            Self::Log(request) => request.serialize(serializer),
            Self::Object(request) => request.serialize(serializer),
        }
    }
}
