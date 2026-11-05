//! Signed controls for exact shared provider objects and snapshot inventories.
use super::{
    error, validate_object_list, LogHead, SealedObject, SnapshotManifest, StreamCover, MAX_WRITERS,
};
use crate::{
    input_digest, limits, AccountId, DecimalU64, Digest, ErrorCode, Id, ProtocolError, Text,
};
use serde::{Deserialize, Serialize, Serializer};

/// Upload authority for the exact object and its logical role.
pub const PREPARE_PATH: &str = "/cloud/shared/v1/objects/prepare";
/// Advance stored-byte verification for the exact submitted object.
pub const COMPLETE_PATH: &str = "/cloud/shared/v1/objects/complete";
/// Bind one ordered inventory page to exact verified objects.
pub const INVENTORY_PAGE_PATH: &str = "/cloud/shared/v1/snapshots/inventory/page";
/// Read the publisher's current snapshot head.
pub const SNAPSHOT_HEAD_PATH: &str = "/cloud/shared/v1/snapshots/head";
/// Resolve exact accepted log ends for a publisher's local checkpoint.
pub const SNAPSHOT_LOG_ENDS_PATH: &str = "/cloud/shared/v1/snapshots/log-ends";
/// Conditionally publish the verified snapshot and covers.
pub const SNAPSHOT_PUBLISH_PATH: &str = "/cloud/shared/v1/snapshots/publish";

/// Exact object references per inventory page, except the final page.
pub const INVENTORY_PAGE_ENTRIES: usize = 512;

/// Bind stored bytes to one logical role in their database.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SharedObjectRole {
    /// Sealed numbered log range.
    Segment {
        /// Origin stream.
        source_stream: Id,
        /// Exclusive predecessor.
        after: Option<LogHead>,
        /// Inclusive end of the segment.
        through: DecimalU64,
    },
    /// File version retained by a record.
    File {
        /// Stream that authored the file version.
        source_stream: Id,
        /// Sequence of the file version.
        sequence: DecimalU64,
    },
    /// An encrypted snapshot manifest or native snapshot file.
    Snapshot {
        /// Publication whose inventory declares the object.
        snapshot_id: Id,
    },
}

/// Prepare or verify one exact immutable shared object.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SharedObjectRequest {
    /// Database that authorizes the object.
    pub database_id: Id,
    /// Exact expected encrypted bytes.
    pub object: SealedObject,
    /// Purpose and immutable dependency identity of the object.
    pub role: SharedObjectRole,
}

impl SharedObjectRequest {
    /// Validate the exact object and any numbered position.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.object.validate()?;
        match &self.role {
            SharedObjectRole::Segment { after, through, .. } => {
                if let Some(after) = after {
                    after.validate()?;
                }
                if through.get() <= after.as_ref().map_or(0, |head| head.sequence.get()) {
                    return Err(ProtocolError::invalid("shared.object.segment"));
                }
            }
            SharedObjectRole::File { sequence, .. } if sequence.get() == 0 => {
                return Err(ProtocolError::invalid("shared.object.file"));
            }
            _ => {}
        }
        Ok(())
    }
}

/// One temporary create-once PUT grant, bound to exact sealed bytes.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SharedUploadGrant {
    /// Object bytes expected by the service.
    pub object: SealedObject,
    /// Expiry in UTC milliseconds.
    pub expires_at_ms: DecimalU64,
    /// Opaque signed HTTPS URL. Keep query credentials out of logs.
    pub url: Text<8192>,
}

impl SharedUploadGrant {
    /// Require the exact requested object and a live grant.
    pub fn validate_for(
        &self,
        request: &SharedObjectRequest,
        now: DecimalU64,
    ) -> Result<(), ProtocolError> {
        request.validate()?;
        if self.object != request.object {
            return Err(error(ErrorCode::IntegrityMismatch, "shared.upload.grant"));
        }
        if self.expires_at_ms <= now {
            return Err(error(ErrorCode::SourceUnavailable, "shared.upload.expired"));
        }
        Ok(())
    }
}

/// Preparing an upload returns a PUT grant or the prior verified receipt.
#[derive(Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum SharedObjectPrepare {
    /// Upload is required or can safely be retried.
    Upload {
        /// Exact create-once provider request.
        grant: SharedUploadGrant,
    },
    /// The service already verified the exact object.
    Verified {
        /// Durable encrypted bytes.
        object: SealedObject,
    },
}

/// Verification advances under a fixed bound until the object is durable.
#[derive(Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum SharedObjectStatus {
    /// More verification remains. Retry the exact request.
    Pending {
        /// Number of verified bytes in the durable hash state.
        verified_through: DecimalU64,
    },
    /// The service verified the complete exact bytes.
    Verified {
        /// Durable encrypted bytes.
        object: SealedObject,
    },
}

/// One ordered page of exact snapshot object references.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotInventoryPage {
    /// Database that owns the snapshot.
    pub database_id: Id,
    /// Snapshot being prepared.
    pub snapshot_id: Id,
    /// Zero-based page ordinal.
    pub page_index: u16,
    /// Exact objects sorted by version id. The manifest binds the complete inventory.
    pub objects: Vec<SealedObject>,
}

impl SnapshotInventoryPage {
    /// Check per-page shape and bounds.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.objects.is_empty()
            || self.objects.len() > INVENTORY_PAGE_ENTRIES
            || usize::from(self.page_index)
                >= limits::NATIVE_FILES_MAX.div_ceil(INVENTORY_PAGE_ENTRIES)
        {
            return Err(ProtocolError::limit("shared.snapshot.inventory_page"));
        }
        validate_object_list(&self.objects)?;
        limits::encoded_json_bytes(self, limits::REQUEST_JSON_MAX_BYTES)?;
        Ok(())
    }
}

/// Read the current snapshot identity before preparing a replacement.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotHeadRequest {
    /// Database to read.
    pub database_id: Id,
}

/// A positive local applied sequence in one writer stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotLogPosition {
    /// Origin stream in the requested database.
    pub source_stream: Id,
    /// Exact inclusive end of an accepted log batch.
    pub sequence: DecimalU64,
}

/// Resolve the accepted commitments at the exact positions of a checkpoint cut.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotLogEndsRequest {
    /// Database for which the authenticated device is the publisher.
    pub database_id: Id,
    /// One position per stream, ordered by stream ID. Genesis can be empty.
    pub positions: Vec<SnapshotLogPosition>,
}

impl SnapshotLogEndsRequest {
    /// Require a bounded canonical list of positive, unique stream positions.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.positions.len() > MAX_WRITERS {
            return Err(ProtocolError::limit("shared.snapshot.log_ends"));
        }
        if self
            .positions
            .iter()
            .any(|position| position.sequence.get() == 0)
            || self
                .positions
                .windows(2)
                .any(|pair| pair[0].source_stream >= pair[1].source_stream)
        {
            return Err(ProtocolError::invalid("shared.snapshot.log_end_positions"));
        }
        Ok(())
    }
}

/// Exact accepted covers in the same order as the request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotLogEndsReply {
    /// Database whose accepted log prefixes were read.
    pub database_id: Id,
    /// Every requested batch end and its accepted commitment.
    pub covers: Vec<StreamCover>,
}

impl SnapshotLogEndsReply {
    /// Reject a partial, substituted or reordered response.
    pub fn validate_for(&self, request: &SnapshotLogEndsRequest) -> Result<(), ProtocolError> {
        request.validate()?;
        if self.database_id != request.database_id
            || self.covers.len() != request.positions.len()
            || self
                .covers
                .iter()
                .zip(&request.positions)
                .any(|(cover, position)| {
                    cover.source_stream != position.source_stream
                        || cover.through.sequence != position.sequence
                })
        {
            return Err(error(
                ErrorCode::IntegrityMismatch,
                "shared.snapshot.log_ends",
            ));
        }
        Ok(())
    }
}

/// Authenticate one exact provider or snapshot operation.
#[derive(Clone, Copy)]
pub enum ObjectAuthorityRequest<'a> {
    /// Prepare a create-once PUT.
    Prepare(&'a SharedObjectRequest),
    /// Verify bytes and persist an exact receipt.
    Complete(&'a SharedObjectRequest),
    /// Register an inventory page.
    InventoryPage(&'a SnapshotInventoryPage),
    /// Read the current publisher head.
    Head(&'a SnapshotHeadRequest),
    /// Read exact accepted log ends for the publisher's checkpoint positions.
    LogEnds(&'a SnapshotLogEndsRequest),
    /// Publish the exact manifest and cover map.
    Publish(&'a SnapshotManifest),
}

impl ObjectAuthorityRequest<'_> {
    /// Canonical route for the signed operation.
    #[must_use]
    pub fn path(self) -> &'static str {
        match self {
            Self::Prepare(_) => PREPARE_PATH,
            Self::Complete(_) => COMPLETE_PATH,
            Self::InventoryPage(_) => INVENTORY_PAGE_PATH,
            Self::Head(_) => SNAPSHOT_HEAD_PATH,
            Self::LogEnds(_) => SNAPSHOT_LOG_ENDS_PATH,
            Self::Publish(_) => SNAPSHOT_PUBLISH_PATH,
        }
    }

    /// Bind the device signature to this operation and the exact input.
    pub fn authority_digest(
        self,
        account: &AccountId,
        device: &Id,
        expires_at_ms: DecimalU64,
    ) -> Result<Digest, ProtocolError> {
        let domain = match self {
            Self::Prepare(request) => {
                request.validate()?;
                "lastdb:shared-log:object-prepare-authority:v1"
            }
            Self::Complete(request) => {
                request.validate()?;
                "lastdb:shared-log:object-complete-authority:v1"
            }
            Self::InventoryPage(page) => {
                page.validate()?;
                "lastdb:shared-log:inventory-page-authority:v1"
            }
            Self::Head(_) => "lastdb:shared-log:snapshot-head-authority:v1",
            Self::LogEnds(request) => {
                request.validate()?;
                "lastdb:shared-log:snapshot-log-ends-authority:v1"
            }
            Self::Publish(manifest) => {
                manifest.validate()?;
                "lastdb:shared-log:publish-authority:v1"
            }
        };
        input_digest(domain, &(account, device, expires_at_ms, self))
    }
}

impl Serialize for ObjectAuthorityRequest<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Prepare(request) | Self::Complete(request) => request.serialize(serializer),
            Self::InventoryPage(page) => page.serialize(serializer),
            Self::Head(request) => request.serialize(serializer),
            Self::LogEnds(request) => request.serialize(serializer),
            Self::Publish(manifest) => manifest.serialize(serializer),
        }
    }
}
