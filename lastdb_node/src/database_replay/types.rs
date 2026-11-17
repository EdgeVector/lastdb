//! Values and error classification used during mutation-log replay.

use cloud_backup_protocol::v2::blobs::BlobPath;
use cloud_backup_protocol::CloudScope;

use crate::host::{BlockedDelivery, ReplayBlockReason};

/// One sealed log entry ready for replay. `payload_b64` is unpadded URL-safe base64.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct SnapshotEntry {
    pub accepted_at_ms: u64,
    pub writer_key: String,
    pub writer_device: String,
    pub writer_seq: u64,
    pub authored_at_ms: u64,
    pub namespace: String,
    pub incorporated_from: Option<IncorporatedFrom>,
    pub payload_b64: String,
}

/// Writer identity an incorporated entry was copied from.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct IncorporatedFrom {
    pub writer_key: String,
    pub writer_device: String,
    pub writer_seq: u64,
}

/// File bytes fetched with `delivery/object`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotFileRequest {
    pub database_id: String,
    pub scope: CloudScope,
    pub path: BlobPath,
}

/// Static replay failure. The text is a class, never record bytes or a path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestoreError {
    Scope(&'static str),
    Entry(&'static str),
    File(&'static str),
    Conflict(&'static str),
    Durable(&'static str),
    Apply(&'static str),
    ReplayBlocked {
        reason: ReplayBlockReason,
        delivery_sequence: Option<u64>,
    },
}

impl RestoreError {
    pub(super) fn at_delivery(self, sequence: u64) -> Self {
        match self {
            Self::ReplayBlocked { reason, .. } => Self::ReplayBlocked {
                reason,
                delivery_sequence: Some(sequence),
            },
            _ => self,
        }
    }

    pub(crate) fn blocked_delivery(&self) -> Option<BlockedDelivery> {
        match *self {
            Self::ReplayBlocked {
                reason,
                delivery_sequence: Some(delivery_sequence),
            } => Some(BlockedDelivery {
                delivery_sequence,
                reason,
            }),
            _ => None,
        }
    }
}

impl std::fmt::Display for RestoreError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (kind, detail) = match self {
            Self::Scope(detail) => ("scope", *detail),
            Self::Entry(detail) => ("entry", *detail),
            Self::File(detail) => ("file", *detail),
            Self::Conflict(detail) => ("conflict", *detail),
            Self::Durable(detail) => ("durable", *detail),
            Self::Apply(detail) => ("apply", *detail),
            Self::ReplayBlocked {
                reason,
                delivery_sequence: Some(sequence),
            } => {
                return write!(
                    formatter,
                    "cloud replay blocked at delivery {sequence}: {}",
                    reason.message()
                );
            }
            Self::ReplayBlocked {
                reason,
                delivery_sequence: None,
            } => ("blocked", reason.message()),
        };
        write!(formatter, "snapshot restore {kind}: {detail}")
    }
}

impl std::error::Error for RestoreError {}
