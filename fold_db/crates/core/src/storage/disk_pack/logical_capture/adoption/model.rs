//! A plan describes required work; it never grants replacement authority.

use super::super::{SnapshotDevicePosition, SnapshotInclusion};
use cloud_backup_protocol::{DecimalU64, NativeRoot};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotAdoptionCandidate {
    pub native_root: NativeRoot,
    pub logical_inclusion: Option<SnapshotInclusion>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SnapshotAdoptionPlan {
    pub version: u8,
    pub replacement_allowed: bool,
    pub local_root: NativeRoot,
    pub candidate_root: NativeRoot,
    pub local_map_sha256: String,
    pub candidate_map_sha256: String,
    pub local_origin: String,
    pub origins: Vec<SnapshotAdoptionOrigin>,
    pub requirements: Vec<SnapshotAdoptionRequirement>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SnapshotAdoptionOrigin {
    pub origin: String,
    pub anchor_sha256: String,
    pub local_position: SnapshotAdoptionPosition,
    /// Absence is retained explicitly; it does not certify an empty database.
    pub candidate_position: Option<SnapshotAdoptionPosition>,
    pub prefix: SnapshotAdoptionPrefix,
    pub replay: Option<SnapshotAdoptionReplay>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SnapshotAdoptionPosition {
    pub sequence: DecimalU64,
    pub sha256: String,
}

impl From<&SnapshotDevicePosition> for SnapshotAdoptionPosition {
    fn from(position: &SnapshotDevicePosition) -> Self {
        Self {
            sequence: position.sequence,
            sha256: position.sha256.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SnapshotAdoptionPrefix {
    Equal,
    /// The common hash matches held local metadata, not a replayed event body.
    LocalPrefixVerified,
    CandidateAbsent,
    CandidatePrefixProofRequired {
        at: SnapshotAdoptionPosition,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SnapshotAdoptionReplay {
    /// Exclusive boundary. Replay must preserve this original chain hash.
    pub after: SnapshotAdoptionPosition,
    pub through: SnapshotAdoptionPosition,
    pub source: SnapshotAdoptionReplaySource,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotAdoptionReplaySource {
    LocalAuthoredStream,
    OriginalSourceDevice,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotAdoptionRequirement {
    SharedBaseProof,
    CandidateNativeVerification,
    LocalCutRevalidation,
    LocalIdentityAndAnchorPreservation,
    ReceiptBaseAndControlRewrite,
    AtomicPublication,
    OwnTailReplay,
    OriginalSourceTails,
    CandidatePrefixProof,
}
