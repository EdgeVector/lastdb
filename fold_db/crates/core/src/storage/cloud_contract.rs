//! Cloud format derived from the disk export spec.
//!
//! `CloudFormat` is the disk export spec plus a cloud envelope and a placement
//! policy. `CloudManifest` is a plan over pinned artifact descriptors. This
//! module does not upload, delete local files, or read a disk path. The
//! production filename tables in the backup jobs are not used here.

use std::fmt;

use cloud_backup_protocol::v2::Version;
use cloud_backup_protocol::Digest;

use super::format::export::{validate_spec, EXPORT_PARTS};
use super::format::{HOME_FORMAT, RECORD_PROFILE};

pub use super::format::export::{ExportPresence, ExportRole};
pub use cloud_backup_protocol::v2::Destination;

mod plan;

/// Cloud protocol this plan speaks. It does not name a disk format.
pub const CLOUD_PROTOCOL: u8 = Version::V2.get();
/// Backup profile recorded on the envelope. It does not name a disk format.
pub const BACKUP_PROFILE: u16 = RECORD_PROFILE;

/// Remote packaging around a disk export. Placement is not part of the envelope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CloudEnvelope {
    pub protocol: u8,
    pub backup_profile: u16,
}

/// Where each exported role goes. Local-only parts have no destination.
///
/// There is no default. A caller names every exported role.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlacementPolicy {
    pub snapshot: Destination,
    pub replay_log: Destination,
    pub record_owned_blob: Destination,
}

impl PlacementPolicy {
    fn destination(self, role: ExportRole) -> Option<Destination> {
        match role {
            ExportRole::Snapshot => Some(self.snapshot),
            ExportRole::ReplayLog => Some(self.replay_log),
            ExportRole::RecordOwnedBlob => Some(self.record_owned_blob),
            ExportRole::LocalOnly => None,
        }
    }
}

/// Disk export plus cloud packaging. The disk format comes from the disk definition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudFormat {
    pub disk_format: u32,
    pub protocol: u8,
    pub backup_profile: u16,
    pub policy: PlacementPolicy,
}

impl CloudFormat {
    /// Backup profile 9 and protocol V2 are not disk-format versions.
    #[allow(
        clippy::unused_self,
        reason = "the profile never describes the disk format"
    )]
    pub const fn profile_describes_disk(&self) -> bool {
        false
    }
}

/// How one frozen file was encoded. Cloud does not parse keys inside it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncodingContext {
    pub disk_format: u32,
    pub home_dir: &'static str,
    pub kind: &'static str,
    pub key_label: &'static str,
    pub home_id: String,
    pub generation: u64,
    pub frontier: u64,
}

/// The frozen main and blob homes copied onto every artifact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Checkpoint {
    pub disk_format: u32,
    pub main_home_id: String,
    pub main_generation: u64,
    pub main_frontier: u64,
    pub cas_home_id: String,
    pub cas_generation: u64,
    pub cas_frontier: u64,
}

/// Writer and frontier span of a sealed replay segment. This capture has none.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogSpan {
    pub writer: String,
    pub from_frontier: u64,
    pub to_frontier: u64,
}

/// One committed artifact. The checksum is of the pinned bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactDescriptor {
    pub identity: String,
    pub part_id: &'static str,
    pub role: ExportRole,
    pub bytes: u64,
    pub sha256: Digest,
    pub encoding: EncodingContext,
    pub checkpoint: Checkpoint,
    pub depends_on: Vec<String>,
    pub log_span: Option<LogSpan>,
}

/// One upload object. Its bytes and checksum are copies of the descriptor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudObject {
    pub identity: String,
    pub part_id: &'static str,
    pub role: ExportRole,
    pub bytes: u64,
    pub sha256: Digest,
    pub destination: Destination,
    pub encoding: EncodingContext,
    pub depends_on: Vec<String>,
    pub log_span: Option<LogSpan>,
}

/// One spec row, including rows the capture did not emit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartReport {
    pub part_id: &'static str,
    pub role: ExportRole,
    pub presence: ExportPresence,
    pub artifact_count: usize,
    pub destination: Option<Destination>,
    pub reason: &'static str,
}

/// A plan for one pinned export. Building it does not upload or delete.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudManifest {
    pub disk_format: u32,
    pub protocol: u8,
    pub backup_profile: u16,
    pub objects: Vec<CloudObject>,
    pub parts: Vec<PartReport>,
    pub excluded: Vec<PartReport>,
    pub selected_closure: bool,
    pub restorable: bool,
}

impl CloudManifest {
    /// An upload plan never authorizes deleting local data.
    #[allow(
        clippy::unused_self,
        reason = "an upload plan does not delete local data"
    )]
    pub const fn deletes_local(&self) -> bool {
        false
    }

    /// The embedded disk format is `disk_format`, not the backup profile.
    #[allow(
        clippy::unused_self,
        reason = "the profile never describes the disk format"
    )]
    pub const fn profile_describes_disk(&self) -> bool {
        false
    }
}

/// Why a format could not be derived or a plan could not close.
#[derive(Debug)]
pub enum ContractError {
    UnsupportedDiskFormat(u32),
    UnsupportedProtocol(u8),
    Unclassified(String),
    ReplayGap(String),
    Incomplete(String),
    Spec(String),
    Io(std::io::Error),
    Capture(String),
}

impl ContractError {
    pub(crate) fn spec(detail: impl Into<String>) -> Self {
        Self::Spec(detail.into())
    }

    pub(crate) fn capture(detail: impl Into<String>) -> Self {
        Self::Capture(detail.into())
    }
}

impl fmt::Display for ContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedDiskFormat(found) => {
                write!(formatter, "unsupported disk format {found}")
            }
            Self::UnsupportedProtocol(found) => {
                write!(formatter, "unsupported protocol {found}")
            }
            Self::Unclassified(detail) => {
                write!(formatter, "unclassified part: {detail}")
            }
            Self::ReplayGap(detail) => write!(formatter, "replay gap: {detail}"),
            Self::Incomplete(detail) => write!(formatter, "incomplete closure: {detail}"),
            Self::Spec(detail) => write!(formatter, "export spec: {detail}"),
            Self::Io(error) => write!(formatter, "disk export io: {error}"),
            Self::Capture(detail) => write!(formatter, "disk export capture: {detail}"),
        }
    }
}

impl std::error::Error for ContractError {}

/// Part ids in spec order. Cloud reports every id, including empty and local-only rows.
pub fn export_part_ids() -> Vec<&'static str> {
    EXPORT_PARTS.iter().map(|part| part.id).collect()
}

/// `CloudFormat = derive(DiskFormat.export_spec, CloudEnvelope, PlacementPolicy)`.
pub fn derive_cloud_format(
    envelope: CloudEnvelope,
    policy: PlacementPolicy,
) -> Result<CloudFormat, ContractError> {
    validate_spec().map_err(ContractError::Spec)?;
    if envelope.protocol != CLOUD_PROTOCOL {
        return Err(ContractError::UnsupportedProtocol(envelope.protocol));
    }
    Ok(CloudFormat {
        disk_format: HOME_FORMAT,
        protocol: envelope.protocol,
        backup_profile: envelope.backup_profile,
        policy,
    })
}

/// `CloudManifest = plan(PinnedDiskExport, CloudFormat)` over descriptors only.
pub fn plan(
    descriptors: &[ArtifactDescriptor],
    format: &CloudFormat,
    selected: &[ExportRole],
) -> Result<CloudManifest, ContractError> {
    validate_spec().map_err(ContractError::Spec)?;
    plan::check_format(format)?;
    plan::check_selection(selected)?;
    plan::reject_duplicate_identities(descriptors)?;
    plan::check_artifacts(descriptors)?;
    plan::replay_gap(descriptors, selected)?;
    plan::required_present(descriptors, selected)?;
    let objects = plan::close_objects(descriptors, format, selected)?;
    let parts = plan::part_reports(descriptors, format);
    let restorable = plan::replay_restorable(descriptors, selected);
    Ok(CloudManifest {
        disk_format: HOME_FORMAT,
        protocol: format.protocol,
        backup_profile: format.backup_profile,
        excluded: parts
            .iter()
            .filter(|part| part.presence == ExportPresence::LocalOnly)
            .cloned()
            .collect(),
        parts,
        objects,
        selected_closure: true,
        restorable,
    })
}
