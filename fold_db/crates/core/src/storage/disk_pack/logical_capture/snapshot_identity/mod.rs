//! Bounded controls for an inactive donor snapshot; no activation authority.

use super::*;
use crate::storage::disk_pack::byte_backup::RunBackupProfile;
use cloud_backup_protocol::{DecimalU64, NativeRoot};
use laststore::disk_run::home::RunRead;
use sha2::{Digest, Sha256};

mod controls;
mod model;
mod proof;
pub use model::{SnapshotIdentityPeer, SnapshotIdentityPosition, SnapshotIdentityReport};

pub(crate) const CONTROL_FILE: &str = identity::FILE;

pub(crate) struct SourceIdentity {
    identity: identity::Identity,
    map: SnapshotInclusionMap,
}

pub(crate) fn has_prepared_control(data: &Path) -> StorageResult<bool> {
    identity::Identity::has_prepared_at(data)
}

pub(crate) fn read_source(
    data: &Path,
    main: &mut RunRead,
    profile: &RunBackupProfile,
) -> StorageResult<SourceIdentity> {
    let (main_id, cas_id) = home_ids(profile)?;
    let identity = identity::Identity::load_source(data, main_id, cas_id)?;
    let inclusion = snapshot::signed_inclusion(Some(&identity), main, profile)?;
    let map = enabled(&inclusion)?.clone();
    if map.version != 1 {
        return Err(invalid(
            "snapshot identity source is not an active legacy producer",
        ));
    }
    Ok(SourceIdentity { identity, map })
}

pub(crate) fn prepare_mutations(
    source: &SourceIdentity,
    candidate: &SnapshotAdoptionCandidate,
    main: &mut RunRead,
    profile: &RunBackupProfile,
) -> StorageResult<Vec<HomeMutation>> {
    candidate
        .run_profile
        .validate_root(&candidate.native_root)?;
    if candidate.run_profile != *profile {
        return Err(invalid(
            "snapshot identity target differs from the verified candidate",
        ));
    }
    let map = enabled(
        candidate
            .logical_inclusion
            .as_ref()
            .ok_or_else(|| invalid("snapshot identity candidate inclusion is unknown"))?,
    )?;
    verify_snapshot_inclusion(
        main,
        profile,
        &SnapshotInclusion::Enabled { map: map.clone() },
    )?;
    proof::verify_coverage(source, map, main)?;
    controls::mutations(source, map)
}

pub(crate) fn save_prepared(
    source: &SourceIdentity,
    data: &Path,
    profile: &RunBackupProfile,
) -> StorageResult<()> {
    let (main, cas) = home_ids(profile)?;
    source
        .identity
        .save_prepared(data, main, cas, root_hash(&profile.native_root()?)?)
}

pub(crate) fn inspect_prepared(
    source: &SourceIdentity,
    candidate: &SnapshotAdoptionCandidate,
    data: &Path,
    main: &mut RunRead,
    profile: &RunBackupProfile,
) -> StorageResult<SnapshotIdentityReport> {
    candidate
        .run_profile
        .validate_root(&candidate.native_root)?;
    let candidate_map = enabled(
        candidate
            .logical_inclusion
            .as_ref()
            .ok_or_else(|| invalid("snapshot identity candidate inclusion is unknown"))?,
    )?;
    candidate_map.validate_for(&candidate.native_root)?;
    let (main_id, cas_id) = home_ids(profile)?;
    let prepared_root = profile.native_root()?;
    source
        .identity
        .verify_prepared(data, main_id, cas_id, root_hash(&prepared_root)?)?;
    controls::verify_preparation(source, candidate_map, main)?;
    let logical_inclusion = snapshot::signed_inclusion(Some(&source.identity), main, profile)?;
    let map = enabled(&logical_inclusion)?;
    controls::verify_bases(source, candidate_map, map)?;
    let own = map
        .positions
        .iter()
        .find(|entry| entry.origin == source.map.producer)
        .ok_or_else(|| invalid("prepared local authored base is absent"))?;
    let peers = map
        .positions
        .iter()
        .filter(|entry| entry.origin != map.producer)
        .map(|entry| SnapshotIdentityPeer {
            origin: entry.origin.clone(),
            anchor_sha256: entry.anchor_sha256.clone(),
            base: entry.base.clone().expect("validated map2 base"),
            position: SnapshotIdentityPosition::of(entry),
            retained_receipts: DecimalU64::new(0),
        })
        .collect();
    let report = SnapshotIdentityReport {
        version: 1,
        prepared: true,
        startup_allowed: false,
        replacement_allowed: false,
        source_root: source.map.snapshot.clone(),
        candidate_root: candidate.native_root.clone(),
        prepared_root,
        local_origin: map.producer.clone(),
        anchor_sha256: own.anchor_sha256.clone(),
        authored_base: own.base.clone().expect("validated map2 base"),
        peers,
        logical_inclusion,
    };
    bounded::serialized_size_with_limit(&report, 32 * 1024)?;
    Ok(report)
}

fn enabled(inclusion: &SnapshotInclusion) -> StorageResult<&SnapshotInclusionMap> {
    match inclusion {
        SnapshotInclusion::Enabled { map } => Ok(map),
        SnapshotInclusion::Disabled => Err(invalid("snapshot identity capture is disabled")),
    }
}

fn home_ids(profile: &RunBackupProfile) -> StorageResult<(uuid::Uuid, uuid::Uuid)> {
    profile.validate()?;
    Ok((
        uuid::Uuid::parse_str(profile.main.home_id.as_str())
            .map_err(|_| invalid("snapshot identity main home is invalid"))?,
        uuid::Uuid::parse_str(profile.cas.home_id.as_str())
            .map_err(|_| invalid("snapshot identity CAS home is invalid"))?,
    ))
}

fn root_hash(root: &NativeRoot) -> StorageResult<[u8; 32]> {
    Ok(Sha256::digest(encode(root)?).into())
}

pub(in crate::storage::disk_pack::logical_capture) fn captured_positions(
    main: &mut RunRead,
    head: &[u8],
    roster: Option<&[u8]>,
) -> StorageResult<(String, Vec<SnapshotDevicePosition>)> {
    controls::captured_positions(main, head, roster)
}
