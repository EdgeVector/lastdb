//! Bounded metadata comparison of two cuts. No replay or publication occurs.

mod compare;
mod model;
mod prefix;
mod requirements;

pub use model::*;

use super::{invalid, SnapshotInclusion, SnapshotInclusionMap};
use crate::storage::disk_pack::byte_backup::RunBackupProfile;
use crate::storage::error::StorageResult;
use laststore::disk_run::home::RunRead;

const MAX_ORIGINS: usize = 33;
const MAX_PLAN_BYTES: usize = 32 * 1024;

pub(crate) fn plan(
    main: &mut RunRead,
    profile: &RunBackupProfile,
    local: &SnapshotInclusion,
    candidate: &SnapshotAdoptionCandidate,
) -> StorageResult<SnapshotAdoptionPlan> {
    candidate
        .native_root
        .validate()
        .map_err(|_| invalid("snapshot adoption candidate native root differs"))?;
    let candidate_map = enabled(candidate.logical_inclusion.as_ref())?;
    candidate_map.validate_for(&candidate.native_root)?;
    super::verify_snapshot_inclusion(main, profile, local)?;
    let local_map = enabled(Some(local))?;
    compare::validate_pair(local_map, candidate_map)?;
    prefix::verify(main, local_map, candidate_map)?;
    let origins = compare::origins(local_map, candidate_map);
    let requirements = requirements::for_origins(&origins);
    let plan = SnapshotAdoptionPlan {
        version: 1,
        replacement_allowed: false,
        local_root: profile.native_root()?,
        candidate_root: candidate.native_root.clone(),
        local_map_sha256: local_map.sha256.clone(),
        candidate_map_sha256: candidate_map.sha256.clone(),
        local_origin: local_map.producer.clone(),
        origins,
        requirements,
    };
    super::bounded::serialized_size_with_limit(&plan, MAX_PLAN_BYTES)?;
    Ok(plan)
}

fn enabled(inclusion: Option<&SnapshotInclusion>) -> StorageResult<&SnapshotInclusionMap> {
    match inclusion {
        Some(SnapshotInclusion::Enabled { map }) => Ok(map),
        _ => Err(invalid(
            "snapshot adoption planning requires enabled certified coverage",
        )),
    }
}
