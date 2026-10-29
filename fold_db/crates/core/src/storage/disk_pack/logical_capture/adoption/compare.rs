use super::super::{invalid, SnapshotDevicePosition, SnapshotInclusionMap};
use super::*;
use cloud_backup_protocol::DecimalU64;

pub(super) fn position<'a>(
    map: &'a SnapshotInclusionMap,
    origin: &str,
) -> Option<&'a SnapshotDevicePosition> {
    map.positions
        .binary_search_by(|position| position.origin.as_str().cmp(origin))
        .ok()
        .map(|index| &map.positions[index])
}

pub(super) fn validate_pair(
    local: &SnapshotInclusionMap,
    candidate: &SnapshotInclusionMap,
) -> StorageResult<()> {
    let unknown = candidate
        .positions
        .iter()
        .filter(|entry| position(local, &entry.origin).is_none())
        .count();
    if local.positions.len() + unknown > MAX_ORIGINS {
        return Err(invalid("snapshot adoption origin union exceeds 33"));
    }
    if position(local, &candidate.producer).is_none() {
        return Err(invalid(
            "snapshot adoption producer is not locally enrolled",
        ));
    }
    for included in &candidate.positions {
        let known = position(local, &included.origin)
            .ok_or_else(|| invalid("snapshot adoption candidate has an unknown origin"))?;
        if included.anchor_sha256 != known.anchor_sha256 {
            return Err(invalid("snapshot adoption original anchor differs"));
        }
        if included.sequence == known.sequence && included.sha256 != known.sha256 {
            return Err(invalid(
                "snapshot adoption equal sequence has a different hash",
            ));
        }
        if included.origin == local.producer && included.sequence > known.sequence {
            return Err(invalid(
                "snapshot adoption exceeds the held local authored head",
            ));
        }
    }
    Ok(())
}

pub(super) fn origins(
    local: &SnapshotInclusionMap,
    candidate: &SnapshotInclusionMap,
) -> Vec<SnapshotAdoptionOrigin> {
    local
        .positions
        .iter()
        .map(|known| origin(known, position(candidate, &known.origin), &local.producer))
        .collect()
}

fn origin(
    known: &SnapshotDevicePosition,
    included: Option<&SnapshotDevicePosition>,
    local_origin: &str,
) -> SnapshotAdoptionOrigin {
    let local_position = SnapshotAdoptionPosition::from(known);
    let candidate_position = included.map(SnapshotAdoptionPosition::from);
    let prefix = match included {
        None => SnapshotAdoptionPrefix::CandidateAbsent,
        Some(included) if included.sequence == known.sequence => SnapshotAdoptionPrefix::Equal,
        Some(included) if included.sequence < known.sequence => {
            SnapshotAdoptionPrefix::LocalPrefixVerified
        }
        Some(_) => SnapshotAdoptionPrefix::CandidatePrefixProofRequired {
            at: local_position.clone(),
        },
    };
    let after = candidate_position
        .clone()
        .unwrap_or_else(|| SnapshotAdoptionPosition {
            sequence: DecimalU64::new(0),
            sha256: known.anchor_sha256.clone(),
        });
    let replay = (after.sequence < local_position.sequence).then(|| SnapshotAdoptionReplay {
        after,
        through: local_position.clone(),
        source: if known.origin == local_origin {
            SnapshotAdoptionReplaySource::LocalAuthoredStream
        } else {
            SnapshotAdoptionReplaySource::OriginalSourceDevice
        },
    });
    SnapshotAdoptionOrigin {
        origin: known.origin.clone(),
        anchor_sha256: known.anchor_sha256.clone(),
        local_position,
        candidate_position,
        prefix,
        replay,
    }
}
