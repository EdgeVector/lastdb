//! Close a cloud upload plan from pinned descriptors.
//!
//! Placement comes only from the policy. This module does not read a path.

use std::collections::HashSet;

use super::super::format::export::{part_by_id, ExportPart, EXPORT_PARTS};
use super::super::format::HOME_FORMAT;
use super::{
    ArtifactDescriptor, CloudFormat, CloudObject, ContractError, ExportPresence, ExportRole,
    PartReport, PlacementPolicy, CLOUD_PROTOCOL,
};

pub(super) fn check_format(format: &CloudFormat) -> Result<(), ContractError> {
    if format.disk_format != HOME_FORMAT {
        return Err(ContractError::UnsupportedDiskFormat(format.disk_format));
    }
    if format.protocol != CLOUD_PROTOCOL {
        return Err(ContractError::UnsupportedProtocol(format.protocol));
    }
    Ok(())
}

pub(super) fn check_selection(selected: &[ExportRole]) -> Result<(), ContractError> {
    if selected.is_empty() {
        return Err(ContractError::spec("selection is empty"));
    }
    if selected.contains(&ExportRole::LocalOnly) {
        return Err(ContractError::spec(
            "local-only parts have no cloud destination",
        ));
    }
    Ok(())
}

pub(super) fn reject_duplicate_identities(
    descriptors: &[ArtifactDescriptor],
) -> Result<(), ContractError> {
    let mut seen = HashSet::new();
    for artifact in descriptors {
        if !seen.insert(artifact.identity.as_str()) {
            let identity = &artifact.identity;
            return Err(ContractError::spec(format!(
                "duplicate artifact identity {identity}"
            )));
        }
    }
    Ok(())
}

pub(super) fn check_artifacts(descriptors: &[ArtifactDescriptor]) -> Result<(), ContractError> {
    for artifact in descriptors {
        if artifact.encoding.disk_format != HOME_FORMAT {
            return Err(ContractError::UnsupportedDiskFormat(
                artifact.encoding.disk_format,
            ));
        }
        if artifact.checkpoint.disk_format != HOME_FORMAT {
            return Err(ContractError::UnsupportedDiskFormat(
                artifact.checkpoint.disk_format,
            ));
        }
        let Some(part) = part_by_id(artifact.part_id) else {
            return Err(ContractError::Unclassified(artifact.part_id.to_string()));
        };
        if part.role != artifact.role {
            return Err(ContractError::spec(
                "artifact role differs from the disk export part",
            ));
        }
    }
    let Some(first) = descriptors.first() else {
        return Ok(());
    };
    if descriptors
        .iter()
        .any(|artifact| artifact.checkpoint != first.checkpoint)
    {
        return Err(ContractError::spec("checkpoint differs across artifacts"));
    }
    Ok(())
}

pub(super) fn replay_gap(
    descriptors: &[ArtifactDescriptor],
    selected: &[ExportRole],
) -> Result<(), ContractError> {
    for part in gap_parts(selected) {
        if !has_part(descriptors, part.id) {
            return Err(ContractError::ReplayGap(part.reason.to_string()));
        }
    }
    Ok(())
}

fn gap_parts(selected: &[ExportRole]) -> impl Iterator<Item = &'static ExportPart> + '_ {
    EXPORT_PARTS
        .iter()
        .filter(move |part| part.presence == ExportPresence::Gap && selected.contains(&part.role))
}

pub(super) fn required_present(
    descriptors: &[ArtifactDescriptor],
    selected: &[ExportRole],
) -> Result<(), ContractError> {
    for part in EXPORT_PARTS {
        if part.presence != ExportPresence::Required || !selected.contains(&part.role) {
            continue;
        }
        if !has_part(descriptors, part.id) {
            let id = part.id;
            return Err(ContractError::Incomplete(format!(
                "required part {id} has no artifact"
            )));
        }
    }
    Ok(())
}

pub(super) fn close_objects(
    descriptors: &[ArtifactDescriptor],
    format: &CloudFormat,
    selected: &[ExportRole],
) -> Result<Vec<CloudObject>, ContractError> {
    let included: Vec<&ArtifactDescriptor> = descriptors
        .iter()
        .filter(|artifact| selected.contains(&artifact.role))
        .collect();
    let ids: HashSet<&str> = included
        .iter()
        .map(|artifact| artifact.identity.as_str())
        .collect();
    for artifact in &included {
        missing_dependency(artifact, &ids)?;
    }
    let mut objects = Vec::with_capacity(included.len());
    for artifact in &included {
        objects.push(cloud_object(artifact, format.policy)?);
    }
    objects.sort_by(|left, right| left.identity.cmp(&right.identity));
    Ok(objects)
}

fn missing_dependency(
    artifact: &ArtifactDescriptor,
    ids: &HashSet<&str>,
) -> Result<(), ContractError> {
    for dependency in &artifact.depends_on {
        if ids.contains(dependency.as_str()) {
            continue;
        }
        let identity = &artifact.identity;
        return Err(ContractError::Incomplete(format!(
            "{identity} depends on {dependency}"
        )));
    }
    Ok(())
}

fn cloud_object(
    artifact: &ArtifactDescriptor,
    policy: PlacementPolicy,
) -> Result<CloudObject, ContractError> {
    let Some(destination) = policy.destination(artifact.role) else {
        return Err(ContractError::spec(
            "selected role has no cloud destination",
        ));
    };
    Ok(CloudObject {
        identity: artifact.identity.clone(),
        part_id: artifact.part_id,
        role: artifact.role,
        bytes: artifact.bytes,
        sha256: artifact.sha256.clone(),
        destination,
        encoding: artifact.encoding.clone(),
        depends_on: artifact.depends_on.clone(),
        log_span: artifact.log_span.clone(),
    })
}

pub(super) fn part_reports(
    descriptors: &[ArtifactDescriptor],
    format: &CloudFormat,
) -> Vec<PartReport> {
    EXPORT_PARTS
        .iter()
        .map(|part| PartReport {
            part_id: part.id,
            role: part.role,
            presence: part.presence,
            artifact_count: descriptors
                .iter()
                .filter(|artifact| artifact.part_id == part.id)
                .count(),
            destination: format.policy.destination(part.role),
            reason: part.reason,
        })
        .collect()
}

pub(super) fn replay_restorable(
    descriptors: &[ArtifactDescriptor],
    selected: &[ExportRole],
) -> bool {
    if !selected.contains(&ExportRole::ReplayLog) {
        return false;
    }
    EXPORT_PARTS
        .iter()
        .filter(|part| part.role == ExportRole::ReplayLog)
        .all(|part| replay_part_ready(descriptors, part))
}

fn replay_part_ready(descriptors: &[ArtifactDescriptor], part: &ExportPart) -> bool {
    let count = descriptors
        .iter()
        .filter(|artifact| artifact.part_id == part.id)
        .count();
    match part.presence {
        ExportPresence::Gap | ExportPresence::Required => count > 0,
        ExportPresence::EmptyOk => true,
        ExportPresence::LocalOnly => false,
    }
}

fn has_part(descriptors: &[ArtifactDescriptor], id: &str) -> bool {
    descriptors.iter().any(|artifact| artifact.part_id == id)
}
