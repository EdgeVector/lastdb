//! Pure accepted-recovery dependency closure. This creates no deletion authority.
//!
//! The service must supply a complete frozen account directory, all current
//! heads, owner-kept points and active restore holds. It must independently mark
//! provider allocations from every retained inventory and unresolved obligation.
//! A graph result alone never authorizes a provider operation or quota release.

use super::{AcceptedBackup, Head, Job};
use crate::{CloudScope, Digest, ProtocolError};
use std::collections::{BTreeMap, BTreeSet};

const MAX_POINTS: usize = 32768;
const MAX_DIRECTORY_BYTES: usize = 32 * 1024 * 1024;

/// Exact immutable identity. Native frontiers and atom identities stay private.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryPoint {
    pub job: Job,
    pub head: Head,
}

impl RecoveryPoint {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        self.head.validate()?;
        if self.job.commit_id != self.head.commit_id {
            return Err(ProtocolError::invalid("v2.retention.point"));
        }
        Ok(())
    }

    pub fn of(backup: &AcceptedBackup) -> Self {
        Self {
            job: backup.job().clone(),
            head: backup.head(),
        }
    }
}

/// Deterministic dependency classification, not a saved retirement request.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RecoveryClosure {
    pub retained: Vec<RecoveryPoint>,
    pub candidates: Vec<RecoveryPoint>,
    pub directory_sha256: Digest,
}

/// Close the union of owner selections, every current head and active holds.
/// A caller cannot omit directory members: freezing/completeness is a separate
/// service condition, bound to this result's complete directory digest.
pub fn recovery_closure(
    scope: &CloudScope,
    directory: &[AcceptedBackup],
    roots: &[RecoveryPoint],
) -> Result<RecoveryClosure, ProtocolError> {
    scope.validate()?;
    if directory.is_empty()
        || directory.len() > MAX_POINTS
        || roots.is_empty()
        || roots.len() > MAX_POINTS
    {
        return Err(ProtocolError::limit("v2.retention.directory"));
    }
    let records = records(scope, directory)?;
    let mut pending = Vec::with_capacity(roots.len());
    for root in roots {
        root.validate()?;
        let backup = records
            .get(&root.job.commit_id)
            .ok_or_else(|| ProtocolError::invalid("v2.retention.missing_root"))?;
        if RecoveryPoint::of(backup) != *root {
            return Err(ProtocolError::invalid("v2.retention.root_context"));
        }
        pending.push(root.job.commit_id.clone());
    }
    let mut retained = BTreeSet::new();
    while let Some(id) = pending.pop() {
        if !retained.insert(id.clone()) {
            continue;
        }
        let backup = records[&id];
        for dependency in backup.begin.dependencies() {
            let parent = records
                .get(&dependency.commit_id)
                .ok_or_else(|| ProtocolError::invalid("v2.retention.missing_dependency"))?;
            if parent.job().stream != backup.job().stream
                || parent.head() != *dependency
                || parent.ordinal >= backup.ordinal
            {
                return Err(ProtocolError::invalid("v2.retention.dependency_context"));
            }
            pending.push(dependency.commit_id.clone());
        }
    }
    let ordered: Vec<_> = records.values().copied().collect();
    let directory_sha256 = super::hash::input(
        "lastdb:cloud-byte-backup:retention-directory:v2",
        &(scope, &ordered),
    )?;
    let (kept, candidates): (Vec<_>, Vec<_>) = ordered
        .into_iter()
        .partition(|backup| retained.contains(&backup.job().commit_id));
    Ok(RecoveryClosure {
        retained: kept.into_iter().map(RecoveryPoint::of).collect(),
        candidates: candidates.into_iter().map(RecoveryPoint::of).collect(),
        directory_sha256,
    })
}

fn records<'a>(
    scope: &CloudScope,
    directory: &'a [AcceptedBackup],
) -> Result<BTreeMap<crate::Id, &'a AcceptedBackup>, ProtocolError> {
    let mut records = BTreeMap::new();
    let mut bytes = 0usize;
    for backup in directory {
        backup.validate()?;
        bytes = bytes
            .checked_add(super::hash::canonical_bytes(backup)?.len())
            .filter(|total| *total <= MAX_DIRECTORY_BYTES)
            .ok_or_else(|| ProtocolError::limit("v2.retention.directory_bytes"))?;
        if backup.job().stream.scope != *scope
            || records
                .insert(backup.job().commit_id.clone(), backup)
                .is_some()
        {
            return Err(ProtocolError::invalid("v2.retention.directory"));
        }
    }
    // Reject an incomplete or conflicting directory even outside the chosen
    // root closure. Missing records cannot become deletion permission.
    for backup in records.values() {
        for dependency in backup.begin.dependencies() {
            let parent = records
                .get(&dependency.commit_id)
                .ok_or_else(|| ProtocolError::invalid("v2.retention.missing_dependency"))?;
            if parent.job().stream != backup.job().stream
                || parent.head() != *dependency
                || parent.ordinal >= backup.ordinal
            {
                return Err(ProtocolError::invalid("v2.retention.dependency_context"));
            }
        }
    }
    Ok(records)
}
