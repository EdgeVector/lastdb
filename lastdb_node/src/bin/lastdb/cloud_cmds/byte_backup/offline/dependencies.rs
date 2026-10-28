//! Read real accepted metadata without a daemon, native home, grant or delete.
use super::*;
use cloud_backup_protocol::v2::retention::{
    recovery_closure, RecoveryPoint, MAX_DIRECTORY_BYTES, MAX_POINTS,
};
use cloud_backup_protocol::v2::Head;
use std::collections::BTreeMap;

pub(super) async fn inspect(
    client: &ByteBackupClient,
    selected: AcceptedBackup,
) -> Result<serde_json::Value, String> {
    selected.validate().map_err(|e| e.to_string())?;
    let stream = selected.job().stream.clone();
    let root = RecoveryPoint::of(&selected);
    let mut bytes = 0usize;
    let mut records = BTreeMap::new();
    let mut pending = vec![selected];
    while !pending.is_empty() {
        let mut wanted = BTreeMap::new();
        for backup in pending {
            backup.validate().map_err(|e| e.to_string())?;
            if backup.job().stream != stream {
                return Err("dependency stream differs".into());
            }
            if let Some(previous) = records.get(&backup.job().commit_id) {
                if previous != &backup {
                    return Err("accepted dependency identity conflicts".into());
                }
                continue;
            }
            bytes = bytes
                .checked_add(
                    cloud_backup_protocol::v2::hash::canonical_bytes(&backup)
                        .map_err(|e| e.to_string())?
                        .len(),
                )
                .filter(|count| *count <= MAX_DIRECTORY_BYTES)
                .ok_or("dependency metadata byte limit")?;
            if records.len() >= MAX_POINTS {
                return Err("dependency point limit".into());
            }
            for dependency in backup.begin.dependencies() {
                if dependency.ordinal >= backup.ordinal {
                    return Err("dependency ordinal did not decrease".into());
                }
                if let Some(prior) = wanted.insert(dependency.commit_id.clone(), dependency.clone())
                {
                    if prior != *dependency {
                        return Err("dependency head conflicts".into());
                    }
                }
            }
            records.insert(backup.job().commit_id.clone(), backup);
        }
        let mut fetch = Vec::new();
        for (id, expected) in wanted {
            if let Some(known) = records.get(&id) {
                if known.head() != expected {
                    return Err("accepted dependency hash differs".into());
                }
            } else {
                fetch.push(expected);
            }
        }
        if records.len().saturating_add(fetch.len()) > MAX_POINTS {
            return Err("dependency point limit".into());
        }
        let mut fetched = futures::stream::iter(fetch)
            .map(|expected: Head| {
                let stream = stream.clone();
                async move {
                    let backup =
                        super::accepted(client, stream, Some(expected.commit_id.clone())).await?;
                    if backup.head() != expected {
                        return Err("accepted dependency selection differs".to_owned());
                    }
                    Ok(backup)
                }
            })
            .buffered(4);
        pending = Vec::new();
        let mut pending_bytes = bytes;
        while let Some(backup) = fetched.try_next().await? {
            pending_bytes = pending_bytes
                .checked_add(
                    cloud_backup_protocol::v2::hash::canonical_bytes(&backup)
                        .map_err(|e| e.to_string())?
                        .len(),
                )
                .filter(|count| *count <= MAX_DIRECTORY_BYTES)
                .ok_or("dependency metadata byte limit")?;
            pending.push(backup);
        }
    }
    let directory: Vec<_> = records.into_values().collect();
    report(&root, &directory, bytes)
}

fn report(
    root: &RecoveryPoint,
    directory: &[AcceptedBackup],
    bytes: usize,
) -> Result<serde_json::Value, String> {
    let closure = recovery_closure(
        &root.job.stream.scope,
        directory,
        std::slice::from_ref(root),
    )
    .map_err(|e| e.to_string())?;
    if !closure.candidates.is_empty() {
        return Err("dependency-only inspection produced retirement candidates".into());
    }
    Ok(serde_json::json!({
        "selected": root, "retained_points": closure.retained.len(),
        "legacy_points": directory.iter().filter(|b| b.begin.recovery_dependencies.is_none()).count(),
        "dependency_edges": directory.iter().map(|b| b.begin.dependencies().len()).sum::<usize>(),
        "accepted_hashes_verified": true, "metadata_bytes": bytes,
        "directory_sha256": closure.directory_sha256,
        "closure_only": true, "account_directory_frozen": false,
        "provider_deletion_authorized": false, "provider_deletions": 0,
    }))
}
