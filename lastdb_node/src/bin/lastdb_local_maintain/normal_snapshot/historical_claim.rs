//! Bind inert historical metadata; it never substitutes for current flush proof.

use super::{err, io, NormalSnapshotArgs};
use serde::Deserialize;
use std::path::{Component, Path, PathBuf};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Claim {
    version: u32,
    source_pid: u32,
    source_start_ts: u64,
    flush_proof: String,
    owner_approved: String,
    decision_slug: String,
    copy_path: PathBuf,
}

/// Current clean-session proof must precede this check. The old owner decision
/// is metadata only; neither its approval nor its old copy grants authority.
pub(super) fn check(args: &NormalSnapshotArgs) -> Result<Option<String>, String> {
    let home = std::fs::canonicalize(&args.home).map_err(err)?;
    let path = home.join(lastdb_node::cloud::CLOUD_BACKUP_UNPROVED_FLUSH_CLAIM_FILE);
    match std::fs::symlink_metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(err(error)),
        Ok(_) => {}
    }
    let bytes = io::read(&path, 4096, false)?;
    let claim: Claim = serde_json::from_slice(&bytes).map_err(err)?;
    let approved =
        chrono::NaiveDate::parse_from_str(&claim.owner_approved, "%Y-%m-%d").map_err(err)?;
    let current =
        chrono::DateTime::from_timestamp(i64::try_from(args.expected_start_ts).map_err(err)?, 0)
            .ok_or("the current session timestamp is invalid")?
            .date_naive();
    if claim.version != 1
        || claim.source_pid == 0
        || claim.source_pid == args.expected_pid
        || claim.source_start_ts == 0
        || claim.source_start_ts >= args.expected_start_ts
        || claim.flush_proof != "absent"
        || claim.owner_approved != "2026-10-06"
        || claim.decision_slug != "decision-2026-10-06-cloud-sync-rescue-risk-acceptance"
        || approved >= current
        || !external_copy(&claim.copy_path, &home)?
    {
        return Err(
            "an unproved flush claim is current, unknown, or lacks an external old copy".into(),
        );
    }
    Ok(Some(io::digest(&bytes)))
}

fn external_copy(path: &Path, home: &Path) -> Result<bool, String> {
    if !path.is_absolute()
        || path
            .components()
            .any(|part| !matches!(part, Component::RootDir | Component::Normal(_)))
    {
        return Ok(false);
    }
    let resolved = match std::fs::canonicalize(path) {
        Ok(resolved) => resolved,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let parent = path.parent().ok_or("the old copy has no explicit parent")?;
            std::fs::canonicalize(parent).map_err(err)?.join(
                path.file_name()
                    .ok_or("the old copy has no explicit name")?,
            )
        }
        Err(error) => return Err(err(error)),
    };
    Ok(!resolved.starts_with(home) && !home.starts_with(resolved))
}
