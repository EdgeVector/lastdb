//! The only extra marker link belongs to this exact retained paired capture.
use super::*;
use cloud_backup_protocol::bytes_sha256;
use cloud_backup_protocol::v2::{hash::canonical_bytes, Begin};
use std::os::unix::fs::MetadataExt;

pub(super) fn check(
    home: &Path,
    request: &StoppedBackupRequest,
    node: &std::fs::Metadata,
) -> Result<()> {
    match node.nlink() {
        1 => return Ok(()),
        2 => {}
        _ => return Err(BackupError::Identity),
    }
    let data = home.join("data/disk-run");
    let source = data.join("cloud-byte-source-v2");
    private_directory(&source)?;
    let saved: serde_json::Value =
        super::super::store::read(&source.join("run-intent.json"))?.ok_or(BackupError::Missing)?;
    if saved["sha256"].as_str() != Some(bytes_sha256(&canonical_bytes(&saved["value"])?).as_str()) {
        return Err(BackupError::Identity);
    }
    let begin: Begin = serde_json::from_value(saved["value"]["begin"].clone())
        .map_err(|_| BackupError::Identity)?;
    begin.validate()?;
    if begin.job.commit_id != request.request_id
        || begin.job.stream.scope.account_id != request.account_id
    {
        return Err(BackupError::Identity);
    }
    let id = saved["value"]["reference_id"]
        .as_str()
        .ok_or(BackupError::Identity)?;
    let parsed = uuid::Uuid::parse_str(id).map_err(|_| BackupError::Identity)?;
    if parsed.is_nil() || parsed.to_string() != id {
        return Err(BackupError::Identity);
    }
    let parent = data.join("cloud-byte-holds-v2");
    let held = parent.join(id);
    private_directory(&parent)?;
    private_directory(&held)?;
    let link = std::fs::symlink_metadata(held.join("node"))?;
    if !link.is_file()
        || (
            link.dev(),
            link.ino(),
            link.uid(),
            link.mode(),
            link.nlink(),
            link.len(),
        ) != (
            node.dev(),
            node.ino(),
            node.uid(),
            node.mode(),
            2,
            node.len(),
        )
    {
        return Err(BackupError::Identity);
    }
    Ok(())
}

fn private_directory(path: &Path) -> Result<()> {
    let meta = std::fs::symlink_metadata(path)?;
    if !meta.is_dir()
        || meta.uid() != unsafe { libc::geteuid() }
        || meta.mode() & 0o077 != 0
        || path.canonicalize()? != path
    {
        return Err(BackupError::Identity);
    }
    Ok(())
}
