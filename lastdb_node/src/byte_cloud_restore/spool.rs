use super::{local, RestoreError, RestoreSession, Result};
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::atomic::Ordering;

const MAX: u64 = cloud_backup_protocol::limits::DURABLE_LOCAL_SPOOL_MAX_BYTES
    - lastdb_storage_policy::backup_decode::RESTORE_SPOOL_RESERVE_BYTES;

impl RestoreSession {
    pub(super) fn reserve_spool(&self, bytes: u64) -> Result<()> {
        let allocated = bytes
            .div_ceil(4096)
            .checked_mul(4096)
            .ok_or(RestoreError::Limit("spool bytes"))?;
        if self
            .spool_bytes
            .load(Ordering::Relaxed)
            .checked_add(allocated)
            .is_none_or(|total| total > MAX)
        {
            return Err(RestoreError::Limit("spool bytes"));
        }
        Ok(())
    }
    pub(super) fn add_spool_file(&self, path: &Path) -> Result<()> {
        let bytes = allocated(path)?;
        let total = self
            .spool_bytes
            .load(Ordering::Relaxed)
            .checked_add(bytes)
            .ok_or(RestoreError::Limit("spool bytes"))?;
        if total > MAX {
            return Err(RestoreError::Limit("spool bytes"));
        }
        self.spool_bytes.store(total, Ordering::Relaxed);
        Ok(())
    }
    pub(super) fn remove_spool_file(&self, path: &Path) -> Result<()> {
        let bytes = match fs::symlink_metadata(path) {
            Ok(_) => allocated(path)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        fs::remove_file(path)?;
        self.spool_bytes.store(
            self.spool_bytes
                .load(Ordering::Relaxed)
                .saturating_sub(bytes),
            Ordering::Relaxed,
        );
        Ok(())
    }
    pub(super) fn check_spool(&self) -> Result<()> {
        let mut bytes = 0u64;
        let mut count = 0usize;
        let mut routes = 0usize;
        for folder in ["cache", "entries", "chain"] {
            let path = self.root.join(folder);
            if folder == "chain" && !path.try_exists()? {
                continue;
            }
            local::directory(&path)?;
            for entry in fs::read_dir(path)? {
                let path = entry?.path();
                if route_entry(folder, &path)? {
                    routes += 1;
                } else {
                    count += 1;
                }
                if count
                    > cloud_backup_protocol::limits::NATIVE_FILES_MAX
                        + 2 * cloud_backup_protocol::v2::limits::MAX_PAGES
                        + 8
                    || routes > 2 * super::logs::MAX_CHAIN as usize
                {
                    return Err(RestoreError::Limit("spool entries"));
                }
                bytes = bytes
                    .checked_add(allocated(&path)?)
                    .ok_or(RestoreError::Limit("spool bytes"))?;
                if bytes > MAX {
                    return Err(RestoreError::Limit("spool bytes"));
                }
            }
        }
        self.spool_bytes.store(bytes, Ordering::Relaxed);
        Ok(())
    }
}

fn route_entry(folder: &str, path: &Path) -> Result<bool> {
    let Some(name) = path
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("routes-"))
        .filter(|_| folder == "chain")
    else {
        return Ok(false);
    };
    let index = name
        .strip_suffix(".json")
        .or_else(|| name.strip_suffix(".pending"))
        .filter(|index| index.len() == 8 && index.bytes().all(|byte| byte.is_ascii_digit()))
        .and_then(|index| index.parse::<u32>().ok());
    if index.is_none_or(|index| index >= super::logs::MAX_CHAIN) {
        return Err(RestoreError::Invalid("mutation route spool name"));
    }
    Ok(true)
}

fn allocated(path: &Path) -> Result<u64> {
    let meta = fs::symlink_metadata(path)?;
    if !meta.is_file() {
        return Err(RestoreError::Invalid("spool file"));
    }
    meta.blocks()
        .checked_mul(512)
        .ok_or(RestoreError::Limit("spool bytes"))
}
