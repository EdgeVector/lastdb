//! Bounded local file proof helpers. No cloud request or V1 grant path exists.
use super::{RestoreError, Result};
use cloud_backup_protocol::Digest;
use sha2::{Digest as _, Sha256};
use std::fs::{self, File};
use std::io::Read;
use std::path::Path;

pub(super) fn bytes(path: &Path, max: u64) -> Result<Vec<u8>> {
    if !fs::symlink_metadata(path)?.is_file() {
        return Err(RestoreError::Invalid("object file"));
    }
    let mut bytes = Vec::new();
    File::open(path)?.take(max + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max {
        return Err(RestoreError::Limit("object decode"));
    }
    Ok(bytes)
}
pub(super) fn verify_file(path: &Path, sha: &Digest, length: u64) -> Result<()> {
    if !fs::symlink_metadata(path)?.is_file() {
        return Err(RestoreError::Invalid("native file"));
    }
    let mut input = File::open(path)?;
    if input.metadata()?.len() != length {
        return Err(RestoreError::Invalid("file length"));
    }
    let mut digest = Sha256::new();
    let mut buffer = vec![0u8; cloud_backup_protocol::limits::STREAM_BUFFER_MAX_BYTES as usize];
    let mut left = length;
    while left > 0 {
        let maximum = left.min(buffer.len() as u64) as usize;
        let count = input.read(&mut buffer[..maximum])?;
        if count == 0 {
            return Err(RestoreError::Invalid("file length"));
        }
        digest.update(&buffer[..count]);
        left -= count as u64;
    }
    if input.read(&mut buffer[..1])? != 0 {
        return Err(RestoreError::Invalid("file length"));
    }
    if hex::encode(digest.finalize()) != sha.as_str() {
        return Err(RestoreError::Invalid("file digest"));
    }
    Ok(())
}
pub(super) fn check_capacity(root: &Path, bytes: u64) -> Result<()> {
    let fs = rustix::fs::statvfs(root).map_err(std::io::Error::from)?;
    let available = (fs.f_bavail as u64)
        .checked_mul(fs.f_frsize as u64)
        .ok_or(RestoreError::Limit("disk capacity"))?;
    let required = bytes
        .checked_add(
            laststore::disk_pack::maintenance::SpacePolicy::default().free_space_reserve_bytes,
        )
        .ok_or(RestoreError::Limit("disk capacity"))?;
    if available < required {
        return Err(RestoreError::Limit("disk capacity"));
    }
    Ok(())
}
