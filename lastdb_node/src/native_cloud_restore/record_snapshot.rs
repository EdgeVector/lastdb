//! Actual physical proof after the existing native-record importer completes.
use super::{NativeFileProof, NativeInventory, RestoreError, Result};
use cloud_backup_protocol::{DecimalU64, Digest, Id, NativeRoot};
use sha2::{Digest as _, Sha256};
use std::fs::OpenOptions;
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;

pub(crate) fn rebuild_record_snapshot(
    physical: &Path,
) -> Result<(NativeRoot, NativeInventory, Vec<NativeFileProof>)> {
    let native = laststore::disk_pack::backup::validate_snapshot(physical)
        .map_err(|_| RestoreError::Invalid("rebuilt native snapshot"))?;
    if native.files.len() > cloud_backup_protocol::limits::NATIVE_FILES_MAX {
        return Err(RestoreError::Limit("rebuilt native file count"));
    }
    let mut proofs = Vec::with_capacity(native.files.len());
    let mut bytes = 0u64;
    for file in &native.files {
        let path = file
            .path
            .to_str()
            .ok_or(RestoreError::Invalid("rebuilt native path"))?;
        proofs.push(NativeFileProof {
            path: path.to_owned(),
            sha256: digest(&physical.join(&file.path), file.bytes)?,
            bytes: file.bytes,
        });
        bytes = bytes
            .checked_add(file.bytes)
            .ok_or(RestoreError::Limit("rebuilt native bytes"))?;
    }
    let current_sha256 = proofs
        .iter()
        .find(|proof| proof.path == "CURRENT")
        .ok_or(RestoreError::Invalid("rebuilt native CURRENT"))?
        .sha256
        .clone();
    let root = NativeRoot {
        home_id: Id::try_from(native.home_id.to_string())?,
        home_digest: Digest::try_from(hex::encode(native.current.home_digest))?,
        current_sha256,
        generation: DecimalU64::new(native.current.generation),
        frontier: DecimalU64::new(native.current.frontier),
        required_features: DecimalU64::new(native.current.required_features),
    };
    fold_db::storage::disk_pack::byte_backup::validate_client_native_root(&root)
        .map_err(|_| RestoreError::Invalid("rebuilt native root"))?;
    let inventory = NativeInventory {
        files: proofs.len() as u64,
        bytes,
        dependencies: 0,
    };
    Ok((root, inventory, proofs))
}

fn digest(path: &Path, expected: u64) -> Result<Digest> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let before = file.metadata()?;
    if !before.is_file()
        || before.uid() != unsafe { libc::geteuid() }
        || before.nlink() != 1
        || before.len() != expected
    {
        return Err(RestoreError::Invalid("rebuilt native member"));
    }
    let mut buffer = vec![0; cloud_backup_protocol::limits::STREAM_BUFFER_MAX_BYTES as usize];
    let mut left = expected;
    let mut digest = Sha256::new();
    while left != 0 {
        let maximum = left.min(buffer.len() as u64) as usize;
        let count = file.read(&mut buffer[..maximum])?;
        if count == 0 {
            return Err(RestoreError::Invalid("short rebuilt native file"));
        }
        digest.update(&buffer[..count]);
        left -= count as u64;
    }
    let after = file.metadata()?;
    if file.read(&mut buffer[..1])? != 0
        || before.dev() != after.dev()
        || before.ino() != after.ino()
        || before.len() != after.len()
        || before.mtime() != after.mtime()
        || before.mtime_nsec() != after.mtime_nsec()
    {
        return Err(RestoreError::Invalid("rebuilt native member changed"));
    }
    Ok(Digest::try_from(hex::encode(digest.finalize()))?)
}
