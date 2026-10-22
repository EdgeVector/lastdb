//! Complete only an owned, inactive restore wrapper. No source record is rewritten.
use super::{initialization::NODE_BYTES, map, StorageError, StorageResult, NODE_MARKER};
use laststore::disk_pack::backup::{prepare_snapshot_directories, validate_snapshot};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

/// Validate the native root, check its logical format through the account key,
/// and create the normal node marker/fence. Retain the returned lock until publish.
/// The caller owns a fresh inactive wrapper; it separately certifies file coverage.
pub fn prepare_restored_node(root: &Path, account_key: &[u8; 32]) -> StorageResult<File> {
    if !std::fs::symlink_metadata(root)?.is_dir() {
        return Err(refusal("restore data root is not a directory"));
    }
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        if !matches!(
            entry.file_name().to_str(),
            Some("disk-pack" | NODE_MARKER | "laststore-layout-v1" | "native-file-recovery-v1")
        ) {
            return Err(refusal("unexpected restore wrapper member"));
        }
        if entry.file_type()?.is_symlink() {
            return Err(refusal("restore wrapper contains a symlink"));
        }
    }
    let physical = root.join("disk-pack");
    let validated = validate_snapshot(&physical).map_err(map)?;
    let home = laststore::disk_pack::home::DiskHome::open(&physical).map_err(map)?;
    let mut reader = home
        .read(laststore::disk_pack::ReadBudget {
            pages: 4096,
            blocks: 16384,
            bytes: 8 * 1024 * 1024,
            keys: 1,
        })
        .map_err(map)?;
    let key = crate::db_operations::atom_store::AtomStore::CURRENT_KEY_FORMAT_KEY.as_bytes();
    let route = super::super::laststore::native_routing::write_collection(key);
    let stored = reader
        .get(route, &super::super::laststore::native_routing::encode(key))
        .map_err(map)?
        .ok_or_else(|| refusal("native logical format marker is missing"))?;
    let plain = crate::crypto::at_rest::open_at_rest_bounded(account_key, &stored, 4096)
        .map_err(|_| refusal("native logical format marker cannot be opened"))?;
    if plain != crate::db_operations::atom_store::key_format::KEY_FORMAT {
        return Err(refusal("unsupported restored logical format"));
    }
    drop(reader);
    prepare_snapshot_directories(&physical, &validated.current).map_err(map)?;
    let marker = root.join(NODE_MARKER);
    regular_or_absent(&marker)?;
    let mut lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&marker)?;
    lock.try_lock()
        .map_err(|_| refusal("restore wrapper is already owned"))?;
    finish_file(&mut lock, NODE_BYTES)?;
    let fence = root.join("laststore-layout-v1");
    regular_or_absent(&fence)?;
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(fence)?;
    finish_file(&mut file, laststore::disk_pack::home::LEGACY_FENCE_BYTES)?;
    File::open(root)?.sync_all()?;
    Ok(lock)
}

fn regular_or_absent(path: &Path) -> StorageResult<()> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if !meta.is_file() => Err(refusal("restore marker is not a regular file")),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn finish_file(file: &mut File, expected: &[u8]) -> StorageResult<()> {
    let length = file.metadata()?.len();
    if length > expected.len() as u64 {
        return Err(refusal("restore marker differs"));
    }
    let mut bytes = vec![0; length as usize];
    file.read_exact(&mut bytes)?;
    if !expected.starts_with(&bytes) {
        return Err(refusal("restore marker differs"));
    }
    if length != expected.len() as u64 {
        file.seek(SeekFrom::Start(0))?;
        file.write_all(expected)?;
    }
    // An earlier write can return an uncertain disk barrier with complete bytes.
    file.sync_all()?;
    Ok(())
}

fn refusal(detail: &str) -> StorageError {
    StorageError::InvalidOperation(detail.into())
}

/// Check one offline atom-namespace coordinate with the normal current-key law.
/// Unknown or retired spellings cannot hide an unexamined file-pointer body.
pub fn validate_restored_atom_key(key: &str) -> StorageResult<()> {
    let raw = super::super::laststore::native_routing::decode(key)?;
    super::super::current_keys::validate("main", &raw)?;
    if super::super::laststore::native_routing::write_collection(&raw) != "atoms" {
        return Err(refusal("unsupported key in restored atom namespace"));
    }
    Ok(())
}
