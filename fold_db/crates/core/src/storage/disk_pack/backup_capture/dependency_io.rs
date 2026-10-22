use super::dependency_store::DependencyStore;
use super::{invalid, ExportInner, StorageResult};
use cloud_backup_protocol::Digest;
use laststore::disk_pack::maintenance::SpaceReservation;
use sha2::{Digest as _, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

pub(super) fn input(path: &Path, expected: u64) -> StorageResult<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    if !file.metadata()?.is_file() || file.metadata()?.len() != expected {
        return Err(invalid("backup file spool length or type differs"));
    }
    Ok(file)
}

pub(super) fn existing_blob(
    store: &DependencyStore,
    index: u64,
    expected: u64,
) -> StorageResult<Option<File>> {
    let path = store.blob_path(index);
    if !path.try_exists()? {
        return Ok(None);
    }
    input(&path, expected).map(Some)
}

pub(super) fn verify_blob(
    store: &DependencyStore,
    index: u64,
    expected: u64,
    digest: &Digest,
) -> StorageResult<bool> {
    let Some(mut file) = existing_blob(store, index, expected)? else {
        return Ok(false);
    };
    let mut hash = Sha256::new();
    let mut buffer = vec![0; lastdb_storage_policy::BACKUP_RESOURCES.stream_buffer_bytes as usize];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    if crate::hex::hex_lower(hash.finalize()) != digest.as_str() {
        return Err(invalid("backup file spool digest differs"));
    }
    Ok(true)
}

/// Payload capacity uses filesystem headroom, independently of metadata spool.
pub(super) fn reserve(inner: &ExportInner, bytes: u64) -> StorageResult<SpaceReservation> {
    inner
        .runtime
        .journal
        .lock()
        .map_err(|_| invalid("backup journal lock failed"))?
        .reserve_space(
            bytes
                .checked_add(65536)
                .ok_or_else(|| invalid("backup payload size overflow"))?,
            &inner.runtime.space_policy,
        )
        .map_err(super::super::map)
}

pub(super) fn copy_local(
    store: &DependencyStore,
    expected: u64,
    source: &mut dyn Read,
) -> StorageResult<()> {
    store.release_pending()?;
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(store.pending())?;
    let mut buffer = vec![0; lastdb_storage_policy::BACKUP_RESOURCES.stream_buffer_bytes as usize];
    let mut remaining = expected;
    while remaining > 0 {
        let count = remaining.min(buffer.len() as u64) as usize;
        source.read_exact(&mut buffer[..count])?;
        output.write_all(&buffer[..count])?;
        remaining -= count as u64;
    }
    let mut extra = [0];
    if source.read(&mut extra)? != 0 {
        return Err(invalid("backup local source length changed"));
    }
    output.sync_all()?;
    Ok(())
}

pub(super) fn publish(store: &DependencyStore, index: u64, expected: u64) -> StorageResult<File> {
    let file = input(&store.pending(), expected)?;
    file.sync_all()?;
    fs::rename(store.pending(), store.blob_path(index))?;
    File::open(&store.root)?.sync_all()?;
    let mut file = input(&store.blob_path(index), expected)?;
    file.rewind()?;
    Ok(file)
}
