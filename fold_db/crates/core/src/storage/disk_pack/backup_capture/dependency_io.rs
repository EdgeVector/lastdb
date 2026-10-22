use super::super::file_dependencies::FILE_DEPENDENCY_MAX_STORED_BYTES;
use super::dependency_store::DependencyStore;
use super::store::regular;
use super::{invalid, ExportInner, StorageResult};
use cloud_backup_protocol::{bytes_sha256, ObjectIdentity};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};

pub(super) fn existing_blob(
    store: &DependencyStore,
    index: u64,
    expected: u64,
) -> StorageResult<Option<Vec<u8>>> {
    let path = store.blob_path(index);
    if !path.try_exists()? {
        return Ok(None);
    }
    let file = regular(&path)?;
    if file.metadata()?.len() != expected || expected > FILE_DEPENDENCY_MAX_STORED_BYTES {
        return Err(invalid("backup file spool length differs"));
    }
    let mut bytes = Vec::with_capacity(expected as usize);
    file.take(expected + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 != expected {
        return Err(invalid("backup file spool changed"));
    }
    Ok(Some(bytes))
}

pub(super) fn verify_blob(
    store: &DependencyStore,
    index: u64,
    object: &ObjectIdentity,
) -> StorageResult<bool> {
    let Some(bytes) = existing_blob(store, index, object.bytes.get())? else {
        return Ok(false);
    };
    if bytes_sha256(&bytes) != object.sha256 {
        return Err(invalid("backup file spool digest differs"));
    }
    Ok(true)
}

pub(super) fn save_blob(
    inner: &ExportInner,
    store: &DependencyStore,
    index: u64,
    bytes: &[u8],
) -> StorageResult<()> {
    if bytes.len() as u64 > FILE_DEPENDENCY_MAX_STORED_BYTES {
        return Err(invalid("backup file spool bound"));
    }
    let _space = inner
        .runtime
        .journal
        .lock()
        .map_err(|_| invalid("backup journal lock failed"))?
        .reserve_space(bytes.len() as u64 + 65536, &inner.runtime.space_policy)
        .map_err(super::super::map)?;
    let path = store.pending();
    if path.try_exists()? {
        regular(&path)?;
    }
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(path, store.blob_path(index))?;
    File::open(&store.root)?.sync_all()?;
    Ok(())
}
