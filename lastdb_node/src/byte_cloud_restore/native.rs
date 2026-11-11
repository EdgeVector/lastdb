//! Native file staging and owned local session filesystem primitives.
use super::{
    check_capacity, download, inventory::Inventory, verify_file, ByteBackupReader, NativeFileProof,
    RestoreError, RestoreSession, Result,
};
use cloud_backup_protocol::v2::access::ReadSelector;
use cloud_backup_protocol::Digest;
use fold_db::cloud_backup_plan::ClientNativeFile;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

pub(crate) fn directory(path: &Path) -> Result<()> {
    let meta = fs::symlink_metadata(path)?;
    if !meta.is_dir() || meta.uid() != unsafe { libc::geteuid() } || meta.mode() & 0o077 != 0 {
        return Err(RestoreError::Invalid("owned session directory"));
    }
    Ok(())
}

pub(crate) fn owner_file(path: &Path, new: bool) -> Result<File> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW);
    if new {
        options.create_new(true);
    }
    let file = options.open(path)?;
    validate_file(&file)?;
    Ok(file)
}

pub(crate) fn read_json<T: DeserializeOwned>(path: &Path, maximum: u64) -> Result<T> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    validate_file(&file)?;
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(maximum + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > maximum {
        return Err(RestoreError::Limit("metadata bytes"));
    }
    serde_json::from_slice(&bytes).map_err(|_| RestoreError::Invalid("metadata JSON"))
}

pub(crate) fn ensure_directory(path: &Path) -> Result<()> {
    match fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    directory(path)
}

pub(crate) fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes = cloud_backup_protocol::canonical_bytes(value)?;
    if bytes.len() as u64 > cloud_backup_protocol::v2::limits::CONTROL_JSON_MAX_BYTES {
        return Err(RestoreError::Limit("saved metadata"));
    }
    let pending = path.with_extension("pending");
    let mut file = writable_file(&pending, true)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(pending, path)?;
    File::open(
        path.parent()
            .ok_or(RestoreError::Invalid("metadata parent"))?,
    )?
    .sync_all()?;
    Ok(())
}

fn validate_file(file: &File) -> Result<()> {
    let meta = file.metadata()?;
    if !meta.is_file()
        || meta.uid() != unsafe { libc::geteuid() }
        || meta.mode() & 0o077 != 0
        || meta.nlink() != 1
    {
        return Err(RestoreError::Invalid("owned session file"));
    }
    Ok(())
}

pub(crate) fn writable_file(path: &Path, truncate: bool) -> Result<File> {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    validate_file(&file)?;
    if truncate {
        file.set_len(0)?;
    }
    Ok(file)
}

pub(crate) fn prepare_native_ephemera(data: &Path) -> Result<()> {
    for name in [
        fold_db::storage::disk_pack::RUN_DIR,
        fold_db::storage::disk_pack::CAS_DIR,
    ] {
        let home = data.join(name);
        if name == fold_db::storage::disk_pack::CAS_DIR && !home.try_exists()? {
            continue;
        }
        directory(&home)?;
        ensure_directory(&home.join(laststore::disk_run::home::RUNS))?;
        let path = home.join(laststore::disk_run::home::LOCK);
        let lock = match owner_file(&path, true) {
            Ok(lock) => lock,
            Err(RestoreError::Io(error)) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                owner_file(&path, false)?
            }
            Err(error) => return Err(error),
        };
        if lock.metadata()?.len() != 0 {
            return Err(RestoreError::Invalid("run ephemeral lock"));
        }
        lock.sync_all()?;
        File::open(&home)?.sync_all()?;
    }
    Ok(())
}

pub(super) async fn restore_at(
    session: &RestoreSession,
    client: &ByteBackupReader,
    inventory: &mut Inventory,
    entries: &[ClientNativeFile],
    root: &Path,
) -> Result<()> {
    let mut ids = BTreeSet::new();
    for entry in entries {
        inventory.path(format!("native:{}", entry.path.as_str()).as_bytes())?;
        for extent in &entry.extents {
            let receipt = inventory.receipt(&extent.receipt_id)?;
            if extent
                .offset
                .get()
                .checked_add(extent.length.get())
                .is_none_or(|end| end > receipt.object.bytes.get())
            {
                return Err(RestoreError::Invalid("native extent"));
            }
            inventory.use_receipt(&extent.receipt_id)?;
        }
        if prepare(session, root, entry)? {
            ids.extend(entry.extents.iter().map(|extent| extent.receipt_id.clone()));
        }
    }
    let ids: Vec<_> = ids.into_iter().collect();
    for batch in ids.chunks(cloud_backup_protocol::v2::limits::MAX_BATCH_ITEMS) {
        let selectors = batch
            .iter()
            .map(|receipt_id| ReadSelector::Receipt {
                receipt_id: receipt_id.clone(),
            })
            .collect();
        let grants = download::grants(session, client, selectors).await?;
        for (id, grant) in batch.iter().zip(grants) {
            grant.validate_for_receipt(inventory.receipt(id)?)?;
            let object = download::fetch(session, client, &grant, false).await?;
            copy_object(session, root, &object, id, entries)?;
            session.remove_spool_file(&object)?;
        }
    }
    for entry in entries {
        install(session, root, entry)?;
    }
    Ok(())
}

fn prepare(session: &RestoreSession, root: &Path, entry: &ClientNativeFile) -> Result<bool> {
    let path = root.join(entry.path.as_str());
    create_parents(root, entry.path.as_str())?;
    if path.try_exists()? {
        verify_file(&path, &entry.native_sha256, entry.native_bytes.get())?;
        return Ok(false);
    }
    check_capacity(&session.root, entry.native_bytes.get())?;
    let pending = pending(session, entry.path.as_str());
    writable_file(&pending, true)?;
    Ok(true)
}

fn create_parents(root: &Path, path: &str) -> Result<()> {
    let mut parent = root.to_path_buf();
    let components: Vec<_> = path.split('/').collect();
    for component in &components[..components.len() - 1] {
        parent.push(component);
        ensure_directory(&parent)?;
    }
    Ok(())
}

fn copy_object(
    session: &RestoreSession,
    root: &Path,
    object: &Path,
    id: &Digest,
    entries: &[ClientNativeFile],
) -> Result<()> {
    let mut input = File::open(object)?;
    let mut buffer = vec![0u8; cloud_backup_protocol::limits::STREAM_BUFFER_MAX_BYTES as usize];
    for entry in entries {
        if root.join(entry.path.as_str()).try_exists()? {
            continue;
        }
        let mut offset = 0u64;
        for extent in &entry.extents {
            if extent.receipt_id == *id {
                check_capacity(&session.root, extent.length.get())?;
                let mut output = OpenOptions::new()
                    .write(true)
                    .custom_flags(libc::O_NOFOLLOW)
                    .open(pending(session, entry.path.as_str()))?;
                output.seek(SeekFrom::Start(offset))?;
                input.seek(SeekFrom::Start(extent.offset.get()))?;
                let mut left = extent.length.get();
                while left > 0 {
                    let count = left.min(buffer.len() as u64) as usize;
                    input.read_exact(&mut buffer[..count])?;
                    output.write_all(&buffer[..count])?;
                    left -= count as u64;
                }
                output.sync_all()?;
            }
            offset = offset
                .checked_add(extent.length.get())
                .ok_or(RestoreError::Limit("native extent sum"))?;
        }
        if offset != entry.native_bytes.get() {
            return Err(RestoreError::Invalid("native extent sum"));
        }
    }
    Ok(())
}

fn install(session: &RestoreSession, root: &Path, entry: &ClientNativeFile) -> Result<()> {
    let path = root.join(entry.path.as_str());
    if !path.try_exists()? {
        let pending = pending(session, entry.path.as_str());
        verify_file(&pending, &entry.native_sha256, entry.native_bytes.get())?;
        File::open(&pending)?.sync_all()?;
        fs::rename(pending, &path)?;
    }
    File::open(&path)?.sync_all()?;
    File::open(
        path.parent()
            .ok_or(RestoreError::Invalid("native parent"))?,
    )?
    .sync_all()?;
    let ledger = session
        .root
        .join("entries")
        .join(path_key(entry.path.as_str()));
    let proof = NativeFileProof {
        path: entry.path.as_str().into(),
        sha256: entry.native_sha256.clone(),
        bytes: entry.native_bytes.get(),
    };
    if ledger.try_exists()? {
        let saved: NativeFileProof = read_json(&ledger, 256 * 1024)?;
        if saved.path != proof.path || saved.sha256 != proof.sha256 || saved.bytes != proof.bytes {
            return Err(RestoreError::Invalid("saved native proof"));
        }
    } else {
        session.write_spool_json(&ledger, 4096, &proof)?;
    }
    Ok(())
}

pub(super) fn path_key(path: &str) -> String {
    cloud_backup_protocol::bytes_sha256(path.as_bytes())
        .as_str()
        .into()
}

fn pending(session: &RestoreSession, path: &str) -> PathBuf {
    session
        .root
        .join(format!("native-{}.pending", path_key(path)))
}
