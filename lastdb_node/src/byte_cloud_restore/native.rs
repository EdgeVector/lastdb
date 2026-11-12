//! Owned local restore session filesystem and native file installation primitives.
use super::{
    check_capacity, download, runs::Inventory, verify_file, ByteBackupReader, NativeFileProof,
    RestoreError, RestoreSession, Result,
};
use crate::offline_home::{is_owned_dir, is_private_file};
use cloud_backup_protocol::v2::limits::{CONTROL_JSON_MAX_BYTES, MAX_BATCH_ITEMS};
use cloud_backup_protocol::{bytes_sha256, canonical_bytes, Digest};
use fold_db::cloud_backup_plan::ClientNativeFile;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

pub(super) fn sync_dir(path: &Path) -> Result<()> {
    Ok(File::open(path)?.sync_all()?)
}

pub(super) fn directory(path: &Path) -> Result<()> {
    let m = fs::symlink_metadata(path)?;
    if !is_owned_dir(&m) || m.mode() & 0o077 != 0 {
        return Err(RestoreError::Invalid("owned session directory"));
    }
    Ok(())
}

fn validate_file(file: &File) -> Result<()> {
    if !is_private_file(&file.metadata()?) {
        return Err(RestoreError::Invalid("owned session file"));
    }
    Ok(())
}

pub(super) fn owner_file(path: &Path, new: bool) -> Result<File> {
    let mut opts = OpenOptions::new();
    opts.read(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW);
    if new {
        opts.create_new(true);
    }
    let file = opts.open(path)?;
    validate_file(&file).map(|()| file)
}

pub(super) fn read_json<T: DeserializeOwned>(path: &Path, maximum: u64) -> Result<T> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    validate_file(&file)?;
    let mut bytes = Vec::new();
    file.take(maximum + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > maximum {
        return Err(RestoreError::Limit("metadata bytes"));
    }
    serde_json::from_slice(&bytes).map_err(|_| RestoreError::Invalid("metadata JSON"))
}

pub(super) fn ensure_directory(path: &Path) -> Result<()> {
    match fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.into()),
    }
    directory(path)
}

pub(super) fn writable_file(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    validate_file(&file)?;
    file.set_len(0)?;
    Ok(file)
}

pub(super) fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes = canonical_bytes(value)?;
    if bytes.len() as u64 > CONTROL_JSON_MAX_BYTES {
        return Err(RestoreError::Limit("saved metadata"));
    }
    let pending = path.with_extension("pending");
    let mut file = writable_file(&pending)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(pending, path)?;
    sync_dir(
        path.parent()
            .ok_or(RestoreError::Invalid("metadata parent"))?,
    )
}

pub(super) fn prepare_native_ephemera(data: &Path) -> Result<()> {
    use fold_db::storage::disk_pack::{CAS_DIR, RUN_DIR};
    for name in [RUN_DIR, CAS_DIR] {
        let home = data.join(name);
        if name == CAS_DIR && !home.try_exists()? {
            continue;
        }
        directory(&home)?;
        ensure_directory(&home.join(laststore::disk_run::home::RUNS))?;
        let path = home.join(laststore::disk_run::home::LOCK);
        let lock = match owner_file(&path, true) {
            Err(RestoreError::Io(e)) if e.kind() == io::ErrorKind::AlreadyExists => {
                owner_file(&path, false)?
            }
            other => other?,
        };
        if lock.metadata()?.len() != 0 {
            return Err(RestoreError::Invalid("run ephemeral lock"));
        }
        lock.sync_all()?;
        sync_dir(&home)?;
    }
    Ok(())
}

pub(super) fn path_key(path: &str) -> String {
    bytes_sha256(path.as_bytes()).as_str().into()
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
        for ext in &entry.extents {
            inventory.use_receipt(&ext.receipt_id)?;
        }
        if prepare_file(session, root, entry)? {
            ids.extend(entry.extents.iter().map(|e| e.receipt_id.clone()));
        }
    }
    let ids: Vec<_> = ids.into_iter().collect();
    for batch in ids.chunks(MAX_BATCH_ITEMS) {
        let grants = download::receipt_grants(session, client, inventory, batch).await?;
        for (id, grant) in batch.iter().zip(grants) {
            let object = download::fetch(session, client, &grant, false).await?;
            copy_object(session, root, &object, id, entries)?;
            session.remove_spool_file(&object)?;
        }
    }
    entries
        .iter()
        .try_for_each(|e| install_file(session, root, e))
}

fn prepare_file(session: &RestoreSession, root: &Path, entry: &ClientNativeFile) -> Result<bool> {
    let rel = entry.path.as_str();
    if let Some((dir, _)) = rel.rsplit_once('/') {
        let mut parent = root.to_path_buf();
        for part in dir.split('/') {
            parent.push(part);
            ensure_directory(&parent)?;
        }
    }
    let path = root.join(rel);
    if path.try_exists()? {
        verify_file(&path, &entry.native_sha256, entry.native_bytes.get())?;
        return Ok(false);
    }
    check_capacity(&session.root, entry.native_bytes.get())?;
    writable_file(&pending_path(session, rel)).map(|_| true)
}

fn copy_object(
    session: &RestoreSession,
    root: &Path,
    object: &Path,
    id: &Digest,
    entries: &[ClientNativeFile],
) -> Result<()> {
    let mut input = File::open(object)?;
    for entry in entries {
        let rel = entry.path.as_str();
        if root.join(rel).try_exists()? {
            continue;
        }
        let mut offset = 0u64;
        for ext in &entry.extents {
            let len = ext.length.get();
            if ext.receipt_id == *id {
                check_capacity(&session.root, len)?;
                let mut out = OpenOptions::new()
                    .write(true)
                    .custom_flags(libc::O_NOFOLLOW)
                    .open(pending_path(session, rel))?;
                out.seek(SeekFrom::Start(offset))?;
                input.seek(SeekFrom::Start(ext.offset.get()))?;
                if io::copy(&mut (&mut input).take(len), &mut out)? != len {
                    return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
                }
                out.sync_all()?;
            }
            offset = offset
                .checked_add(len)
                .ok_or(RestoreError::Limit("native extent sum"))?;
        }
        if offset != entry.native_bytes.get() {
            return Err(RestoreError::Invalid("native extent sum"));
        }
    }
    Ok(())
}

fn install_file(session: &RestoreSession, root: &Path, entry: &ClientNativeFile) -> Result<()> {
    let (rel, path) = (entry.path.as_str(), root.join(entry.path.as_str()));
    if !path.try_exists()? {
        let pend = pending_path(session, rel);
        verify_file(&pend, &entry.native_sha256, entry.native_bytes.get())?;
        File::open(&pend)?.sync_all()?;
        fs::rename(pend, &path)?;
    }
    File::open(&path)?.sync_all()?;
    sync_dir(
        path.parent()
            .ok_or(RestoreError::Invalid("native parent"))?,
    )?;
    let proof = NativeFileProof {
        path: rel.into(),
        sha256: entry.native_sha256.clone(),
        bytes: entry.native_bytes.get(),
    };
    session.save_or_verify_spool_json(
        &session.root.join("entries").join(path_key(rel)),
        4096,
        &proof,
        "saved native proof",
    )
}

fn pending_path(session: &RestoreSession, path: &str) -> PathBuf {
    session
        .root
        .join(format!("native-{}.pending", path_key(path)))
}
