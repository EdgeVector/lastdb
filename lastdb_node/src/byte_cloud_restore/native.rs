use super::{
    download, inventory::Inventory, local, ByteBackupClient, NativeFileProof, RestoreError,
    RestoreSession, Result,
};
use cloud_backup_protocol::v2::access::ReadSelector;
use cloud_backup_protocol::Digest;
use fold_db::storage::disk_pack::byte_backup::ClientNativeFile;
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

pub(super) async fn restore_page(
    session: &RestoreSession,
    client: &ByteBackupClient,
    inventory: &mut Inventory,
    entries: &[ClientNativeFile],
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
        if prepare(session, entry)? {
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
            copy_object(session, &object, id, entries)?;
            session.remove_spool_file(&object)?;
        }
    }
    for entry in entries {
        install(session, entry)?;
    }
    Ok(())
}

fn prepare(session: &RestoreSession, entry: &ClientNativeFile) -> Result<bool> {
    let path = session.physical().join(entry.path.as_str());
    create_parents(session, entry.path.as_str())?;
    if path.try_exists()? {
        crate::native_cloud_restore::verify_file(
            &path,
            &entry.native_sha256,
            entry.native_bytes.get(),
        )?;
        return Ok(false);
    }
    crate::native_cloud_restore::check_capacity(&session.root, entry.native_bytes.get())?;
    let pending = pending(session, entry.path.as_str());
    local::writable_file(&pending, true)?;
    Ok(true)
}

fn create_parents(session: &RestoreSession, path: &str) -> Result<()> {
    let mut parent = session.physical();
    let components: Vec<_> = path.split('/').collect();
    for component in &components[..components.len() - 1] {
        parent.push(component);
        match fs::DirBuilder::new().mode(0o700).create(&parent) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        if !fs::symlink_metadata(&parent)?.is_dir() {
            return Err(RestoreError::Invalid("native parent"));
        }
    }
    Ok(())
}

fn copy_object(
    session: &RestoreSession,
    object: &Path,
    id: &Digest,
    entries: &[ClientNativeFile],
) -> Result<()> {
    let mut input = File::open(object)?;
    let mut buffer = vec![0u8; cloud_backup_protocol::limits::STREAM_BUFFER_MAX_BYTES as usize];
    for entry in entries {
        if session.physical().join(entry.path.as_str()).try_exists()? {
            continue;
        }
        let mut offset = 0u64;
        for extent in &entry.extents {
            if extent.receipt_id == *id {
                crate::native_cloud_restore::check_capacity(&session.root, extent.length.get())?;
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

fn install(session: &RestoreSession, entry: &ClientNativeFile) -> Result<()> {
    let path = session.physical().join(entry.path.as_str());
    if !path.try_exists()? {
        let pending = pending(session, entry.path.as_str());
        crate::native_cloud_restore::verify_file(
            &pending,
            &entry.native_sha256,
            entry.native_bytes.get(),
        )?;
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
        let saved: NativeFileProof = local::read_json(&ledger, 256 * 1024)?;
        if saved.path != proof.path || saved.sha256 != proof.sha256 || saved.bytes != proof.bytes {
            return Err(RestoreError::Invalid("saved native proof"));
        }
    } else {
        session.remove_spool_file(&ledger.with_extension("pending"))?;
        session.reserve_spool(4096)?;
        if let Err(error) = local::atomic_json(&ledger, &proof) {
            if ledger.with_extension("pending").try_exists()? {
                session.add_spool_file(&ledger.with_extension("pending"))?;
            }
            return Err(error);
        }
        session.add_spool_file(&ledger)?;
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
