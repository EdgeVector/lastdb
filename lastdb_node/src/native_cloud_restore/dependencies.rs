//! Assemble exact backup-owned file ciphertext; keys remain in native atoms.
use super::{download, RestoreError, RestoreSession, RestoreSource, Result};
use cloud_backup_protocol::access::ReadSelector;
use cloud_backup_protocol::transfer::ObjectReceipt;
use cloud_backup_protocol::{Digest, ExternalFileDependency, ObjectKind};
use fold_db::storage::disk_pack::file_recovery::{self, RecoveryDirectory, RecoveryFile};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;

pub(super) fn prepare(session: &RestoreSession) -> Result<()> {
    let accepted = session.accepted();
    file_recovery::prepare_directory(
        &data_root(session),
        &RecoveryDirectory {
            version: 1,
            home_id: accepted.native_root.home_id.clone(),
            source_current_sha256: accepted.native_root.current_sha256.clone(),
            commit_sha256: accepted.commit_sha256.clone(),
            account_public_key: accepted.recovery.account_public_key.clone(),
        },
    )
    .map_err(|_| RestoreError::Invalid("file recovery directory"))
}

pub(super) fn data_root(session: &RestoreSession) -> PathBuf {
    session.root.join("home/data")
}

pub(super) async fn restore_page(
    session: &RestoreSession,
    source: &dyn RestoreSource,
    dependencies: &[ExternalFileDependency],
    receipts: &BTreeMap<[u8; 32], ObjectReceipt>,
) -> Result<()> {
    let mut ids = BTreeSet::new();
    for dependency in dependencies {
        let entry = RecoveryFile::from_dependency(dependency)
            .map_err(|_| RestoreError::Unsupported("file recovery size or suite"))?;
        for extent in &dependency.extents {
            let receipt = receipts
                .get(&raw_digest(&extent.receipt_id)?)
                .ok_or(RestoreError::Invalid("file dependency receipt"))?;
            if receipt.object.kind != ObjectKind::BlobPart
                || extent
                    .offset
                    .get()
                    .checked_add(extent.length.get())
                    .is_none_or(|end| end > receipt.object.bytes.get())
            {
                return Err(RestoreError::Invalid("file dependency extent"));
            }
        }
        let path = file_recovery::object_path(&data_root(session), &entry.stored_source_sha256);
        if path.try_exists()? {
            download::verify_file(
                &path,
                &entry.stored_source_sha256,
                entry.stored_source_bytes.get(),
            )?;
        } else {
            download::check_capacity(&session.root, entry.stored_source_bytes.get())?;
            let pending = path.with_extension("pending");
            if pending.try_exists()? && !fs::symlink_metadata(&pending)?.is_file() {
                return Err(RestoreError::Invalid("file recovery temporary"));
            }
            OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW)
                .open(pending)?;
            ids.extend(
                dependency
                    .extents
                    .iter()
                    .map(|extent| extent.receipt_id.clone()),
            );
        }
    }
    // Every selector is known before RPCs. Metadata stays bounded to four grants.
    let ids: Vec<_> = ids.into_iter().collect();
    for batch in ids.chunks(4) {
        let selectors = batch
            .iter()
            .map(|receipt_id| ReadSelector::Receipt {
                receipt_id: receipt_id.clone(),
            })
            .collect();
        let grants = download::grants(session, source, selectors).await?;
        for (id, grant) in batch.iter().zip(grants) {
            grant.validate_for_receipt(&receipts[&raw_digest(id)?])?;
            let object = download::fetch(session, source, &grant, false).await?;
            copy_object(session, &object, id, dependencies)?;
            session.remove_spool_file(&object)?;
        }
    }
    for dependency in dependencies {
        install(session, dependency)?;
    }
    Ok(())
}

fn install(session: &RestoreSession, dependency: &ExternalFileDependency) -> Result<()> {
    let entry = RecoveryFile::from_dependency(dependency)
        .map_err(|_| RestoreError::Unsupported("file recovery bounds"))?;
    let root = data_root(session);
    let path = file_recovery::object_path(&root, &entry.stored_source_sha256);
    if !path.try_exists()? {
        let pending = path.with_extension("pending");
        download::verify_file(
            &pending,
            &entry.stored_source_sha256,
            entry.stored_source_bytes.get(),
        )?;
        File::open(&pending)?.sync_all()?;
        fs::rename(pending, &path)?;
    }
    // Repeat barriers after an uncertain previous rename before saving its map.
    File::open(&path)?.sync_all()?;
    File::open(
        path.parent()
            .ok_or(RestoreError::Invalid("file recovery parent"))?,
    )?
    .sync_all()?;
    file_recovery::save_entry(&root, &entry)
        .map_err(|_| RestoreError::Invalid("file recovery entry"))
}

fn copy_object(
    session: &RestoreSession,
    object: &std::path::Path,
    id: &Digest,
    dependencies: &[ExternalFileDependency],
) -> Result<()> {
    let mut input = File::open(object)?;
    let mut buffer = [0u8; 65536];
    for dependency in dependencies {
        let path =
            file_recovery::object_path(&data_root(session), &dependency.stored_source_sha256);
        if path.try_exists()? {
            continue;
        }
        let mut offset = 0;
        for extent in &dependency.extents {
            if extent.receipt_id == *id {
                download::check_capacity(&session.root, extent.length.get())?;
                let mut output = OpenOptions::new()
                    .write(true)
                    .custom_flags(libc::O_NOFOLLOW)
                    .open(path.with_extension("pending"))?;
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
                .ok_or(RestoreError::Limit("file dependency extent sum"))?;
        }
    }
    Ok(())
}

fn raw_digest(digest: &Digest) -> Result<[u8; 32]> {
    hex::decode(digest.as_str())
        .map_err(|_| RestoreError::Invalid("digest"))?
        .try_into()
        .map_err(|_| RestoreError::Invalid("digest"))
}
