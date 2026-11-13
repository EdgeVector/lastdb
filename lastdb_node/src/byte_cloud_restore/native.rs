//! Owned local restore session filesystem, checkpoint routing, and native file installation.
use super::{
    check_capacity, download,
    logs::{self, MutationLogRoot, LOG_VERSION, MAX_BATCH_ENTRIES},
    runs::{stage_pages, Inventory},
    verify_file, ByteBackupReader, NativeFileProof, RecoveryDecryption, RestoreError,
    RestoreSession, Result,
};
use crate::cloud::{is_owned_dir, open_private_path, read_private_path, PrivatePathError};
use cloud_backup_protocol::v2::{
    limits::{CONTROL_JSON_MAX_BYTES, MAX_BATCH_ITEMS},
    AcceptedBackup, Begin, Destination,
};
use cloud_backup_protocol::{bytes_sha256, canonical_bytes, Digest};
use fold_db::cloud_backup_plan::ClientNativeFile;
use fold_db::storage::disk_pack::{
    byte_backup::{decode_metadata, validate_encoded_page},
    run_restore, CAS_DIR, RUN_DIR,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MutationLogPage {
    pub version: u16,
    pub page_index: u32,
    pub begin_sha256: Digest,
    pub descriptor_sha256: Digest,
    pub files: Vec<ClientNativeFile>,
}

impl MutationLogPage {
    fn decode(bytes: &[u8], key: &[u8; 32], begin: &Begin) -> Result<Self> {
        let page: Self =
            decode_metadata(bytes, key).map_err(|_| RestoreError::Invalid("mutation file page"))?;
        let ctx = (&page.begin_sha256, &page.descriptor_sha256);
        validate_encoded_page(
            &page,
            (page.version, LOG_VERSION),
            page.page_index,
            ctx,
            &page.files,
            begin,
        )
        .map_err(|_| RestoreError::Invalid("mutation file page"))?;
        logs::require_log_destinations(begin).map(|()| page)
    }
}

fn private_err(err: PrivatePathError, limit: &'static str) -> RestoreError {
    match err {
        PrivatePathError::Io(e) => RestoreError::Io(e),
        PrivatePathError::Invalid => RestoreError::Invalid("owned session file"),
        PrivatePathError::Limit => RestoreError::Limit(limit),
    }
}

pub(super) fn sync_dir(path: &Path) -> Result<()> {
    Ok(File::open(path)?.sync_all()?)
}

pub(super) fn directory(path: &Path) -> Result<()> {
    is_owned_dir(&fs::symlink_metadata(path)?)
        .then_some(())
        .ok_or(RestoreError::Invalid("owned session directory"))
}

pub(super) fn owner_file(path: &Path, new: bool) -> Result<File> {
    open_private_path(path, new, new, false).map_err(|e| private_err(e, "metadata bytes"))
}

pub(super) fn writable_file(path: &Path) -> Result<File> {
    open_private_path(path, true, false, true).map_err(|e| private_err(e, "metadata bytes"))
}

pub(super) fn read_json<T: DeserializeOwned>(path: &Path, maximum: u64) -> Result<T> {
    let bytes = read_private_path(path, maximum)
        .map_err(|e| private_err(e, "metadata bytes"))?
        .ok_or_else(|| RestoreError::Io(io::Error::from(io::ErrorKind::NotFound)))?;
    serde_json::from_slice(&bytes).map_err(|_| RestoreError::Invalid("metadata JSON"))
}

pub(super) fn ensure_directory(path: &Path) -> Result<()> {
    if let Err(e) = fs::DirBuilder::new().mode(0o700).create(path) {
        if e.kind() != io::ErrorKind::AlreadyExists {
            return Err(e.into());
        }
    }
    directory(path)
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

pub(super) fn prepare_native_ephemera(data: &Path, key: &[u8; 32]) -> Result<()> {
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
    run_restore::install_empty_cas_home(data, key).map_err(RestoreError::native_proof)
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
        return verify_file(&path, &entry.native_sha256, entry.native_bytes.get()).map(|()| false);
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
    let dst = session.root.join("entries").join(path_key(rel));
    session.save_or_verify_spool_json(&dst, 4096, &proof, "saved native proof")
}

fn pending_path(session: &RestoreSession, path: &str) -> PathBuf {
    session
        .root
        .join(format!("native-{}.pending", path_key(path)))
}

fn check_route_entries(root: &MutationLogRoot) -> Result<u64> {
    let metadata_objects = u64::try_from(root.pages.len())
        .ok()
        .and_then(|c| c.checked_add(1))
        .ok_or(RestoreError::Limit("mutation route metadata count"))?;
    if !(1..=MAX_BATCH_ENTRIES).contains(&root.entries.len()) {
        return Err(RestoreError::Invalid("mutation route entry bound"));
    }
    let mut pos = root.from.clone();
    for entry in &root.entries {
        pos = entry
            .position_after(&pos)
            .map_err(|_| RestoreError::Invalid("mutation route entry identity"))?;
    }
    (pos == root.through)
        .then_some(metadata_objects)
        .ok_or(RestoreError::Invalid("mutation route complete position"))
}

fn decode_log_page(
    totals: &mut (u64, u64),
    accepted: &AcceptedBackup,
    root: &MutationLogRoot,
    key: &[u8; 32],
    index: u32,
    bytes: &[u8],
) -> Result<Vec<(ClientNativeFile, Destination)>> {
    let page = MutationLogPage::decode(bytes, key, &accepted.begin)?;
    root.validate_page(&page)?;
    if page.page_index != index {
        return Err(RestoreError::Invalid("mutation page order"));
    }
    for file in &page.files {
        if file.path.as_str().split('/').nth(3) != Some("main") {
            return Err(RestoreError::Invalid("mutation route role"));
        }
        totals.0 = totals
            .0
            .checked_add(1)
            .ok_or(RestoreError::Limit("mutation route total"))?;
        totals.1 = totals
            .1
            .checked_add(file.native_bytes.get())
            .ok_or(RestoreError::Limit("mutation route total"))?;
    }
    if totals.0 > root.native_file_count || totals.1 > root.native_bytes {
        return Err(RestoreError::Invalid("mutation file totals"));
    }
    page.files
        .into_iter()
        .map(|f| {
            root.validate_file(&f)
                .map(|d| (f, d))
                .map_err(|_| RestoreError::Invalid("mutation native file identity"))
        })
        .collect()
}

pub(super) async fn download_checkpoint(
    session: &RestoreSession,
    client: &ByteBackupReader,
    decryption: &RecoveryDecryption,
    root: &MutationLogRoot,
    inventory: Inventory,
    link_index: u32,
) -> Result<()> {
    let accepted = session.accepted();
    let objects = u64::try_from(inventory.receipts.len())
        .map_err(|_| RestoreError::Limit("mutation route object count"))?;
    let metadata_objects = check_route_entries(root)?;
    let mut totals = (0u64, 0u64);
    let target = session.root.join("replay-files");
    stage_pages(session, client, &root.pages, inventory, &target, |i, b| {
        decode_log_page(&mut totals, accepted, root, &decryption.content, i, b)
    })
    .await?;
    if totals != (root.native_file_count, root.native_bytes)
        || objects != accepted.inventory_root.object_count.get()
        || metadata_objects > objects
    {
        return Err(RestoreError::Invalid("complete mutation route totals"));
    }
    (read_json::<AcceptedBackup>(&logs::link_path(session, link_index), 256 * 1024)? == *accepted)
        .then_some(())
        .ok_or(RestoreError::Invalid("mutation route accepted link"))
}
