//! Object downloads and local restore spool accounting.
use super::{
    local, reader_error, runs::Inventory, ByteBackupReader, RestoreError, RestoreSession, Result,
};
use cloud_backup_protocol::limits::{
    DURABLE_LOCAL_SPOOL_MAX_BYTES, NATIVE_FILES_MAX, STREAM_BUFFER_MAX_BYTES,
};
use cloud_backup_protocol::v2::{
    access::{ReadGrant, ReadGrantItemResult, ReadGrantRequest, ReadSelector},
    limits::MAX_PAGES,
    Version,
};
use cloud_backup_protocol::Digest;
use serde::{de::DeserializeOwned, Serialize};
use sha2::{Digest as _, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use tokio::io::AsyncReadExt;

const MAX_SPOOL: u64 = DURABLE_LOCAL_SPOOL_MAX_BYTES
    - fold_db::storage_policy::backup_decode::RESTORE_SPOOL_RESERVE_BYTES;

pub(super) async fn grants(
    session: &RestoreSession,
    client: &ByteBackupReader,
    objects: Vec<ReadSelector>,
) -> Result<Vec<ReadGrant>> {
    let req = ReadGrantRequest {
        version: Version::V2,
        job: session.accepted().job().clone(),
        objects,
    };
    req.validate()?;
    let resp = client
        .read_grants(&req, session.accepted())
        .await
        .map_err(|e| reader_error(client, &e))?;
    resp.validate_for(&req, session.accepted())?;
    resp.items
        .into_iter()
        .map(|item| match item {
            ReadGrantItemResult::Ok { grant, .. } => Ok(grant),
            ReadGrantItemResult::Error { .. } => Err(RestoreError::Source("object grant")),
        })
        .collect()
}

pub(super) async fn receipt_grants(
    session: &RestoreSession,
    client: &ByteBackupReader,
    inventory: &Inventory,
    ids: &[Digest],
) -> Result<Vec<ReadGrant>> {
    let sels = ids
        .iter()
        .cloned()
        .map(|receipt_id| ReadSelector::Receipt { receipt_id })
        .collect();
    let batch = grants(session, client, sels).await?;
    for (id, grant) in ids.iter().zip(&batch) {
        grant.validate_for_receipt(inventory.receipt(id)?)?;
    }
    Ok(batch)
}

pub(super) async fn one(
    session: &RestoreSession,
    client: &ByteBackupReader,
    selector: ReadSelector,
) -> Result<ReadGrant> {
    grants(session, client, vec![selector])
        .await?
        .into_iter()
        .next()
        .ok_or(RestoreError::Invalid("object grant count"))
}

pub(super) async fn fetch(
    session: &RestoreSession,
    client: &ByteBackupReader,
    grant: &ReadGrant,
    cache: bool,
) -> Result<PathBuf> {
    grant.validate()?;
    let name = if cache {
        format!("{}.object", grant.sha256.as_str())
    } else {
        "payload.object".into()
    };
    let path = session.root.join("cache").join(name);
    if cache && path.try_exists()? {
        verify_file(&path, &grant.sha256, grant.bytes.get())?;
        return Ok(path);
    }
    let tmp = session.root.join("cache/download.pending");
    session.remove_spool_file(&tmp)?;
    if !cache {
        session.remove_spool_file(&path)?;
    }
    let len = grant.bytes.get();
    session.reserve_spool(len)?;
    check_capacity(&session.root, len)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&tmp)?;
    if let Err(error) = stream_to(client, grant, &mut file).await {
        file.sync_all()?;
        session.add_spool_file(&tmp)?;
        return Err(error);
    }
    file.sync_all()?;
    fs::rename(&tmp, &path)?;
    local::sync_dir(&session.root.join("cache"))?;
    session.add_spool_file(&path).map(|()| path)
}

async fn stream_to(client: &ByteBackupReader, grant: &ReadGrant, file: &mut File) -> Result<()> {
    let mut stream = client
        .open(grant)
        .await
        .map_err(|_| RestoreError::Source("byte transport"))?;
    let mut buf = vec![0u8; STREAM_BUFFER_MAX_BYTES as usize];
    let (mut left, mut digest) = (grant.bytes.get(), Sha256::new());
    while left > 0 {
        let cap = left.min(buf.len() as u64) as usize;
        let count = stream.read(&mut buf[..cap]).await?;
        if count == 0 {
            return Err(RestoreError::Invalid("short object"));
        }
        file.write_all(&buf[..count])?;
        digest.update(&buf[..count]);
        left -= count as u64;
    }
    if stream.read(&mut buf[..1]).await? != 0 {
        return Err(RestoreError::Invalid("long object"));
    }
    if hex::encode(digest.finalize()) != grant.sha256.as_str() {
        return Err(RestoreError::Invalid("object digest"));
    }
    Ok(())
}

impl RestoreSession {
    fn checked_spool(&self, add: u64) -> Result<u64> {
        self.spool_bytes
            .load(Ordering::Relaxed)
            .checked_add(add)
            .filter(|&n| n <= MAX_SPOOL)
            .ok_or(RestoreError::Limit("spool bytes"))
    }

    pub(super) fn reserve_spool(&self, bytes: u64) -> Result<()> {
        let alloc = bytes
            .div_ceil(4096)
            .checked_mul(4096)
            .ok_or(RestoreError::Limit("spool bytes"))?;
        self.checked_spool(alloc).map(|_| ())
    }

    pub(super) fn add_spool_file(&self, path: &Path) -> Result<()> {
        let bytes =
            allocated_if_present(path)?.ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;
        self.spool_bytes
            .store(self.checked_spool(bytes)?, Ordering::Relaxed);
        Ok(())
    }

    pub(super) fn remove_spool_file(&self, path: &Path) -> Result<()> {
        if let Some(bytes) = allocated_if_present(path)? {
            fs::remove_file(path)?;
            let prev = self.spool_bytes.load(Ordering::Relaxed);
            self.spool_bytes
                .store(prev.saturating_sub(bytes), Ordering::Relaxed);
        }
        Ok(())
    }

    pub(super) fn write_spool_json(
        &self,
        path: &Path,
        reserve: u64,
        value: &impl Serialize,
    ) -> Result<()> {
        let previous = allocated_if_present(path)?.unwrap_or(0);
        let pending = path.with_extension("pending");
        self.remove_spool_file(&pending)?;
        self.reserve_spool(reserve)?;
        if let Err(error) = local::atomic_json(path, value) {
            if pending.try_exists()? {
                self.add_spool_file(&pending)?;
            }
            return Err(error);
        }
        self.spool_bytes.fetch_sub(previous, Ordering::Relaxed);
        self.add_spool_file(path)
    }

    pub(super) fn save_or_verify_spool_json<T: Serialize + DeserializeOwned + PartialEq>(
        &self,
        path: &Path,
        reserve: u64,
        value: &T,
        invalid: &'static str,
    ) -> Result<()> {
        if !path.try_exists()? {
            return self.write_spool_json(path, reserve, value);
        }
        if local::read_json::<T>(path, 256 * 1024)? != *value {
            return Err(RestoreError::Invalid(invalid));
        }
        Ok(())
    }

    pub(super) fn check_spool(&self) -> Result<()> {
        let (mut bytes, mut count, mut routes) = (0u64, 0usize, 0usize);
        for folder in ["cache", "entries", "chain"] {
            let dir = self.root.join(folder);
            local::directory(&dir)?;
            for entry in fs::read_dir(dir)? {
                let path = entry?.path();
                if route_entry(folder, &path)? {
                    routes += 1;
                } else {
                    count += 1;
                }
                if count > NATIVE_FILES_MAX + 2 * MAX_PAGES + 8
                    || routes > 2 * super::logs::MAX_CHAIN as usize
                {
                    return Err(RestoreError::Limit("spool entries"));
                }
                let used = allocated_if_present(&path)?
                    .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;
                bytes = bytes
                    .checked_add(used)
                    .filter(|&n| n <= MAX_SPOOL)
                    .ok_or(RestoreError::Limit("spool bytes"))?;
            }
        }
        self.spool_bytes.store(bytes, Ordering::Relaxed);
        Ok(())
    }
}

fn route_entry(folder: &str, path: &Path) -> Result<bool> {
    let Some(name) = path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .and_then(|n| n.strip_prefix("routes-"))
        .filter(|_| folder == "chain")
    else {
        return Ok(false);
    };
    name.strip_suffix(".json")
        .or_else(|| name.strip_suffix(".pending"))
        .filter(|s| s.len() == 8 && s.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|s| s.parse::<u32>().ok())
        .filter(|&i| i < super::logs::MAX_CHAIN)
        .map(|_| true)
        .ok_or(RestoreError::Invalid("mutation route spool name"))
}

fn allocated_if_present(path: &Path) -> Result<Option<u64>> {
    match fs::symlink_metadata(path) {
        Ok(m) if m.is_file() => m
            .blocks()
            .checked_mul(512)
            .map(Some)
            .ok_or(RestoreError::Limit("spool bytes")),
        Ok(_) => Err(RestoreError::Invalid("spool file")),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub(super) fn bounded_bytes(path: &Path, max: u64) -> Result<Vec<u8>> {
    if !fs::symlink_metadata(path)?.is_file() {
        return Err(RestoreError::Invalid("object file"));
    }
    let mut bytes = Vec::new();
    File::open(path)?.take(max + 1).read_to_end(&mut bytes)?;
    (bytes.len() as u64 <= max)
        .then_some(bytes)
        .ok_or(RestoreError::Limit("object decode"))
}

pub(super) fn verify_file(path: &Path, sha: &Digest, length: u64) -> Result<()> {
    let meta = fs::symlink_metadata(path)?;
    let mut digest = Sha256::new();
    if !meta.is_file()
        || meta.len() != length
        || io::copy(&mut File::open(path)?.take(length + 1), &mut digest)? != length
    {
        return Err(RestoreError::Invalid("file length"));
    }
    (hex::encode(digest.finalize()) == sha.as_str())
        .then_some(())
        .ok_or(RestoreError::Invalid("file digest"))
}

pub(super) fn check_capacity(root: &Path, bytes: u64) -> Result<()> {
    let vfs = rustix::fs::statvfs(root).map_err(io::Error::from)?;
    let avail = (vfs.f_bavail as u64).checked_mul(vfs.f_frsize as u64);
    let req = bytes.checked_add(laststore::space::SpacePolicy::default().free_space_reserve_bytes);
    avail
        .zip(req)
        .filter(|&(a, r)| a >= r)
        .map(|_| ())
        .ok_or(RestoreError::Limit("disk capacity"))
}
