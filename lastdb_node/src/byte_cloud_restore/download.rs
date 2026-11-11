//! Object downloads, spool accounting, and bounded local file proofs.
use super::{local, reader_error, ByteBackupReader, RestoreError, RestoreSession, Result};
use cloud_backup_protocol::v2::access::{
    ReadGrant, ReadGrantItemResult, ReadGrantRequest, ReadSelector,
};
use cloud_backup_protocol::v2::Version;
use cloud_backup_protocol::Digest;
use sha2::{Digest as _, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use tokio::io::AsyncReadExt;

const MAX_SPOOL: u64 = cloud_backup_protocol::limits::DURABLE_LOCAL_SPOOL_MAX_BYTES
    - fold_db::storage_policy::backup_decode::RESTORE_SPOOL_RESERVE_BYTES;

pub(super) async fn grants(
    session: &RestoreSession,
    client: &ByteBackupReader,
    selectors: Vec<ReadSelector>,
) -> Result<Vec<ReadGrant>> {
    let request = ReadGrantRequest {
        version: Version::V2,
        job: session.accepted().job().clone(),
        objects: selectors,
    };
    request.validate()?;
    let response = client
        .read_grants(&request, session.accepted())
        .await
        .map_err(|error| reader_error(client, &error))?;
    response.validate_for(&request, session.accepted())?;
    response
        .items
        .into_iter()
        .map(|item| match item {
            ReadGrantItemResult::Ok { grant, .. } => Ok(grant),
            ReadGrantItemResult::Error { .. } => Err(RestoreError::Source("object grant")),
        })
        .collect()
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
    let temporary = session.root.join("cache/download.pending");
    session.remove_spool_file(&temporary)?;
    if !cache {
        session.remove_spool_file(&path)?;
    }
    session.reserve_spool(grant.bytes.get())?;
    check_capacity(&session.root, grant.bytes.get())?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&temporary)?;
    if let Err(error) = stream_to(client, grant, &mut file).await {
        file.sync_all()?;
        session.add_spool_file(&temporary)?;
        return Err(error);
    }
    file.sync_all()?;
    fs::rename(&temporary, &path)?;
    File::open(session.root.join("cache"))?.sync_all()?;
    session.add_spool_file(&path)?;
    Ok(path)
}

async fn stream_to(client: &ByteBackupReader, grant: &ReadGrant, file: &mut File) -> Result<()> {
    let mut stream = client
        .open(grant)
        .await
        .map_err(|_| RestoreError::Source("byte transport"))?;
    let mut buffer = vec![0u8; cloud_backup_protocol::limits::STREAM_BUFFER_MAX_BYTES as usize];
    let mut left = grant.bytes.get();
    let mut digest = Sha256::new();
    while left > 0 {
        let maximum = left.min(buffer.len() as u64) as usize;
        let count = stream.read(&mut buffer[..maximum]).await?;
        if count == 0 {
            return Err(RestoreError::Invalid("short object"));
        }
        file.write_all(&buffer[..count])?;
        digest.update(&buffer[..count]);
        left -= count as u64;
    }
    if stream.read(&mut buffer[..1]).await? != 0 {
        return Err(RestoreError::Invalid("long object"));
    }
    if hex::encode(digest.finalize()) != grant.sha256.as_str() {
        return Err(RestoreError::Invalid("object digest"));
    }
    Ok(())
}

impl RestoreSession {
    pub(super) fn reserve_spool(&self, bytes: u64) -> Result<()> {
        let allocated = bytes
            .div_ceil(4096)
            .checked_mul(4096)
            .ok_or(RestoreError::Limit("spool bytes"))?;
        if self
            .spool_bytes
            .load(Ordering::Relaxed)
            .checked_add(allocated)
            .is_none_or(|total| total > MAX_SPOOL)
        {
            return Err(RestoreError::Limit("spool bytes"));
        }
        Ok(())
    }
    pub(super) fn add_spool_file(&self, path: &Path) -> Result<()> {
        let bytes = allocated(path)?;
        let total = self
            .spool_bytes
            .load(Ordering::Relaxed)
            .checked_add(bytes)
            .ok_or(RestoreError::Limit("spool bytes"))?;
        if total > MAX_SPOOL {
            return Err(RestoreError::Limit("spool bytes"));
        }
        self.spool_bytes.store(total, Ordering::Relaxed);
        Ok(())
    }
    pub(super) fn remove_spool_file(&self, path: &Path) -> Result<()> {
        let Some(bytes) = allocated_if_present(path)? else {
            return Ok(());
        };
        fs::remove_file(path)?;
        self.spool_bytes.store(
            self.spool_bytes
                .load(Ordering::Relaxed)
                .saturating_sub(bytes),
            Ordering::Relaxed,
        );
        Ok(())
    }
    pub(super) fn write_spool_json(
        &self,
        path: &Path,
        reserve: u64,
        value: &impl serde::Serialize,
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
    pub(super) fn check_spool(&self) -> Result<()> {
        let mut bytes = 0u64;
        let mut count = 0usize;
        let mut routes = 0usize;
        for folder in ["cache", "entries", "chain"] {
            let path = self.root.join(folder);
            local::directory(&path)?;
            for entry in fs::read_dir(path)? {
                let path = entry?.path();
                if route_entry(folder, &path)? {
                    routes += 1;
                } else {
                    count += 1;
                }
                if count
                    > cloud_backup_protocol::limits::NATIVE_FILES_MAX
                        + 2 * cloud_backup_protocol::v2::limits::MAX_PAGES
                        + 8
                    || routes > 2 * super::logs::MAX_CHAIN as usize
                {
                    return Err(RestoreError::Limit("spool entries"));
                }
                bytes = bytes
                    .checked_add(allocated(&path)?)
                    .ok_or(RestoreError::Limit("spool bytes"))?;
                if bytes > MAX_SPOOL {
                    return Err(RestoreError::Limit("spool bytes"));
                }
            }
        }
        self.spool_bytes.store(bytes, Ordering::Relaxed);
        Ok(())
    }
}

fn route_entry(folder: &str, path: &Path) -> Result<bool> {
    let Some(name) = path
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("routes-"))
        .filter(|_| folder == "chain")
    else {
        return Ok(false);
    };
    let index = name
        .strip_suffix(".json")
        .or_else(|| name.strip_suffix(".pending"))
        .filter(|index| index.len() == 8 && index.bytes().all(|byte| byte.is_ascii_digit()))
        .and_then(|index| index.parse::<u32>().ok());
    if index.is_none_or(|index| index >= super::logs::MAX_CHAIN) {
        return Err(RestoreError::Invalid("mutation route spool name"));
    }
    Ok(true)
}

fn allocated_if_present(path: &Path) -> Result<Option<u64>> {
    match fs::symlink_metadata(path) {
        Ok(_) => allocated(path).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn allocated(path: &Path) -> Result<u64> {
    let meta = fs::symlink_metadata(path)?;
    if !meta.is_file() {
        return Err(RestoreError::Invalid("spool file"));
    }
    meta.blocks()
        .checked_mul(512)
        .ok_or(RestoreError::Limit("spool bytes"))
}

pub(super) fn bounded_bytes(path: &Path, max: u64) -> Result<Vec<u8>> {
    if !fs::symlink_metadata(path)?.is_file() {
        return Err(RestoreError::Invalid("object file"));
    }
    let mut bytes = Vec::new();
    File::open(path)?.take(max + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max {
        return Err(RestoreError::Limit("object decode"));
    }
    Ok(bytes)
}

pub(super) fn verify_file(path: &Path, sha: &Digest, length: u64) -> Result<()> {
    if !fs::symlink_metadata(path)?.is_file() {
        return Err(RestoreError::Invalid("native file"));
    }
    let mut input = File::open(path)?;
    if input.metadata()?.len() != length {
        return Err(RestoreError::Invalid("file length"));
    }
    let mut digest = Sha256::new();
    if std::io::copy(&mut (&mut input).take(length + 1), &mut digest)? != length {
        return Err(RestoreError::Invalid("file length"));
    }
    if hex::encode(digest.finalize()) != sha.as_str() {
        return Err(RestoreError::Invalid("file digest"));
    }
    Ok(())
}

pub(crate) fn check_capacity(root: &Path, bytes: u64) -> Result<()> {
    let fs = rustix::fs::statvfs(root).map_err(std::io::Error::from)?;
    let available = (fs.f_bavail as u64)
        .checked_mul(fs.f_frsize as u64)
        .ok_or(RestoreError::Limit("disk capacity"))?;
    let required = bytes
        .checked_add(laststore::space::SpacePolicy::default().free_space_reserve_bytes)
        .ok_or(RestoreError::Limit("disk capacity"))?;
    if available < required {
        return Err(RestoreError::Limit("disk capacity"));
    }
    Ok(())
}
