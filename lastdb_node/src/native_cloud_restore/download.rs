use super::{RestoreError, RestoreSession, RestoreSource, Result};
use cloud_backup_protocol::access::{ReadGrant, ReadGrantRequest, ReadSelector};
use cloud_backup_protocol::{bytes_sha256, Digest, Version};
use sha2::{Digest as _, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use tokio::io::AsyncReadExt;

pub(super) async fn grant(
    session: &RestoreSession,
    source: &dyn RestoreSource,
    selector: ReadSelector,
) -> Result<ReadGrant> {
    grants(session, source, vec![selector])
        .await?
        .into_iter()
        .next()
        .ok_or(RestoreError::Invalid("object grant count"))
}

pub(super) async fn grants(
    session: &RestoreSession,
    source: &dyn RestoreSource,
    selectors: Vec<ReadSelector>,
) -> Result<Vec<ReadGrant>> {
    let request = ReadGrantRequest {
        version: Version::V1,
        job: session.accepted().job.clone(),
        objects: selectors,
    };
    request.validate()?;
    let response = source.grants(&request).await?;
    response.validate_for(&request)?;
    response
        .items
        .into_iter()
        .map(|item| match item {
            cloud_backup_protocol::access::ReadGrantItemResult::Ok { grant, .. } => Ok(grant),
            _ => Err(RestoreError::Source("object grant")),
        })
        .collect()
}
pub(super) async fn fetch(
    session: &RestoreSession,
    source: &dyn RestoreSource,
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
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&temporary)?;
    let transfer = stream_to(source, grant, &mut file).await;
    if let Err(error) = transfer {
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

async fn stream_to(source: &dyn RestoreSource, grant: &ReadGrant, file: &mut File) -> Result<()> {
    let mut stream = source.open(grant).await?;
    let mut buffer = vec![0u8; cloud_backup_protocol::limits::STREAM_BUFFER_MAX_BYTES as usize];
    let mut left = grant.bytes.get();
    let mut digest = Sha256::new();
    while left > 0 {
        let maximum = (left.min(buffer.len() as u64)) as usize;
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
pub(super) fn bytes(path: &Path, max: u64) -> Result<Vec<u8>> {
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
    let mut buffer = vec![0u8; cloud_backup_protocol::limits::STREAM_BUFFER_MAX_BYTES as usize];
    loop {
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    if hex::encode(digest.finalize()) != sha.as_str() {
        return Err(RestoreError::Invalid("file digest"));
    }
    Ok(())
}
pub(super) fn path_key(path: &str) -> String {
    bytes_sha256(path.as_bytes()).as_str().to_owned()
}
pub(super) fn check_capacity(root: &Path, bytes: u64) -> Result<()> {
    let fs = rustix::fs::statvfs(root).map_err(std::io::Error::from)?;
    let available = (fs.f_bavail as u64)
        .checked_mul(fs.f_frsize as u64)
        .ok_or(RestoreError::Limit("disk capacity"))?;
    let required = bytes
        .checked_add(
            laststore::disk_pack::maintenance::SpacePolicy::default().free_space_reserve_bytes,
        )
        .ok_or(RestoreError::Limit("disk capacity"))?;
    if available < required {
        return Err(RestoreError::Limit("disk capacity"));
    }
    Ok(())
}
