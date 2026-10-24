use super::{source_error, ByteBackupClient, RestoreError, RestoreSession, Result};
use cloud_backup_protocol::v2::access::{
    ReadGrant, ReadGrantItemResult, ReadGrantRequest, ReadSelector,
};
use cloud_backup_protocol::v2::Version;
use sha2::{Digest as _, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use tokio::io::AsyncReadExt;

pub(super) async fn grants(
    session: &RestoreSession,
    client: &ByteBackupClient,
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
        .map_err(source_error)?;
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
    client: &ByteBackupClient,
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
    client: &ByteBackupClient,
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
        crate::native_cloud_restore::verify_file(&path, &grant.sha256, grant.bytes.get())?;
        return Ok(path);
    }
    let temporary = session.root.join("cache/download.pending");
    session.remove_spool_file(&temporary)?;
    if !cache {
        session.remove_spool_file(&path)?;
    }
    session.reserve_spool(grant.bytes.get())?;
    crate::native_cloud_restore::check_capacity(&session.root, grant.bytes.get())?;
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

async fn stream_to(client: &ByteBackupClient, grant: &ReadGrant, file: &mut File) -> Result<()> {
    let mut stream = client.open(grant).await.map_err(source_error)?;
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
