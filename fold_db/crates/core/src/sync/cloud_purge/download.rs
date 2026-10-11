//! Download only the immutable ready inventory into an inactive owner directory.

use super::files;
use super::protocol::{
    CloudPurgeControl, CloudPurgeDownload, CloudPurgeObject, CloudPurgePhase, CloudReplacementRoot,
};
use crate::sync::auth::AuthClient;
use crate::sync::{SyncError, SyncResult};
use futures::{stream, StreamExt, TryStreamExt};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::AsyncWriteExt;

/// Result files are named by stored-byte SHA. Caller validates/decrypts the
/// manifest and recovery descriptor before installing any chunk.
pub async fn download_replacement_objects(
    auth: &AuthClient,
    control: &CloudPurgeControl,
    root: &CloudReplacementRoot,
    directory: &Path,
) -> SyncResult<Vec<(CloudPurgeObject, PathBuf)>> {
    control.validate_identity().map_err(SyncError::Storage)?;
    if control.phase != CloudPurgePhase::Ready || !control.deletion_is_complete() {
        return Err(SyncError::Storage(
            "replacement download requires the verified ready control".into(),
        ));
    }
    let replacement: super::protocol::CloudReplacementManifest = serde_json::from_value(
        control
            .replacement_manifest
            .clone()
            .ok_or_else(|| SyncError::Storage("ready replacement manifest absent".into()))?,
    )
    .map_err(|_| SyncError::Storage("ready replacement manifest invalid".into()))?;
    if replacement.account != control.account
        || replacement.operation_id != control.operation_id
        || replacement.generation != control.generation
        || !replacement.roots.iter().any(|candidate| {
            serde_json::to_value(candidate).ok() == serde_json::to_value(root).ok()
        })
    {
        return Err(SyncError::Storage(
            "requested replacement root differs from the immutable control".into(),
        ));
    }
    files::owner_directory(directory)?;
    files::owner_directory(&directory.join("objects"))?;
    files::require_capacity(
        directory,
        root.objects
            .iter()
            .fold(0u64, |sum, object| sum.saturating_add(object.bytes)),
    )?;
    let client = reqwest::Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(30))
        .read_timeout(Duration::from_secs(60))
        .build()
        .map_err(|_| SyncError::Storage("replacement download transport unavailable".into()))?;
    let mut retained = Vec::new();
    for batch in root.objects.chunks(32) {
        for object in batch {
            object.validate().map_err(SyncError::Storage)?;
            if object.logical_root != root.logical_root {
                return Err(SyncError::Storage(
                    "replacement inventory crosses its root".into(),
                ));
            }
        }
        let response = auth
            .cloud_control_download(
                &control.account,
                control.generation,
                &control.operation_id,
                batch,
            )
            .await?;
        let returned: std::collections::BTreeSet<_> = response
            .downloads
            .iter()
            .map(|item| (&item.object.logical_root, &item.object.relative_key))
            .collect();
        if returned.len() != batch.len()
            || response.downloads.len() != batch.len()
            || response
                .downloads
                .iter()
                .any(|item| !batch.contains(&item.object))
        {
            return Err(SyncError::Storage(
                "replacement download inventory differs from the ready source".into(),
            ));
        }
        let paths: Vec<_> = stream::iter(
            response
                .downloads
                .into_iter()
                .map(|item| download_one(&client, directory, item)),
        )
        .buffer_unordered(4)
        .try_collect()
        .await?;
        retained.extend(paths);
    }
    Ok(retained)
}

async fn download_one(
    client: &reqwest::Client,
    directory: &Path,
    item: CloudPurgeDownload,
) -> SyncResult<(CloudPurgeObject, PathBuf)> {
    if item.method != "GET" {
        return Err(SyncError::Storage(
            "replacement download method differs".into(),
        ));
    }
    let destination = files::retained_path(directory, &item.object.sha256)?;
    if destination.exists() {
        files::verify_file(&destination, item.object.bytes, &item.object.sha256)?;
        return Ok((item.object, destination));
    }
    let mut request = client.get(cloud_url(&item.url)?);
    for (name, value) in &item.headers {
        request = request.header(name, value);
    }
    let response = request
        .send()
        .await
        .map_err(|_| SyncError::Storage("replacement object download failed".into()))?;
    if !response.status().is_success() {
        return Err(SyncError::Storage(
            "replacement object download was refused".into(),
        ));
    }
    let temporary = destination.with_file_name(format!(".download-{}", uuid::Uuid::new_v4()));
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut output = options.open(&temporary).await?;
    let mut digest = Sha256::new();
    let mut count = 0u64;
    let mut body = response.bytes_stream();
    while let Some(part) = body.next().await {
        let part =
            part.map_err(|_| SyncError::Storage("replacement object stream failed".into()))?;
        count = count.saturating_add(part.len() as u64);
        if count > item.object.bytes {
            return Err(SyncError::Storage(
                "replacement object exceeds its exact byte length".into(),
            ));
        }
        output.write_all(&part).await?;
        digest.update(&part);
    }
    if count != item.object.bytes || crate::hex::hex_lower(digest.finalize()) != item.object.sha256
    {
        return Err(SyncError::Crypto(
            "replacement ciphertext differs from its immutable inventory".into(),
        ));
    }
    output.sync_all().await?;
    drop(output);
    tokio::fs::rename(&temporary, &destination).await?;
    std::fs::File::open(
        destination
            .parent()
            .ok_or_else(|| SyncError::Storage("download parent absent".into()))?,
    )?
    .sync_all()?;
    Ok((item.object, destination))
}

fn cloud_url(url: &str) -> SyncResult<reqwest::Url> {
    let parsed = reqwest::Url::parse(url)
        .map_err(|_| SyncError::Network("invalid replacement transfer URL".into()))?;
    if parsed.scheme() != "https"
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
    {
        return Err(SyncError::Network(
            "replacement transfers require HTTPS".into(),
        ));
    }
    Ok(parsed)
}
