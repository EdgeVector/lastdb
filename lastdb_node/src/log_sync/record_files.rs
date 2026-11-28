//! File bytes of a record, as they enter and leave a sealed blob.
//!
//! Personal multi-device sync and shared-database sync use this same path.
//! The sealed envelope, the relay blob id, and the open-and-apply steps are
//! identical; only the database read key that seals the bytes differs.

use std::path::{Path, PathBuf};

use fold_db::record_value::FileMarker;
use mutation_log::{DeviceId, NodeKey};
use sha2::{Digest, Sha256};

use crate::personal_seal::Sealer;
use crate::profile::private_fs as disk;

/// A sealed blob is the plaintext inside the envelope: version byte, nonce (12)
/// and tag (16). The size the relay reports for a blob is the file size plus this.
pub(crate) const ENVELOPE_OVERHEAD: u64 = 29;

/// A sealed file staged under `log-sync/<database_id>/blobs/<sha256>`.
#[allow(dead_code)] // path/bytes/marker stay for callers that stage then upload by handle
pub(crate) struct StagedBlob {
    pub sha256: String,
    pub path: PathBuf,
    pub bytes: u64,
    pub marker: FileMarker,
}

/// The SHA-256 of `bytes`, as 64 lowercase hex characters.
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    app_identity_crypto::hex_lower(&Sha256::digest(bytes))
}

/// True when `bytes` are the file the marker describes.
pub(crate) fn is_file_of(bytes: &[u8], marker: &FileMarker) -> bool {
    marker.size_bytes == bytes.len() as u64 && sha256_hex(bytes) == marker.sha256
}

/// Where and how a sealed blob is staged for one log entry position.
pub(crate) struct SealStage<'a> {
    pub home: &'a Path,
    pub database_id: &'a str,
    pub sealer: &'a Sealer,
    pub node_key: NodeKey,
    pub device_id: DeviceId,
    pub writer_seq: u64,
}

/// Seal `plain` for the entry at this position and stage it for relay upload.
pub(crate) fn seal_and_stage(
    stage: &SealStage<'_>,
    plain: &[u8],
    marker: FileMarker,
) -> Result<StagedBlob, String> {
    if !is_file_of(plain, &marker) {
        return Err("file bytes do not match their marker".into());
    }
    let sealed = stage
        .sealer
        .seal(stage.node_key, stage.device_id, stage.writer_seq, plain)?;
    let sha256 = sha256_hex(&sealed);
    let dir = stage
        .home
        .join("log-sync")
        .join(stage.database_id)
        .join("blobs");
    std::fs::create_dir_all(&dir).map_err(|error| format!("cannot create blob stage: {error}"))?;
    let path = dir.join(&sha256);
    if !path.is_file() {
        disk::write_private(&path, &sealed)
            .map_err(|error| format!("cannot stage sealed file: {error}"))?;
    }
    Ok(StagedBlob {
        bytes: sealed.len() as u64,
        sha256,
        path,
        marker,
    })
}

/// Path of a staged sealed blob for this database.
pub(crate) fn staged_path(home: &Path, database_id: &str, sha256: &str) -> PathBuf {
    home.join("log-sync")
        .join(database_id)
        .join("blobs")
        .join(sha256)
}

/// Destination path for a blob fetched for materialize or personal apply.
pub(crate) fn incoming_path(home: &Path, database_id: &str, sha256: &str) -> PathBuf {
    home.join("log-sync")
        .join(database_id)
        .join("incoming")
        .join(sha256)
}

/// Fetch a sealed blob from the relay into `log-sync/<id>/incoming/<sha>`.
pub(crate) async fn fetch_incoming(
    client: &crate::log_sync::relay::RelayClient,
    home: &Path,
    database_id: &str,
    sha256: &str,
    sealed_bytes: u64,
) -> Result<PathBuf, String> {
    let dest = incoming_path(home, database_id, sha256);
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("cannot create incoming blob dir: {error}"))?;
    }
    client
        .blob_get(database_id, sha256, sealed_bytes, &dest)
        .await?;
    Ok(dest)
}

/// Remove staged sealed blobs that uploaded entries named.
pub(crate) fn gc_staged(home: &Path, database_id: &str, sha256s: &[String]) {
    for sha256 in sha256s {
        let path = staged_path(home, database_id, sha256);
        let _ = std::fs::remove_file(path);
    }
}
