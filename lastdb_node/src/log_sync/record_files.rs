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

/// Seal `plain` for the entry at this position and stage it for relay upload.
pub(crate) fn seal_and_stage(
    home: &Path,
    database_id: &str,
    sealer: &Sealer,
    node_key: NodeKey,
    device_id: DeviceId,
    writer_seq: u64,
    plain: &[u8],
    marker: FileMarker,
) -> Result<StagedBlob, String> {
    if !is_file_of(plain, &marker) {
        return Err("file bytes do not match their marker".into());
    }
    let sealed = sealer.seal(node_key, device_id, writer_seq, plain)?;
    let sha256 = sha256_hex(&sealed);
    let dir = home.join("log-sync").join(database_id).join("blobs");
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
