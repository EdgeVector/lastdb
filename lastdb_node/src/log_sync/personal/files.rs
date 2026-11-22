//! File bytes of a record, as they enter and leave a sealed blob.

use std::fs::OpenOptions;
use std::io::Read;
use std::os::unix::fs::OpenOptionsExt;

use fold_db::record_value::FileMarker;
use sha2::{Digest, Sha256};

/// A sealed blob is the plaintext inside the envelope: version byte, nonce (12)
/// and tag (16). The size the relay reports for a blob is the file size plus this.
pub(super) const ENVELOPE_OVERHEAD: u64 = 29;

/// The SHA-256 of `bytes`, as 64 lowercase hex characters.
pub(super) fn sha256_hex(bytes: &[u8]) -> String {
    app_identity_crypto::hex_lower(&Sha256::digest(bytes))
}

/// The plaintext of the file that a spool entry carries: the sealed row it holds,
/// or the file it names in local custody. The result matches `marker`.
pub(super) fn plain_of(
    content_key: &[u8; 32],
    marker: &FileMarker,
    row: Option<&[u8]>,
    reference: Option<&str>,
) -> Result<Vec<u8>, String> {
    const FAILED: &str = "a file of an accepted write cannot be read for upload";
    let plain = match (row, reference) {
        (Some(row), _) => {
            fold_db::storage::disk_backup::accepted::open_file_row(content_key, marker, row)
                .map_err(|_| FAILED.to_owned())?
        }
        (None, Some(path)) => read_reference(path, marker).ok_or(FAILED)?,
        (None, None) => return Err(FAILED.into()),
    };
    is_file_of(&plain, marker)
        .then_some(plain)
        .ok_or_else(|| FAILED.to_owned())
}

/// True when `bytes` are the file the marker describes.
pub(super) fn is_file_of(bytes: &[u8], marker: &FileMarker) -> bool {
    marker.size_bytes == bytes.len() as u64 && sha256_hex(bytes) == marker.sha256
}

fn read_reference(path: &str, marker: &FileMarker) -> Option<Vec<u8>> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .ok()?;
    let metadata = file.metadata().ok()?;
    if !metadata.is_file() || metadata.len() != marker.size_bytes {
        return None;
    }
    let mut bytes = Vec::new();
    file.take(marker.size_bytes + 1)
        .read_to_end(&mut bytes)
        .ok()?;
    Some(bytes)
}
