//! File bytes of a record, as they enter and leave a sealed blob.

use fold_db::record_value::FileMarker;
use sha2::{Digest, Sha256};

/// A sealed blob is the plaintext inside the envelope: version byte, nonce (12)
/// and tag (16). The size the relay reports for a blob is the file size plus this.
pub(super) const ENVELOPE_OVERHEAD: u64 = 29;

/// The SHA-256 of `bytes`, as 64 lowercase hex characters.
pub(super) fn sha256_hex(bytes: &[u8]) -> String {
    app_identity_crypto::hex_lower(&Sha256::digest(bytes))
}

/// True when `bytes` are the file the marker describes.
pub(super) fn is_file_of(bytes: &[u8], marker: &FileMarker) -> bool {
    marker.size_bytes == bytes.len() as u64 && sha256_hex(bytes) == marker.sha256
}
