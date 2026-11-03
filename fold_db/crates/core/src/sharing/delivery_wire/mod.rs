//! Versioned portable wire format for consent-gated delivery.
//!
//! The payload is intentionally plain JSON after decryption: a non-LastDB
//! consumer can parse schemas, signed field keys, content-addressed records, and
//! first-class binary blobs without linking Rust code. Encryption uses
//! standard JWE JSON serialization (`alg=dir`, `enc=A256GCM`) so decryptors
//! can use off-the-shelf JOSE tooling.
//!
//! **Files:** structured field values live in `records[]` (JSON CAS). A
//! record's file is not a field value and is not carried in a slice. The
//! sealed-row codec of a record file (`ContentAddressedBlob`, `FileBlobAccess`)
//! lives here too.

mod blob;
mod jwe;
mod types;
mod validate;

pub use blob::*;
pub use jwe::*;
pub use types::*;
pub use validate::*;

pub const LASTDB_SLICE_PAYLOAD_VERSION: &str = "lastdb.slice.v1";
pub const LASTDB_SLICE_MEDIA_TYPE: &str = "application/vnd.lastdb.slice+json;v=1";
/// JWE protected-header claim: gzip the signed-slice JSON before A256GCM.
///
/// Compress-before-encrypt so Exemem's ~64KB sealed-message cap can hold a
/// full kanban board. Receivers without this claim keep the legacy path.
pub const JWE_ZIP_GZIP: &str = "gzip";

/// The cipher suite of every sealed file row: the account-v2 file key and
/// AES-256-GCM, addressed by the ciphertext SHA-256.
pub const FILE_BLOB_CIPHER_SUITE: &str = "lastdb-file-blob-account-v2:aes-256-gcm-ciphertext-sha256";

/// True when `suite` is [`FILE_BLOB_CIPHER_SUITE`].
#[must_use]
pub fn file_blob_cipher_suite_supported(suite: &str) -> bool {
    suite == FILE_BLOB_CIPHER_SUITE
}
