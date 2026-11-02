//! Versioned portable wire format for consent-gated delivery.
//!
//! The payload is intentionally plain JSON after decryption: a non-LastDB
//! consumer can parse schemas, signed field keys, content-addressed records, and
//! first-class binary blobs without linking Rust code. Encryption uses
//! standard JWE JSON serialization (`alg=dir`, `enc=A256GCM`) so decryptors
//! can use off-the-shelf JOSE tooling.
//!
//! **Files vs field values:** structured field values live in `records[]` (JSON
//! CAS). Binary file bytes live in `blobs[]` (raw content CAS, base64 on the
//! JSON wire). File fields on records point at blobs via `$lastdb_file`
//! values ([`lastdb_file_pointer_with_access_and_thumbnail`]) — they do
//! **not** embed file bytes.

mod blob;
mod jwe;
mod pointer;
mod types;
mod validate;

pub use blob::*;
pub use jwe::*;
pub use pointer::*;
pub use types::*;
pub use validate::*;

pub const LASTDB_SLICE_PAYLOAD_VERSION: &str = "lastdb.slice.v1";
pub const LASTDB_SLICE_MEDIA_TYPE: &str = "application/vnd.lastdb.slice+json;v=1";
/// JWE protected-header claim: gzip the signed-slice JSON before A256GCM.
///
/// Compress-before-encrypt so Exemem's ~64KB sealed-message cap can hold a
/// full kanban board. Receivers without this claim keep the legacy path.
pub const JWE_ZIP_GZIP: &str = "gzip";

/// JSON object key for a portable file pointer stored as a record value.
/// Canonical home is `lastdb_model::file_pointer`; re-exported here for the
/// wire-format callers that have always imported it from delivery_wire.
pub use lastdb_model::file_pointer::{
    blob_ref_from_value, FILE_BLOB_CIPHER_SUITE, LASTDB_FILE_KEY,
};
/// Optional key for a reference-only generated thumbnail/poster inside a file pointer.
pub const LASTDB_FILE_THUMBNAIL_KEY: &str = "thumbnail";

/// True when `suite` is [`FILE_BLOB_CIPHER_SUITE`].
#[must_use]
pub fn file_blob_cipher_suite_supported(suite: &str) -> bool {
    suite == FILE_BLOB_CIPHER_SUITE
}
/// Remote storage tier used for generated thumbnail/poster objects.
pub const FILE_THUMBNAIL_TIER: &str = "r2-thumbs";
pub const FILE_THUMBNAIL_MAX_ENCRYPTED_SIZE_BYTES: u64 = 65_536;
