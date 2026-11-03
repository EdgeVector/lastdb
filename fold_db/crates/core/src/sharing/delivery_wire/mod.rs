//! Versioned portable wire format for consent-gated delivery.
//!
//! The payload is intentionally plain JSON after decryption: a non-LastDB
//! consumer can parse schemas, signed field keys and content-addressed records
//! without linking Rust code. Encryption uses
//! standard JWE JSON serialization (`alg=dir`, `enc=A256GCM`) so decryptors
//! can use off-the-shelf JOSE tooling.
//!
//! Field values live in `records[]` (JSON CAS). A record's file is not a field
//! value and is not carried in a slice.

mod jwe;
mod types;

use crate::error::FoldDbError;
use lastdb_model::hex::hex_lower;
use serde_json::Value;
use sha2::{Digest, Sha256};

pub use jwe::*;
pub use types::*;

pub const LASTDB_SLICE_PAYLOAD_VERSION: &str = "lastdb.slice.v1";
pub const LASTDB_SLICE_MEDIA_TYPE: &str = "application/vnd.lastdb.slice+json;v=1";
/// JWE protected-header claim: gzip the signed-slice JSON before A256GCM.
///
/// Compress-before-encrypt so Exemem's ~64KB sealed-message cap can hold a
/// full kanban board. Receivers without this claim keep the legacy path.
pub const JWE_ZIP_GZIP: &str = "gzip";

/// Content-address one field value for `records[]`.
pub fn content_addressed_value(value: Value) -> Result<ContentAddressedValue, FoldDbError> {
    let bytes = serde_json::to_vec(&value).map_err(FoldDbError::from)?;
    let content_sha256 = hex_lower(Sha256::digest(bytes));
    Ok(ContentAddressedValue {
        value_ref: format!("sha256:{content_sha256}"),
        value,
        content_sha256,
    })
}
