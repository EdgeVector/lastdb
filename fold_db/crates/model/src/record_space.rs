//! The record key space of a format 7 home (the record model).
//!
//! A format 7 home stores one record at each primary key:
//! `rk:{S}:{esc(hash)}\0{range}`. A tombstone uses the same key. `S` is the
//! record space of one schema. The record space owns one key bundle, as a
//! molecule does: its blind key blinds the hash, and the typed range profile
//! of the primary key orders the range payload. So the records under one hash
//! are one ordered key range.
//!
//! An alternate entry keeps its `ak:` key and its value in format 7. Its
//! identity already names one schema and one key name.

use crate::atom::molecule_key_codec::escape_segment;
use crate::atom::{MoleculeKeyCodec, MoleculeKeyCodecError};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use sha2::{Digest, Sha256};

/// Storage key prefix of a record or a tombstone in a format 7 home.
pub const RECORD_PREFIX: &str = "rk:";

/// The record space of one schema: the unpadded base64url SHA-256 of a
/// framed domain and the schema name.
///
/// The identity has the 43-character form of a molecule identity, so the key
/// bundle store and the key checks accept it without a new rule. Its domain
/// differs from the field and alternate domains, so no molecule shares it.
#[must_use]
pub fn record_space_uuid(schema_name: &str) -> String {
    let mut hasher = Sha256::new();
    for part in [b"lastdb:record-space:v1".as_slice(), schema_name.as_bytes()] {
        crate::canonical::push_field(&mut hasher, part);
    }
    URL_SAFE_NO_PAD.encode(hasher.finalize())
}

/// `rk:{S}:{esc(storage_hash)}\0{storage_range}`: the key of one record.
///
/// The coordinates are in storage form: the blinded hash and the typed range
/// payload of the record space codec.
#[must_use]
pub fn record_key(space: &str, storage_hash: &str, storage_range: &str) -> String {
    format!(
        "{RECORD_PREFIX}{space}:{}\0{storage_range}",
        escape_segment(storage_hash)
    )
}

/// `rk:{S}:{esc(storage_hash)}\0`: the prefix of every record under one hash.
/// The escape keeps the hash `a` apart from the hash `ab`.
#[must_use]
pub fn record_hash_prefix(space: &str, storage_hash: &str) -> String {
    format!("{RECORD_PREFIX}{space}:{}\0", escape_segment(storage_hash))
}

impl MoleculeKeyCodec {
    /// The record key from API coordinates. The record space is the blind
    /// domain of the hash.
    pub fn api_record_key(
        &self,
        space: &str,
        api_hash: &str,
        api_range: &str,
    ) -> Result<String, MoleculeKeyCodecError> {
        let hash = self.storage_hash(space, api_hash)?;
        let range = self.storage_range(space, api_range)?;
        Ok(record_key(space, &hash, &range))
    }
}
