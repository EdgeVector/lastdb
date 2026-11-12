//! Record key space (`rk:{S}:{esc(hash)}\0{range}`).

use crate::field_key_codec::{escape_segment, FieldKeyCodec, FieldKeyCodecError};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use sha2::{Digest, Sha256};

pub const RECORD_PREFIX: &str = "rk:";

#[must_use]
pub fn record_space_uuid(schema_name: &str) -> String {
    let mut hasher = Sha256::new();
    for part in [b"lastdb:record-space:v1".as_slice(), schema_name.as_bytes()] {
        crate::canonical::push_field(&mut hasher, part);
    }
    URL_SAFE_NO_PAD.encode(hasher.finalize())
}

#[must_use]
pub fn record_key(space: &str, storage_hash: &str, storage_range: &str) -> String {
    format!(
        "{RECORD_PREFIX}{space}:{}\0{storage_range}",
        escape_segment(storage_hash)
    )
}

#[must_use]
pub fn record_hash_prefix(space: &str, storage_hash: &str) -> String {
    format!("{RECORD_PREFIX}{space}:{}\0", escape_segment(storage_hash))
}

impl FieldKeyCodec {
    pub fn api_record_key(
        &self,
        space: &str,
        api_hash: &str,
        api_range: &str,
    ) -> Result<String, FieldKeyCodecError> {
        let hash = self.storage_hash(space, api_hash)?;
        let range = self.storage_range(space, api_range)?;
        Ok(record_key(space, &hash, &range))
    }
}
