//! V2 commitment domains never reuse a V1 identity. Stored bytes use plain SHA256.
use crate::{Digest, ProtocolError};
use serde::Serialize;
use sha2::{Digest as _, Sha256};

pub fn canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, ProtocolError> {
    let value = serde_json::to_value(value).map_err(|_| ProtocolError::invalid("v2.json"))?;
    json_canon::to_vec(&value).map_err(|_| ProtocolError::invalid("v2.canonical_json"))
}

pub fn input<T: Serialize>(domain: &'static str, value: &T) -> Result<Digest, ProtocolError> {
    if !domain.starts_with("lastdb:cloud-byte-backup:") || !domain.ends_with(":v2") {
        return Err(ProtocolError::invalid("v2.digest_domain"));
    }
    let bytes = canonical_bytes(value)?;
    let domain_length =
        u32::try_from(domain.len()).map_err(|_| ProtocolError::limit("v2.domain"))?;
    let body_length = u64::try_from(bytes.len()).map_err(|_| ProtocolError::limit("v2.body"))?;
    let mut hash = Sha256::new();
    hash.update(domain_length.to_be_bytes());
    hash.update(domain.as_bytes());
    hash.update(body_length.to_be_bytes());
    hash.update(bytes);
    encode(&hash.finalize())
}

pub fn stored_bytes(bytes: &[u8]) -> Result<Digest, ProtocolError> {
    encode(&Sha256::digest(bytes))
}

fn encode(bytes: &[u8]) -> Result<Digest, ProtocolError> {
    Digest::try_from(
        bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
    )
}
