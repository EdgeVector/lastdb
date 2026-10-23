use crate::{Digest, ProtocolError};
use serde::Serialize;
use sha2::{Digest as _, Sha256};

/// RFC 8785 bytes. Protocol u64 fields are strings, never binary64 numbers.
pub fn canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, ProtocolError> {
    let value = serde_json::to_value(value).map_err(|_| ProtocolError::invalid("json"))?;
    json_canon::to_vec(&value).map_err(|_| ProtocolError::invalid("canonical_json"))
}

/// Hash framed domain and canonical input; this is not an origin-mutation signature.
pub fn input_digest<T: Serialize>(domain: &str, value: &T) -> Result<Digest, ProtocolError> {
    let bytes = canonical_bytes(value)?;
    let domain_length =
        u32::try_from(domain.len()).map_err(|_| ProtocolError::invalid("digest.domain"))?;
    let body_length =
        u64::try_from(bytes.len()).map_err(|_| ProtocolError::invalid("digest.body"))?;
    let mut hash = Sha256::new();
    hash.update(domain_length.to_be_bytes());
    hash.update(domain.as_bytes());
    hash.update(body_length.to_be_bytes());
    hash.update(bytes);
    let result = hash.finalize();
    let encoded: String = result.iter().map(|byte| format!("{byte:02x}")).collect();
    Digest::try_from(encoded)
}

/// SHA-256 of the exact stored bytes, without a domain frame.
pub fn bytes_sha256(bytes: &[u8]) -> Digest {
    let hash = Sha256::digest(bytes);
    let encoded: String = hash.iter().map(|byte| format!("{byte:02x}")).collect();
    // Hex produced above always satisfies the digest scalar contract.
    Digest::try_from(encoded).expect("SHA-256 produces 32 bytes")
}
