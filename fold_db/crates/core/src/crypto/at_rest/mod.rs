//! Shared synchronous at-rest value codec (`ENB:` ++ flags ++ envelope)
//! sealed under a raw 32-byte key.

use super::envelope::decrypt_envelope;
use super::envelope::encrypt_envelope;
use super::error::{CryptoError, CryptoResult};

/// Prefix marker for binary at-rest values.
///
/// The stored form is `ENB:` + one flags byte + the raw ciphertext envelope.
/// The flags byte is zero. Other values are reserved and rejected so a future
/// extension cannot be misread by an older binary.
pub const AT_REST_ENC_BINARY_PREFIX: &str = "ENB:";

/// True when `value` carries an at-rest binary envelope marker (`ENB:`).
#[must_use]
pub fn is_sealed_at_rest(value: &[u8]) -> bool {
    value.starts_with(AT_REST_ENC_BINARY_PREFIX.as_bytes())
}

/// Seal `plaintext` as `ENB:` + flags + raw ciphertext envelope.
pub fn seal_at_rest_raw(key: &[u8; 32], plaintext: &[u8]) -> CryptoResult<Vec<u8>> {
    let ciphertext = encrypt_envelope(key, plaintext)?;
    Ok(encode_binary_ciphertext(&ciphertext))
}

pub(crate) fn encode_binary_ciphertext(ciphertext: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(AT_REST_ENC_BINARY_PREFIX.len() + 1 + ciphertext.len());
    out.extend_from_slice(AT_REST_ENC_BINARY_PREFIX.as_bytes());
    out.push(0);
    out.extend_from_slice(ciphertext);
    out
}

pub(crate) fn decode_binary_ciphertext(stored: &[u8]) -> CryptoResult<Option<&[u8]>> {
    if !stored.starts_with(AT_REST_ENC_BINARY_PREFIX.as_bytes()) {
        return Ok(None);
    }
    let Some((&flags, ciphertext)) = stored[AT_REST_ENC_BINARY_PREFIX.len()..].split_first() else {
        return Err(CryptoError::InvalidFormat(
            "at-rest ENB envelope is missing its flags byte".to_string(),
        ));
    };
    if flags != 0 {
        return Err(CryptoError::InvalidFormat(format!(
            "at-rest ENB envelope has unsupported flags 0x{flags:02x}"
        )));
    }
    Ok(Some(ciphertext))
}

/// Open a sealed value with a caller-owned bound, independent of environment.
pub fn open_at_rest_bounded(key: &[u8; 32], stored: &[u8], max: u64) -> CryptoResult<Vec<u8>> {
    if max == 0
        || max.checked_add(1).is_none()
        || usize::try_from(max).is_err()
        || stored.len() as u64 > max
        || !is_sealed_at_rest(stored)
    {
        return Err(CryptoError::InvalidFormat(
            "bounded encrypted value required".into(),
        ));
    }
    let Some(ciphertext) = decode_binary_ciphertext(stored)? else {
        return Err(CryptoError::InvalidFormat(
            "at-rest value has no ENB: marker".into(),
        ));
    };
    let plain = decrypt_envelope(key, ciphertext)?;
    if plain.len() as u64 > max {
        return Err(CryptoError::InvalidFormat(
            "decoded value exceeds explicit bound".into(),
        ));
    }
    Ok(plain)
}
