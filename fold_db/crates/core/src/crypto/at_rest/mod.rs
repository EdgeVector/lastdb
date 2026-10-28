//! Shared synchronous at-rest value codec for values sealed under a raw
//! 32-byte key, outside the [`EncryptingNamespacedStore`](crate::storage)
//! seam.
//!
//! Some values are sealed inside a record or a field rather than by the
//! namespaced encryption seam: molecule key bundles and sharing delivery
//! blobs. Each must encrypt its value at rest with the same framing, so every
//! one of them seals and opens through this module rather than its own wrapper
//! (see `docs/security/at-rest-threat-model.md`).
//!
//! This is that single audit point. It is deliberately **synchronous** —
//! keyed directly on the 32-byte E2E content key rather than the async
//! [`CryptoProvider`](super::CryptoProvider) trait — so the sync read paths
//! of those callers can use it without an async cascade, and it reuses the
//! same [`encrypt_envelope`]/[`decrypt_envelope`] AES-256-GCM primitive the
//! rest of the crate uses (no second crypto implementation).
//!
//! **Text wire format — `ENC:` ++ base64(envelope).** The stored bytes stay
//! valid UTF-8, for a storage boundary that requires a JSON string
//! ([`seal_at_rest_utf8`]).
//!
//! **Binary wire format — `ENB:` ++ flags ++ envelope.** The low bit of the
//! one-byte flags field records whether the plaintext was deflated before it
//! was sealed. The raw-key seal ([`seal_at_rest_raw`]) never deflates, and
//! [`open_at_rest`] refuses a deflated value.
//!
//! Both prefixes are 4 bytes and end in `:`, which is outside the base64
//! alphabet, so neither can be confused with the other, with a base64 body,
//! or with JSON (`{`/`[`). A value without one of them is not sealed, and
//! [`open_at_rest`] rejects it.
//!
//! **The compression half serves the KV seam.** The namespaced
//! `EncryptingKvStore`, which wraps only the local CAS blob records
//! (`cas_blobs`), seals through the async
//! [`CryptoProvider`](super::CryptoProvider) rather than a raw key. It writes
//! `ENB:` only, and it uses the compress-or-skip decision
//! (`compress_for_seal`), the bounded inflate (`inflate_at_rest`), and these
//! counters. It is the only seal site that compresses.

mod compress;
mod config;
mod stats;

pub(crate) use compress::*;
pub use config::*;
pub use stats::*;

use super::envelope::decrypt_envelope;
use super::envelope::encrypt_envelope;
use super::error::{CryptoError, CryptoResult};
use base64::{engine::general_purpose::STANDARD as B64, Engine};

/// Prefix marker for at-rest-encrypted values. A sealed value is
/// `ENC:` ++ base64(ciphertext envelope) and is therefore valid UTF-8.
///
/// `:` is not in the base64 alphabet, so a base64-encoded legacy value (and
/// JSON, which starts with `{`/`[`) can never be mistaken for a sealed one.
pub const AT_REST_ENC_PREFIX: &str = "ENC:";

/// Prefix marker for binary at-rest values.
///
/// The stored form is `ENB:` + one flags byte + the raw ciphertext envelope.
/// Bit 0 means the plaintext was deflated before encryption. Other bits are
/// reserved and rejected so a future extension cannot be misread by an older
/// binary.
pub const AT_REST_ENC_BINARY_PREFIX: &str = "ENB:";

const AT_REST_BINARY_FLAG_DEFLATED: u8 = 0x01;
const AT_REST_BINARY_KNOWN_FLAGS: u8 = AT_REST_BINARY_FLAG_DEFLATED;

/// True when `value` carries an at-rest envelope marker (`ENC:` or `ENB:`).
#[must_use]
pub fn is_sealed_at_rest(value: &[u8]) -> bool {
    value.starts_with(AT_REST_ENC_PREFIX.as_bytes())
        || value.starts_with(AT_REST_ENC_BINARY_PREFIX.as_bytes())
}

/// Seal `plaintext` as UTF-8 `ENC:` + base64(envelope).
///
/// Use this only at a storage boundary that requires a JSON/string value.
/// Byte-oriented stores should use [`seal_at_rest_raw`], which carries no
/// base64 tax.
pub fn seal_at_rest_utf8(key: &[u8; 32], plaintext: &[u8]) -> CryptoResult<Vec<u8>> {
    let ciphertext = encrypt_envelope(key, plaintext)?;
    let encoded = B64.encode(&ciphertext);
    let mut out = Vec::with_capacity(AT_REST_ENC_PREFIX.len() + encoded.len());
    out.extend_from_slice(AT_REST_ENC_PREFIX.as_bytes());
    out.extend_from_slice(encoded.as_bytes());
    Ok(out)
}

/// Seal `plaintext` as `ENB:` + flags + raw ciphertext envelope.
pub fn seal_at_rest_raw(key: &[u8; 32], plaintext: &[u8]) -> CryptoResult<Vec<u8>> {
    let ciphertext = encrypt_envelope(key, plaintext)?;
    Ok(encode_binary_ciphertext(&ciphertext, false))
}

pub(crate) fn encode_binary_ciphertext(ciphertext: &[u8], deflated: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(AT_REST_ENC_BINARY_PREFIX.len() + 1 + ciphertext.len());
    out.extend_from_slice(AT_REST_ENC_BINARY_PREFIX.as_bytes());
    out.push(u8::from(deflated) * AT_REST_BINARY_FLAG_DEFLATED);
    out.extend_from_slice(ciphertext);
    out
}

pub(crate) fn decode_binary_ciphertext(stored: &[u8]) -> CryptoResult<Option<(bool, &[u8])>> {
    if !stored.starts_with(AT_REST_ENC_BINARY_PREFIX.as_bytes()) {
        return Ok(None);
    }
    let Some((&flags, ciphertext)) = stored[AT_REST_ENC_BINARY_PREFIX.len()..].split_first() else {
        return Err(CryptoError::InvalidFormat(
            "at-rest ENB envelope is missing its flags byte".to_string(),
        ));
    };
    if flags & !AT_REST_BINARY_KNOWN_FLAGS != 0 {
        return Err(CryptoError::InvalidFormat(format!(
            "at-rest ENB envelope has unsupported flags 0x{flags:02x}"
        )));
    }
    Ok(Some((
        flags & AT_REST_BINARY_FLAG_DEFLATED != 0,
        ciphertext,
    )))
}

/// Open a sealed value. An `ENC:` value is base64-decoded and decrypted
/// under `key`. An `ENB:` value reads the raw envelope after the flags byte.
/// A value without a sealed prefix, or a deflated `ENB:` value, is an error.
pub fn open_at_rest(key: &[u8; 32], stored: &[u8]) -> CryptoResult<Vec<u8>> {
    open_sealed(key, stored)
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
    let plain = open_sealed(key, stored)?;
    if plain.len() as u64 > max {
        return Err(CryptoError::InvalidFormat(
            "decoded value exceeds explicit bound".into(),
        ));
    }
    Ok(plain)
}

fn open_sealed(key: &[u8; 32], stored: &[u8]) -> CryptoResult<Vec<u8>> {
    if let Some((deflated, ciphertext)) = decode_binary_ciphertext(stored)? {
        if deflated {
            return Err(CryptoError::InvalidFormat(
                "a raw-key ENB envelope is never deflated".into(),
            ));
        }
        return decrypt_envelope(key, ciphertext);
    }
    let Some(encoded) = stored.strip_prefix(AT_REST_ENC_PREFIX.as_bytes()) else {
        return Err(CryptoError::InvalidFormat(
            "at-rest value has no ENC: or ENB: marker".into(),
        ));
    };
    let ciphertext = B64
        .decode(encoded)
        .map_err(|e| CryptoError::InvalidFormat(format!("at-rest base64 decode failed: {e}")))?;
    decrypt_envelope(key, &ciphertext)
}
