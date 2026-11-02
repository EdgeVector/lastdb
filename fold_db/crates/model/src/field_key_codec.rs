//! Native per-key field key storage codec.
//!
//! The field key part of a key is `{F}:{esc(hash)}\0{range}`. Alternate keys
//! (`ak:`) build their keys with it and add their own family prefix.
//! Callers add an optional storage scope with `build_storage_key`. Hash fields
//! use an empty range; HashRange fields use both coordinates.
//!
//! The supplied field key UUID is an exact identity. Reads and writes never
//! substitute a hex or base64url spelling. Hash blinding binds that same
//! identity. One fixed key bundle defines each current field key.
//!
//! Free key builders take storage-form coordinates. API coordinates pass once
//! through [`FieldKeyCodec::storage_hash`] and
//! [`FieldKeyCodec::storage_range`], or through an `api_*` builder. Every
//! non-empty hash is HMAC-blinded (`blind_v1`) with the field key's blind key.
//! The hash is byte-stuffed before the separator, so hash prefixes cannot
//! overlap. The typed range profile of the field key orders its range payloads.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{Hmac, Mac};
use schema_types::field_range_profile::FieldRangeProfile;
use sha2::Sha256;

mod profile;
mod range_decode;

/// The logical key format of a home: typed range keys. Physical run blocks
/// keep their own format. The field history setting has its own row.
pub const KEY_FORMAT: &[u8] = b"typed-ranges";
pub fn current_key_format(marker: &[u8]) -> bool {
    marker == KEY_FORMAT
}

/// Deterministic storage-facing HashKey token (design-lastdb-hashkey-blind-v1).
///
/// MAC input (exact bytes):
/// ```text
/// hk|v1\0{field_key_id}\0{plaintext_hash}
/// ```
///
/// Domain tag `hk|v1` is disjoint from a raw search term. The encoding is
/// HMAC-SHA256, the first 16 bytes, then base64url with no padding, so
/// `escape_segment` remains safe. `blind_key` is the field key bundle's key.
/// A pure function of its inputs; core's `E2eKeys::blind_hash_key` calls it.
#[must_use]
pub fn blind_hash_key(blind_key: &[u8; 32], field_key_id: &str, plaintext_hash: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(blind_key).expect("HMAC accepts any key length");
    mac.update(b"hk|v1");
    mac.update(&[0u8]);
    mac.update(field_key_id.as_bytes());
    mac.update(&[0u8]);
    mac.update(plaintext_hash.as_bytes());
    let result = mac.finalize().into_bytes();
    URL_SAFE_NO_PAD.encode(&result[..16])
}

/// Chokepoint: API HashKey/RangeKey → storage segments for one field key.
///
/// Free `hash_range_*` builders below take **storage-form** segments.
#[derive(Clone)]
pub struct FieldKeyCodec {
    /// The field key's HMAC key for blinded hash segments.
    blind_key: [u8; 32],
    range_profile: FieldRangeProfile,
}

impl std::fmt::Debug for FieldKeyCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FieldKeyCodec")
            .field("range_profile", &self.range_profile)
            .finish_non_exhaustive()
    }
}

impl FieldKeyCodec {
    #[must_use]
    pub fn new(blind_key: [u8; 32], range_profile: FieldRangeProfile) -> Self {
        Self {
            blind_key,
            range_profile,
        }
    }

    /// Public format identity only; no encryption key material enters a cursor.
    ///
    /// The `kc1:`/`qc1:` cursor identity hashes this text, so it must stay
    /// byte-identical or open client cursors stop resolving.
    #[must_use]
    pub fn comparison_format(&self) -> &'static str {
        "blind-v1/plain:typed-scalar-v1"
    }

    /// Map API HashKey to storage-form hash segment.
    ///
    /// Empty API hash (range-only / Single slots) is never blinded.
    pub fn storage_hash(
        &self,
        field_key_id: &str,
        api_hash: &str,
    ) -> Result<String, FieldKeyCodecError> {
        if api_hash.is_empty() {
            return Ok(String::new());
        }
        Ok(blind_hash_key(&self.blind_key, field_key_id, api_hash))
    }

    /// Map API RangeKey to storage-form range segment: the typed profile
    /// payload.
    pub fn storage_range(
        &self,
        _field_key_id: &str,
        api_range: &str,
    ) -> Result<String, FieldKeyCodecError> {
        self.range_profile
            .encode_value(api_range)
            .map_err(FieldKeyCodecError::InvalidRange)
    }

    /// `{F}:{esc(storage_hash)}\0{storage_range}` from API hash + range.
    ///
    /// Reads and writes use one key under the caller's exact `field_key_id`.
    pub fn api_hash_range_record_key(
        &self,
        field_key_id: &str,
        api_hash: &str,
        api_range: &str,
    ) -> Result<String, FieldKeyCodecError> {
        let h = self.storage_hash(field_key_id, api_hash)?;
        let r = self.storage_range(field_key_id, api_range)?;
        Ok(hash_range_record_key(field_key_id, &h, &r))
    }

    /// `{F}:{esc(storage_hash(api_hash))}\0` scan prefix from API hash.
    pub fn api_hash_range_scan_prefix_for_hash(
        &self,
        field_key_id: &str,
        api_hash: &str,
    ) -> Result<String, FieldKeyCodecError> {
        let h = self.storage_hash(field_key_id, api_hash)?;
        Ok(hash_range_scan_prefix_for_hash(field_key_id, &h))
    }
}

/// Errors from [`FieldKeyCodec`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldKeyCodecError {
    /// The field key's range profile rejects the coordinate or token.
    InvalidRange(String),
}

impl std::fmt::Display for FieldKeyCodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidRange(reason) => write!(f, "invalid range key: {reason}"),
        }
    }
}

impl std::error::Error for FieldKeyCodecError {}

const SEP: char = '\u{0}'; // hash↔range separator (never appears in esc(hash))
const ESC: char = '\u{1}'; // escape byte for the hash segment

/// Byte-stuff a hash so it contains no `\0` (and no bare `\u{1}`), making the
/// `\0` separator and the escape unambiguous. `\0 -> \x01\x01`, `\x01 -> \x01\x02`.
pub(crate) fn escape_segment(segment: &str) -> String {
    // Fast path: the overwhelmingly common case has neither control byte.
    if !segment.contains(SEP) && !segment.contains(ESC) {
        return segment.to_string();
    }
    let mut out = String::with_capacity(segment.len() + 2);
    for c in segment.chars() {
        match c {
            SEP => {
                out.push(ESC);
                out.push('\u{1}');
            }
            ESC => {
                out.push(ESC);
                out.push('\u{2}');
            }
            other => out.push(other),
        }
    }
    out
}

/// Inverse of [`escape_segment`]. Returns `None` on a malformed escape sequence.
fn unescape_segment(escaped: &str) -> Option<String> {
    if !escaped.contains(ESC) {
        return Some(escaped.to_string());
    }
    let mut out = String::with_capacity(escaped.len());
    let mut chars = escaped.chars();
    while let Some(c) = chars.next() {
        if c == ESC {
            match chars.next() {
                Some('\u{1}') => out.push(SEP),
                Some('\u{2}') => out.push(ESC),
                _ => return None,
            }
        } else {
            out.push(c);
        }
    }
    Some(out)
}

mod record_keys;
pub use record_keys::*;
