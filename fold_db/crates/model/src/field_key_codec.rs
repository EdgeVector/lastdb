//! Native per-key field key storage codec (`{F}:{esc(hash)}\0{range}`).

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{Hmac, Mac};
use schema_types::field_range_profile::FieldRangeProfile;
use sha2::Sha256;

/// Logical key format marker of a home: typed range keys.
pub const KEY_FORMAT: &[u8] = b"typed-ranges";

pub fn current_key_format(marker: &[u8]) -> bool {
    marker == KEY_FORMAT
}

/// Derive the deterministic 32-byte blind key for `field_key_id` from `node_key`
/// via RFC 5869 HKDF-SHA256.
#[must_use]
pub fn derive_blind_key(node_key: &[u8; 32], field_key_id: &str) -> [u8; 32] {
    let mut extract = Hmac::<Sha256>::new_from_slice(b"lastdb-field-blind-key-v1")
        .expect("HMAC accepts any key length");
    extract.update(node_key);
    let prk = extract.finalize().into_bytes();
    let mut expand = Hmac::<Sha256>::new_from_slice(&prk).expect("HMAC accepts any key length");
    expand.update(field_key_id.as_bytes());
    expand.update(&[1u8]);
    expand.finalize().into_bytes().into()
}

/// Deterministic storage-facing HashKey token (`hk|v1\0{field_key_id}\0{plaintext_hash}`).
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

/// Chokepoint mapping API `HashKey`/`RangeKey` coordinates to storage segments.
#[derive(Clone)]
pub struct FieldKeyCodec {
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

    #[must_use]
    pub fn from_node_key(
        node_key: &[u8; 32],
        field_key_id: &str,
        range_profile: FieldRangeProfile,
    ) -> Self {
        Self::new(derive_blind_key(node_key, field_key_id), range_profile)
    }

    #[must_use]
    pub fn range_profile(&self) -> FieldRangeProfile {
        self.range_profile
    }

    pub fn require_text_range_prefix(&self) -> Result<(), FieldKeyCodecError> {
        self.range_profile
            .require_text_prefix()
            .map_err(FieldKeyCodecError::InvalidRange)
    }

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

    pub fn storage_range(
        &self,
        _field_key_id: &str,
        api_range: &str,
    ) -> Result<String, FieldKeyCodecError> {
        self.range_profile
            .encode_value(api_range)
            .map_err(FieldKeyCodecError::InvalidRange)
    }

    pub fn api_range(&self, stored: &str) -> Result<String, FieldKeyCodecError> {
        self.range_profile
            .decode_value(stored)
            .map_err(FieldKeyCodecError::InvalidRange)
    }

    pub fn api_hash_range_member_prefix(
        &self,
        field_key_id: &str,
        api_hash: &str,
        api_range: &str,
    ) -> Result<String, FieldKeyCodecError> {
        let hash = self.storage_hash(field_key_id, api_hash)?;
        let range = self.storage_range(field_key_id, api_range)?;
        Ok(hash_range_record_key(
            field_key_id,
            &hash,
            &escape_segment(&range),
        ))
    }

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

const SEP: char = '\u{0}';
const ESC: char = '\u{1}';

/// Byte-stuff a segment so it contains no `\0` or bare `\u{1}`.
pub(crate) fn escape_segment(segment: &str) -> String {
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

/// `{F}:{esc(hash)}\0{range}` — the field key part of a key.
#[must_use]
pub fn hash_range_record_key(field_key_id: &str, hash: &str, range: &str) -> String {
    format!("{field_key_id}:{}{SEP}{range}", escape_segment(hash))
}

/// `{F}:{esc(hash)}\0` — prefix selecting one hash's ranges in a HashRange field.
#[must_use]
pub fn hash_range_scan_prefix_for_hash(field_key_id: &str, hash: &str) -> String {
    format!("{field_key_id}:{}{SEP}", escape_segment(hash))
}

/// Parse `{esc(hash)}\0{range}` back into `(hash, range)`.
#[must_use]
pub fn decode_hash_range_suffix(suffix: &str) -> Option<(String, String)> {
    let (esc, range) = suffix.split_once(SEP)?;
    let hash = unescape_segment(esc)?;
    Some((hash, range.to_string()))
}
