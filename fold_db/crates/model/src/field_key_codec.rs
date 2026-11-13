//! Native per-key field key storage codec (`{F}:{esc(hash)}\0{range}`).

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{Hmac, Mac};
use schema_types::field_range_profile::FieldRangeProfile;
use sha2::Sha256;

/// Logical key format marker of a home: typed range keys.
pub const KEY_FORMAT: &[u8] = b"typed-ranges";
const SEP: char = '\u{0}';
const ESC: char = '\u{1}';

pub fn current_key_format(marker: &[u8]) -> bool {
    marker == KEY_FORMAT
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
    pub fn from_node_key(
        node_key: &[u8; 32],
        field_key_id: &str,
        range_profile: FieldRangeProfile,
    ) -> Self {
        let mut extract = Hmac::<Sha256>::new_from_slice(b"lastdb-field-blind-key-v1")
            .expect("HMAC accepts any key length");
        extract.update(node_key);
        let prk = extract.finalize().into_bytes();
        let mut expand = Hmac::<Sha256>::new_from_slice(&prk).expect("HMAC accepts any key length");
        expand.update(field_key_id.as_bytes());
        expand.update(&[1u8]);
        Self {
            blind_key: expand.finalize().into_bytes().into(),
            range_profile,
        }
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
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.blind_key).expect("HMAC accepts any key length");
        mac.update(b"hk|v1\0");
        mac.update(field_key_id.as_bytes());
        mac.update(b"\0");
        mac.update(api_hash.as_bytes());
        Ok(URL_SAFE_NO_PAD.encode(&mac.finalize().into_bytes()[..16]))
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
        Ok(format!(
            "{field_key_id}:{}{SEP}{}",
            escape_segment(&hash),
            escape_segment(&range)
        ))
    }

    pub fn api_hash_range_scan_prefix_for_hash(
        &self,
        field_key_id: &str,
        api_hash: &str,
    ) -> Result<String, FieldKeyCodecError> {
        let h = self.storage_hash(field_key_id, api_hash)?;
        Ok(format!("{field_key_id}:{}{SEP}", escape_segment(&h)))
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

pub(crate) fn escape_segment(segment: &str) -> String {
    if !segment.contains([SEP, ESC]) {
        return segment.to_owned();
    }
    let mut out = String::with_capacity(segment.len() + 2);
    for c in segment.chars() {
        match c {
            SEP => out.push_str("\u{1}\u{1}"),
            ESC => out.push_str("\u{1}\u{2}"),
            other => out.push(other),
        }
    }
    out
}

#[must_use]
pub fn decode_hash_range_suffix(suffix: &str) -> Option<(String, String)> {
    let (esc, range) = suffix.split_once(SEP)?;
    if !esc.contains(ESC) {
        return Some((esc.to_owned(), range.to_owned()));
    }
    let mut hash = String::with_capacity(esc.len());
    let mut chars = esc.chars();
    while let Some(c) = chars.next() {
        if c == ESC {
            match chars.next()? {
                '\u{1}' => hash.push(SEP),
                '\u{2}' => hash.push(ESC),
                _ => return None,
            }
        } else {
            hash.push(c);
        }
    }
    Some((hash, range.to_owned()))
}
