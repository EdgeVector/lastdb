//! Native per-key molecule storage codec.
//!
//! A tip uses `mk:{M}:{esc(hash)}\0{range}` and its header uses `mh:{M}`.
//! Callers add an optional storage scope with `build_storage_key`. Hash fields
//! use an empty range; HashRange fields use both coordinates. Physical tools
//! can also decode the historical empty-slot layouts.
//!
//! The supplied molecule UUID is an exact identity. Reads and writes never
//! substitute a hex or base64url spelling. Hash and range encryption bind that
//! same identity. One fixed key bundle defines each current molecule.
//!
//! Free key builders take storage-form coordinates. API coordinates pass once
//! through [`MoleculeKeyCodec::storage_hash`] and
//! [`MoleculeKeyCodec::storage_range`], or through an `api_*` builder. The hash
//! is byte-stuffed before the separator, so hash prefixes cannot overlap.
//! Each OPE key preserves range order within its own encoded range stream.

use crate::crypto::E2eKeys;

/// Chokepoint: API HashKey/RangeKey → storage segments.
///
/// Free `hash_range_*` builders below take **storage-form** segments.
#[derive(Debug, Clone)]
pub struct MoleculeKeyCodec {
    hash_encoding: HashKeyEncoding,
    range_encoding: RangeKeyEncoding,
    /// Required when hash encoding is blind.
    index_key: Option<[u8; 32]>,
    /// Required when range encoding is OPE.
    ope_key: Option<[u8; 32]>,
}

impl Default for MoleculeKeyCodec {
    fn default() -> Self {
        Self {
            hash_encoding: HashKeyEncoding::Plain,
            range_encoding: RangeKeyEncoding::Plain,
            index_key: None,
            ope_key: None,
        }
    }
}

impl MoleculeKeyCodec {
    /// HashKey-only constructor (range stays plain). Kept for call-site compatibility.
    #[must_use]
    pub fn new(encoding: HashKeyEncoding, index_key: Option<[u8; 32]>) -> Self {
        Self {
            hash_encoding: encoding,
            range_encoding: RangeKeyEncoding::Plain,
            index_key,
            ope_key: None,
        }
    }

    /// Full constructor for HashKey blind + RangeKey OPE.
    #[must_use]
    pub fn with_encodings(
        hash_encoding: HashKeyEncoding,
        range_encoding: RangeKeyEncoding,
        index_key: Option<[u8; 32]>,
        ope_key: Option<[u8; 32]>,
    ) -> Self {
        Self {
            hash_encoding,
            range_encoding,
            index_key,
            ope_key,
        }
    }

    #[must_use]
    pub fn plain() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn encoding(&self) -> HashKeyEncoding {
        self.hash_encoding
    }

    #[must_use]
    pub fn range_encoding(&self) -> RangeKeyEncoding {
        self.range_encoding
    }

    /// Public format identity only; no encryption key material enters a cursor.
    #[must_use]
    pub fn comparison_format(&self) -> &'static str {
        match (self.hash_encoding, self.range_encoding) {
            (HashKeyEncoding::Plain, RangeKeyEncoding::Plain) => "plain/plain:utf8-v1",
            (HashKeyEncoding::Plain, RangeKeyEncoding::OpeV1) => "plain/ope-v1:utf8-v1",
            (HashKeyEncoding::BlindV1, RangeKeyEncoding::Plain) => "blind-v1/plain:utf8-v1",
            (HashKeyEncoding::BlindV1, RangeKeyEncoding::OpeV1) => "blind-v1/ope-v1:utf8-v1",
        }
    }

    #[must_use]
    pub fn key_order(&self) -> schema_types::key_comparison::KeyOrderVersion {
        match self.range_encoding {
            RangeKeyEncoding::Plain | RangeKeyEncoding::OpeV1 => {
                schema_types::key_comparison::KeyOrderVersion::LegacyUtf8V1
            }
        }
    }

    /// Map API HashKey to storage-form hash segment.
    ///
    /// Empty API hash (range-only / Single slots) is never blinded.
    pub fn storage_hash(
        &self,
        molecule_uuid: &str,
        api_hash: &str,
    ) -> Result<String, MoleculeKeyCodecError> {
        if api_hash.is_empty() {
            return Ok(String::new());
        }
        match self.hash_encoding {
            HashKeyEncoding::Plain => Ok(api_hash.to_string()),
            HashKeyEncoding::BlindV1 => {
                let key = self
                    .index_key
                    .as_ref()
                    .ok_or(MoleculeKeyCodecError::MissingIndexKey)?;
                Ok(E2eKeys::blind_hash_key(key, molecule_uuid, api_hash))
            }
        }
    }

    /// Map API RangeKey to storage-form range segment.
    ///
    /// Empty range is never OPE-encoded (hash-only / Single empty slot).
    pub fn storage_range(
        &self,
        molecule_uuid: &str,
        api_range: &str,
    ) -> Result<String, MoleculeKeyCodecError> {
        if api_range.is_empty() {
            return Ok(String::new());
        }
        match self.range_encoding {
            RangeKeyEncoding::Plain => Ok(api_range.to_string()),
            RangeKeyEncoding::OpeV1 => {
                let key = self
                    .ope_key
                    .as_ref()
                    .ok_or(MoleculeKeyCodecError::MissingOpeKey)?;
                Ok(E2eKeys::ope_encode_range(key, molecule_uuid, api_range))
            }
        }
    }

    /// `mk:{M}:{esc(storage_hash)}\0{storage_range}` from API hash + range.
    ///
    /// Reads and writes use one key under the caller's exact `molecule_uuid`.
    pub fn api_hash_range_record_key(
        &self,
        molecule_uuid: &str,
        api_hash: &str,
        api_range: &str,
    ) -> Result<String, MoleculeKeyCodecError> {
        let h = self.storage_hash(molecule_uuid, api_hash)?;
        let r = self.storage_range(molecule_uuid, api_range)?;
        Ok(hash_range_record_key(molecule_uuid, &h, &r))
    }

    /// `mk:{M}:{esc(storage_hash(api_hash))}\0` scan prefix from API hash.
    pub fn api_hash_range_scan_prefix_for_hash(
        &self,
        molecule_uuid: &str,
        api_hash: &str,
    ) -> Result<String, MoleculeKeyCodecError> {
        let h = self.storage_hash(molecule_uuid, api_hash)?;
        Ok(hash_range_scan_prefix_for_hash(molecule_uuid, &h))
    }
}

/// Errors from [`MoleculeKeyCodec`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MoleculeKeyCodecError {
    /// Blind encoding requested but no E2E index key is configured.
    MissingIndexKey,
    /// OPE encoding requested but no E2E ope key is configured.
    MissingOpeKey,
}

impl std::fmt::Display for MoleculeKeyCodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingIndexKey => write!(
                f,
                "hash_key_encoding requires E2E index_key (set encoding=plain or provide index key)"
            ),
            Self::MissingOpeKey => write!(
                f,
                "range_key_encoding requires E2E ope_key (set encoding=plain or provide ope key)"
            ),
        }
    }
}

impl std::error::Error for MoleculeKeyCodecError {}

/// Sled key prefix for a per-key molecule record.
pub const MK_PREFIX: &str = "mk:";
/// Sled key prefix for a molecule header record.
pub const MH_PREFIX: &str = "mh:";
/// Active molecule-generation pointer. The pointer is the only mutable row in
/// a generation activation; generation bodies are immutable.
pub const MOLECULE_GENERATION_POINTER_PREFIX: &str = "mgp:v1:";
/// Immutable molecule-generation body rows.
pub const MOLECULE_GENERATION_RECORD_PREFIX: &str = "mgr:v1:";
/// Sparse post-generation slot deletions.
pub const MOLECULE_GENERATION_DELETE_PREFIX: &str = "mgd:v1:";

const SEP: char = '\u{0}'; // hash↔range separator (never appears in esc(hash))
const ESC: char = '\u{1}'; // escape byte for the hash segment

/// The separator, exported for keys built *outside* this module that must land
/// in the same LastStore partition as a molecule's per-key records.
///
/// LastStore's `HashGroupKey::PartitionPrefix` placement treats everything up to
/// and including the first occurrence of this byte as the partition. That is why
/// the hash segment is byte-stuffed: it makes "the first separator" an
/// unambiguous boundary rather than a guess. [`crate::atom::atom_key_codec`]
/// relies on both properties to give an atom body the same partition as its tip.
pub const PARTITION_SEP: char = SEP;

/// Byte-stuff a hash so it contains no `\0` (and no bare `\u{1}`), making the
/// `\0` separator and the escape unambiguous. `\0 -> \x01\x01`, `\x01 -> \x01\x02`.
fn escape_segment(segment: &str) -> String {
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

mod encoding;
pub use encoding::*;
mod generation_keys;
pub use generation_keys::*;
mod history_keys;
pub use history_keys::*;
mod record_keys;
pub use record_keys::*;
