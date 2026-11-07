//! Data-only record-owned file marker. The whole record supplies its one stamp.
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileMarker {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    pub size_bytes: u64,
    pub sha256: String,
}

impl FileMarker {
    #[must_use]
    pub fn has_valid_hash(&self) -> bool {
        self.sha256.len() == 64
            && self
                .sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }
}

/// The largest plaintext of an inline file
/// (`decision-2026-11-07-lastdb-inline-file-cap-64kib`). Larger bytes use a
/// blob file.
pub const MAX_INLINE_FILE_BYTES: usize = 64 * 1024;
/// The longest inline file name or media type.
pub const MAX_INLINE_LABEL_BYTES: usize = 255;
/// Room for the marker and JSON framing of an encoded inline file.
const INLINE_MARKER_ALLOWANCE: usize = 1024;
/// The largest encoded inline file: its row, or its room in a portable entry.
pub const MAX_INLINE_STORED_BYTES: usize =
    MAX_INLINE_FILE_BYTES.div_ceil(3) * 4 + INLINE_MARKER_ALLOWANCE;

impl FileMarker {
    /// The bounds of an inline file marker: a valid digest, short labels and
    /// a size within the inline cap.
    #[must_use]
    pub fn fits_inline(&self) -> bool {
        let label_fits = |label: &Option<String>| {
            label
                .as_ref()
                .is_none_or(|label| label.len() <= MAX_INLINE_LABEL_BYTES)
        };
        self.has_valid_hash()
            && label_fits(&self.name)
            && label_fits(&self.media_type)
            && usize::try_from(self.size_bytes).is_ok_and(|size| size <= MAX_INLINE_FILE_BYTES)
    }
}

/// A small file of one record (a thumbnail, an icon)
/// (`decision-2026-11-07-lastdb-inline-file-own-row`). The bytes live in
/// their own row next to the record in the `records` collection, so the
/// backup sends them with the records (R2) and a restore brings them back.
/// The record value holds only the marker, so a record read never carries
/// the bytes. The run block encryption of the row protects the bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InlineFile {
    pub marker: FileMarker,
    /// Standard base64 of the plaintext.
    pub data: String,
}

impl InlineFile {
    /// Check the size and the labels, then hash and encode the plaintext.
    pub fn new(
        bytes: &[u8],
        name: Option<String>,
        media_type: Option<String>,
    ) -> Result<Self, String> {
        if bytes.len() > MAX_INLINE_FILE_BYTES {
            return Err(format!(
                "an inline file is {} bytes; maximum is {MAX_INLINE_FILE_BYTES}; store a larger file as the record's blob file",
                bytes.len()
            ));
        }
        let file = Self {
            marker: FileMarker {
                name,
                media_type,
                size_bytes: bytes.len() as u64,
                sha256: crate::hex::hex_lower(Sha256::digest(bytes)),
            },
            data: base64::engine::general_purpose::STANDARD.encode(bytes),
        };
        if !file.is_well_formed() {
            return Err(format!(
                "an inline file name or media type exceeds {MAX_INLINE_LABEL_BYTES} bytes"
            ));
        }
        Ok(file)
    }

    /// The plaintext, checked against the marker size and digest.
    pub fn bytes(&self) -> Result<Vec<u8>, String> {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(self.data.as_bytes())
            .map_err(|error| format!("inline file data is invalid base64: {error}"))?;
        if bytes.len() as u64 != self.marker.size_bytes
            || crate::hex::hex_lower(Sha256::digest(&bytes)) != self.marker.sha256
        {
            return Err("inline file bytes differ from their marker".into());
        }
        Ok(bytes)
    }

    /// How much this file adds to a portable entry above the record cap.
    #[must_use]
    pub fn stored_allowance(&self) -> usize {
        self.data.len() + INLINE_MARKER_ALLOWANCE
    }

    /// The bounds a decoder can check without hashing the bytes.
    #[must_use]
    pub fn is_well_formed(&self) -> bool {
        self.marker.fits_inline()
            && usize::try_from(self.marker.size_bytes)
                .is_ok_and(|size| self.data.len() == size.div_ceil(3) * 4)
    }

    /// The row value of this file.
    pub fn encode(&self) -> Result<Vec<u8>, String> {
        if !self.is_well_formed() {
            return Err("the inline file has an invalid marker, size or encoding".into());
        }
        serde_json::to_vec(self).map_err(|error| format!("encode inline file: {error}"))
    }

    /// Decode a row value and check its bytes against its marker.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() > MAX_INLINE_STORED_BYTES {
            return Err("an inline file row exceeds its bound".into());
        }
        let file: Self = serde_json::from_slice(bytes)
            .map_err(|error| format!("inline file row format: {error}"))?;
        if !file.is_well_formed() {
            return Err("the inline file has an invalid marker, size or encoding".into());
        }
        file.bytes()?;
        Ok(file)
    }
}

/// The inline file operation of one local write. Like `FileWrite`, it is
/// never serialized, so it is never captured, synced or replayed.
#[derive(Clone, Debug, PartialEq)]
pub enum InlineWrite {
    Put(InlineFile),
    Clear,
}
