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
/// Room for the marker and JSON framing of an inline file in its record.
const INLINE_MARKER_ALLOWANCE: usize = 1024;
/// The most that one inline file adds to its stored record.
pub const MAX_INLINE_STORED_BYTES: usize =
    MAX_INLINE_FILE_BYTES.div_ceil(3) * 4 + INLINE_MARKER_ALLOWANCE;

/// A small file kept inside its record value
/// (`decision-2026-11-07-lastdb-inline-file-in-record-value`). It lives in
/// the `records` collection, so the backup sends it with the records (R2) and
/// a restore brings it back with them. The run block encryption of the record
/// row protects the bytes.
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

    /// How much this file may add to its record above the record cap.
    #[must_use]
    pub fn stored_allowance(&self) -> usize {
        self.data.len() + INLINE_MARKER_ALLOWANCE
    }

    /// The bounds a decoder can check without hashing the bytes.
    #[must_use]
    pub fn is_well_formed(&self) -> bool {
        let label_fits = |label: &Option<String>| {
            label
                .as_ref()
                .is_none_or(|label| label.len() <= MAX_INLINE_LABEL_BYTES)
        };
        self.marker.has_valid_hash()
            && label_fits(&self.marker.name)
            && label_fits(&self.marker.media_type)
            && usize::try_from(self.marker.size_bytes).is_ok_and(|size| {
                size <= MAX_INLINE_FILE_BYTES && self.data.len() == size.div_ceil(3) * 4
            })
    }
}

/// The inline file operation of one local write. Like `FileWrite`, it is
/// never serialized, so it is never captured, synced or replayed.
#[derive(Clone, Debug, PartialEq)]
pub enum InlineWrite {
    Put(InlineFile),
    Clear,
}
