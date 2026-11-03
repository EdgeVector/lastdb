//! The file marker of one record (design `design-lastdb-no-shared-file-blobs`).
//!
//! A record has at most one file. The marker says that the record has one and
//! describes it. It holds no path and no key: the file lives
//! at a path made from the schema and the record key. Only the file routes set
//! or remove the marker, so a record write cannot name another record's file.
//!
//! The marker has its own stamp, ordered like a field stamp. The newer stamp
//! wins, and a delete at or above the stamp removes the marker.

use super::{FieldStamp, StoredRecord};
use serde::{Deserialize, Serialize};

/// What a record says about its file. `sha256` is the plaintext SHA-256 in
/// lowercase hex: it checks the bytes after decryption, and it is the input of
/// the file encryption key.
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
    /// True when `sha256` is 64 lowercase hex digits.
    #[must_use]
    pub fn has_valid_hash(&self) -> bool {
        self.sha256.len() == 64
            && self
                .sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }
}

/// The stored marker: the marker and the stamp of the write that set it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordFile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    pub size_bytes: u64,
    pub sha256: String,
    /// The stamp of the write that set this file.
    pub stamp: FieldStamp,
}

impl RecordFile {
    #[must_use]
    pub fn new(marker: FileMarker, stamp: FieldStamp) -> Self {
        Self {
            name: marker.name,
            media_type: marker.media_type,
            size_bytes: marker.size_bytes,
            sha256: marker.sha256,
            stamp,
        }
    }

    /// The marker without its stamp, as a read reply returns it.
    #[must_use]
    pub fn marker(&self) -> FileMarker {
        FileMarker {
            name: self.name.clone(),
            media_type: self.media_type.clone(),
            size_bytes: self.size_bytes,
            sha256: self.sha256.clone(),
        }
    }
}

impl StoredRecord {
    /// Set the file of the record. The newer stamp wins, as for a field.
    /// Returns true when the marker changed: the caller then writes the new
    /// bytes at the record's file path in the same batch.
    pub fn set_file(&mut self, marker: FileMarker, stamp: FieldStamp) -> bool {
        if self.file_refuses(&stamp) {
            return false;
        }
        self.file = Some(RecordFile::new(marker, stamp));
        true
    }

    /// Remove the file of the record. Returns true when the record had a file
    /// with an older stamp: the caller then deletes the file path in the same
    /// batch.
    pub fn clear_file(&mut self, stamp: &FieldStamp) -> bool {
        if self.file_refuses(stamp) || self.file.is_none() {
            return false;
        }
        self.file = None;
        true
    }

    /// True when the delete floor or the stored file stamp is the same as or
    /// newer than `stamp`.
    fn file_refuses(&self, stamp: &FieldStamp) -> bool {
        self.deleted.as_ref().is_some_and(|floor| stamp <= floor)
            || self.file.as_ref().is_some_and(|file| *stamp <= file.stamp)
    }
}
