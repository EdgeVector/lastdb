//! Authenticated restore instructions. Native files retain their stored bytes.

use crate::limits::*;
use crate::{bytes_sha256, DecimalU64, Digest, Id, ProtocolError, Text};
use serde::{Deserialize, Serialize};

/// Counted receipts add bit 512; older supported native backups remain valid.
pub const NATIVE_SUPPORTED_FEATURES_V1: u64 = 0x3fd;
pub const NATIVE_REQUIRED_FEATURES_V1: u64 = 0x1c5;

/// Service-visible summary. Exact CURRENT and home bytes remain authoritative
/// for directory roots, codecs, logical markers and all native format details.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeRoot {
    pub home_id: Id,
    pub home_digest: Digest,
    pub current_sha256: Digest,
    pub generation: DecimalU64,
    pub frontier: DecimalU64,
    pub required_features: DecimalU64,
}

impl NativeRoot {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        let features = self.required_features.get();
        if self.generation.get() == 0
            || self.frontier.get() == u64::MAX
            || features & !NATIVE_SUPPORTED_FEATURES_V1 != 0
            || features & NATIVE_REQUIRED_FEATURES_V1 != NATIVE_REQUIRED_FEATURES_V1
            || self.home_digest.as_str().bytes().all(|byte| byte == b'0')
        {
            return Err(ProtocolError::invalid("native_root"));
        }
        Ok(())
    }
}

/// The complete inventory must also verify bounds against the selected receipt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectExtent {
    pub receipt_id: Digest,
    pub offset: DecimalU64,
    pub length: DecimalU64,
}

impl ObjectExtent {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.length.get() == 0
            || self
                .offset
                .get()
                .checked_add(self.length.get())
                .is_none_or(|end| end > CLOUD_OBJECT_MAX_BYTES)
        {
            return Err(ProtocolError::invalid("object_extent"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileDependencyKind {
    /// `/$lastdb_file`: blob_ref, dek and cipher_suite use the existing main access format.
    Main,
    /// `/$lastdb_file/thumbnail` with kind `image`; keys use its `access` object.
    Thumbnail,
    /// `/$lastdb_file/thumbnail` with kind `video-poster`; keys use its `access` object.
    Poster,
}

/// All extents select backup-owned objects. Authorized source access remains an
/// export requirement. Existing encrypted pointer data retains its per-file key.
/// Extents concatenate to the complete stored source.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalFileDependency {
    pub kind: FileDependencyKind,
    pub logical_pointer_sha256: Digest,
    pub stored_source_sha256: Digest,
    pub stored_source_bytes: DecimalU64,
    pub encryption_suite: Text<128>,
    pub extents: Vec<ObjectExtent>,
}

/// Hash the exact decrypted whole file-pointer value, including its outer
/// `$lastdb_file` wrapper and every optional member, with RFC 8785 canonical JSON.
/// The caller validates the existing pointer format before this pure hash step.
/// Atom identifiers, atom metadata and encryption envelopes are not inputs.
pub fn logical_pointer_sha256(pointer: &serde_json::Value) -> Result<Digest, ProtocolError> {
    // The input already is a JSON value; canonical_bytes would clone its tree.
    let bytes =
        json_canon::to_vec(pointer).map_err(|_| ProtocolError::invalid("canonical_json"))?;
    Ok(bytes_sha256(&bytes))
}

impl ExternalFileDependency {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.stored_source_bytes.get() == 0
            || self.stored_source_bytes.get() > BACKUP_STORED_BYTES_MAX
            || self.extents.is_empty()
            || self.extents.len() > MANIFEST_PAGE_ENTRIES_MAX
            || !matches!(
                self.encryption_suite.as_str(),
                "lastdb-file-blob-convergent-v1:aes-256-gcm"
                    | "lastdb-file-blob-dek-v1:aes-256-gcm"
                    | "lastdb-file-blob-account-v2:aes-256-gcm-ciphertext-sha256"
            )
        {
            return Err(ProtocolError::invalid("file_dependency"));
        }
        let mut bytes = 0u64;
        for extent in &self.extents {
            extent.validate()?;
            bytes = bytes
                .checked_add(extent.length.get())
                .ok_or_else(|| ProtocolError::invalid("file_dependency.extent_bytes"))?;
        }
        if bytes != self.stored_source_bytes.get() {
            return Err(ProtocolError::invalid("file_dependency.extent_bytes"));
        }
        Ok(())
    }
}
