//! Authenticated restore instructions. Native files retain their stored bytes.

use crate::limits::*;
use crate::{bytes_sha256, DecimalU64, Digest, Id, ProtocolError, Text};
use serde::{Deserialize, Serialize};

/// Service-visible summary. Exact CURRENT and home bytes remain authoritative
/// for directory roots, codecs, logical markers and all native format details.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeRoot {
    /// Id of the backed-up home.
    pub home_id: Id,
    /// Digest of the home.
    pub home_digest: Digest,
    /// SHA-256 of the home's CURRENT list.
    pub current_sha256: Digest,
    /// Home generation.
    pub generation: DecimalU64,
    /// Native frontier.
    pub frontier: DecimalU64,
    /// Feature bits of the run home that wrote this root.
    pub required_features: DecimalU64,
}

/// The complete inventory must also verify bounds against the selected receipt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectExtent {
    /// Receipt whose object holds these bytes.
    pub receipt_id: Digest,
    /// Start of this slice in the object, in bytes.
    pub offset: DecimalU64,
    /// Length of this slice. Zero is rejected, and offset plus length must fit in one cloud object.
    pub length: DecimalU64,
}

impl ObjectExtent {
    /// Reject a zero length or a slice that extends past `CLOUD_OBJECT_MAX_BYTES`.
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
/// Which stored file an extent list restores.
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
    /// Which file role these extents restore.
    pub kind: FileDependencyKind,
    /// SHA-256 of the decrypted file-pointer JSON, including its wrapper.
    pub logical_pointer_sha256: Digest,
    /// SHA-256 of the stored source bytes.
    pub stored_source_sha256: Digest,
    /// Stored source length. The extent lengths must add up to this.
    pub stored_source_bytes: DecimalU64,
    /// Encryption suite name. Validation accepts only the account-v2 AES-256-GCM suite string.
    pub encryption_suite: Text<128>,
    /// Object slices that concatenate to the stored source.
    pub extents: Vec<ObjectExtent>,
}

/// Hash the exact decrypted whole file-pointer value, including its outer
/// `$lastdb_file` wrapper and every optional member, with RFC 8785 canonical JSON.
/// The caller validates the existing pointer format before this pure hash step.
/// Record identifiers, record metadata and encryption envelopes are not inputs.
pub fn logical_pointer_sha256(pointer: &serde_json::Value) -> Result<Digest, ProtocolError> {
    // The input already is a JSON value; canonical_bytes would clone its tree.
    let bytes =
        json_canon::to_vec(pointer).map_err(|_| ProtocolError::invalid("canonical_json"))?;
    Ok(bytes_sha256(&bytes))
}

impl ExternalFileDependency {
    /// Reject a bad length, an empty or oversized extent list, the wrong suite, or extents that do not sum to the stored length.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.stored_source_bytes.get() == 0
            || self.stored_source_bytes.get() > BACKUP_STORED_BYTES_MAX
            || self.extents.is_empty()
            || self.extents.len() > MANIFEST_PAGE_ENTRIES_MAX
            || self.encryption_suite.as_str()
                != "lastdb-file-blob-account-v2:aes-256-gcm-ciphertext-sha256"
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
