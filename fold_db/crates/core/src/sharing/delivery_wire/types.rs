//! Wire-format record types for a delivery slice.

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LastDbSlicePayload {
    pub version: String,
    pub provenance: SliceProvenance,
    pub schemas: Vec<SliceSchema>,
    pub molecules: Vec<SignedMolecule>,
    pub atoms: Vec<ContentAddressedAtom>,
    /// First-class binary content-addressed files (photos, PDFs, …).
    ///
    /// Optional for back-compat with payloads that only carried JSON atoms.
    /// When present, file-bearing fields in `atoms[]` should reference these
    /// via `$lastdb_file.blob_ref` (see [`lastdb_file_pointer`]).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blobs: Vec<ContentAddressedBlob>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SliceProvenance {
    pub source: String,
    pub mode: String,
    pub created_at: u64,
    pub sender_public_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SliceSchema {
    pub schema_name: String,
    pub definition: Value,
    pub fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedMolecule {
    pub schema_name: String,
    pub record_key: String,
    pub field_name: String,
    pub atom_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub molecule_uuid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer_pubkey: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentAddressedAtom {
    pub atom_ref: String,
    pub value: Value,
    pub content_sha256: String,
}

/// First-class binary blob in a delivery slice.
///
/// Content is content-addressed as `sha256:<hex>` over the **raw file bytes**
/// (not the base64 encoding). On the JSON wire the bytes travel as
/// `bytes_b64`; consumers recompute the hash after decode to verify.
///
/// ## Operation Trinity (Holy Ghost) — local CAS
/// When [`Self::local_cipher_suite`] is set, `bytes_b64` holds the UTF-8
/// `ENC:…` envelope of the file bytes sealed under the **file KDK** (per-blob
/// DEK). The DEK itself is **not** stored in `cas_blobs`. At rest in the
/// local store it lives in the `$lastdb_file` pointer of the atom row
/// (`FileBlobAccess.dek`), which the run-file block encryption protects. A
/// delivery slice also carries it (`Self::access`). A Search inbox batch
/// keeps only the pointer's `name` and `media_type`; inbox files written
/// before that rule can still hold a copy of the pointer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentAddressedBlob {
    /// `sha256:<hex>` of the ciphertext (v2) or plaintext (older suites).
    pub blob_ref: String,
    /// Local v2 CAS rows store the ciphertext digest here, never plaintext SHA.
    /// Plain delivery blobs and older local rows retain their plaintext digest.
    pub content_sha256: String,
    /// Standard base64 of the raw file bytes, **or** (when
    /// `local_cipher_suite` is set) the UTF-8 `ENC:…` ciphertext under the
    /// file KDK (value is not raw-file base64 in that case).
    pub bytes_b64: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Raw byte length (decoded plaintext size).
    pub size: u64,
    /// Metadata that grants access to the matching encrypted remote CAS object.
    /// For Trinity local sealed cache entries this is typically omitted so the
    /// DEK is not dual-stored in plain on disk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<FileBlobAccess>,
    /// When set (e.g. [`FILE_BLOB_CIPHER_SUITE`]), `bytes_b64` is sealed local
    /// ciphertext under the file KDK — not plaintext file bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_cipher_suite: Option<String>,
    /// RFC3339 instant this row was durably stored (stamped by
    /// `put_blob_sealed_with_access_dedup_in_ops`). Information only: the blob counter decides
    /// when a row may go (`db_operations::atom_store::blob_counter`). Not part
    /// of content addressing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stored_at: Option<String>,
}

/// Shareable metadata for opening one content-addressed encrypted file blob.
///
/// `file_hash` is the sha256 hex of the plaintext bytes. New `blob_ref`s name
/// ciphertext instead. `dek` is the 32-byte per-blob key, hex
/// encoded. Slices carry this alongside file pointers so recipients can open
/// the same remote CAS object without re-encrypting or re-uploading it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileBlobAccess {
    pub blob_ref: String,
    pub file_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_scope: Option<String>,
    pub cipher_suite: String,
    pub dek: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encrypted_size_bytes: Option<u64>,
}

/// Minimal access metadata for an encrypted generated thumbnail/poster object.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileThumbnailAccess {
    pub dek: String,
    pub cipher_suite: String,
    pub encrypted_size_bytes: u64,
}

/// Reference-only thumbnail/poster metadata embedded in a `$lastdb_file` pointer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileThumbnailRef {
    pub blob_ref: String,
    /// Plaintext digest, required for ciphertext-addressed derivatives.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_hash: Option<String>,
    pub tier: String,
    pub access: FileThumbnailAccess,
    pub media_type: String,
    pub width: u32,
    pub height: u32,
    pub kind: String,
    pub source_hash: String,
}

impl FileThumbnailRef {
    /// Normalize old and new thumbnail pointers without exposing their key.
    pub fn file_access(&self) -> Result<FileBlobAccess, crate::error::FoldDbError> {
        let file_hash = match &self.file_hash {
            Some(hash) => hash.clone(),
            None if self.access.cipher_suite != super::FILE_BLOB_CIPHER_SUITE => {
                crate::sharing::file_blob_seal::file_blob_storage_hash(&self.blob_ref)?.to_owned()
            }
            None => {
                return Err(crate::error::FoldDbError::SecurityError(
                    "ciphertext-addressed thumbnail requires its plaintext digest".into(),
                ));
            }
        };
        let access = FileBlobAccess {
            blob_ref: self.blob_ref.clone(),
            file_hash,
            owner_scope: None,
            cipher_suite: self.access.cipher_suite.clone(),
            dek: self.access.dek.clone(),
            encrypted_size_bytes: Some(self.access.encrypted_size_bytes),
        };
        super::validate_file_blob_access_for_ref(&access, &self.blob_ref)?;
        Ok(access)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedLastDbSlicePayload {
    pub payload: LastDbSlicePayload,
    pub payload_sha256: String,
    pub signer_public_key: String,
    pub signature: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JweEnvelope {
    pub protected: String,
    #[serde(default)]
    pub encrypted_key: String,
    pub iv: String,
    pub ciphertext: String,
    pub tag: String,
}

#[derive(Debug, Clone)]
pub struct ExtractedFile {
    pub bytes: Vec<u8>,
    pub name: Option<String>,
    pub media_type: Option<String>,
}
