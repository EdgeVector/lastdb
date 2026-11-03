//! Wire-format record types for a delivery slice.

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LastDbSlicePayload {
    pub version: String,
    pub provenance: SliceProvenance,
    pub schemas: Vec<SliceSchema>,
    pub field_keys: Vec<SliceFieldKey>,
    pub records: Vec<ContentAddressedValue>,
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

/// One delivered field of one record: where it lives and the content address
/// of its value in `records`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SliceFieldKey {
    pub schema_name: String,
    pub record_key: String,
    pub field_name: String,
    pub value_ref: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentAddressedValue {
    pub value_ref: String,
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
/// local store it lives in the `$lastdb_file` pointer of the record row
/// (`FileBlobAccess.dek`), which the run-file block encryption protects. A
/// delivery slice also carries it (`Self::access`). A Search inbox batch
/// keeps only the pointer's `name` and `media_type`; inbox files written
/// before that rule can still hold a copy of the pointer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentAddressedBlob {
    /// `sha256:<hex>` of the ciphertext. A delivery blob without `access`
    /// names the plaintext instead.
    pub blob_ref: String,
    /// Local CAS rows store the ciphertext digest here, never plaintext SHA.
    /// A delivery blob stores its plaintext digest.
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
    /// RFC3339 instant this row was stored (stamped by `put-blob`). Information
    /// only: the row goes when its record loses its file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stored_at: Option<String>,
}

/// Shareable metadata for opening one content-addressed encrypted file blob.
///
/// `file_hash` is the sha256 hex of the plaintext bytes. `blob_ref` names the
/// ciphertext. `dek` is the 32-byte per-blob key, hex
/// encoded. The node derives it when it seals or opens a record's file; no
/// record stores it.
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
