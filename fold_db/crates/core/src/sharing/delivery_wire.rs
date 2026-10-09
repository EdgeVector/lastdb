//! Versioned portable wire format for consent-gated delivery.
//!
//! The payload is intentionally plain JSON after decryption: a non-LastDB
//! consumer can parse schemas, signed molecules, content-addressed atoms, and
//! first-class binary blobs without linking Rust code. Encryption uses
//! standard JWE JSON serialization (`alg=dir`, `enc=A256GCM`) so decryptors
//! can use off-the-shelf JOSE tooling.
//!
//! **Files vs field values:** structured field values live in `atoms[]` (JSON
//! CAS). Binary file bytes live in `blobs[]` (raw content CAS, base64 on the
//! JSON wire). File fields on records point at blobs via
//! [`lastdb_file_pointer`] values — they do **not** embed file bytes.

use crate::canonical::CanonicalWriter;
use crate::error::FoldDbError;
use crate::hex::hex_lower;
use crate::security::{Ed25519KeyPair, KeyUtils};
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use base64::{
    engine::general_purpose::{STANDARD as B64_STD, URL_SAFE_NO_PAD},
    Engine as _,
};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use rand::{rngs::OsRng, RngCore};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::{Read, Write};

pub const LASTDB_SLICE_PAYLOAD_VERSION: &str = "lastdb.slice.v1";
pub const LASTDB_SLICE_MEDIA_TYPE: &str = "application/vnd.lastdb.slice+json;v=1";
/// JWE protected-header claim: gzip the signed-slice JSON before A256GCM.
///
/// Compress-before-encrypt so Exemem's ~64KB sealed-message cap can hold a
/// full kanban board. Receivers without this claim keep the legacy path.
pub const JWE_ZIP_GZIP: &str = "gzip";

/// JSON object key for a portable file pointer stored as an atom value.
/// Canonical home is `crate::atom::file_pointer`; re-exported here for the
/// wire-format callers that have always imported it from delivery_wire.
pub use crate::atom::file_pointer::{blob_ref_from_atom_value, LASTDB_FILE_KEY};
/// Optional key for a reference-only generated thumbnail/poster inside a file pointer.
pub const LASTDB_FILE_THUMBNAIL_KEY: &str = "thumbnail";
/// Legacy spike key that embedded base64 content inside the atom (deprecated).
pub const LEGACY_FILE_KEY: &str = "$file";
/// Portable cipher label for per-file blob DEK metadata (convergent seal).
///
/// DEK + AES-GCM nonce are derived from the plaintext SHA-256 so concurrent
/// multi-device seals of the same content produce identical ciphertext at the
/// content-addressed CAS key.
pub const FILE_BLOB_CIPHER_SUITE: &str = "lastdb-file-blob-convergent-v1:aes-256-gcm";
/// Pre-migration random-DEK suite. Open paths must keep accepting this so
/// existing pointers can decrypt objects that were never re-sealed.
pub const FILE_BLOB_CIPHER_SUITE_LEGACY: &str = "lastdb-file-blob-dek-v1:aes-256-gcm";

/// True when `suite` is a known file-blob cipher suite (current or legacy).
#[must_use]
pub fn file_blob_cipher_suite_supported(suite: &str) -> bool {
    suite == FILE_BLOB_CIPHER_SUITE || suite == FILE_BLOB_CIPHER_SUITE_LEGACY
}
/// Remote storage tier used for generated thumbnail/poster objects.
pub const FILE_THUMBNAIL_TIER: &str = "r2-thumbs";
pub const FILE_THUMBNAIL_MAX_ENCRYPTED_SIZE_BYTES: u64 = 65_536;

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
    pub molecule_version: Option<u64>,
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
/// DEK). The DEK itself is **not** stored in `cas_blobs` — it lives only in
/// sealed atom content (`FileBlobAccess.dek`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentAddressedBlob {
    /// `sha256:<hex>` of the raw bytes.
    pub blob_ref: String,
    /// Hex sha256 of the raw bytes (without `sha256:` prefix).
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
    /// `blob_cas::put_raw_record`, or by the first `gc-file-blobs` pass to see
    /// an unstamped row). The GC age gate: an unreferenced blob is only
    /// reclaimable once this predates the run's `scan_started_at`, so a row
    /// whose referencing atom lands mid-scan is never deleted on the run that
    /// could not have seen that atom. Not part of content addressing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stored_at: Option<String>,
}

/// Shareable metadata for opening one content-addressed encrypted file blob.
///
/// `file_hash` is the sha256 hex of the plaintext bytes and must match
/// `blob_ref = sha256:{file_hash}`. `dek` is the 32-byte per-blob key, hex
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
    pub tier: String,
    pub access: FileThumbnailAccess,
    pub media_type: String,
    pub width: u32,
    pub height: u32,
    pub kind: String,
    pub source_hash: String,
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

pub fn canonical_payload_bytes(payload: &LastDbSlicePayload) -> Result<Vec<u8>, FoldDbError> {
    serde_json::to_vec(payload).map_err(FoldDbError::from)
}

pub fn payload_sha256(payload: &LastDbSlicePayload) -> Result<String, FoldDbError> {
    let bytes = canonical_payload_bytes(payload)?;
    Ok(hex_lower(Sha256::digest(bytes)))
}

pub fn sign_slice_payload(
    payload: LastDbSlicePayload,
    keypair: &Ed25519KeyPair,
) -> Result<SignedLastDbSlicePayload, FoldDbError> {
    let payload_sha256 = payload_sha256(&payload)?;
    let signed_bytes = CanonicalWriter::new()
        .field(b"folddb:lastdb_slice:v1")
        .field(payload_sha256.as_bytes())
        .finish();
    let signature = KeyUtils::signature_to_base64(&keypair.sign(&signed_bytes));
    Ok(SignedLastDbSlicePayload {
        payload,
        payload_sha256,
        signer_public_key: keypair.public_key_base64(),
        signature,
    })
}

/// Gzip (RFC 1952) the signed-slice JSON that becomes JWE plaintext.
fn gzip_bytes(raw: &[u8]) -> Result<Vec<u8>, FoldDbError> {
    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    enc.write_all(raw)
        .map_err(|e| FoldDbError::SecurityError(format!("gzip compress: {e}")))?;
    enc.finish()
        .map_err(|e| FoldDbError::SecurityError(format!("gzip finish: {e}")))
}

/// Inverse of [`gzip_bytes`].
fn gunzip_bytes(raw: &[u8]) -> Result<Vec<u8>, FoldDbError> {
    let mut dec = GzDecoder::new(raw);
    let mut out = Vec::new();
    dec.read_to_end(&mut out)
        .map_err(|e| FoldDbError::SecurityError(format!("gzip decompress: {e}")))?;
    Ok(out)
}

pub fn encrypt_signed_slice_jwe(
    signed: &SignedLastDbSlicePayload,
    content_key: &[u8; 32],
) -> Result<JweEnvelope, FoldDbError> {
    // Always gzip the signed JSON before A256GCM. Kanban full-board slices
    // exceed Exemem's ~64KB sealed-message cap without compress-before-encrypt.
    // Protected header carries zip=gzip so receivers can gunzip after decrypt
    // (legacy un-zipped envelopes still decrypt via decrypt_signed_slice_jwe).
    let protected = URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(&serde_json::json!({
            "alg": "dir",
            "enc": "A256GCM",
            "typ": LASTDB_SLICE_MEDIA_TYPE,
            "zip": JWE_ZIP_GZIP,
        }))
        .map_err(FoldDbError::from)?,
    );
    let json = serde_json::to_vec(signed).map_err(FoldDbError::from)?;
    let plaintext = gzip_bytes(&json)?;
    let mut iv = [0u8; 12];
    OsRng.fill_bytes(&mut iv);
    let key = Key::<Aes256Gcm>::from_slice(content_key);
    let cipher = Aes256Gcm::new(key);
    let encrypted = cipher
        .encrypt(
            Nonce::from_slice(&iv),
            aes_gcm::aead::Payload {
                msg: &plaintext,
                aad: protected.as_bytes(),
            },
        )
        .map_err(|e| FoldDbError::SecurityError(format!("JWE A256GCM encrypt failed: {e}")))?;
    let (ciphertext, tag) = encrypted.split_at(encrypted.len().saturating_sub(16));
    Ok(JweEnvelope {
        protected,
        encrypted_key: String::new(),
        iv: URL_SAFE_NO_PAD.encode(iv),
        ciphertext: URL_SAFE_NO_PAD.encode(ciphertext),
        tag: URL_SAFE_NO_PAD.encode(tag),
    })
}

pub fn decrypt_signed_slice_jwe(
    envelope: &JweEnvelope,
    content_key: &[u8; 32],
) -> Result<SignedLastDbSlicePayload, FoldDbError> {
    let protected_bytes = URL_SAFE_NO_PAD
        .decode(&envelope.protected)
        .map_err(|e| FoldDbError::SecurityError(format!("JWE protected header base64url: {e}")))?;
    let header: BTreeMap<String, String> =
        serde_json::from_slice(&protected_bytes).map_err(FoldDbError::from)?;
    if header.get("alg").map(String::as_str) != Some("dir")
        || header.get("enc").map(String::as_str) != Some("A256GCM")
    {
        return Err(FoldDbError::SecurityError(
            "unsupported JWE header; expected alg=dir enc=A256GCM".to_string(),
        ));
    }
    let zip = header.get("zip").map(String::as_str);
    if let Some(z) = zip {
        if z != JWE_ZIP_GZIP {
            return Err(FoldDbError::SecurityError(format!(
                "unsupported JWE zip={z}; expected {JWE_ZIP_GZIP} or absent"
            )));
        }
    }
    let iv = URL_SAFE_NO_PAD
        .decode(&envelope.iv)
        .map_err(|e| FoldDbError::SecurityError(format!("JWE iv base64url: {e}")))?;
    if iv.len() != 12 {
        return Err(FoldDbError::SecurityError(
            "JWE iv must be 12 bytes".to_string(),
        ));
    }
    let mut sealed = URL_SAFE_NO_PAD
        .decode(&envelope.ciphertext)
        .map_err(|e| FoldDbError::SecurityError(format!("JWE ciphertext base64url: {e}")))?;
    let tag = URL_SAFE_NO_PAD
        .decode(&envelope.tag)
        .map_err(|e| FoldDbError::SecurityError(format!("JWE tag base64url: {e}")))?;
    sealed.extend_from_slice(&tag);

    let key = Key::<Aes256Gcm>::from_slice(content_key);
    let cipher = Aes256Gcm::new(key);
    let mut plaintext = cipher
        .decrypt(
            Nonce::from_slice(&iv),
            aes_gcm::aead::Payload {
                msg: &sealed,
                aad: envelope.protected.as_bytes(),
            },
        )
        .map_err(|e| FoldDbError::SecurityError(format!("JWE A256GCM decrypt failed: {e}")))?;
    if zip == Some(JWE_ZIP_GZIP) {
        plaintext = gunzip_bytes(&plaintext)?;
    }
    serde_json::from_slice(&plaintext).map_err(FoldDbError::from)
}

pub fn content_addressed_atom(value: Value) -> Result<ContentAddressedAtom, FoldDbError> {
    let bytes = serde_json::to_vec(&value).map_err(FoldDbError::from)?;
    let content_sha256 = hex_lower(Sha256::digest(bytes));
    Ok(ContentAddressedAtom {
        atom_ref: format!("sha256:{content_sha256}"),
        value,
        content_sha256,
    })
}

/// Build a first-class content-addressed blob from raw file bytes.
pub fn content_addressed_blob(
    raw_bytes: &[u8],
    media_type: Option<String>,
    name: Option<String>,
) -> ContentAddressedBlob {
    content_addressed_blob_with_access(raw_bytes, media_type, name, None)
}

/// Build a first-class content-addressed blob and attach share access metadata.
pub fn content_addressed_blob_with_access(
    raw_bytes: &[u8],
    media_type: Option<String>,
    name: Option<String>,
    access: Option<FileBlobAccess>,
) -> ContentAddressedBlob {
    let content_sha256 = hex_lower(Sha256::digest(raw_bytes));
    ContentAddressedBlob {
        blob_ref: format!("sha256:{content_sha256}"),
        content_sha256,
        bytes_b64: B64_STD.encode(raw_bytes),
        media_type,
        name,
        size: raw_bytes.len() as u64,
        access,
        local_cipher_suite: None,
        stored_at: None,
    }
}

/// Build a **local CAS** blob sealed under the file KDK (Operation Trinity).
///
/// `bytes_b64` stores the UTF-8 `ENC:…` envelope (not raw-file base64). The DEK
/// is **not** persisted on the blob record — callers must open with the DEK from
/// sealed atom content.
pub fn content_addressed_blob_sealed_under_dek(
    raw_bytes: &[u8],
    media_type: Option<String>,
    name: Option<String>,
    dek_hex: &str,
) -> Result<ContentAddressedBlob, FoldDbError> {
    let dek = decode_hex_32_key(dek_hex)?;
    let sealed = crate::crypto::seal_at_rest_utf8(&dek, raw_bytes)
        .map_err(|e| FoldDbError::SecurityError(format!("seal local CAS under file KDK: {e}")))?;
    let sealed_utf8 = String::from_utf8(sealed)
        .map_err(|e| FoldDbError::SecurityError(format!("sealed local CAS not utf8: {e}")))?;
    let content_sha256 = hex_lower(Sha256::digest(raw_bytes));
    Ok(ContentAddressedBlob {
        blob_ref: format!("sha256:{content_sha256}"),
        content_sha256,
        bytes_b64: sealed_utf8,
        media_type,
        name,
        size: raw_bytes.len() as u64,
        access: None,
        local_cipher_suite: Some(FILE_BLOB_CIPHER_SUITE.to_string()),
        stored_at: None,
    })
}

fn decode_hex_32_key(input: &str) -> Result<[u8; 32], FoldDbError> {
    crate::hex::hex_decode_array::<32>(input).ok_or_else(|| {
        FoldDbError::SecurityError(format!(
            "file KDK must be 64 hex chars, got {} chars",
            input.len()
        ))
    })
}

/// Decode and verify a blob's raw bytes against `blob_ref` / `content_sha256`.
///
/// Sealed local CAS entries (`local_cipher_suite` set) **fail closed** here —
/// use [`decode_blob_bytes_with_dek`].
pub fn decode_blob_bytes(blob: &ContentAddressedBlob) -> Result<Vec<u8>, FoldDbError> {
    if blob.local_cipher_suite.is_some() {
        return Err(FoldDbError::SecurityError(
            "local CAS blob is sealed under file KDK; open with decode_blob_bytes_with_dek".into(),
        ));
    }
    let raw = B64_STD
        .decode(&blob.bytes_b64)
        .map_err(|e| FoldDbError::SecurityError(format!("blob bytes_b64 decode: {e}")))?;
    verify_blob_plaintext(blob, &raw)?;
    if let Some(access) = &blob.access {
        validate_file_blob_access_for_ref(access, &blob.blob_ref)?;
    }
    Ok(raw)
}

/// Open a Trinity sealed local CAS blob under the file KDK.
pub fn decode_blob_bytes_with_dek(
    blob: &ContentAddressedBlob,
    dek_hex: &str,
) -> Result<Vec<u8>, FoldDbError> {
    match blob.local_cipher_suite.as_deref() {
        None => return decode_blob_bytes(blob),
        Some(suite) if file_blob_cipher_suite_supported(suite) => {}
        other => {
            return Err(FoldDbError::SecurityError(format!(
                "unsupported local CAS cipher suite {other:?}"
            )));
        }
    }
    let dek = decode_hex_32_key(dek_hex)?;
    let sealed = blob.bytes_b64.as_bytes();
    if !crate::crypto::is_sealed_at_rest(sealed) {
        return Err(FoldDbError::SecurityError(
            "local CAS claims sealed suite but bytes are not ENC:…".into(),
        ));
    }
    let raw = crate::crypto::open_at_rest(&dek, sealed)
        .map_err(|e| FoldDbError::SecurityError(format!("open local CAS under file KDK: {e}")))?;
    verify_blob_plaintext(blob, &raw)?;
    Ok(raw)
}

fn verify_blob_plaintext(blob: &ContentAddressedBlob, raw: &[u8]) -> Result<(), FoldDbError> {
    let digest = hex_lower(Sha256::digest(raw));
    if digest != blob.content_sha256 {
        return Err(FoldDbError::SecurityError(format!(
            "blob content hash mismatch: expected {}, got {digest}",
            blob.content_sha256
        )));
    }
    let expected_ref = format!("sha256:{digest}");
    if blob.blob_ref != expected_ref {
        return Err(FoldDbError::SecurityError(format!(
            "blob_ref mismatch: expected {expected_ref}, got {}",
            blob.blob_ref
        )));
    }
    if blob.size as usize != raw.len() {
        return Err(FoldDbError::SecurityError(format!(
            "blob size mismatch: declared {}, actual {}",
            blob.size,
            raw.len()
        )));
    }
    Ok(())
}

/// Atom value that points at a first-class blob (no embedded file bytes).
pub fn lastdb_file_pointer(blob_ref: &str, name: Option<&str>, media_type: Option<&str>) -> Value {
    lastdb_file_pointer_with_access(blob_ref, name, media_type, None)
}

/// Atom value that points at a first-class blob and carries access metadata.
pub fn lastdb_file_pointer_with_access(
    blob_ref: &str,
    name: Option<&str>,
    media_type: Option<&str>,
    access: Option<&FileBlobAccess>,
) -> Value {
    lastdb_file_pointer_with_access_and_thumbnail(blob_ref, name, media_type, access, None)
}

/// Atom value that points at a first-class blob, carries access metadata, and
/// optionally references a generated thumbnail/poster object.
pub fn lastdb_file_pointer_with_access_and_thumbnail(
    blob_ref: &str,
    name: Option<&str>,
    media_type: Option<&str>,
    access: Option<&FileBlobAccess>,
    thumbnail: Option<&FileThumbnailRef>,
) -> Value {
    let mut inner = serde_json::Map::new();
    inner.insert("blob_ref".into(), Value::String(blob_ref.to_string()));
    if let Some(n) = name {
        inner.insert("name".into(), Value::String(n.to_string()));
    }
    if let Some(m) = media_type {
        inner.insert("media_type".into(), Value::String(m.to_string()));
    }
    if let Some(access) = access {
        inner.insert("file_hash".into(), Value::String(access.file_hash.clone()));
        if let Some(owner_scope) = &access.owner_scope {
            inner.insert("owner_scope".into(), Value::String(owner_scope.clone()));
        }
        inner.insert(
            "cipher_suite".into(),
            Value::String(access.cipher_suite.clone()),
        );
        inner.insert("dek".into(), Value::String(access.dek.clone()));
        if let Some(size) = access.encrypted_size_bytes {
            inner.insert(
                "encrypted_size_bytes".into(),
                Value::Number(serde_json::Number::from(size)),
            );
        }
    }
    if let Some(thumbnail) = thumbnail {
        inner.insert(
            LASTDB_FILE_THUMBNAIL_KEY.into(),
            serde_json::to_value(thumbnail).unwrap_or(Value::Null),
        );
    }
    let mut outer = serde_json::Map::new();
    outer.insert(LASTDB_FILE_KEY.into(), Value::Object(inner));
    Value::Object(outer)
}

/// Extract portable file-blob access metadata from a `$lastdb_file` pointer.
pub fn file_blob_access_from_atom_value(
    value: &Value,
) -> Result<Option<FileBlobAccess>, FoldDbError> {
    let Some(obj) = value.get(LASTDB_FILE_KEY).and_then(|v| v.as_object()) else {
        return Ok(None);
    };
    let Some(blob_ref) = obj.get("blob_ref").and_then(|v| v.as_str()) else {
        return Ok(None);
    };
    let Some(file_hash) = obj.get("file_hash").and_then(|v| v.as_str()) else {
        return Ok(None);
    };
    let Some(cipher_suite) = obj.get("cipher_suite").and_then(|v| v.as_str()) else {
        return Ok(None);
    };
    let Some(dek) = obj.get("dek").and_then(|v| v.as_str()) else {
        return Ok(None);
    };
    let access = FileBlobAccess {
        blob_ref: blob_ref.to_string(),
        file_hash: file_hash.to_string(),
        owner_scope: obj
            .get("owner_scope")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        cipher_suite: cipher_suite.to_string(),
        dek: dek.to_string(),
        encrypted_size_bytes: obj
            .get("encrypted_size_bytes")
            .and_then(serde_json::Value::as_u64),
    };
    validate_file_blob_access_for_ref(&access, blob_ref)?;
    Ok(Some(access))
}

/// Extract and validate an optional thumbnail/poster reference from a `$lastdb_file` pointer.
pub fn file_thumbnail_ref_from_atom_value(
    value: &Value,
) -> Result<Option<FileThumbnailRef>, FoldDbError> {
    let Some(obj) = value.get(LASTDB_FILE_KEY).and_then(|v| v.as_object()) else {
        return Ok(None);
    };
    let Some(thumbnail_value) = obj.get(LASTDB_FILE_THUMBNAIL_KEY) else {
        return Ok(None);
    };
    let thumbnail: FileThumbnailRef =
        serde_json::from_value(thumbnail_value.clone()).map_err(FoldDbError::from)?;
    let parent_access = file_blob_access_from_atom_value(value)?.ok_or_else(|| {
        FoldDbError::SecurityError(
            "thumbnail reference requires parent file blob access metadata".to_string(),
        )
    })?;
    validate_file_thumbnail_ref_for_pointer(obj, &parent_access, &thumbnail)?;
    Ok(Some(thumbnail))
}

/// Extract raw bytes + metadata from a field value that carries file content.
///
/// Recognizes:
/// - `$lastdb_file` with only a pointer → needs companion blob (returns None for bytes)
/// - legacy `$file` with `content_b64` → returns decoded bytes
pub fn extract_embedded_file(value: &Value) -> Result<Option<ExtractedFile>, FoldDbError> {
    if let Some(obj) = value.get(LEGACY_FILE_KEY).and_then(|v| v.as_object()) {
        let b64 = obj
            .get("content_b64")
            .and_then(|v| v.as_str())
            .ok_or_else(|| FoldDbError::Other("legacy $file missing content_b64".into()))?;
        let raw = B64_STD
            .decode(b64)
            .map_err(|e| FoldDbError::Other(format!("legacy $file content_b64: {e}")))?;
        return Ok(Some(ExtractedFile {
            bytes: raw,
            name: obj.get("name").and_then(|v| v.as_str()).map(str::to_string),
            media_type: obj
                .get("media_type")
                .and_then(|v| v.as_str())
                .map(str::to_string),
        }));
    }
    Ok(None)
}

#[derive(Debug, Clone)]
pub struct ExtractedFile {
    pub bytes: Vec<u8>,
    pub name: Option<String>,
    pub media_type: Option<String>,
}

/// Ensure every `$lastdb_file` / `$blob_ref` atom has a matching `blobs[]` entry.
pub fn validate_blob_refs(payload: &LastDbSlicePayload) -> Result<(), FoldDbError> {
    let known: BTreeMap<&str, &ContentAddressedBlob> = payload
        .blobs
        .iter()
        .map(|b| (b.blob_ref.as_str(), b))
        .collect();
    for atom in &payload.atoms {
        if let Some(r) = blob_ref_from_atom_value(&atom.value) {
            let Some(blob) = known.get(r) else {
                return Err(FoldDbError::Other(format!(
                    "atom {} references missing blob {r}",
                    atom.atom_ref
                )));
            };
            if let Some(pointer_access) = file_blob_access_from_atom_value(&atom.value)? {
                if blob.access.as_ref() != Some(&pointer_access) {
                    return Err(FoldDbError::Other(format!(
                        "atom {} carries blob access metadata that does not match blob {r}",
                        atom.atom_ref
                    )));
                }
                file_thumbnail_ref_from_atom_value(&atom.value)?;
            } else if let Some(obj) = atom
                .value
                .get(LASTDB_FILE_KEY)
                .and_then(serde_json::Value::as_object)
            {
                if obj.contains_key(LASTDB_FILE_THUMBNAIL_KEY) {
                    return Err(FoldDbError::SecurityError(format!(
                        "atom {} thumbnail reference requires parent file blob access metadata",
                        atom.atom_ref
                    )));
                }
            }
        }
    }
    for blob in &payload.blobs {
        decode_blob_bytes(blob)?;
    }
    Ok(())
}

pub fn validate_file_thumbnail_ref_for_pointer(
    pointer: &serde_json::Map<String, Value>,
    parent_access: &FileBlobAccess,
    thumbnail: &FileThumbnailRef,
) -> Result<(), FoldDbError> {
    if let Some(inline_key) = thumbnail_inline_bytes_key(
        pointer
            .get(LASTDB_FILE_THUMBNAIL_KEY)
            .and_then(serde_json::Value::as_object),
    ) {
        return Err(FoldDbError::SecurityError(format!(
            "thumbnail reference must not embed inline bytes key '{inline_key}'",
        )));
    }
    let parent_media_type = pointer
        .get("media_type")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            FoldDbError::SecurityError("thumbnail reference requires parent media_type".to_string())
        })?;
    if !is_thumbnail_eligible_media_type(parent_media_type) {
        return Err(FoldDbError::SecurityError(format!(
            "thumbnail reference parent media_type must be image/* or video/*, got {parent_media_type}",
        )));
    }
    if thumbnail.tier != FILE_THUMBNAIL_TIER {
        return Err(FoldDbError::SecurityError(format!(
            "unsupported thumbnail tier '{}'",
            thumbnail.tier
        )));
    }
    if thumbnail.source_hash != parent_access.file_hash {
        return Err(FoldDbError::SecurityError(
            "thumbnail source_hash must match parent file_hash".to_string(),
        ));
    }
    if thumbnail.access.encrypted_size_bytes > FILE_THUMBNAIL_MAX_ENCRYPTED_SIZE_BYTES {
        return Err(FoldDbError::SecurityError(format!(
            "thumbnail encrypted_size_bytes must be <= {FILE_THUMBNAIL_MAX_ENCRYPTED_SIZE_BYTES}",
        )));
    }
    if !file_blob_cipher_suite_supported(&thumbnail.access.cipher_suite) {
        return Err(FoldDbError::SecurityError(format!(
            "unsupported thumbnail cipher suite '{}'",
            thumbnail.access.cipher_suite
        )));
    }
    if thumbnail.access.dek.len() != 64
        || !thumbnail.access.dek.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(FoldDbError::SecurityError(
            "thumbnail access DEK must be 64 hex chars".to_string(),
        ));
    }
    if thumbnail.width == 0 || thumbnail.height == 0 {
        return Err(FoldDbError::SecurityError(
            "thumbnail width and height must be non-zero".to_string(),
        ));
    }
    if thumbnail.kind != "image" && thumbnail.kind != "video-poster" {
        return Err(FoldDbError::SecurityError(
            "thumbnail kind must be image or video-poster".to_string(),
        ));
    }
    Ok(())
}

fn is_thumbnail_eligible_media_type(media_type: &str) -> bool {
    media_type.starts_with("image/") || media_type.starts_with("video/")
}

fn thumbnail_inline_bytes_key(
    obj: Option<&serde_json::Map<String, Value>>,
) -> Option<&'static str> {
    let obj = obj?;
    [
        "bytes",
        "bytes_b64",
        "content",
        "content_b64",
        "data",
        "data_b64",
        "inline_bytes",
    ]
    .into_iter()
    .find(|key| obj.contains_key(*key))
}

pub fn validate_file_blob_access_for_ref(
    access: &FileBlobAccess,
    blob_ref: &str,
) -> Result<(), FoldDbError> {
    if access.blob_ref != blob_ref {
        return Err(FoldDbError::SecurityError(format!(
            "file blob access ref mismatch: expected {blob_ref}, got {}",
            access.blob_ref
        )));
    }
    if access.file_hash.len() != 64 || !access.file_hash.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(FoldDbError::SecurityError(
            "file blob access file_hash must be 64 hex chars".to_string(),
        ));
    }
    let expected_ref = format!("sha256:{}", access.file_hash);
    if expected_ref != blob_ref {
        return Err(FoldDbError::SecurityError(format!(
            "file blob access hash mismatch: expected {blob_ref}, got {expected_ref}",
        )));
    }
    if let Some(owner_scope) = &access.owner_scope {
        if owner_scope.is_empty()
            || owner_scope.contains('/')
            || owner_scope.contains("..")
            || owner_scope.bytes().any(|b| b.is_ascii_control())
        {
            return Err(FoldDbError::SecurityError(
                "file blob access owner_scope must be a safe storage scope component".to_string(),
            ));
        }
    }
    if !file_blob_cipher_suite_supported(&access.cipher_suite) {
        return Err(FoldDbError::SecurityError(format!(
            "unsupported file blob cipher suite '{}'",
            access.cipher_suite
        )));
    }
    if access.dek.len() != 64 || !access.dek.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(FoldDbError::SecurityError(
            "file blob access DEK must be 64 hex chars".to_string(),
        ));
    }
    Ok(())
}
