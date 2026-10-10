//! Pure convergent seal for personal file blobs.
//!
//! These helpers hold no network, no store and no cloud-sync state. They derive
//! the per-blob key and the AES-GCM envelope from the plaintext hash alone, so
//! both the cloud-sync upload path and the local `cas_blobs` write path agree on
//! the same `blob_ref`, the same DEK and the same pointer. They compile with the
//! `sharing` feature only; the cloud-sync engine imports them.

use crate::hex::hex_lower;
use crate::sharing::delivery_wire::{FileBlobAccess, FILE_BLOB_CIPHER_SUITE};
use hkdf::Hkdf;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Domain-separation salt for HKDF materials (not a secrecy secret — secrecy
/// of personal CAS remains owner-scoped presign auth; the DEK is still stored
/// on the pointer for share grants and for open without re-derivation).
const FILE_BLOB_HKDF_SALT: &[u8] = b"lastdb-file-blob-convergent-v1";

/// Bytes the v1 envelope adds to the plaintext: version (1) + nonce (12) +
/// AES-GCM tag (16). `encrypted_size_bytes = plaintext.len() + this`.
pub const FILE_BLOB_ENVELOPE_OVERHEAD: u64 = 1 + 12 + 16;

/// Failure of a pure seal step. The message is the same text the cloud-sync
/// engine reported before the helpers moved, so `SyncError::Crypto` renders
/// unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileBlobSealError(String);

impl FileBlobSealError {
    fn new(message: String) -> Self {
        Self(message)
    }

    /// The failure text, without any error-class prefix.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for FileBlobSealError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for FileBlobSealError {}

impl From<FileBlobSealError> for crate::error::FoldDbError {
    fn from(error: FileBlobSealError) -> Self {
        Self::SecurityError(error.0)
    }
}

/// Metadata that authorizes opening one content-addressed file blob.
///
/// `file_hash` / `blob_ref` address the B2 object by SHA-256 of the plaintext.
/// `dek` is the raw 32-byte blob key, hex encoded for storage in the DB pointer
/// or delivery metadata. Sharing this metadata is what grants access to the
/// ciphertext; the account sync key is not involved in blob decryption.
///
/// Under [`FILE_BLOB_CIPHER_SUITE`], the DEK is derived deterministically from
/// the plaintext hash (convergent encryption) so concurrent seals of the same
/// content agree on both the DEK and the remote ciphertext.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileBlobRef {
    pub blob_ref: String,
    pub file_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_scope: Option<String>,
    pub cipher_suite: String,
    pub dek: String,
    pub encrypted_size_bytes: u64,
}

impl FileBlobRef {
    /// The pointer-side access record for this blob.
    #[must_use]
    pub fn to_access(&self) -> FileBlobAccess {
        FileBlobAccess {
            blob_ref: self.blob_ref.clone(),
            file_hash: self.file_hash.clone(),
            owner_scope: self.owner_scope.clone(),
            cipher_suite: self.cipher_suite.clone(),
            dek: self.dek.clone(),
            encrypted_size_bytes: Some(self.encrypted_size_bytes),
        }
    }
}

pub(crate) fn decode_hex_32(input: &str) -> Result<[u8; 32], FileBlobSealError> {
    crate::hex::hex_decode_array::<32>(input).ok_or_else(|| {
        FileBlobSealError::new(format!(
            "file blob DEK must be 64 hex chars, got {} chars",
            input.len()
        ))
    })
}

/// Derive the convergent file-blob DEK and AES-GCM nonce from the plaintext
/// SHA-256 (hex). Same content → same materials on every device, so concurrent
/// seals produce byte-identical envelopes and cannot poison the CAS key.
pub(crate) fn derive_convergent_file_blob_materials(
    file_hash_hex: &str,
) -> Result<([u8; 32], [u8; 12]), FileBlobSealError> {
    let hash_bytes = decode_hex_32(file_hash_hex)?;
    let hk = Hkdf::<Sha256>::new(Some(FILE_BLOB_HKDF_SALT), &hash_bytes);
    let mut dek = [0u8; 32];
    hk.expand(b"dek", &mut dek)
        .map_err(|e| FileBlobSealError::new(format!("file blob DEK HKDF expand failed: {e}")))?;
    let mut nonce = [0u8; 12];
    hk.expand(b"nonce", &mut nonce)
        .map_err(|e| FileBlobSealError::new(format!("file blob nonce HKDF expand failed: {e}")))?;
    Ok((dek, nonce))
}

/// Seal under a fixed nonce using the same v1 envelope wire format as
/// [`crate::crypto::envelope::encrypt_envelope`] so
/// `LocalCryptoProvider::decrypt` can open the result.
///
/// Only the cloud-sync upload needs the ciphertext; the local path computes the
/// size from [`FILE_BLOB_ENVELOPE_OVERHEAD`] and seals under the at-rest seam.
#[cfg(feature = "cloud-sync")]
pub(crate) fn encrypt_envelope_with_nonce(
    key: &[u8; 32],
    nonce_bytes: &[u8; 12],
    plaintext: &[u8],
) -> Result<Vec<u8>, FileBlobSealError> {
    use aes_gcm::{
        aead::{Aead, KeyInit},
        Aes256Gcm, Nonce,
    };

    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| FileBlobSealError::new(format!("file blob AES-GCM key invalid: {e}")))?;
    let nonce = Nonce::from_slice(nonce_bytes);
    let ciphertext = cipher
        .encrypt(nonce, plaintext)
        .map_err(|e| FileBlobSealError::new(format!("file blob AES-GCM encrypt failed: {e}")))?;
    // Wire format: version(1) || nonce(12) || ciphertext+tag
    let mut envelope = Vec::with_capacity(1 + 12 + ciphertext.len());
    envelope.push(0x01);
    envelope.extend_from_slice(nonce_bytes);
    envelope.extend_from_slice(&ciphertext);
    Ok(envelope)
}

/// Convergent seal: the blob reference and the ciphertext for `plaintext`.
#[cfg(feature = "cloud-sync")]
pub(crate) fn seal_file_blob(
    plaintext: &[u8],
) -> Result<(FileBlobRef, Vec<u8>), FileBlobSealError> {
    let file_hash = hex_lower(Sha256::digest(plaintext));
    let (dek, nonce) = derive_convergent_file_blob_materials(&file_hash)?;
    // One plaintext per DEK (content-addressed): fixed nonce is safe and makes
    // re-seals of the same bytes produce identical ciphertext.
    let encrypted = encrypt_envelope_with_nonce(&dek, &nonce, plaintext)?;
    let blob_ref = file_blob_ref(file_hash, &dek, encrypted.len() as u64);
    Ok((blob_ref, encrypted))
}

/// The reference [`seal_file_blob`] would produce for `plaintext`, without
/// sealing it: hash, derive the DEK, and compute the envelope size. No network
/// and no allocation proportional to the plaintext.
pub fn local_file_blob_ref(plaintext: &[u8]) -> Result<FileBlobRef, FileBlobSealError> {
    let file_hash = hex_lower(Sha256::digest(plaintext));
    let (dek, _nonce) = derive_convergent_file_blob_materials(&file_hash)?;
    let encrypted_size = plaintext.len() as u64 + FILE_BLOB_ENVELOPE_OVERHEAD;
    Ok(file_blob_ref(file_hash, &dek, encrypted_size))
}

fn file_blob_ref(file_hash: String, dek: &[u8; 32], encrypted_size_bytes: u64) -> FileBlobRef {
    FileBlobRef {
        blob_ref: format!("sha256:{file_hash}"),
        file_hash,
        owner_scope: None,
        cipher_suite: FILE_BLOB_CIPHER_SUITE.to_string(),
        dek: hex_lower(dek),
        encrypted_size_bytes,
    }
}
