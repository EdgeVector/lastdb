//! Pure account-derived encryption for personal file blobs.
//!
//! These helpers hold no network, no store and no cloud-sync state. They derive
//! the per-blob key from the private account key and plaintext hash, so
//! both the cloud-sync upload path and the local `cas_blobs` write path agree on
//! the same `blob_ref`, the same DEK and the same pointer. They compile with the
//! `sharing` feature only; the cloud-sync engine imports them.

use crate::sharing::delivery_wire::{
    file_blob_cipher_suite_supported, FileBlobAccess, FILE_BLOB_CIPHER_SUITE,
};
use hkdf::Hkdf;
use lastdb_model::hex::hex_lower;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const FILE_BLOB_ACCOUNT_SALT: &[u8] = b"lastdb-file-blob-account-v2";
const FILE_BLOB_NONCE_SALT: &[u8] = b"lastdb-file-blob-account-v2:nonce";

/// Bytes the v1 envelope adds to the plaintext: version (1) + nonce (12) +
/// AES-GCM tag (16). `encrypted_size_bytes = plaintext.len() + this`.
pub const FILE_BLOB_ENVELOPE_OVERHEAD: u64 = 1 + 12 + 16;

/// Failure of a pure seal step. The message keeps the text the retired
/// cloud-sync engine reported.
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
/// `file_hash` is the plaintext SHA-256. `blob_ref` names the ciphertext SHA-256.
/// `dek` is the raw 32-byte blob key, hex encoded for storage in the DB pointer
/// or delivery metadata. Sharing this metadata is what grants access to the
/// ciphertext; the account sync key is not involved in blob decryption.
///
/// Under [`FILE_BLOB_CIPHER_SUITE`], the private account key and plaintext hash
/// produce one file key. The same account's devices produce identical ciphertext.
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
    lastdb_model::hex::hex_decode_array::<32>(input).ok_or_else(|| {
        FileBlobSealError::new(format!(
            "file blob DEK must be 64 hex chars, got {} chars",
            input.len()
        ))
    })
}

/// Bind a file key to the private account key and original plaintext digest.
fn derive_account_file_blob_key(
    account_key: &[u8; 32],
    file_hash_hex: &str,
) -> Result<[u8; 32], FileBlobSealError> {
    let hash_bytes = decode_hex_32(file_hash_hex)?;
    let hk = Hkdf::<Sha256>::new(Some(FILE_BLOB_ACCOUNT_SALT), account_key);
    let mut dek = [0u8; 32];
    hk.expand(
        &[b"file-dek\0".as_slice(), hash_bytes.as_slice()].concat(),
        &mut dek,
    )
    .map_err(|e| FileBlobSealError::new(format!("file blob DEK HKDF expand failed: {e}")))?;
    Ok(dek)
}

/// The pointer's file key is sufficient to reproduce a repair or recipient copy.
fn file_blob_nonce(dek: &[u8; 32]) -> Result<[u8; 12], FileBlobSealError> {
    let hk = Hkdf::<Sha256>::new(Some(FILE_BLOB_NONCE_SALT), dek);
    let mut nonce = [0u8; 12];
    hk.expand(b"aes-256-gcm-nonce", &mut nonce)
        .map_err(|e| FileBlobSealError::new(format!("file blob nonce HKDF expand failed: {e}")))?;
    Ok(nonce)
}

/// Seal under a fixed nonce using the same v1 envelope wire format as
/// [`crate::crypto::envelope::encrypt_envelope`] so
/// `LocalCryptoProvider::decrypt` can open the result.
///
/// Local CAS stores these exact bytes inside its existing `ENC:` text wrapper.
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

/// Account-derived file reference and its exact deterministic ciphertext.
pub(crate) fn seal_file_blob(
    plaintext: &[u8],
    account_key: &[u8; 32],
) -> Result<(FileBlobRef, Vec<u8>), FileBlobSealError> {
    let file_hash = hex_lower(Sha256::digest(plaintext));
    let dek = derive_account_file_blob_key(account_key, &file_hash)?;
    let nonce = file_blob_nonce(&dek)?;
    // One plaintext per DEK (content-addressed): fixed nonce is safe and makes
    // re-seals of the same bytes produce identical ciphertext.
    let encrypted = encrypt_envelope_with_nonce(&dek, &nonce, plaintext)?;
    let blob_ref = file_blob_ref(file_hash, &dek, &encrypted);
    Ok((blob_ref, encrypted))
}

fn file_blob_ref(file_hash: String, dek: &[u8; 32], ciphertext: &[u8]) -> FileBlobRef {
    FileBlobRef {
        blob_ref: format!("sha256:{}", hex_lower(Sha256::digest(ciphertext))),
        file_hash,
        owner_scope: None,
        cipher_suite: FILE_BLOB_CIPHER_SUITE.to_string(),
        dek: hex_lower(dek),
        encrypted_size_bytes: ciphertext.len() as u64,
    }
}

/// The public storage digest: the ciphertext SHA-256, not the plaintext digest.
pub fn file_blob_storage_hash(blob_ref: &str) -> Result<&str, FileBlobSealError> {
    blob_ref
        .strip_prefix("sha256:")
        .filter(|hash| hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit()))
        .ok_or_else(|| FileBlobSealError::new("file blob storage digest is invalid".into()))
}

/// Check authenticated plaintext/ciphertext digests against the original pointer.
/// Stream readers can supply these digests without a file-sized allocation.
pub fn verify_file_blob_identity(
    access: &FileBlobAccess,
    plaintext_sha256: &str,
    ciphertext_sha256: &str,
) -> Result<(), FileBlobSealError> {
    if !file_blob_cipher_suite_supported(&access.cipher_suite)
        || access.file_hash != plaintext_sha256
    {
        return Err(FileBlobSealError::new(
            "file blob plaintext identity differs".into(),
        ));
    }
    if file_blob_storage_hash(&access.blob_ref)? != ciphertext_sha256 {
        return Err(FileBlobSealError::new(
            "file blob storage identity differs".into(),
        ));
    }
    Ok(())
}

/// Reproduce a [`FILE_BLOB_CIPHER_SUITE`] envelope exactly from the pointer key.
pub fn seal_file_blob_for_access(
    plaintext: &[u8],
    access: &FileBlobAccess,
) -> Result<Vec<u8>, FileBlobSealError> {
    if !file_blob_cipher_suite_supported(&access.cipher_suite) {
        return Err(FileBlobSealError::new(format!(
            "unsupported file blob cipher suite '{}'",
            access.cipher_suite
        )));
    }
    let plaintext_sha256 = hex_lower(Sha256::digest(plaintext));
    let key = decode_hex_32(&access.dek)?;
    let nonce = file_blob_nonce(&key)?;
    let encrypted = encrypt_envelope_with_nonce(&key, &nonce, plaintext)?;
    verify_file_blob_identity(
        access,
        &plaintext_sha256,
        &hex_lower(Sha256::digest(&encrypted)),
    )?;
    if access
        .encrypted_size_bytes
        .is_some_and(|size| size != encrypted.len() as u64)
    {
        return Err(FileBlobSealError::new(
            "file blob ciphertext size differs".into(),
        ));
    }
    Ok(encrypted)
}
