//! Content-addressed atoms and blobs: construction, sealing and verified decode.

use super::types::{ContentAddressedAtom, ContentAddressedBlob, FileBlobAccess};
use super::validate::validate_file_blob_access_for_ref;
use super::FILE_BLOB_CIPHER_SUITE;
use crate::error::FoldDbError;
use base64::{engine::general_purpose::STANDARD as B64_STD, Engine as _};
use lastdb_model::hex::hex_lower;
use serde_json::Value;
use sha2::{Digest, Sha256};

pub fn content_addressed_atom(value: Value) -> Result<ContentAddressedAtom, FoldDbError> {
    let bytes = serde_json::to_vec(&value).map_err(FoldDbError::from)?;
    let content_sha256 = hex_lower(Sha256::digest(bytes));
    Ok(ContentAddressedAtom {
        atom_ref: format!("sha256:{content_sha256}"),
        value,
        content_sha256,
    })
}

/// Build a first-class content-addressed blob and attach share access metadata.
pub fn content_addressed_blob_with_access(
    raw_bytes: &[u8],
    media_type: Option<String>,
    name: Option<String>,
    access: Option<FileBlobAccess>,
) -> ContentAddressedBlob {
    let content_sha256 = hex_lower(Sha256::digest(raw_bytes));
    let blob_ref = access.as_ref().map_or_else(
        || format!("sha256:{content_sha256}"),
        |access| access.blob_ref.clone(),
    );
    ContentAddressedBlob {
        blob_ref,
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

/// Store an already authenticated original binary envelope without a second seal.
/// The caller verifies its plaintext digest. This function verifies size and address.
pub fn content_addressed_blob_from_envelope(
    plaintext_size: u64,
    media_type: Option<String>,
    name: Option<String>,
    access: &FileBlobAccess,
    envelope: &[u8],
) -> Result<ContentAddressedBlob, FoldDbError> {
    validate_file_blob_access_for_ref(access, &access.blob_ref)?;
    if envelope.first() != Some(&1)
        || plaintext_size.checked_add(crate::sharing::file_blob_seal::FILE_BLOB_ENVELOPE_OVERHEAD)
            != Some(envelope.len() as u64)
        || access
            .encrypted_size_bytes
            .is_some_and(|size| size != envelope.len() as u64)
    {
        return Err(FoldDbError::SecurityError(
            "file blob envelope size differs".into(),
        ));
    }
    let digest = hex_lower(Sha256::digest(envelope));
    crate::sharing::file_blob_seal::verify_file_blob_identity(access, &access.file_hash, &digest)?;
    let mut bytes_b64 = String::with_capacity(4 + envelope.len().div_ceil(3) * 4);
    bytes_b64.push_str("ENC:");
    B64_STD.encode_string(envelope, &mut bytes_b64);
    Ok(ContentAddressedBlob {
        blob_ref: access.blob_ref.clone(),
        content_sha256: digest,
        bytes_b64,
        media_type,
        name,
        size: plaintext_size,
        access: None,
        local_cipher_suite: Some(access.cipher_suite.clone()),
        stored_at: None,
    })
}

fn decode_hex_32_key(input: &str) -> Result<[u8; 32], FoldDbError> {
    lastdb_model::hex::hex_decode_array::<32>(input).ok_or_else(|| {
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

/// Open a sealed local CAS blob under the file KDK.
///
/// The row must carry [`FILE_BLOB_CIPHER_SUITE`]. A row without a suite holds
/// plaintext, and LastDB writes no such row, so it fails closed here.
pub fn decode_blob_bytes_with_dek(
    blob: &ContentAddressedBlob,
    dek_hex: &str,
) -> Result<Vec<u8>, FoldDbError> {
    if blob.local_cipher_suite.as_deref() != Some(FILE_BLOB_CIPHER_SUITE) {
        return Err(FoldDbError::SecurityError(format!(
            "unsupported local CAS cipher suite {:?}",
            blob.local_cipher_suite
        )));
    }
    let dek = decode_hex_32_key(dek_hex)?;
    if !crate::crypto::is_sealed_at_rest(blob.bytes_b64.as_bytes()) {
        return Err(FoldDbError::SecurityError(
            "local CAS claims sealed suite but bytes are not ENC:…".into(),
        ));
    }
    let encoded = blob
        .bytes_b64
        .strip_prefix("ENC:")
        .ok_or_else(|| FoldDbError::SecurityError("v2 local CAS requires ENC text".into()))?;
    let envelope = B64_STD
        .decode(encoded)
        .map_err(|error| FoldDbError::SecurityError(error.to_string()))?;
    let digest = hex_lower(Sha256::digest(&envelope));
    if blob.content_sha256 != digest || blob.blob_ref != format!("sha256:{digest}") {
        return Err(FoldDbError::SecurityError(
            "local CAS ciphertext identity differs".into(),
        ));
    }
    let raw = crate::crypto::envelope::decrypt_envelope(&dek, &envelope)
        .map_err(|e| FoldDbError::SecurityError(format!("open local CAS under file KDK: {e}")))?;
    verify_blob_plaintext(blob, &raw)?;
    Ok(raw)
}

fn verify_blob_plaintext(blob: &ContentAddressedBlob, raw: &[u8]) -> Result<(), FoldDbError> {
    if blob.size != raw.len() as u64 {
        return Err(FoldDbError::SecurityError(
            "blob plaintext size differs".into(),
        ));
    }
    if blob.local_cipher_suite.as_deref() == Some(FILE_BLOB_CIPHER_SUITE) {
        return Ok(());
    }
    let digest = hex_lower(Sha256::digest(raw));
    if digest != blob.content_sha256 {
        return Err(FoldDbError::SecurityError(format!(
            "blob content hash mismatch: expected {}, got {digest}",
            blob.content_sha256
        )));
    }
    if let Some(access) = &blob.access {
        crate::sharing::file_blob_seal::seal_file_blob_for_access(raw, access)?;
        return Ok(());
    }
    let expected_ref = format!("sha256:{digest}");
    if blob.blob_ref != expected_ref {
        return Err(FoldDbError::SecurityError(format!(
            "blob_ref mismatch: expected {expected_ref}, got {}",
            blob.blob_ref
        )));
    }
    Ok(())
}
