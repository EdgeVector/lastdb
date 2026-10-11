//! Slice signing and JWE (`alg=dir`, `enc=A256GCM`) envelope encryption.

use super::types::{JweEnvelope, LastDbSlicePayload, SignedLastDbSlicePayload};
use super::{JWE_ZIP_GZIP, LASTDB_SLICE_MEDIA_TYPE};
use crate::canonical::CanonicalWriter;
use crate::error::FoldDbError;
use crate::hex::hex_lower;
use crate::security::{Ed25519KeyPair, KeyUtils};
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use flate2::write::GzEncoder;
use flate2::Compression;
use rand::{rngs::OsRng, RngCore};
use sha2::{Digest, Sha256};
use std::io::Write;

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
