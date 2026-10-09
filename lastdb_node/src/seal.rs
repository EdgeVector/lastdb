//! Sealed-box + SignedEnvelope transport crypto for Mini deliver.
//!
//! Mirrors fold_db_node `transport::auth_envelope` + `transport::connection`
//! so admin SPA `kanban-crypto` can open the same blobs. Kept inside
//! `lastdb_node` (not fold_db) until a shared crate is extracted.

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use fold_db::canonical::CanonicalWriter;
use fold_db::clock::unix_secs;
use fold_db::security::{Ed25519KeyPair, KeyUtils};
use hkdf::Hkdf;
use rand::rngs::OsRng;
use rand::RngCore;
use serde::Serialize;
use sha2::Sha256;
use x25519_dalek::{PublicKey, StaticSecret};

const SIGNED_ENVELOPE_MESSAGE_TYPE: &str = "signed_envelope";
const SIGNED_ENVELOPE_TTL_SECS: u64 = 7 * 24 * 60 * 60;
const HKDF_INFO: &[u8] = b"connection-request-aes";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SignedEnvelope {
    pub message_type: String,
    pub sender_public_key: String,
    pub expires_at: u64,
    pub nonce: String,
    pub signature: String,
    pub payload: serde_json::Value,
}

fn signed_envelope_canonical_bytes(envelope: &SignedEnvelope) -> Result<Vec<u8>, String> {
    let payload_json =
        serde_json::to_vec(&envelope.payload).map_err(|e| format!("payload json: {e}"))?;
    Ok(CanonicalWriter::new()
        .field(b"folddb:signed_envelope:v1")
        .field(envelope.sender_public_key.as_bytes())
        .u64(envelope.expires_at)
        .field(envelope.nonce.as_bytes())
        .field(&payload_json)
        .finish())
}

/// Seal `payload` into a SignedEnvelope signed by the node keypair.
pub fn seal_signed_envelope<T: Serialize>(
    payload: &T,
    keypair: &Ed25519KeyPair,
) -> Result<SignedEnvelope, String> {
    let payload_value =
        serde_json::to_value(payload).map_err(|e| format!("serialize payload: {e}"))?;
    let mut nonce = [0u8; 16];
    OsRng.fill_bytes(&mut nonce);
    let mut envelope = SignedEnvelope {
        message_type: SIGNED_ENVELOPE_MESSAGE_TYPE.to_string(),
        sender_public_key: keypair.public_key_base64(),
        expires_at: unix_secs().saturating_add(SIGNED_ENVELOPE_TTL_SECS),
        nonce: B64.encode(nonce),
        signature: String::new(),
        payload: payload_value,
    };
    let bytes = signed_envelope_canonical_bytes(&envelope)?;
    let sig = keypair.sign(&bytes);
    envelope.signature = KeyUtils::signature_to_base64(&sig);
    Ok(envelope)
}

/// Encrypt a SignedEnvelope (or any serializable) to a recipient X25519 pubkey.
///
/// Wire: `[ephemeral_pk: 32][nonce: 12][AES-256-GCM ciphertext+tag]`.
pub fn encrypt_to_x25519<T: Serialize>(
    target_public_key: &[u8; 32],
    payload: &T,
) -> Result<Vec<u8>, String> {
    let target_pk = PublicKey::from(*target_public_key);
    let ephemeral_secret = StaticSecret::random_from_rng(OsRng);
    let ephemeral_public = PublicKey::from(&ephemeral_secret);
    let shared = ephemeral_secret.diffie_hellman(&target_pk);

    let hk = Hkdf::<Sha256>::new(None, shared.as_bytes());
    let mut aes_key = [0u8; 32];
    hk.expand(HKDF_INFO, &mut aes_key)
        .map_err(|e| format!("HKDF expand failed: {e}"))?;

    let plaintext = serde_json::to_vec(payload).map_err(|e| format!("serialize: {e}"))?;
    let cipher =
        Aes256Gcm::new_from_slice(&aes_key).map_err(|e| format!("Invalid AES key: {e}"))?;
    let mut nonce_bytes = [0u8; 12];
    OsRng.fill_bytes(&mut nonce_bytes);
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce_bytes), plaintext.as_slice())
        .map_err(|e| format!("Encryption failed: {e}"))?;

    let mut output = Vec::with_capacity(32 + 12 + ciphertext.len());
    output.extend_from_slice(ephemeral_public.as_bytes());
    output.extend_from_slice(&nonce_bytes);
    output.extend_from_slice(&ciphertext);
    Ok(output)
}

/// Seal + encrypt in one step (node → recipient messaging key).
pub fn seal_and_encrypt_message<T: Serialize>(
    target_public_key: &[u8; 32],
    payload: &T,
    keypair: &Ed25519KeyPair,
) -> Result<Vec<u8>, String> {
    let envelope = seal_signed_envelope(payload, keypair)?;
    encrypt_to_x25519(target_public_key, &envelope)
}
