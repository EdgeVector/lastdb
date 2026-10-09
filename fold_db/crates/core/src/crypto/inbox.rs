use super::error::{CryptoError, CryptoResult};
use crate::crypto::envelope::{decrypt_envelope, encrypt_envelope};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use ed25519_compact::{x25519, PublicKey as Ed25519PublicKey};
use hkdf::Hkdf;
use sha2::Sha256;

/// Seal an inbox message for a target recipient given their base64 Ed25519 public key.
pub fn seal_box_base64(recipient_ed25519_b64: &str, plaintext: &[u8]) -> CryptoResult<Vec<u8>> {
    let target_pub_bytes = STANDARD
        .decode(recipient_ed25519_b64)
        .map_err(|e| CryptoError::InvalidFormat(format!("decode public key: {e}")))?;
    let target_ed_pub = Ed25519PublicKey::from_slice(&target_pub_bytes)
        .map_err(|e| CryptoError::InvalidFormat(format!("parse public key: {e:?}")))?;

    let target_x25519 = x25519::PublicKey::from_ed25519(&target_ed_pub)
        .map_err(|e| CryptoError::InvalidFormat(format!("convert to x25519 failed: {e:?}")))?;

    // Generate ephemeral X25519 key pair for Sender
    let ephemeral_kp = x25519::KeyPair::generate();

    // Shared secret = DH(ephemeral_secret, target_public)
    let shared_secret = target_x25519
        .dh(&ephemeral_kp.sk)
        .map_err(|e| CryptoError::EncryptionFailed(format!("DH failed {e:?}")))?;

    let hk = Hkdf::<Sha256>::new(None, &*shared_secret);
    let mut aes_key = [0u8; 32];
    hk.expand(b"inbox-encryption-v1", &mut aes_key)
        .map_err(|e| CryptoError::EncryptionFailed(format!("HKDF failed: {e}")))?;

    let envelope = encrypt_envelope(&aes_key, plaintext)?;

    // Output is [ephemeral_public (32)] || [envelope]
    let mut out = Vec::with_capacity(32 + envelope.len());
    out.extend_from_slice(ephemeral_kp.pk.as_ref());
    out.extend_from_slice(&envelope);

    Ok(out)
}

/// Open an inbox message using our base64 Ed25519 secret key.
pub fn open_box_base64(my_ed25519_sec_b64: &str, ciphertext: &[u8]) -> CryptoResult<Vec<u8>> {
    if ciphertext.len() < 32 {
        return Err(CryptoError::InvalidFormat("Box too small".to_string()));
    }

    let ephemeral_pub_bytes: [u8; 32] = ciphertext[0..32].try_into().unwrap();
    let envelope = &ciphertext[32..];

    let ephemeral_pub = x25519::PublicKey::from_slice(&ephemeral_pub_bytes)
        .map_err(|e| CryptoError::InvalidFormat(format!("parse ephemeral pk: {e:?}")))?;

    let sec_bytes = STANDARD
        .decode(my_ed25519_sec_b64)
        .map_err(|e| CryptoError::InvalidFormat(format!("decode secret key: {e}")))?;
    // Ed25519 secret keys are stored either as a 32-byte raw seed or as
    // the 64-byte `seed || public` form (older identity writers, see
    // `security::keys::Ed25519KeyPair::from_secret_key`). Accept both —
    // the seed is the first 32 bytes — and reject any other length with
    // an error instead of panicking on the fixed-size array conversion.
    let seed_bytes: [u8; 32] = match sec_bytes.len() {
        32 | 64 => sec_bytes[..32]
            .try_into()
            .expect("slice is exactly 32 bytes"),
        n => {
            return Err(CryptoError::InvalidFormat(format!(
                "Ed25519 secret key must be 32 or 64 bytes, got {n}"
            )));
        }
    };
    let seed = ed25519_compact::Seed::new(seed_bytes);
    let my_ed_sec = ed25519_compact::KeyPair::from_seed(seed).sk;

    let my_x25519_sec = x25519::SecretKey::from_ed25519(&my_ed_sec)
        .map_err(|e| CryptoError::InvalidFormat(format!("convert to x25519 failed: {e:?}")))?;

    // Shared secret = DH(my_static_secret, ephemeral_public)
    let shared_secret = ephemeral_pub
        .dh(&my_x25519_sec)
        .map_err(|e| CryptoError::DecryptionFailed(format!("DH failed {e:?}")))?;

    let hk = Hkdf::<Sha256>::new(None, &*shared_secret);
    let mut aes_key = [0u8; 32];
    hk.expand(b"inbox-encryption-v1", &mut aes_key)
        .map_err(|e| CryptoError::DecryptionFailed(format!("HKDF failed: {e}")))?;

    decrypt_envelope(&aes_key, envelope)
}
