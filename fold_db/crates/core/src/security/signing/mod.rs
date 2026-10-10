//! Message signing and verification functionality

use crate::security::{Ed25519PublicKey, KeyUtils};

/// Verifies a molecule signature against canonical bytes.
/// Returns true if valid, false otherwise. Never panics.
pub fn verify_molecule_signature(
    canonical_bytes: &[u8],
    signature_base64: &str,
    writer_pubkey_base64: &str,
) -> bool {
    let Ok(pubkey) = Ed25519PublicKey::from_base64(writer_pubkey_base64) else {
        return false;
    };
    let Ok(signature) = KeyUtils::signature_from_base64(signature_base64) else {
        return false;
    };
    pubkey.verify(canonical_bytes, &signature)
}
