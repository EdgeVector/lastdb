use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use ed25519_dalek::{Signature, Signer};
use sha2::{Digest, Sha256};

pub use ed25519_dalek::{SigningKey, VerifyingKey};

use crate::error::KeyParseError;
use crate::hex::hex_lower;

/// Length in bytes of an Ed25519 public key.
pub const PUBLIC_KEY_LEN: usize = ed25519_dalek::PUBLIC_KEY_LENGTH;

/// Parse a base64 (standard alphabet, padded) Ed25519 public key into a
/// [`VerifyingKey`].
///
/// This is the developer's `dev_pubkey` as it travels on the wire — both
/// in a [`DevCert`](crate::DevCert) and as the key a `schema_claim` /
/// `app_register` envelope is verified against. Centralised here so the
/// base64 alphabet and length checks match across every consumer.
///
/// Per RFC 8032 §5.1.3 the y-coordinate of an Ed25519 public key must lie
/// in `[0, p)` where `p = 2^255 - 19`. `VerifyingKey::from_bytes` is lenient
/// — it reduces `y mod p` during decompression and accepts the result, so a
/// non-canonical encoding (`y ∈ [p, 2^255)`) parses as the same logical
/// point as its canonical sibling `y - p`. Because [`key_id`] is the
/// SHA-256 of the *encoded* 32 bytes, two encodings of the same point
/// produce two different `key_id`s — ambiguous identity that breaks
/// revocation / dedup by `dev_pubkey` or `key_id`. We reject the
/// non-canonical encoding up front so each Ed25519 point has exactly one
/// accepted wire form.
///
/// Small-order ("weak") points — the eight Ed25519 points whose order
/// divides the cofactor 8 (identity, its negative, and the six other
/// torsion points) — are also rejected here. `from_bytes` accepts them
/// because they are on-curve and canonically encoded, but every
/// signature verification against a small-order key fails at
/// [`verify_strict`](VerifyingKey::verify_strict). Returning `Ok` for a
/// key that can never verify anything is a lie: callers downstream
/// (schema_service, fold_db_node) treat a successful parse as "this is
/// the developer's usable identity," then see `BadSig` at every
/// envelope check and have no way to attribute the failure to the key
/// itself. Reject at parse time so the failure mode points at the
/// actual cause; a legitimately-generated keypair lands on a
/// small-order point with probability ≈ 2⁻²⁵², so no honest developer
/// is affected.
pub fn verifying_key_from_base64(b64: &str) -> Result<VerifyingKey, KeyParseError> {
    let bytes = BASE64
        .decode(b64.as_bytes())
        .map_err(|_| KeyParseError::NotBase64)?;
    let arr: [u8; PUBLIC_KEY_LEN] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| KeyParseError::WrongLength)?;
    if !is_canonical_y(&arr) {
        return Err(KeyParseError::InvalidPoint);
    }
    let vk = VerifyingKey::from_bytes(&arr).map_err(|_| KeyParseError::InvalidPoint)?;
    if vk.is_weak() {
        return Err(KeyParseError::WeakKey);
    }
    Ok(vk)
}

/// `true` iff the y-coordinate encoded in `bytes` is strictly less than
/// `p = 2^255 - 19`. Sign-of-x lives in the high bit of `bytes[31]` and is
/// masked off before the comparison; the remaining 255 bits are the y
/// value (little-endian).
fn is_canonical_y(bytes: &[u8; PUBLIC_KEY_LEN]) -> bool {
    // p in little-endian: [0xED, 0xFF, 0xFF, ..., 0xFF, 0x7F].
    // Walk from the high byte down — the first differing byte decides.
    for i in (0..PUBLIC_KEY_LEN).rev() {
        let y_byte = if i == 31 { bytes[i] & 0x7F } else { bytes[i] };
        let p_byte = match i {
            0 => 0xED,
            31 => 0x7F,
            _ => 0xFF,
        };
        if y_byte < p_byte {
            return true;
        }
        if y_byte > p_byte {
            return false;
        }
    }
    // All bytes equal → y == p, which is non-canonical (must be < p).
    false
}

/// Length in bytes of an Ed25519 signature.
pub const SIGNATURE_LEN: usize = ed25519_dalek::SIGNATURE_LENGTH;

/// Sign `payload` with `signing_key`. Returns the raw 64-byte signature.
///
/// The payload is whatever the caller wants signed — for envelope use,
/// this is the JCS canonicalization of the envelope minus its `sig`
/// field (see [`crate::sign_envelope`]).
pub fn sign(signing_key: &SigningKey, payload: &[u8]) -> [u8; SIGNATURE_LEN] {
    signing_key.sign(payload).to_bytes()
}

/// Verify that `sig` is a valid Ed25519 signature over `payload` under
/// `verifying_key`.
///
/// Uses the strict verification path from `ed25519-dalek` so signatures
/// that pass through it match the Ed25519 spec without the historical
/// malleability loopholes that some older libraries leave open.
pub fn verify(
    verifying_key: &VerifyingKey,
    sig: &[u8; SIGNATURE_LEN],
    payload: &[u8],
) -> Result<(), ed25519_dalek::SignatureError> {
    let signature = Signature::from_bytes(sig);
    verifying_key.verify_strict(payload, &signature)
}

/// Derive the envelope `key_id` for a verifying key: the lowercase hex
/// SHA-256 of the 32-byte Ed25519 public key.
///
/// The design doc pins `key_id = sha256(signing_pubkey)`. Hex (not
/// base64) keeps the value greppable in logs and stable across
/// canonicalizers (no padding choice, no URL-safe variant).
pub fn key_id(verifying_key: &VerifyingKey) -> String {
    let digest = Sha256::digest(verifying_key.to_bytes());
    hex_lower(&digest)
}
