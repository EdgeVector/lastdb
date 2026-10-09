use super::error::{CryptoError, CryptoResult};
use aes_gcm::{
    aead::{Aead, KeyInit, OsRng, Payload},
    Aes256Gcm, Nonce,
};
use aes_gcm_siv::{Aes256GcmSiv, Nonce as SivNonce};
use rand::RngCore;

/// Version 1 envelope: AES-256-GCM, 12-byte nonce, 16-byte tag, no
/// key-id, no associated data. `[version:1][nonce:12][ciphertext+tag]`.
///
/// Retained verbatim for backward compatibility — every value already
/// sealed at rest is a v1 envelope, and v1 stays readable forever (see
/// the at-rest threat model §5.2 "migration"). New seals at a single
/// static key keep emitting v1; v2 is opt-in via [`encrypt_envelope_v2`].
pub const ENVELOPE_VERSION: u8 = 0x01;

/// Version 2 envelope: **AES-256-GCM-SIV** (nonce-misuse-resistant), with
/// a 4-byte `key_id` and a storage context bound as associated data (AAD).
/// `[version:1][key_id:4][nonce:12][ciphertext+tag]`.
///
/// Three properties v1 lacks (threat model §5.2):
/// - **`key_id`** makes "which key sealed this?" a *lookup* (via
///   [`peek_envelope`]) instead of trial decryption — the precondition
///   for the wrapped-DEK keyring and cheap key rotation (Gap G5).
/// - **AAD** *can* bind each ciphertext to its location (namespace + Sled
///   key), so that a ciphertext cannot be swapped to another slot within
///   the store and still decrypt. This is a capability of the codec — it
///   is only realized when the caller actually supplies that storage
///   context as `aad`. The current production adapter
///   ([`KeyringCryptoProvider`](super::keyring_provider::KeyringCryptoProvider))
///   binds an **empty** AAD because the `CryptoProvider` seam is
///   context-free, so location-binding is NOT yet enforced on the store;
///   threading the per-record context through the seam is tracked with the
///   seam-wiring effort. The codec and its tests exercise the binding so
///   it is ready the moment the seam can supply context.
/// - **Nonce-misuse resistance** — v2 uses AES-GCM-SIV rather than plain
///   AES-GCM. With a random 96-bit nonce under a single long-lived DEK,
///   plain GCM hits the birthday bound (~2^-32 reuse at 2^32 seals), and
///   a GCM nonce *reuse* is catastrophic (leaks the auth key → forgery).
///   SIV degrades gracefully instead: a repeated (nonce, key, aad, msg)
///   only reveals that two plaintexts were equal — no key/forgery loss —
///   so v2 needs no per-DEK write ceiling. v2 is new (nothing seals at it
///   yet), so adopting the misuse-resistant AEAD now is free.
///
/// This codec is the foundation; nothing seals at v2 until the keyring
/// slice wires per-purpose DEKs behind it.
pub const ENVELOPE_VERSION_V2: u8 = 0x02;

/// Size constants for the envelope format
const NONCE_SIZE: usize = 12;
const VERSION_SIZE: usize = 1;
const KEY_ID_SIZE: usize = 4;
const TAG_SIZE: usize = 16;
/// Minimum v1 ciphertext size: version(1) + nonce(12) + tag(16)
const MIN_ENVELOPE_SIZE: usize = VERSION_SIZE + NONCE_SIZE + TAG_SIZE;
/// Minimum v2 ciphertext size: version(1) + key_id(4) + nonce(12) + tag(16)
const MIN_ENVELOPE_SIZE_V2: usize = VERSION_SIZE + KEY_ID_SIZE + NONCE_SIZE + TAG_SIZE;

/// Identifies the key that sealed a v2 envelope. Four opaque bytes — the
/// keyring (Gap G5) assigns and resolves them; this codec only reads and
/// writes the field. Stored big-endian on the wire so a numeric key_id
/// sorts the same on disk as in memory.
///
/// Deliberately carries no reserved/well-known values here: which key_id
/// maps to which wrapped DEK (including the reserved id for the legacy
/// `E2eKeys::from_ed25519_seed` key used during migration) is a keyring
/// concern, decided when the keyring file lands — not a property of the
/// wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct KeyId([u8; KEY_ID_SIZE]);

impl KeyId {
    /// Wrap four raw bytes as a key id.
    pub const fn from_bytes(bytes: [u8; KEY_ID_SIZE]) -> Self {
        Self(bytes)
    }

    /// The raw four bytes, as written on the wire.
    pub const fn as_bytes(&self) -> &[u8; KEY_ID_SIZE] {
        &self.0
    }

    /// Interpret a `u32` as a key id (big-endian), for keyrings that
    /// allocate ids as integers.
    pub const fn from_u32(id: u32) -> Self {
        Self(id.to_be_bytes())
    }

    /// The big-endian `u32` view of this key id.
    pub const fn to_u32(self) -> u32 {
        u32::from_be_bytes(self.0)
    }
}

/// The parsed, undecrypted header of an at-rest envelope. Lets a keyring
/// answer "which DEK do I need to open this?" by reading the leading
/// bytes alone — no key, no decryption.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnvelopeHeader {
    /// Format version byte (`0x01` or `0x02`).
    pub version: u8,
    /// The sealing key id — `Some` for v2, `None` for v1 (v1 predates
    /// key ids and is implicitly the single static key).
    pub key_id: Option<KeyId>,
}

/// Encrypt plaintext using AES-256-GCM with a random nonce.
///
/// Returns a self-describing binary envelope:
/// ```text
/// [version: 1B] [nonce: 12B] [ciphertext+tag: variable]
/// ```
///
/// The GCM authentication tag (16 bytes) is appended to the ciphertext
/// by the `aes-gcm` crate automatically.
pub fn encrypt_envelope(key: &[u8; 32], plaintext: &[u8]) -> CryptoResult<Vec<u8>> {
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| CryptoError::EncryptionFailed(format!("Invalid key: {e}")))?;

    // Generate a random 12-byte nonce
    let mut nonce_bytes = [0u8; NONCE_SIZE];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, plaintext)
        .map_err(|e| CryptoError::EncryptionFailed(format!("AES-GCM encrypt: {e}")))?;

    // Build envelope: version || nonce || ciphertext+tag
    let mut envelope = Vec::with_capacity(VERSION_SIZE + NONCE_SIZE + ciphertext.len());
    envelope.push(ENVELOPE_VERSION);
    envelope.extend_from_slice(&nonce_bytes);
    envelope.extend_from_slice(&ciphertext);

    Ok(envelope)
}

/// Decrypt an envelope produced by `encrypt_envelope`.
///
/// Parses the version byte, extracts the nonce, and decrypts the ciphertext.
pub fn decrypt_envelope(key: &[u8; 32], envelope: &[u8]) -> CryptoResult<Vec<u8>> {
    if envelope.len() < MIN_ENVELOPE_SIZE {
        return Err(CryptoError::InvalidFormat(format!(
            "Envelope too short: {} bytes (minimum {})",
            envelope.len(),
            MIN_ENVELOPE_SIZE
        )));
    }

    let version = envelope[0];
    if version != ENVELOPE_VERSION {
        return Err(CryptoError::UnsupportedVersion(version));
    }

    let nonce_bytes = &envelope[VERSION_SIZE..VERSION_SIZE + NONCE_SIZE];
    let ciphertext = &envelope[VERSION_SIZE + NONCE_SIZE..];

    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| CryptoError::DecryptionFailed(format!("Invalid key: {e}")))?;

    let nonce = Nonce::from_slice(nonce_bytes);

    cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| CryptoError::DecryptionFailed(format!("AES-GCM decrypt: {e}")))
}

/// Encrypt plaintext as a version-2 envelope: AES-256-GCM-SIV
/// (nonce-misuse-resistant) with a 4-byte `key_id` recorded in the clear
/// and `aad` bound as associated data.
///
/// Returns:
/// ```text
/// [version: 1B = 0x02] [key_id: 4B] [nonce: 12B] [ciphertext+tag: variable]
/// ```
///
/// The `key_id` rides in the clear (so a keyring can resolve the DEK via
/// [`peek_envelope`] before decrypting) but is *authenticated*: the
/// envelope header `[version][key_id]` is prepended to `aad` and the
/// whole thing is bound as the AEAD associated data. So flipping the
/// version (a v2→v1 downgrade) or the key_id is caught by the tag check
/// even if the same key is somehow selected.
///
/// `aad` (the storage context — namespace + Sled key) is NOT stored in
/// the envelope; the decryptor must supply byte-identical context or the
/// tag check fails, which is what binds the ciphertext to its location.
/// Use the empty slice when there is no context to bind.
///
/// v2 uses AES-256-GCM-SIV rather than plain AES-GCM precisely so the
/// random 96-bit nonce carries no per-DEK write ceiling: under SIV a
/// nonce collision is non-catastrophic (it only reveals plaintext
/// equality, never the auth key), where under plain GCM it would be a
/// forgery break (see [`ENVELOPE_VERSION_V2`]).
pub fn encrypt_envelope_v2(
    key: &[u8; 32],
    key_id: KeyId,
    plaintext: &[u8],
    aad: &[u8],
) -> CryptoResult<Vec<u8>> {
    let cipher = Aes256GcmSiv::new_from_slice(key)
        .map_err(|e| CryptoError::EncryptionFailed(format!("Invalid key: {e}")))?;

    let mut nonce_bytes = [0u8; NONCE_SIZE];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = SivNonce::from_slice(&nonce_bytes);

    let full_aad = v2_associated_data(key_id, aad);
    let ciphertext = cipher
        .encrypt(
            nonce,
            Payload {
                msg: plaintext,
                aad: &full_aad,
            },
        )
        .map_err(|e| CryptoError::EncryptionFailed(format!("AES-GCM-SIV encrypt: {e}")))?;

    // Build envelope: version || key_id || nonce || ciphertext+tag
    let mut envelope = Vec::with_capacity(MIN_ENVELOPE_SIZE_V2 - TAG_SIZE + ciphertext.len());
    envelope.push(ENVELOPE_VERSION_V2);
    envelope.extend_from_slice(key_id.as_bytes());
    envelope.extend_from_slice(&nonce_bytes);
    envelope.extend_from_slice(&ciphertext);

    Ok(envelope)
}

/// The associated data bound by a v2 envelope: the header
/// `[version:0x02][key_id:4]` followed by the caller's storage context.
/// Binding the header makes the version and key_id tamper-evident under
/// the tag (defends against a v2→v1 downgrade and key_id swap), on top of
/// the location binding the context already provides.
fn v2_associated_data(key_id: KeyId, context: &[u8]) -> Vec<u8> {
    let mut aad = Vec::with_capacity(VERSION_SIZE + KEY_ID_SIZE + context.len());
    aad.push(ENVELOPE_VERSION_V2);
    aad.extend_from_slice(key_id.as_bytes());
    aad.extend_from_slice(context);
    aad
}

/// Decrypt a version-2 envelope produced by [`encrypt_envelope_v2`].
///
/// `aad` must be byte-identical to what was supplied at seal time
/// (the storage context); otherwise the GCM tag check fails with
/// [`CryptoError::DecryptionFailed`]. A v1 envelope handed here is
/// rejected as [`CryptoError::UnsupportedVersion`] — route it through
/// [`decrypt_envelope`] (or the version-dispatching
/// [`decrypt_envelope_with_context`]) instead.
pub fn decrypt_envelope_v2(key: &[u8; 32], envelope: &[u8], aad: &[u8]) -> CryptoResult<Vec<u8>> {
    // Identify the version first so a wrong-version buffer (e.g. a short
    // v1 envelope) is reported as UnsupportedVersion rather than masked as
    // "too short for v2".
    let version = *envelope
        .first()
        .ok_or_else(|| CryptoError::InvalidFormat("empty envelope".to_string()))?;
    if version != ENVELOPE_VERSION_V2 {
        return Err(CryptoError::UnsupportedVersion(version));
    }
    if envelope.len() < MIN_ENVELOPE_SIZE_V2 {
        return Err(CryptoError::InvalidFormat(format!(
            "v2 envelope too short: {} bytes (minimum {})",
            envelope.len(),
            MIN_ENVELOPE_SIZE_V2
        )));
    }

    let mut id = [0u8; KEY_ID_SIZE];
    id.copy_from_slice(&envelope[VERSION_SIZE..VERSION_SIZE + KEY_ID_SIZE]);
    let key_id = KeyId::from_bytes(id);

    let nonce_start = VERSION_SIZE + KEY_ID_SIZE;
    let ct_start = nonce_start + NONCE_SIZE;
    let nonce_bytes = &envelope[nonce_start..ct_start];
    let ciphertext = &envelope[ct_start..];

    let cipher = Aes256GcmSiv::new_from_slice(key)
        .map_err(|e| CryptoError::DecryptionFailed(format!("Invalid key: {e}")))?;

    let nonce = SivNonce::from_slice(nonce_bytes);

    let full_aad = v2_associated_data(key_id, aad);
    cipher
        .decrypt(
            nonce,
            Payload {
                msg: ciphertext,
                aad: &full_aad,
            },
        )
        .map_err(|e| CryptoError::DecryptionFailed(format!("AES-GCM-SIV decrypt: {e}")))
}

/// Read an envelope's header without a key or any decryption: the
/// version byte, and for v2 the `key_id`. This is what lets the keyring
/// resolve "which DEK opens this?" by lookup rather than trial
/// decryption (threat model §5.2).
///
/// Returns [`CryptoError::InvalidFormat`] for an empty/truncated buffer
/// and [`CryptoError::UnsupportedVersion`] for an unknown version byte.
pub fn peek_envelope(envelope: &[u8]) -> CryptoResult<EnvelopeHeader> {
    let version = *envelope
        .first()
        .ok_or_else(|| CryptoError::InvalidFormat("empty envelope".to_string()))?;
    match version {
        ENVELOPE_VERSION => Ok(EnvelopeHeader {
            version,
            key_id: None,
        }),
        ENVELOPE_VERSION_V2 => {
            if envelope.len() < VERSION_SIZE + KEY_ID_SIZE {
                return Err(CryptoError::InvalidFormat(format!(
                    "v2 envelope too short to carry a key_id: {} bytes",
                    envelope.len()
                )));
            }
            let mut id = [0u8; KEY_ID_SIZE];
            id.copy_from_slice(&envelope[VERSION_SIZE..VERSION_SIZE + KEY_ID_SIZE]);
            Ok(EnvelopeHeader {
                version,
                key_id: Some(KeyId::from_bytes(id)),
            })
        }
        other => Err(CryptoError::UnsupportedVersion(other)),
    }
}

/// Version-dispatching decrypt for callers that hold a mixed stream of
/// v1 and v2 envelopes (the migration window at the KvStore seam, Gap
/// G1). v1 envelopes decrypt with empty AAD — they predate the binding,
/// so `aad` is ignored for them; v2 envelopes are bound to `aad`.
///
/// The caller still chooses the *key* (a real keyring resolves it from
/// [`peek_envelope`]'s `key_id`); this helper only routes on version so
/// the seam does not hand-parse the version byte at every call site.
pub fn decrypt_envelope_with_context(
    key: &[u8; 32],
    envelope: &[u8],
    aad: &[u8],
) -> CryptoResult<Vec<u8>> {
    let version = *envelope
        .first()
        .ok_or_else(|| CryptoError::InvalidFormat("empty envelope".to_string()))?;
    match version {
        ENVELOPE_VERSION => decrypt_envelope(key, envelope),
        ENVELOPE_VERSION_V2 => decrypt_envelope_v2(key, envelope, aad),
        other => Err(CryptoError::UnsupportedVersion(other)),
    }
}
