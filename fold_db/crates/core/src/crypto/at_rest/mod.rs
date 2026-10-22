//! Shared synchronous at-rest value codec for direct-Sled-tree stores that
//! bypass the [`EncryptingNamespacedStore`](crate::storage) seam.
//!
//! Several stores open a Sled tree directly instead of routing through the
//! namespaced encryption seam — the node-config store, the app-consent
//! ledger, the revocation ledger, the job tracker, the trigger-runner's
//! per-view state, and the sync engine's download-cursor bookkeeping (see
//! `docs/security/at-rest-threat-model.md` §5.3). Each must encrypt its
//! values at rest, and before this module each hand-rolled the *same*
//! `ENC:<base64>` framing — exactly the per-store drift the threat model
//! warns about ("N stores × N implementations of the same wrapper; no
//! single audit point").
//!
//! This is that single audit point. It is deliberately **synchronous** —
//! keyed directly on the 32-byte E2E content key rather than the async
//! [`CryptoProvider`](super::CryptoProvider) trait — so the sync read paths
//! of those stores can adopt it without an async cascade, and it reuses the
//! same [`encrypt_envelope`]/[`decrypt_envelope`] AES-256-GCM primitive the
//! rest of the crate uses (no second crypto implementation).
//!
//! **Legacy wire format:** `ENC:` ++ base64(envelope) — identical to the
//! [`EncryptingNamespacedStore`](crate::storage::EncryptingNamespacedStore) seam, so a value sealed here reads
//! exactly like a value sealed there and the stored bytes stay valid UTF-8.
//! Values *without* the prefix are treated as legacy pre-migration plaintext
//! and returned verbatim (**dual-read**), enabling lazy rewrite-on-write
//! migration with no separate pass — a node upgraded across the seal keeps
//! reading its old rows, and the next write re-seals them.
//!
//! **Second legacy wire format — `ENZ:` ++
//! base64(envelope(deflate(plaintext))).**
//! Ciphertext is incompressible, so a payload that is sealed and never
//! compressed can never be shrunk again by any downstream plane: compaction,
//! the backup chunk store, and the cloud bill all carry the full size
//! forever. [`seal_at_rest_deflate`] therefore compresses *before* sealing —
//! the order Tom locked in `preference-lastdb-storage-compress-atoms-and-blobs`
//! (plaintext -> compress -> encrypt, never encrypt first).
//!
//! **Binary wire format — `ENB:` ++ flags ++ envelope.** The low bit of the
//! one-byte flags field records whether the plaintext was deflated before it
//! was sealed. This format removes the base64 storage tax while one marker
//! still covers both compressed and uncompressed ciphertext.
//!
//! All prefixes are 4 bytes and end in `:`, which is outside the base64
//! alphabet, so none can be confused with another, with a base64 body, or
//! with JSON (`{`/`[`). [`is_sealed_at_rest`] accepts all three — a caller that
//! gates on "is this sealed?" (Operation Trinity strict open, idempotent
//! reseal) must never see `ENZ:` as unsealed.
//!
//! **Reads never depend on the write-side switch.** [`open_at_rest`] always
//! understands `ENZ:`, whatever [`at_rest_compression_enabled`] says, so
//! disabling compression stops new compressed writes without stranding a
//! single row already written.
//!
//! **The compression half is shared with the KV seam.** The namespaced
//! `EncryptingKvStore` — which wraps
//! `atoms`, `tips`, `indexes`, `metadata`, `change_feed`, `cas_blobs`, the
//! order log and the locators, i.e. essentially the whole store — cannot reuse
//! [`seal_at_rest_deflate`] because it seals through the async
//! [`CryptoProvider`](super::CryptoProvider) rather than a raw key. It reuses
//! the parts that must not drift instead: the `ENZ:` prefix, the
//! compress-or-skip decision (`compress_for_seal`), the bounded inflate
//! (`inflate_at_rest`), and these counters. One codec, two seal sites.

mod compress;
mod config;
mod stats;

pub use compress::*;
pub use config::*;
pub use stats::*;

use super::envelope::decrypt_envelope;
use super::envelope::encrypt_envelope;
use super::error::{CryptoError, CryptoResult};
use base64::{engine::general_purpose::STANDARD as B64, Engine};

/// Prefix marker for at-rest-encrypted values. A sealed value is
/// `ENC:` ++ base64(ciphertext envelope) and is therefore valid UTF-8.
///
/// `:` is not in the base64 alphabet, so a base64-encoded legacy value (and
/// JSON, which starts with `{`/`[`) can never be mistaken for a sealed one.
pub const AT_REST_ENC_PREFIX: &str = "ENC:";

/// Prefix marker for binary at-rest values.
///
/// The stored form is `ENB:` + one flags byte + the raw ciphertext envelope.
/// Bit 0 means the plaintext was deflated before encryption. Other bits are
/// reserved and rejected so a future extension cannot be misread by an older
/// binary.
pub const AT_REST_ENC_BINARY_PREFIX: &str = "ENB:";

const AT_REST_BINARY_FLAG_DEFLATED: u8 = 0x01;
const AT_REST_BINARY_KNOWN_FLAGS: u8 = AT_REST_BINARY_FLAG_DEFLATED;

/// True when `value` carries an at-rest envelope marker (`ENC:`, `ENZ:`, or
/// `ENB:`). All current markers must match: an encrypted atom read rejects
/// anything unencrypted, including a compressed value with an unknown marker.
#[must_use]
pub fn is_sealed_at_rest(value: &[u8]) -> bool {
    value.starts_with(AT_REST_ENC_PREFIX.as_bytes())
        || value.starts_with(AT_REST_ENC_DEFLATE_PREFIX.as_bytes())
        || value.starts_with(AT_REST_ENC_BINARY_PREFIX.as_bytes())
}

/// Seal `plaintext` under the 32-byte content key.
///
/// `LASTDB_AT_REST_RAW=1` emits the binary `ENB:` form. The default remains
/// the legacy UTF-8 `ENC:` form so the read side can ship before any node
/// writes the new wire format.
pub fn seal_at_rest(key: &[u8; 32], plaintext: &[u8]) -> CryptoResult<Vec<u8>> {
    if at_rest_raw_enabled() {
        return seal_at_rest_raw(key, plaintext);
    }
    seal_at_rest_utf8(key, plaintext)
}

/// Seal `plaintext` as legacy UTF-8 `ENC:` + base64(envelope).
///
/// Use this only at a storage boundary that requires a JSON/string value.
/// Byte-oriented stores should use [`seal_at_rest`] so the binary write switch
/// can remove the base64 tax after the fleet can read `ENB:`.
pub fn seal_at_rest_utf8(key: &[u8; 32], plaintext: &[u8]) -> CryptoResult<Vec<u8>> {
    let ciphertext = encrypt_envelope(key, plaintext)?;
    let encoded = B64.encode(&ciphertext);
    let mut out = Vec::with_capacity(AT_REST_ENC_PREFIX.len() + encoded.len());
    out.extend_from_slice(AT_REST_ENC_PREFIX.as_bytes());
    out.extend_from_slice(encoded.as_bytes());
    Ok(out)
}

/// Seal `plaintext` as `ENB:` + flags + raw ciphertext envelope.
pub fn seal_at_rest_raw(key: &[u8; 32], plaintext: &[u8]) -> CryptoResult<Vec<u8>> {
    let ciphertext = encrypt_envelope(key, plaintext)?;
    Ok(encode_binary_ciphertext(&ciphertext, false))
}

pub(crate) fn encode_binary_ciphertext(ciphertext: &[u8], deflated: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(AT_REST_ENC_BINARY_PREFIX.len() + 1 + ciphertext.len());
    out.extend_from_slice(AT_REST_ENC_BINARY_PREFIX.as_bytes());
    out.push(u8::from(deflated) * AT_REST_BINARY_FLAG_DEFLATED);
    out.extend_from_slice(ciphertext);
    out
}

pub(crate) fn decode_binary_ciphertext(stored: &[u8]) -> CryptoResult<Option<(bool, &[u8])>> {
    if !stored.starts_with(AT_REST_ENC_BINARY_PREFIX.as_bytes()) {
        return Ok(None);
    }
    let Some((&flags, ciphertext)) = stored[AT_REST_ENC_BINARY_PREFIX.len()..].split_first() else {
        return Err(CryptoError::InvalidFormat(
            "at-rest ENB envelope is missing its flags byte".to_string(),
        ));
    };
    if flags & !AT_REST_BINARY_KNOWN_FLAGS != 0 {
        return Err(CryptoError::InvalidFormat(format!(
            "at-rest ENB envelope has unsupported flags 0x{flags:02x}"
        )));
    }
    Ok(Some((
        flags & AT_REST_BINARY_FLAG_DEFLATED != 0,
        ciphertext,
    )))
}

/// Prefix marker for at-rest values that were **compressed before sealing**.
/// A sealed value is `ENZ:` ++ base64(envelope(deflate(plaintext))).
///
/// Same shape and same guarantees as [`AT_REST_ENC_PREFIX`]: 4 bytes ending in
/// `:`, which is not in the base64 alphabet, so it can never collide with a
/// base64 body or with JSON.
pub const AT_REST_ENC_DEFLATE_PREFIX: &str = "ENZ:";

/// Open a stored value. Values *without* a sealed prefix are returned
/// verbatim (legacy pre-migration plaintext, dual-read). `ENC:`- and
/// `ENZ:`-prefixed values are base64-decoded and decrypted under `key`, and
/// `ENB:` values read the raw envelope after the flags byte. Compressed forms
/// are additionally inflated.
///
/// `ENZ:` is decoded unconditionally — it is **not** gated on
/// [`at_rest_compression_enabled`]. The kill switch stops new compressed
/// writes; it must never strand rows already on disk.
pub fn open_at_rest(key: &[u8; 32], stored: &[u8]) -> CryptoResult<Vec<u8>> {
    open_at_rest_with_ceiling(key, stored, at_rest_max_inflated_bytes)
}

/// Open existing formats with a caller-owned bound, independent of environment.
/// Unlike the historical helper, this entry point requires an encrypted value.
pub fn open_at_rest_bounded(key: &[u8; 32], stored: &[u8], max: u64) -> CryptoResult<Vec<u8>> {
    if max == 0
        || max.checked_add(1).is_none()
        || usize::try_from(max).is_err()
        || stored.len() as u64 > max
        || !is_sealed_at_rest(stored)
    {
        return Err(CryptoError::InvalidFormat(
            "bounded encrypted value required".into(),
        ));
    }
    let plain = open_at_rest_with_ceiling(key, stored, || max)?;
    if plain.len() as u64 > max {
        return Err(CryptoError::InvalidFormat(
            "decoded value exceeds explicit bound".into(),
        ));
    }
    Ok(plain)
}

// Verbatim reads do not inflate, so they do not need an environment lookup
// for the inflate ceiling. Compressed reads still fetch the current ceiling
// for each open and enforce it after authenticated decryption.
fn open_at_rest_with_ceiling<F>(
    key: &[u8; 32],
    stored: &[u8],
    max_bytes: F,
) -> CryptoResult<Vec<u8>>
where
    F: FnOnce() -> u64,
{
    if let Some((deflated, ciphertext)) = decode_binary_ciphertext(stored)? {
        let plaintext = decrypt_envelope(key, ciphertext)?;
        return if deflated {
            inflate_bounded(&plaintext, max_bytes())
        } else {
            Ok(plaintext)
        };
    }
    let deflated = stored.starts_with(AT_REST_ENC_DEFLATE_PREFIX.as_bytes());
    if !deflated && !stored.starts_with(AT_REST_ENC_PREFIX.as_bytes()) {
        // Legacy plaintext written before this tree was sealed. Dual-read
        // keeps it readable; the next write through `seal_at_rest` re-seals it.
        return Ok(stored.to_vec());
    }
    // Both markers are the same width, so one slice serves both.
    let b64_part = &stored[AT_REST_ENC_PREFIX.len()..];
    let ciphertext = B64
        .decode(b64_part)
        .map_err(|e| CryptoError::InvalidFormat(format!("at-rest base64 decode failed: {e}")))?;
    let plaintext = decrypt_envelope(key, &ciphertext)?;
    if deflated {
        return inflate_bounded(&plaintext, max_bytes());
    }
    Ok(plaintext)
}
