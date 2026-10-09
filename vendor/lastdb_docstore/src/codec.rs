//! At-rest body encryption (+ transparent compression) for the document store.
//!
//! Reuses the AES-256-GCM envelope v1 (`[0x01][nonce12][ct||tag]`) already
//! proven by the cloud module, but with a **local at-rest key** — never the
//! cloud content key derivation. P1 decision recorded here: CAS blob refs
//! hash the **plaintext** logical bytes, so re-encrypting (rekey) never
//! changes a blob's identity.
//!
//! P2a compression policy: bodies at/over [`COMPRESS_THRESHOLD`] bytes are
//! zstd-compressed (level [`COMPRESS_LEVEL`]) **before** encryption — never
//! after, since ciphertext does not compress. The pre-encrypt plaintext is
//! framed with the `ZST1` magic; frameless plaintext reads back as raw, so
//! legacy uncompressed bodies in the same store keep working. A raw body
//! that happens to start with the magic is force-compressed to keep decode
//! unambiguous. Tiny bodies (thin tips, bools) skip compression entirely.

use super::BodyCodec;
use crate::envelope::{decrypt_envelope, encrypt_envelope};
use crate::{Error, Result};
use zeroize::Zeroize;

/// Pre-encrypt frame magic marking a zstd-compressed body.
pub const COMPRESS_MAGIC: &[u8; 4] = b"ZST1";
/// Bodies smaller than this stay uncompressed (thin tips, small scalars).
pub const COMPRESS_THRESHOLD: usize = 512;
/// zstd write level — fast, still ~3-5x on prose-like JSON.
pub const COMPRESS_LEVEL: i32 = 3;

/// AES-256-GCM at-rest codec (envelope v1) with thresholded zstd.
pub struct AesGcmCodec {
    key: [u8; 32],
}

impl AesGcmCodec {
    pub fn new(key: [u8; 32]) -> Self {
        Self { key }
    }

    /// Test/dev helper: derive a key from a passphrase-like string via
    /// SHA-256. Real deployments should pass a proper 32-byte key.
    pub fn from_secret(secret: &str) -> Self {
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(secret.as_bytes());
        let mut key = [0u8; 32];
        key.copy_from_slice(&digest);
        Self { key }
    }
}

impl Drop for AesGcmCodec {
    fn drop(&mut self) {
        self.key.zeroize();
    }
}

impl BodyCodec for AesGcmCodec {
    fn name(&self) -> &'static str {
        "aes-256-gcm-v1"
    }

    fn encode(&self, plain: &[u8]) -> Result<Vec<u8>> {
        // Compress BEFORE encrypt (ciphertext is incompressible). Also
        // force-compress bodies that collide with the frame magic so raw
        // vs compressed stays unambiguous on read.
        let framed = if plain.len() >= COMPRESS_THRESHOLD || plain.starts_with(COMPRESS_MAGIC) {
            let mut out = COMPRESS_MAGIC.to_vec();
            out.extend(
                zstd::bulk::compress(plain, COMPRESS_LEVEL)
                    .map_err(|e| Error::Corrupt(format!("zstd compress: {e}")))?,
            );
            out
        } else {
            plain.to_vec()
        };
        encrypt_envelope(&self.key, &framed).map_err(Error::Corrupt)
    }

    fn decode(&self, stored: &[u8]) -> Result<Vec<u8>> {
        let framed = decrypt_envelope(&self.key, stored).map_err(Error::Corrupt)?;
        match framed.strip_prefix(COMPRESS_MAGIC) {
            None => Ok(framed),
            Some(compressed) => zstd::stream::decode_all(compressed)
                .map_err(|e| Error::Corrupt(format!("zstd decompress: {e}"))),
        }
    }
}
