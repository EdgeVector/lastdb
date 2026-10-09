use super::error::{CryptoError, CryptoResult};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use ed25519_compact::x25519;
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::path::Path;
use tokio::fs;
use zeroize::{Zeroize, Zeroizing};

const HKDF_SALT: &[u8] = b"fold:e2e:v1";
const CONTENT_KEY_INFO: &[u8] = b"fold:content-key";
const INDEX_KEY_INFO: &[u8] = b"fold:index-key";
/// HKDF info for RangeKey OPE (design-lastdb-rangekey-ope-v1). Distinct from
/// content and index keys so knowing one never yields the others without the seed.
const RANGE_OPE_KEY_INFO: &[u8] = b"fold:range-ope-v1";

/// Holds E2E keys derived from a single passkey/identity secret:
/// - `encryption_key`: AES-256-GCM key for atom content
/// - `index_key`: HMAC-SHA256 key for blind index / HashKey tokens
/// - `ope_key`: key for RangeKey order-preserving encoding
///
/// All fields are zeroized when the struct (or any clone of it) is dropped, so
/// the derived key bytes do not linger in freed memory (at-rest threat model
/// Gap G3, `docs/security/at-rest-threat-model.md`). The by-value accessors
/// below still hand out short-lived stack copies — those are consumed
/// immediately into another zeroizing holder and are out of scope here.
#[derive(Clone, Zeroize, zeroize::ZeroizeOnDrop)]
pub struct E2eKeys {
    encryption_key: [u8; 32],
    index_key: [u8; 32],
    ope_key: [u8; 32],
}

impl E2eKeys {
    /// Derive E2E keys from a 32-byte secret using HKDF-SHA256.
    pub fn from_secret(secret: &[u8; 32]) -> Self {
        let hk = Hkdf::<Sha256>::new(Some(HKDF_SALT), secret);

        let mut encryption_key = [0u8; 32];
        hk.expand(CONTENT_KEY_INFO, &mut encryption_key)
            .expect("32 bytes is a valid HKDF-SHA256 output length");

        let mut index_key = [0u8; 32];
        hk.expand(INDEX_KEY_INFO, &mut index_key)
            .expect("32 bytes is a valid HKDF-SHA256 output length");

        let mut ope_key = [0u8; 32];
        hk.expand(RANGE_OPE_KEY_INFO, &mut ope_key)
            .expect("32 bytes is a valid HKDF-SHA256 output length");

        Self {
            encryption_key,
            index_key,
            ope_key,
        }
    }

    /// Derive E2E keys from an Ed25519 private key seed.
    ///
    /// Converts the Ed25519 seed to an X25519 secret key, then derives
    /// encryption + index keys via HKDF. This allows a single Ed25519
    /// key pair to serve as both identity and encryption root.
    pub fn from_ed25519_seed(seed: &[u8; 32]) -> CryptoResult<Self> {
        let ed_seed = ed25519_compact::Seed::new(*seed);
        let ed_kp = ed25519_compact::KeyPair::from_seed(ed_seed);
        let x25519_sk = x25519::SecretKey::from_ed25519(&ed_kp.sk).map_err(|e| {
            CryptoError::KeyError(format!("Ed25519→X25519 conversion failed: {e:?}"))
        })?;
        let x_bytes: &[u8] = x25519_sk.as_ref();
        // Scrub the X25519 secret copy on drop; `from_secret` only borrows it.
        let mut secret = Zeroizing::new([0u8; 32]);
        secret.copy_from_slice(x_bytes);
        Ok(Self::from_secret(&secret))
    }

    /// Load a 32-byte secret from `key_path`, or generate a random one if the
    /// file does not exist. Then derive both E2E keys via [`from_secret`].
    pub async fn load_or_generate(key_path: &Path) -> CryptoResult<Self> {
        let secret: Zeroizing<[u8; 32]> = if key_path.exists() {
            let mut bytes = fs::read(key_path)
                .await
                .map_err(|e| CryptoError::KeyError(format!("Failed to read E2E key file: {e}")))?;

            if bytes.len() != 32 {
                bytes.zeroize();
                return Err(CryptoError::KeyError(format!(
                    "E2E key file has invalid length: {} (expected 32)",
                    bytes.len()
                )));
            }

            let mut secret = Zeroizing::new([0u8; 32]);
            secret.copy_from_slice(&bytes);
            bytes.zeroize();
            secret
        } else {
            let mut secret = Zeroizing::new([0u8; 32]);
            use rand::RngCore;
            rand::rngs::OsRng.fill_bytes(secret.as_mut_slice());

            if let Some(parent) = key_path.parent() {
                fs::create_dir_all(parent).await.map_err(|e| {
                    CryptoError::KeyError(format!("Failed to create E2E key directory: {e}"))
                })?;
            }

            fs::write(key_path, secret.as_slice())
                .await
                .map_err(|e| CryptoError::KeyError(format!("Failed to write E2E key file: {e}")))?;

            // Restrict key file permissions to owner-only (Unix)
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let perms = std::fs::Permissions::from_mode(0o600);
                std::fs::set_permissions(key_path, perms).map_err(|e| {
                    CryptoError::KeyError(format!("Failed to set E2E key file permissions: {e}"))
                })?;
            }

            tracing::info!("Generated new E2E key at {}", key_path.display());
            tracing::warn!("Back up your E2E key! Without it, encrypted data cannot be recovered.");

            secret
        };

        Ok(Self::from_secret(&secret))
    }

    /// AES-256-GCM key for atom content encryption.
    pub fn encryption_key(&self) -> [u8; 32] {
        self.encryption_key
    }

    /// HMAC-SHA256 key for blind index tokens.
    pub fn index_key(&self) -> [u8; 32] {
        self.index_key
    }

    /// Key material for RangeKey order-preserving encoding (OPE v1).
    pub fn ope_key(&self) -> [u8; 32] {
        self.ope_key
    }

    /// Compute a blind index token: HMAC-SHA256(index_key, term), truncated to
    /// 16 bytes and base64url-encoded (no padding).
    ///
    /// Used for **search keyword** blinding. For storage HashKey partitions use
    /// [`Self::blind_hash_key`] (domain-separated; bound to molecule uuid).
    pub fn blind_token(index_key: &[u8; 32], term: &str) -> String {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(index_key).expect("HMAC accepts any key length");
        mac.update(term.as_bytes());
        let result = mac.finalize().into_bytes();
        URL_SAFE_NO_PAD.encode(&result[..16])
    }

    /// Deterministic storage-facing HashKey token (design-lastdb-hashkey-blind-v1).
    ///
    /// MAC input (exact bytes):
    /// ```text
    /// hk|v1\0{molecule_uuid}\0{plaintext_hash}
    /// ```
    ///
    /// Domain tag `hk|v1` never collides with search [`Self::blind_token`] terms.
    /// Output encoding matches `blind_token` (HMAC-SHA256 → first 16 bytes →
    /// base64url no pad) so `escape_segment` remains safe.
    pub fn blind_hash_key(
        index_key: &[u8; 32],
        molecule_uuid: &str,
        plaintext_hash: &str,
    ) -> String {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(index_key).expect("HMAC accepts any key length");
        mac.update(b"hk|v1");
        mac.update(&[0u8]);
        mac.update(molecule_uuid.as_bytes());
        mac.update(&[0u8]);
        mac.update(plaintext_hash.as_bytes());
        let result = mac.finalize().into_bytes();
        URL_SAFE_NO_PAD.encode(&result[..16])
    }

    /// Order-preserving encoding of a RangeKey for storage (design-lastdb-rangekey-ope-v1).
    ///
    /// **Properties:**
    /// - Deterministic for fixed `(ope_key, molecule_uuid, plaintext)`
    /// - UTF-8/byte order of plaintexts is preserved in the encoded strings
    /// - Output is hex only (no `\0` / control bytes) — safe in `mk:` raw range
    ///   position and in `mhr` escaped range segments
    /// - Opaque: plaintext substrings do not appear greppable in the encoding
    ///
    /// **Order leakage is by design** (accepted). Dense domains may be
    /// reconstructible from rank alone.
    ///
    /// **Byte-prefix-preserving:** `ope(prefix)` is a string prefix of
    /// `ope(prefix||suffix)`, so RangePrefix walks remain correct.
    ///
    /// Algorithm (per-byte fixed-width mono map):
    /// for each byte index `i` and byte `b`, emit 8 hex chars of
    /// `(HMAC(ope_key, "rk|ope|v1" || M || i)[..4] as u32 & !0xFF) | b`.
    /// Same position ⇒ same high bits; order of low byte decides comparison.
    pub fn ope_encode_range(
        ope_key: &[u8; 32],
        molecule_uuid: &str,
        plaintext_range: &str,
    ) -> String {
        if plaintext_range.is_empty() {
            return String::new();
        }
        let bytes = plaintext_range.as_bytes();
        let mut out = String::with_capacity(bytes.len() * 8);
        for (i, &b) in bytes.iter().enumerate() {
            let mut mac =
                Hmac::<Sha256>::new_from_slice(ope_key).expect("HMAC accepts any key length");
            mac.update(b"rk|ope|v1");
            mac.update(&[0u8]);
            mac.update(molecule_uuid.as_bytes());
            mac.update(&[0u8]);
            mac.update(&(i as u32).to_be_bytes());
            let digest = mac.finalize().into_bytes();
            let mut word = u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]);
            // Keep high 24 bits of PRF; low 8 bits carry the plaintext byte so
            // order within a position matches byte order.
            word = (word & 0xFFFF_FF00) | u32::from(b);
            out.push_str(&format!("{word:08x}"));
        }
        out
    }

    /// Recover API plaintext from a v1 OPE range encoding.
    ///
    /// Each 8 hex chars pack `(prf_high24 << 8) | plaintext_byte`. The low
    /// byte is intentional order leakage (and sufficient to reverse the map
    /// without the OPE key). Used to surface API-space RangeKeys in query
    /// responses after storage-form scans/filters.
    ///
    /// Returns `None` if `encoded` is empty, odd-length, non-hex, or not a
    /// multiple of 8 hex digits (not a v1 OPE token).
    #[must_use]
    pub fn ope_decode_range_plaintext(encoded: &str) -> Option<String> {
        if encoded.is_empty() {
            return Some(String::new());
        }
        if !encoded.len().is_multiple_of(8) {
            return None;
        }
        let mut bytes = Vec::with_capacity(encoded.len() / 8);
        for chunk in encoded.as_bytes().chunks(8) {
            let hex = std::str::from_utf8(chunk).ok()?;
            let word = u32::from_str_radix(hex, 16).ok()?;
            bytes.push((word & 0xFF) as u8);
        }
        String::from_utf8(bytes).ok()
    }
}

/// HKDF domain-separation salt for the at-rest keyring KEK (distinct from the
/// `fold:e2e:v1` salt above, so the KEK never collides with any E2E key).
const AT_REST_KEK_SALT: &[u8] = b"fold:at-rest-kek:v1";
const AT_REST_KEK_INFO: &[u8] = b"fold:keyring-kek";

/// Derive the at-rest keyring KEK from the node's 32-byte Ed25519 identity
/// seed (`<home>/identity.key`) — the single-key-root path
/// (`decision-local-security-loose-stance`, 2026-07-10).
///
/// The seed is the ONE local key root: node identity, E2E encryption root
/// ([`E2eKeys::from_ed25519_seed`]), and — via this HKDF-SHA256 derivation
/// under a dedicated salt/info pair — the KEK that wraps the per-purpose DEKs
/// in `keyring.enc`. Deterministic: the same seed always derives the same KEK,
/// so a node boots with zero prompts (key exists → up; no key → generate → up)
/// while the data stays encrypted at rest under the DEK hierarchy.
///
/// Domain separation: the salt/info pair here is disjoint from every E2E
/// derivation, so knowing the KEK never reveals an E2E key and vice versa
/// (both still reduce to the seed, which is the explicit trust root).
pub fn at_rest_kek_from_seed(seed: &[u8; 32]) -> [u8; 32] {
    let hk = Hkdf::<Sha256>::new(Some(AT_REST_KEK_SALT), seed);
    let mut kek = [0u8; 32];
    hk.expand(AT_REST_KEK_INFO, &mut kek)
        .expect("32 bytes is a valid HKDF-SHA256 output length");
    kek
}
