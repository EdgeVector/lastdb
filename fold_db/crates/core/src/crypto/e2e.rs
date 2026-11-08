use super::error::{CryptoError, CryptoResult};
use ed25519_compact::x25519;
use hkdf::Hkdf;
use sha2::Sha256;
use zeroize::{Zeroize, Zeroizing};

const HKDF_SALT: &[u8] = b"fold:e2e:v1";
const CONTENT_KEY_INFO: &[u8] = b"fold:content-key";

/// Holds the E2E key derived from a single passkey/identity secret:
/// `encryption_key`, the AES-256-GCM account content key. HashKey tokens use
/// each field key bundle's own blind key, not a key derived here.
///
/// The key is zeroized when the struct (or any clone of it) is dropped, so
/// the derived key bytes do not linger in freed memory (at-rest threat model
/// Gap G3). The by-value accessors
/// below still hand out short-lived stack copies — those are consumed
/// immediately into another zeroizing holder and are out of scope here.
#[derive(Clone, Zeroize, zeroize::ZeroizeOnDrop)]
pub struct E2eKeys {
    encryption_key: [u8; 32],
}

impl E2eKeys {
    /// Derive the E2E key from a 32-byte secret using HKDF-SHA256.
    pub fn from_secret(secret: &[u8; 32]) -> Self {
        let hk = Hkdf::<Sha256>::new(Some(HKDF_SALT), secret);

        let mut encryption_key = [0u8; 32];
        hk.expand(CONTENT_KEY_INFO, &mut encryption_key)
            .expect("32 bytes is a valid HKDF-SHA256 output length");

        Self { encryption_key }
    }

    /// Derive the E2E key from an Ed25519 private key seed.
    ///
    /// Converts the Ed25519 seed to an X25519 secret key, then derives
    /// the encryption key via HKDF. This allows a single Ed25519
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

    /// AES-256-GCM content key. The disk home and the cloud backup use it.
    pub fn encryption_key(&self) -> [u8; 32] {
        self.encryption_key
    }
}
