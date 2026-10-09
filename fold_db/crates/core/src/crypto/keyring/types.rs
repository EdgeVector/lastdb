use super::super::envelope::KeyId;
use super::super::error::{CryptoError, CryptoResult};
use rand::rngs::OsRng;
use rand::RngCore;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// AAD domain separator bound into every wrapped DEK. Combined with the
/// one-byte purpose tag, it ties a wrapped DEK to its declared purpose.
pub(super) const KEYRING_DEK_AAD_DOMAIN: &[u8] = b"folddb/keyring/dek";

/// A 32-byte data-encryption key, scrubbed from memory on drop so the
/// unwrapped key does not linger in freed pages (threat model Gap G3).
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct Dek([u8; 32]);

impl Dek {
    /// Wrap raw key bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Generate a fresh random DEK from the OS CSPRNG.
    pub fn generate() -> Self {
        let mut bytes = [0u8; 32];
        OsRng.fill_bytes(&mut bytes);
        Self(bytes)
    }

    /// The raw 32 bytes, for use as an AES-256-GCM key.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Debug for Dek {
    /// Never print key material.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Dek(<redacted>)")
    }
}

/// What a DEK protects. Each purpose gets its own DEK so a compromise or
/// rotation is scoped to one surface.
///
/// The wire tag is stable and persisted in `keyring.enc`; never renumber
/// an existing variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyPurpose {
    /// The main key/value store (atom content at the `KvStore` seam).
    Store,
    /// The embedding / blind-index store.
    Index,
    /// Out-of-line blob storage.
    Blob,
    /// Identity / secure-store material.
    Identity,
}

impl KeyPurpose {
    /// Every purpose, in wire-tag order.
    pub const ALL: [Self; 4] = [Self::Store, Self::Index, Self::Blob, Self::Identity];

    /// Stable one-byte wire tag.
    pub(super) const fn wire_tag(self) -> u8 {
        match self {
            Self::Store => 0x01,
            Self::Index => 0x02,
            Self::Blob => 0x03,
            Self::Identity => 0x04,
        }
    }

    /// Parse a wire tag back to a purpose.
    pub(super) fn from_wire_tag(tag: u8) -> CryptoResult<Self> {
        match tag {
            0x01 => Ok(Self::Store),
            0x02 => Ok(Self::Index),
            0x03 => Ok(Self::Blob),
            0x04 => Ok(Self::Identity),
            other => Err(CryptoError::InvalidFormat(format!(
                "unknown keyring purpose tag: {other:#04x}"
            ))),
        }
    }

    /// The AAD bound when wrapping a DEK of this purpose:
    /// `b"folddb/keyring/dek" || purpose_tag`.
    pub(super) fn wrap_aad(self) -> Vec<u8> {
        let mut aad = Vec::with_capacity(KEYRING_DEK_AAD_DOMAIN.len() + 1);
        aad.extend_from_slice(KEYRING_DEK_AAD_DOMAIN);
        aad.push(self.wire_tag());
        aad
    }
}

/// One keyring entry: a DEK, its id, its purpose, and whether it is the
/// active key for that purpose.
#[derive(Clone)]
pub(super) struct Entry {
    pub(super) key_id: KeyId,
    pub(super) purpose: KeyPurpose,
    pub(super) active: bool,
    pub(super) dek: Dek,
}

/// An in-memory set of wrapped DEKs, one or more per [`KeyPurpose`].
///
/// Construct empty with [`Keyring::new`], add keys with
/// [`Keyring::mint_dek`], persist with [`Keyring::serialize`], and reload
/// with [`Keyring::deserialize`]. Resolution is by `key_id` (decrypt
/// path) or by active-per-purpose (encrypt path).
#[derive(Default, Clone)]
pub struct Keyring {
    pub(super) entries: Vec<Entry>,
}

impl Keyring {
    /// The reserved key id standing for the pre-keyring single static
    /// key.
    pub const LEGACY_KEY_ID: KeyId = KeyId::from_u32(0);

    /// A fresh, empty keyring. Holds no keys until [`Keyring::mint_dek`]
    /// is called.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of DEKs held (across all purposes and rotation generations).
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the keyring holds no DEKs.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
