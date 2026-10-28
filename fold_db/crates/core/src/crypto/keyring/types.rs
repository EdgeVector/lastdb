use super::super::envelope::KeyId;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// A 32-byte data-encryption key, scrubbed from memory on drop so the
/// unwrapped key does not linger in freed pages (threat model Gap G3).
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct Dek([u8; 32]);

impl Dek {
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

/// One keyring entry: a DEK, its id, its purpose, and whether it is the
/// active key for that purpose.
#[derive(Clone)]
pub(super) struct Entry {
    pub(super) key_id: KeyId,
    pub(super) purpose: KeyPurpose,
    pub(super) active: bool,
    pub(super) dek: Dek,
}

/// An in-memory set of DEKs, one or more per [`KeyPurpose`].
///
/// Construct empty with [`Keyring::new`]. Resolution is by `key_id`
/// (decrypt path) or by active-per-purpose (encrypt path).
#[derive(Default, Clone)]
pub struct Keyring {
    pub(super) entries: Vec<Entry>,
}

impl Keyring {
    /// The reserved key id standing for the pre-keyring single static
    /// key.
    pub const LEGACY_KEY_ID: KeyId = KeyId::from_u32(0);

    /// A fresh, empty keyring.
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
