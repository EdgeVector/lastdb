//! The keyring-backed [`CryptoProvider`] — the adapter that lets the
//! [`EncryptingNamespacedStore`](crate::storage) seam encrypt under the
//! wrapped-DEK [`Keyring`] instead of a single static key (Gap G1 + G5
//! convergence, at-rest threat model §5.5/§5.6,
//! `docs/security/at-rest-threat-model.md`).
//!
//! The seam holds `Arc<dyn CryptoProvider>` and calls only
//! `encrypt(plaintext) -> ciphertext` / `decrypt(ciphertext) -> plaintext`
//! (the [`EncryptingNamespacedStore`](crate::storage::EncryptingNamespacedStore) layer adds the `ENC:<base64>`
//! framing on top). This adapter maps those two calls onto
//! [`Keyring::seal`] / [`Keyring::open`] for a single [`KeyPurpose`]:
//!
//! - **encrypt** seals under the purpose's *active* DEK, stamping its
//!   [`KeyId`](super::KeyId) into the envelope-v2 header so reads resolve
//!   the right DEK by lookup rather than trial decryption (§5.2), and so a
//!   future KEK/DEK rotation is a re-wrap, not a re-encrypt (§5.6).
//! - **decrypt** opens via the keyring by `key_id` only. Pre-keyring v1
//!   envelopes and v2 envelopes stamped [`Keyring::LEGACY_KEY_ID`] are no
//!   longer accepted on this path; values must be re-sealed under a current
//!   keyring DEK before booting with the keyring provider.
//!
//! ## Scope of this slice
//!
//! This is the **adapter only**: turning a loaded [`Keyring`] into a
//! [`CryptoProvider`] the seam can hold. It does *not*
//! resolve the real KEK (the master key from the node's `secure_store`),
//! call [`load_or_init`](super::keyring_store::load_or_init), or swap the
//! factory's wiring away from [`LocalCryptoProvider`](super::LocalCryptoProvider)
//! — that boot-time wiring (which touches the node's keychain machinery and
//! the os-keychain feature combo) is the convergence's runtime half and
//! lands separately. Keeping the adapter pure-core means it is fully
//! exercised on CI without a keychain.
//!
//! ## AAD at this seam
//!
//! [`Keyring::seal`] / [`Keyring::open`] can bind a storage-context AAD
//! (namespace ++ key), but the [`CryptoProvider`] trait is context-free —
//! it sees only the value bytes, exactly as
//! [`LocalCryptoProvider`](super::LocalCryptoProvider) does today. So this
//! adapter binds an **empty** AAD: no regression versus the single-key
//! provider it replaces (whose v1 envelopes bind nothing either), and the
//! key_id-resolved rotation story is a strict gain. Threading the
//! per-record namespace/key context through the seam so it can be bound
//! under the GCM tag is a separate, larger change tracked with the seam
//! wiring, not this adapter.

use super::error::CryptoResult;
use super::keyring::{KeyPurpose, Keyring};
use super::provider::CryptoProvider;
use async_trait::async_trait;
use std::sync::Arc;

/// A [`CryptoProvider`] that seals/opens through a loaded [`Keyring`] for a
/// single [`KeyPurpose`].
pub struct KeyringCryptoProvider {
    /// The loaded keyring. Shared (immutable) for the provider's lifetime;
    /// a rotation re-loads the keyring and builds a fresh provider rather
    /// than mutating this one.
    keyring: Arc<Keyring>,
    /// Which purpose's active DEK new writes seal under.
    purpose: KeyPurpose,
}

impl KeyringCryptoProvider {
    /// Build a provider that seals under `purpose`'s active DEK in `keyring`
    /// and decrypts only key-id-stamped envelopes held by the keyring.
    ///
    /// `keyring` must already hold an active DEK for `purpose` (see
    /// [`load_or_init`](super::keyring_store::load_or_init)); construction
    /// does not check this, but the first `encrypt` will error rather than
    /// mint one (no-silent-mint).
    pub fn new(keyring: Arc<Keyring>, purpose: KeyPurpose) -> Self {
        Self { keyring, purpose }
    }

    /// The purpose this provider seals under.
    pub fn purpose(&self) -> KeyPurpose {
        self.purpose
    }
}

#[async_trait]
impl CryptoProvider for KeyringCryptoProvider {
    async fn encrypt(&self, plaintext: &[u8]) -> CryptoResult<Vec<u8>> {
        // Empty AAD: the CryptoProvider seam is context-free (see module docs).
        self.keyring.seal(self.purpose, plaintext, &[])
    }

    async fn decrypt(&self, ciphertext: &[u8]) -> CryptoResult<Vec<u8>> {
        self.keyring.open(ciphertext, &[])
    }
}
