use zeroize::Zeroizing;

use super::NodeConfigStore;
use crate::crypto::{is_sealed_at_rest, open_at_rest, seal_at_rest_utf8};
use crate::storage::error::StorageError;

impl NodeConfigStore {
    /// Encrypt a plaintext string into the `ENC:<base64>` wire format via the
    /// shared at-rest value codec.
    pub(super) fn encrypt_sensitive(&self, plaintext: &str) -> Result<String, StorageError> {
        let key = Zeroizing::new(self.identity_key.ok_or_else(|| {
            StorageError::BackendError(
                "NodeConfigStore has no identity encryption key configured;                  refusing to write sensitive field in plaintext"
                    .into(),
            )
        })?);
        let sealed = seal_at_rest_utf8(&key, plaintext.as_bytes())
            .map_err(|e| StorageError::BackendError(format!("identity encryption failed: {e}")))?;
        // The explicit UTF-8 codec emits `ENC:` + base64, which is ASCII.
        String::from_utf8(sealed).map_err(|e| {
            StorageError::BackendError(format!("identity ciphertext is not valid UTF-8: {e}"))
        })
    }

    /// Decrypt a stored value via the shared at-rest value codec. Transparently
    /// handles pre-migration plaintext and all supported envelope formats.
    /// Returns an error only if a sealed value has no configured key, or
    /// decryption fails.
    pub(super) fn decrypt_sensitive(&self, stored: String) -> Result<String, StorageError> {
        if !is_sealed_at_rest(stored.as_bytes()) {
            // Legacy plaintext written by a pre-encryption build.
            return Ok(stored);
        }
        let key = Zeroizing::new(self.identity_key.ok_or_else(|| {
            StorageError::BackendError(
                "encrypted identity field found in node config but no decryption key                  configured on this NodeConfigStore handle"
                    .into(),
            )
        })?);
        let plaintext_bytes = open_at_rest(&key, stored.as_bytes())
            .map_err(|e| StorageError::BackendError(format!("identity decryption failed: {e}")))?;
        String::from_utf8(plaintext_bytes).map_err(|e| {
            StorageError::BackendError(format!("identity plaintext is not valid UTF-8: {e}"))
        })
    }
}
