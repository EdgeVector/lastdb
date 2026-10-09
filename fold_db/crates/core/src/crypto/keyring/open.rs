use super::super::envelope::{decrypt_envelope_v2, encrypt_envelope_v2, peek_envelope, KeyId};
use super::super::error::{CryptoError, CryptoResult};
use super::{Dek, KeyPurpose, Keyring};

impl Keyring {
    /// The active DEK for `purpose` (the one new seals should use), with
    /// its id, or `None` if no DEK has been minted for that purpose.
    pub fn active_dek(&self, purpose: KeyPurpose) -> Option<(KeyId, &Dek)> {
        self.entries
            .iter()
            .find(|e| e.purpose == purpose && e.active)
            .map(|e| (e.key_id, &e.dek))
    }

    /// The DEK identified by `key_id` (the decrypt path), or `None` if
    /// absent. Absence is reported, never silently minted.
    pub fn dek(&self, key_id: KeyId) -> Option<&Dek> {
        self.entries
            .iter()
            .find(|e| e.key_id == key_id)
            .map(|e| &e.dek)
    }

    /// The purpose a `key_id` was minted for, or `None` if absent.
    pub fn purpose_of(&self, key_id: KeyId) -> Option<KeyPurpose> {
        self.entries
            .iter()
            .find(|e| e.key_id == key_id)
            .map(|e| e.purpose)
    }

    /// Seal `plaintext` for `purpose` under that purpose's active DEK.
    pub fn seal(&self, purpose: KeyPurpose, plaintext: &[u8], aad: &[u8]) -> CryptoResult<Vec<u8>> {
        let (key_id, dek) = self.active_dek(purpose).ok_or_else(|| {
            CryptoError::KeyError(format!("no active DEK for purpose {purpose:?}"))
        })?;
        encrypt_envelope_v2(dek.as_bytes(), key_id, plaintext, aad)
    }

    /// Open an at-rest `envelope`, resolving the DEK from the `key_id`
    /// peeked from its v2 header. Legacy v1 envelopes and v2 envelopes stamped
    /// with [`Keyring::LEGACY_KEY_ID`] are rejected: the keyring read path is
    /// current-key-id-only.
    pub fn open(&self, envelope: &[u8], aad: &[u8]) -> CryptoResult<Vec<u8>> {
        let header = peek_envelope(envelope)?;
        match header.key_id {
            None => Err(CryptoError::UnsupportedVersion(header.version)),
            Some(id) if id == Self::LEGACY_KEY_ID => Err(CryptoError::KeyError(
                "legacy key_id 0 fallback is retired".to_string(),
            )),
            Some(id) => {
                let dek = self.dek(id).ok_or_else(|| {
                    CryptoError::KeyError(format!(
                        "envelope names key_id {} not held by the keyring",
                        id.to_u32()
                    ))
                })?;
                decrypt_envelope_v2(dek.as_bytes(), envelope, aad)
            }
        }
    }
}
