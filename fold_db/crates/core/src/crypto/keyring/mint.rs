use super::super::envelope::KeyId;
use super::super::error::{CryptoError, CryptoResult};
use super::{Dek, Entry, KeyPurpose, Keyring};

impl Keyring {
    /// Mint a fresh random DEK for `purpose`, make it the active key for
    /// that purpose, and return its newly allocated [`KeyId`].
    ///
    /// Ids are allocated as `max(existing) + 1`, starting at `1`
    /// ([`Keyring::LEGACY_KEY_ID`] = 0 is reserved).
    pub fn mint_dek(&mut self, purpose: KeyPurpose) -> KeyId {
        self.try_mint_dek(purpose)
            .expect("keyring key-id space exhausted (u32::MAX ids allocated)")
    }

    /// Fallible [`Keyring::mint_dek`]: mint a fresh DEK for `purpose`, or
    /// error when the id space is exhausted rather than wrapping back to
    /// [`Keyring::LEGACY_KEY_ID`].
    pub fn try_mint_dek(&mut self, purpose: KeyPurpose) -> CryptoResult<KeyId> {
        let key_id = self.next_key_id()?;
        for e in &mut self.entries {
            if e.purpose == purpose {
                e.active = false;
            }
        }
        self.entries.push(Entry {
            key_id,
            purpose,
            active: true,
            dek: Dek::generate(),
        });
        Ok(key_id)
    }

    /// Smallest unused id `>= 1` (0 is [`Keyring::LEGACY_KEY_ID`]).
    fn next_key_id(&self) -> CryptoResult<KeyId> {
        let max = self
            .entries
            .iter()
            .map(|e| e.key_id.to_u32())
            .max()
            .unwrap_or(0);
        let next = max.checked_add(1).ok_or_else(|| {
            CryptoError::KeyError("keyring key-id space exhausted (u32::MAX ids allocated)".into())
        })?;
        Ok(KeyId::from_u32(next))
    }
}
