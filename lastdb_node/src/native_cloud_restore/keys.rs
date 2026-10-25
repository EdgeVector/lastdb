use super::{RestoreError, Result};
use fold_db::crypto::E2eKeys;
use zeroize::Zeroizing;

/// The caller supplies the phrase again after restart; the session stores no secret.
pub struct RecoveryKeys {
    pub(super) seed: Zeroizing<[u8; 32]>,
    pub(super) content: Zeroizing<[u8; 32]>,
    account_public_key: String,
}

impl RecoveryKeys {
    /// V2 keeps recovery identity in its client descriptor and authenticated root.
    pub fn from_account_phrase(
        phrase: &str,
        account_public_key: &cloud_backup_protocol::Digest,
    ) -> Result<Self> {
        let seed = Zeroizing::new(
            crate::cloud::seed_from_phrase(phrase)
                .map_err(|_| RestoreError::Invalid("recovery phrase"))?,
        );
        let pair = fold_db::security::Ed25519KeyPair::from_secret_key(seed.as_ref())
            .map_err(|_| RestoreError::Invalid("recovery key"))?;
        if fold_db::hex::hex_lower(pair.public_key_bytes()) != account_public_key.as_str() {
            return Err(RestoreError::Invalid("recovery public key"));
        }
        let keys = E2eKeys::from_ed25519_seed(&seed)
            .map_err(|_| RestoreError::Invalid("recovery derivation"))?;
        Ok(Self {
            seed,
            content: Zeroizing::new(keys.encryption_key()),
            account_public_key: fold_db::hex::hex_lower(pair.public_key_bytes()),
        })
    }
    pub(crate) fn validate_account(
        &self,
        account_public_key: &cloud_backup_protocol::Digest,
    ) -> Result<()> {
        if self.account_public_key != account_public_key.as_str() {
            return Err(RestoreError::Invalid("recovery public key"));
        }
        Ok(())
    }

    pub(crate) fn content_key(&self) -> &[u8; 32] {
        &self.content
    }
}
