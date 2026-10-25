use super::{RestoreError, Result};
use fold_db::crypto::E2eKeys;
use zeroize::Zeroizing;

/// The caller supplies the phrase again after restart; the session stores no secret.
pub struct RecoveryKeys {
    pub(super) seed: Zeroizing<[u8; 32]>,
    pub(super) content: Zeroizing<[u8; 32]>,
    account_public_key: String,
    account_id: cloud_backup_protocol::AccountId,
}

impl RecoveryKeys {
    /// Legacy V2 also exposes recovery identity in its client descriptor.
    pub fn from_account_phrase(
        phrase: &str,
        account_public_key: &cloud_backup_protocol::Digest,
    ) -> Result<Self> {
        let keys = Self::from_phrase(phrase)?;
        keys.validate_account(account_public_key)?;
        Ok(keys)
    }

    /// Derive locally before the opaque encrypted root is available.
    pub fn from_scope_phrase(
        phrase: &str,
        account_id: &cloud_backup_protocol::AccountId,
    ) -> Result<Self> {
        let keys = Self::from_phrase(phrase)?;
        if keys.account_id != *account_id {
            return Err(RestoreError::Invalid("recovery account"));
        }
        Ok(keys)
    }

    fn from_phrase(phrase: &str) -> Result<Self> {
        let seed = Zeroizing::new(
            crate::cloud::seed_from_phrase(phrase)
                .map_err(|_| RestoreError::Invalid("recovery phrase"))?,
        );
        let pair = fold_db::security::Ed25519KeyPair::from_secret_key(seed.as_ref())
            .map_err(|_| RestoreError::Invalid("recovery key"))?;
        let keys = E2eKeys::from_ed25519_seed(&seed)
            .map_err(|_| RestoreError::Invalid("recovery derivation"))?;
        let account_id = lastdb_identity::user_hash_from_pubkey(&pair.public_key_base64())
            .map_err(|_| RestoreError::Invalid("recovery account"))?;
        Ok(Self {
            seed,
            content: Zeroizing::new(keys.encryption_key()),
            account_public_key: fold_db::hex::hex_lower(pair.public_key_bytes()),
            account_id: cloud_backup_protocol::AccountId::try_from(account_id)
                .map_err(|_| RestoreError::Invalid("recovery account"))?,
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
