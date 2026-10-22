use super::{RestoreError, Result};
use cloud_backup_protocol::publication::AcceptedBackup;
use fold_db::crypto::E2eKeys;
use zeroize::Zeroizing;

/// The caller supplies the phrase again after restart; the session stores no secret.
pub struct RecoveryKeys {
    pub(super) seed: Zeroizing<[u8; 32]>,
    pub(super) content: Zeroizing<[u8; 32]>,
    account_public_key: String,
}

impl RecoveryKeys {
    pub fn from_phrase(phrase: &str, accepted: &AcceptedBackup) -> Result<Self> {
        accepted.validate()?;
        let seed = Zeroizing::new(
            crate::cloud::seed_from_phrase(phrase)
                .map_err(|_| RestoreError::Invalid("recovery phrase"))?,
        );
        let pair = fold_db::security::Ed25519KeyPair::from_secret_key(seed.as_ref())
            .map_err(|_| RestoreError::Invalid("recovery key"))?;
        if fold_db::hex::hex_lower(pair.public_key_bytes())
            != accepted.recovery.account_public_key.as_str()
        {
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
    pub(super) fn validate_for(&self, accepted: &AcceptedBackup) -> Result<()> {
        accepted.validate()?;
        if self.account_public_key != accepted.recovery.account_public_key.as_str() {
            return Err(RestoreError::Invalid("recovery public key"));
        }
        Ok(())
    }
}
