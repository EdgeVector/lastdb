//! Protected backup credentials and account identity. No legacy cloud activation.
mod credentials;
pub(crate) use credentials::auth_for_recovery;

use cloud_backup_protocol::AccountId;

pub fn account_from_phrase(phrase: &str) -> Result<AccountId, String> {
    let seed = zeroize::Zeroizing::new(
        crate::cloud::seed_from_phrase(phrase).map_err(|_| "invalid recovery phrase".to_owned())?,
    );
    let pair = fold_db::security::Ed25519KeyPair::from_secret_key(seed.as_ref())
        .map_err(|_| "invalid recovery identity".to_owned())?;
    let account = lastdb_identity::user_hash_from_pubkey(&pair.public_key_base64())?;
    AccountId::try_from(account).map_err(|_| "invalid recovery account".to_owned())
}
