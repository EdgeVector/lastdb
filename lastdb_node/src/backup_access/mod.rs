//! Protected backup credentials and account identity.
use cloud_backup_client::AuthClient;
use cloud_backup_protocol::AccountId;
use std::path::Path;

/// Shared credential read; the caller chooses its separately versioned protocol.
/// `crate::cloud::load_saved_credentials` keeps the private owner-file checks.
pub(crate) fn auth_for_recovery(home: &Path) -> Result<AuthClient, String> {
    let mut credentials = crate::cloud::load_saved_credentials(home)?
        .ok_or("native backup needs existing account credentials")?;
    let api_key = std::mem::take(&mut *credentials.api_key);
    Ok(AuthClient::new(credentials.api_url, api_key))
}

pub fn account_from_phrase(phrase: &str) -> Result<AccountId, String> {
    let seed = zeroize::Zeroizing::new(
        crate::cloud::seed_from_phrase(phrase).map_err(|_| "invalid recovery phrase".to_owned())?,
    );
    let pair = fold_db::security::Ed25519KeyPair::from_secret_key(seed.as_ref())
        .map_err(|_| "invalid recovery identity".to_owned())?;
    let account = lastdb_identity::user_hash_from_pubkey(&pair.public_key_base64())?;
    AccountId::try_from(account).map_err(|_| "invalid recovery account".to_owned())
}
