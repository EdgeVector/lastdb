//! Account identity, the saved credentials loader, and explicit backup enrollment.
use std::path::Path;

use crate::cloud_backup_client::AuthClient;
use cloud_backup_protocol::AccountId;
use fold_db::security::Ed25519KeyPair;

mod credentials;
mod register;

/// The saved account credentials and, after an owner connect, the manual
/// backup consent: one file, replaced atomically. No cloud work starts from
/// it alone.
pub const CLOUD_CREDENTIALS_FILE: &str = "cloud-credentials.json";
pub use credentials::*;
pub use register::*;

/// Shared credential read; the caller chooses its separately versioned protocol.
pub(crate) fn auth_for_recovery(home: &Path) -> Result<AuthClient, String> {
    let mut credentials =
        load_saved_credentials(home)?.ok_or("native backup needs existing account credentials")?;
    let api_key = std::mem::take(&mut *credentials.api_key);
    Ok(AuthClient::new(credentials.api_url, api_key))
}

pub fn account_from_phrase(phrase: &str) -> Result<AccountId, String> {
    let seed = zeroize::Zeroizing::new(
        seed_from_phrase(phrase).map_err(|_| "invalid recovery phrase".to_owned())?,
    );
    let pair = Ed25519KeyPair::from_secret_key(seed.as_ref())
        .map_err(|_| "invalid recovery identity".to_owned())?;
    let account = crate::identity::user_hash_from_pubkey(&pair.public_key_base64())?;
    AccountId::try_from(account).map_err(|_| "invalid recovery account".to_owned())
}

/// The Exemem environment this home is connected to, from the `api_url`
/// that explicit backup enrollment saved in `cloud-credentials.json`.
pub fn home_cloud_environment(home: &Path) -> Option<crate::profile::endpoints::Environment> {
    let credentials = match load_saved_credentials(home) {
        Ok(credentials) => credentials?,
        Err(error) => {
            tracing::warn!(
                target: "lastdb_node::cloud",
                error = %error,
                "saved cloud credentials failed the private owner-file check; \
                 the schema service environment does not follow the home"
            );
            return None;
        }
    };
    crate::profile::endpoints::environment_for_exemem_api_url(&credentials.api_url)
}

/// Daemon boot: pin the schema-service environment to the home's cloud environment.
pub fn pin_schema_service_environment_from_home(home: &Path) {
    use crate::profile::endpoints::{self, Environment};
    let Some(cloud_env) = home_cloud_environment(home) else {
        return;
    };
    endpoints::set_home_environment(cloud_env);
    let effective = Environment::from_env();
    let schema_url = endpoints::schema_service_url();
    let expected_url = endpoints::schema_service_url_for(cloud_env);
    if effective != cloud_env || schema_url != expected_url {
        tracing::warn!(
            target: "lastdb_node::cloud",
            cloud_env = %cloud_env,
            schema_service_env = %effective,
            schema_service_url = %schema_url,
            expected_schema_service_url = %expected_url,
            "ENVIRONMENT MISMATCH: this home is connected to the {cloud_env} Exemem cloud \
             (cloud-credentials.json), but schema resolve/register go to {schema_url}. \
             An env var (ENVIRONMENT / EXEMEM_ENV / FOLD_SCHEMA_SERVICE_URL) overrides \
             the home. Unset it, or reconnect the home to the matching environment."
        );
    } else {
        tracing::info!(
            target: "lastdb_node::cloud",
            cloud_env = %cloud_env,
            schema_service_url = %schema_url,
            "schema service environment follows the home cloud environment"
        );
    }
}
