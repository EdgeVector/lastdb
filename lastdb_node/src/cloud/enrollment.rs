//! Home identity key loading, recovery-phrase derivation, environment pinning, and device enrollment.
use super::{load_saved_credentials, SavedConnection};
use crate::cloud_backup_client::{build_shared_http_client, https, read_bounded_bytes};
use crate::profile::endpoints::{self, exemem_api_url_for, Environment};
use crate::profile::user_hash_from_pubkey;
use app_identity_crypto::SigningKey;
use cloud_backup_protocol::AccountId;
use fold_db::security::Ed25519KeyPair;
use hkdf::Hkdf;
use sha2::Sha256;
use std::path::Path;
use std::time::Duration;
use zeroize::Zeroizing;

pub struct VerifiedHomeCredentials {
    pub env: Environment,
    pub api_url: &'static str,
    pub api_key: Zeroizing<String>,
    pub(crate) connection: Option<SavedConnection>,
    pub expected_account_id: Option<String>,
}

fn load_home_account_id(home: &Path) -> Result<Option<String>, String> {
    load_home_keypair(home)?
        .map(|pair| user_hash_from_pubkey(&pair.public_key_base64()))
        .transpose()
}

pub fn load_verified_cloud_credentials(
    home: &Path,
) -> Result<Option<VerifiedHomeCredentials>, String> {
    let Some(creds) = load_saved_credentials(home)? else {
        return Ok(None);
    };
    let env = endpoints::environment_for_exemem_api_url(&creds.api_url)
        .ok_or("saved cloud credentials do not match a compiled Exemem environment")?;
    let expected_account_id = load_home_account_id(home)?;
    if let Some(conn) = &creds.connection {
        if conn.environment != env.as_str() {
            return Err("saved cloud connection environment does not match credentials URL".into());
        }
        if let Some(expected) = &expected_account_id {
            if conn.account_id.as_str() != expected {
                return Err(
                    "saved cloud connection account does not match this home's identity.key".into(),
                );
            }
        }
    }
    Ok(Some(VerifiedHomeCredentials {
        env,
        api_url: exemem_api_url_for(env),
        api_key: creds.api_key,
        connection: creds.connection,
        expected_account_id,
    }))
}

/// Recover the stable, domain-separated publisher signing key for a LastDB account.
pub fn app_publisher_signing_key(home: &Path) -> Result<SigningKey, String> {
    let pair =
        load_home_keypair(home)?.ok_or("connect a native LastDB home before publishing an app")?;
    let account_seed = Zeroizing::new(pair.secret_key_bytes());
    let mut seed = Zeroizing::new([0; 32]);
    Hkdf::<Sha256>::new(None, account_seed.as_ref())
        .expand(b"lastdb:app-registry:publisher-key:v1", seed.as_mut())
        .map_err(|_| "cannot derive the app publisher signing key")?;
    Ok(SigningKey::from_bytes(&seed))
}

fn load_home_keypair(home: &Path) -> Result<Option<Ed25519KeyPair>, String> {
    crate::offline_home::read_home_identity_seed(home)?
        .map(|seed| {
            Ed25519KeyPair::from_secret_key(seed.as_ref())
                .map_err(|_| "invalid home identity.key".into())
        })
        .transpose()
}

pub fn seed_from_phrase(words: &str) -> Result<[u8; 32], String> {
    let entropy = bip39::Mnemonic::parse(words.trim())
        .map_err(|_| "Invalid recovery phrase".to_string())?
        .to_entropy();
    entropy.as_slice().try_into().map_err(|_| {
        format!(
            "Recovery phrase must encode 32 bytes, got {}",
            entropy.len()
        )
    })
}

pub fn account_from_phrase(phrase: &str) -> Result<AccountId, String> {
    let seed = Zeroizing::new(seed_from_phrase(phrase).map_err(|_| "invalid recovery phrase")?);
    let pair =
        Ed25519KeyPair::from_secret_key(seed.as_ref()).map_err(|_| "invalid recovery identity")?;
    let hash = user_hash_from_pubkey(&pair.public_key_base64())?;
    AccountId::try_from(hash).map_err(|_| "invalid recovery account".into())
}

pub fn pin_schema_service_environment_from_home(home: &Path) {
    let creds = match load_saved_credentials(home) {
        Ok(Some(c)) => c,
        Ok(None) => return,
        Err(error) => {
            tracing::warn!(target: "lastdb_node::cloud", %error, "saved cloud credentials failed the private owner-file check; the schema service environment does not follow the home");
            return;
        }
    };
    let Some(cloud_env) = endpoints::environment_for_exemem_api_url(&creds.api_url) else {
        return;
    };
    endpoints::set_home_environment(cloud_env);
    let (eff, url, exp) = (
        Environment::from_env(),
        endpoints::schema_service_url(),
        endpoints::schema_service_url_for(cloud_env),
    );
    if (eff, url.as_str()) == (cloud_env, exp) {
        tracing::info!(target: "lastdb_node::cloud", %cloud_env, schema_service_url = %url, "schema service environment follows the home cloud environment");
    } else {
        tracing::warn!(
            target: "lastdb_node::cloud", %cloud_env, schema_service_env = %eff,
            schema_service_url = %url, expected_schema_service_url = %exp,
            "ENVIRONMENT MISMATCH: home is connected to {cloud_env} Exemem cloud, \
             but schema service resolves to {url}; unset overriding env vars or reconnect."
        );
    }
}

pub async fn register_native_device(
    environment: Environment,
    keypair: &Ed25519KeyPair,
    device_id: &str,
) -> Result<(String, String), String> {
    let parsed =
        uuid::Uuid::parse_str(device_id).map_err(|_| "invalid native registration device ID")?;
    if parsed.get_version() != Some(uuid::Version::Random) {
        return Err("native registration requires a random UUIDv4 device ID".into());
    }
    let (pub_hex, ts) = (
        fold_db::hex::hex_lower(keypair.public_key_bytes()),
        chrono::Utc::now().timestamp(),
    );
    let sig = fold_db::security::KeyUtils::signature_to_base64(
        &keypair.sign(format!("{pub_hex}:{ts}").as_bytes()),
    );
    let body = serde_json::json!({ "public_key": pub_hex, "timestamp": ts, "signature": sig, "device_id": device_id });
    let url = https(&format!(
        "{}/api/auth/cli/register",
        exemem_api_url_for(environment)
    ))
    .map_err(|_| "Invalid Exemem API URL for register".to_owned())?;
    let expected = user_hash_from_pubkey(&keypair.public_key_base64())
        .map_err(|_| "Cannot derive native account identity".to_owned())?;
    let resp = build_shared_http_client()
        .map_err(|_| "cannot construct native registration client")?
        .post(url.clone())
        .timeout(Duration::from_secs(30))
        .json(&body)
        .send()
        .await
        .map_err(|_| "native registration request failed")?;
    if resp.status().is_redirection() || resp.url() != &url {
        return Err("native registration redirected".into());
    }
    if !resp.status().is_success() {
        return Err(format!(
            "native registration ({environment}) HTTP status {}",
            resp.status().as_u16()
        ));
    }
    let bytes = read_bounded_bytes(resp, 65_536)
        .await
        .map_err(|_| "cannot read native registration response")?;
    let json: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|_| "Register response was not valid JSON")?;
    if json.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
        return Err("Register request was rejected".into());
    }
    let field = |name: &str| {
        json.get(name)
            .and_then(serde_json::Value::as_str)
            .filter(|v| !v.trim().is_empty())
            .map(str::to_owned)
            .ok_or_else(|| format!("Register response missing nonempty {name}"))
    };
    let (api_key, user_hash) = (field("api_key")?, field("user_hash")?);
    (user_hash == expected)
        .then_some((api_key, user_hash))
        .ok_or_else(|| "Register returned credentials for another signed account".into())
}
