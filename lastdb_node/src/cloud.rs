//! Account identity, the saved credentials loader, and explicit backup enrollment.
use std::fs::{self, OpenOptions};
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::cloud_backup_client::AuthClient;
use crate::profile::endpoints::{self, exemem_api_url_for, Environment};
use cloud_backup_protocol::{AccountId, Id};
use fold_db::security::Ed25519KeyPair;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use zeroize::Zeroizing;

/// The saved account credentials and, after an owner connect, the manual
/// backup consent: one file, replaced atomically. No cloud work starts from
/// it alone.
pub const CLOUD_CREDENTIALS_FILE: &str = "cloud-credentials.json";

const RESPONSE_LIMIT: usize = 65_536;

/// The saved account credentials file of a home.
pub fn cloud_credentials_path(home: &Path) -> PathBuf {
    home.join(CLOUD_CREDENTIALS_FILE)
}

/// `{"api_url","api_key"}`, plus `connection` once the owner consents to
/// manual backup of this home. The key zeroizes on drop. Do not derive
/// `Debug`: `Zeroizing` prints its contents.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SavedCredentials {
    pub api_url: String,
    #[serde(serialize_with = "write_key", deserialize_with = "read_key")]
    pub api_key: Zeroizing<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection: Option<SavedConnection>,
}

/// The owner's manual backup consent: the account and the source home it
/// binds, and the backup environment and endpoint.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SavedConnection {
    pub account_id: AccountId,
    pub source_home: Id,
    pub environment: String,
    pub endpoint: String,
}

/// The saved bytes are not the credential JSON. Carries no file content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidCredentials;

impl SavedCredentials {
    pub fn new(api_url: impl Into<String>, api_key: String) -> Self {
        Self {
            api_url: api_url.into(),
            api_key: Zeroizing::new(api_key),
            connection: None,
        }
    }

    /// Compact JSON of `api_url` and `api_key` only.
    pub fn key_json(&self) -> serde_json::Result<Zeroizing<Vec<u8>>> {
        Self::new(&self.api_url, self.api_key.as_str().to_owned()).to_json()
    }

    /// The serde error can quote a value, so the error drops it.
    pub fn parse(bytes: &[u8]) -> Result<Self, InvalidCredentials> {
        serde_json::from_slice(bytes).map_err(|_| InvalidCredentials)
    }

    /// Compact JSON of the credentials and optional connection.
    pub fn to_json(&self) -> serde_json::Result<Zeroizing<Vec<u8>>> {
        serde_json::to_vec(self).map(Zeroizing::new)
    }
}

fn write_key<S: Serializer>(key: &Zeroizing<String>, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(key.as_str())
}

fn read_key<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Zeroizing<String>, D::Error> {
    String::deserialize(deserializer).map(Zeroizing::new)
}

/// Load the home's saved credentials from a private owner file (`Ok(None)` when absent).
pub fn load_saved_credentials(home: &Path) -> Result<Option<SavedCredentials>, String> {
    let path = cloud_credentials_path(home);
    match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.is_file() => {}
        Ok(_) => return Err("native backup credential path must be a regular file".into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("cannot inspect native backup credential path".into()),
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&path)
        .map_err(|_| "cannot open native backup credentials".to_owned())?;
    let metadata = file
        .metadata()
        .map_err(|_| "cannot inspect native backup credentials".to_owned())?;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o077 != 0
        || metadata.nlink() != 1
    {
        return Err("native backup credentials must be a private owner file".into());
    }
    let limit = cloud_backup_protocol::limits::REQUEST_JSON_MAX_BYTES;
    let mut bytes = Zeroizing::new(Vec::new());
    file.take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read native backup credentials".to_owned())?;
    if bytes.len() as u64 > limit {
        return Err("native backup credential file exceeds its byte limit".into());
    }
    let credentials = SavedCredentials::parse(&bytes)
        .map_err(|_| "invalid native backup credential file".to_owned())?;
    if credentials.api_key.is_empty() {
        return Err("native backup credential is empty".into());
    }
    Ok(Some(credentials))
}

/// Shared credential read; the caller chooses its separately versioned protocol.
pub(crate) fn auth_for_recovery(home: &Path) -> Result<AuthClient, String> {
    let mut credentials =
        load_saved_credentials(home)?.ok_or("native backup needs existing account credentials")?;
    let api_key = std::mem::take(&mut *credentials.api_key);
    Ok(AuthClient::new(credentials.api_url, api_key))
}

/// Parse a 24-word BIP39 recovery phrase into the account's 32-byte Ed25519 seed.
pub fn seed_from_phrase(words: &str) -> Result<[u8; 32], String> {
    let mnemonic =
        bip39::Mnemonic::parse(words.trim()).map_err(|_| "Invalid recovery phrase".to_string())?;
    let entropy = mnemonic.to_entropy();
    entropy.as_slice().try_into().map_err(|_| {
        format!(
            "Recovery phrase must encode 32 bytes, got {}",
            entropy.len()
        )
    })
}

pub fn account_from_phrase(phrase: &str) -> Result<AccountId, String> {
    let seed =
        Zeroizing::new(seed_from_phrase(phrase).map_err(|_| "invalid recovery phrase".to_owned())?);
    let pair = Ed25519KeyPair::from_secret_key(seed.as_ref())
        .map_err(|_| "invalid recovery identity".to_owned())?;
    let account = crate::identity::user_hash_from_pubkey(&pair.public_key_base64())?;
    AccountId::try_from(account).map_err(|_| "invalid recovery account".to_owned())
}

/// Daemon boot: pin the schema-service environment to the home's cloud environment.
pub fn pin_schema_service_environment_from_home(home: &Path) {
    let credentials = match load_saved_credentials(home) {
        Ok(Some(credentials)) => credentials,
        Ok(None) => return,
        Err(error) => {
            tracing::warn!(
                target: "lastdb_node::cloud",
                error = %error,
                "saved cloud credentials failed the private owner-file check; \
                 the schema service environment does not follow the home"
            );
            return;
        }
    };
    let Some(cloud_env) = endpoints::environment_for_exemem_api_url(&credentials.api_url) else {
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

/// Credentials minted by a successful signed register.
pub struct Registered {
    pub api_key: String,
    pub user_hash: String,
}

fn signed_register_request(
    api_url: &str,
    keypair: &Ed25519KeyPair,
    invite_code: Option<&str>,
    device_id: &str,
) -> Result<(reqwest::Url, serde_json::Value, String), String> {
    let public_key_hex = fold_db::hex::hex_lower(keypair.public_key_bytes());
    let timestamp = chrono::Utc::now().timestamp();
    let payload = format!("{public_key_hex}:{timestamp}");
    let signature = keypair.sign(payload.as_bytes());
    let signature_b64 = fold_db::security::KeyUtils::signature_to_base64(&signature);

    let mut body = serde_json::json!({
        "public_key": public_key_hex,
        "timestamp": timestamp,
        "signature": signature_b64,
        "device_id": device_id,
    });
    if let Some(code) = invite_code {
        body["invite_code"] = serde_json::Value::String(code.to_string());
    }

    let url = reqwest::Url::parse(&format!("{api_url}/api/auth/cli/register"))
        .map_err(|_| "Invalid Exemem API URL for register".to_string())?;
    if url.scheme() != "https" || !url.username().is_empty() || url.password().is_some() {
        return Err("Register requires an HTTPS endpoint without embedded credentials".into());
    }
    let expected_user_hash = crate::identity::user_hash_from_pubkey(&keypair.public_key_base64())
        .map_err(|_| "Cannot derive native account identity".to_string())?;
    Ok((url, body, expected_user_hash))
}

fn parse_registered_response(text: &str, expected_user_hash: &str) -> Result<Registered, String> {
    let json: serde_json::Value =
        serde_json::from_str(text).map_err(|_| "Register response was not valid JSON")?;
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
    let registered = Registered {
        api_key: field("api_key")?,
        user_hash: field("user_hash")?,
    };
    (registered.user_hash == expected_user_hash)
        .then_some(registered)
        .ok_or_else(|| "Register returned credentials for another signed account".into())
}

/// Explicit compiled-environment registration without a local store or recipient.
pub async fn register_native_device(
    environment: Environment,
    keypair: &Ed25519KeyPair,
    device_id: &str,
    invite_code: Option<&str>,
) -> Result<Registered, String> {
    let parsed = uuid::Uuid::parse_str(device_id).map_err(|_| "invalid DEV recovery device ID")?;
    if parsed.get_version() != Some(uuid::Version::Random) {
        return Err("DEV recovery requires a random device ID".into());
    }
    let (url, body, expected_user_hash) = signed_register_request(
        exemem_api_url_for(environment),
        keypair,
        invite_code,
        device_id,
    )?;
    let client = crate::cloud_backup_client::build_shared_http_client()
        .map_err(|_| "cannot construct DEV recovery register client")?;
    let mut response = client
        .post(url.clone())
        .timeout(Duration::from_secs(30))
        .json(&body)
        .send()
        .await
        .map_err(|_| "DEV recovery register request failed")?;
    if response.status().is_redirection() || response.url() != &url {
        return Err("DEV recovery register redirected".into());
    }
    if !response.status().is_success() {
        return Err(format!(
            "DEV recovery register HTTP status {}",
            response.status().as_u16()
        ));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| "cannot read DEV recovery response")?
    {
        if chunk.len() > RESPONSE_LIMIT.saturating_sub(bytes.len()) {
            return Err("DEV recovery response exceeds supported size".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    let text = std::str::from_utf8(&bytes).map_err(|_| "DEV recovery response is not UTF-8")?;
    parse_registered_response(text, &expected_user_hash)
}
