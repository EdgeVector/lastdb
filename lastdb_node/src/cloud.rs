//! Account identity, the saved credentials loader, and explicit backup enrollment.
use crate::cloud_backup_client::AuthClient;
use crate::profile::endpoints::{self, exemem_api_url_for, Environment};
use cloud_backup_protocol::{limits::REQUEST_JSON_MAX_BYTES, AccountId, Id};
use fold_db::security::Ed25519KeyPair;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fs::{self, Metadata, OpenOptions};
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;
use zeroize::Zeroizing;

pub const CLOUD_CREDENTIALS_FILE: &str = "cloud-credentials.json";

pub fn cloud_credentials_path(home: &Path) -> PathBuf {
    home.join(CLOUD_CREDENTIALS_FILE)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SavedCredentials {
    pub api_url: String,
    #[serde(serialize_with = "write_key", deserialize_with = "read_key")]
    pub api_key: Zeroizing<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection: Option<SavedConnection>,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SavedConnection {
    pub account_id: AccountId,
    pub source_home: Id,
    pub environment: String,
    pub endpoint: String,
}

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

    pub fn key_json(&self) -> serde_json::Result<Zeroizing<Vec<u8>>> {
        Self::new(&self.api_url, self.api_key.as_str().to_owned()).to_json()
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, InvalidCredentials> {
        serde_json::from_slice(bytes).map_err(|_| InvalidCredentials)
    }

    pub fn to_json(&self) -> serde_json::Result<Zeroizing<Vec<u8>>> {
        serde_json::to_vec(self).map(Zeroizing::new)
    }
}

fn write_key<S: Serializer>(key: &Zeroizing<String>, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(key.as_str())
}
fn read_key<'de, D: Deserializer<'de>>(d: D) -> Result<Zeroizing<String>, D::Error> {
    String::deserialize(d).map(Zeroizing::new)
}

pub(crate) fn is_private_file(meta: &Metadata) -> bool {
    meta.is_file()
        && meta.uid() == unsafe { libc::geteuid() }
        && meta.mode() & 0o077 == 0
        && meta.nlink() == 1
}

/// Load the home's saved credentials from a private owner file (`Ok(None)` when absent).
pub fn load_saved_credentials(home: &Path) -> Result<Option<SavedCredentials>, String> {
    let path = cloud_credentials_path(home);
    match fs::symlink_metadata(&path) {
        Ok(m) if m.is_file() => {}
        Ok(_) => return Err("native backup credential path must be a regular file".into()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("cannot inspect native backup credential path".into()),
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&path)
        .map_err(|_| "cannot open native backup credentials")?;
    let m = file
        .metadata()
        .map_err(|_| "cannot inspect native backup credentials")?;
    if !is_private_file(&m) {
        return Err("native backup credentials must be a private owner file".into());
    }
    let mut bytes = Zeroizing::new(Vec::new());
    file.take(REQUEST_JSON_MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read native backup credentials")?;
    if bytes.len() as u64 > REQUEST_JSON_MAX_BYTES {
        return Err("native backup credential file exceeds its byte limit".into());
    }
    let creds =
        SavedCredentials::parse(&bytes).map_err(|_| "invalid native backup credential file")?;
    (!creds.api_key.is_empty())
        .then_some(Some(creds))
        .ok_or_else(|| "native backup credential is empty".into())
}

pub(crate) fn auth_for_recovery(home: &Path) -> Result<AuthClient, String> {
    let mut c =
        load_saved_credentials(home)?.ok_or("cloud sync needs existing account credentials")?;
    Ok(AuthClient::new(c.api_url, std::mem::take(&mut *c.api_key)))
}

/// Saved-credential client for account scope and sealed file upload.
pub fn client_from_home(
    home: &Path,
) -> Result<crate::cloud_backup_client::byte_backup::ByteBackupClient, String> {
    crate::cloud_backup_client::byte_backup::ByteBackupClient::from_auth(auth_for_recovery(home)?)
        .map_err(|error| error.to_string())
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
    let hash = crate::identity::user_hash_from_pubkey(&pair.public_key_base64())?;
    AccountId::try_from(hash).map_err(|_| "invalid recovery account".into())
}

pub fn pin_schema_service_environment_from_home(home: &Path) {
    let creds = match load_saved_credentials(home) {
        Ok(Some(c)) => c,
        Ok(None) => return,
        Err(error) => {
            tracing::warn!(
                target: "lastdb_node::cloud", %error,
                "saved cloud credentials failed the private owner-file check; the schema service environment does not follow the home"
            );
            return;
        }
    };
    let Some(cloud_env) = endpoints::environment_for_exemem_api_url(&creds.api_url) else {
        return;
    };
    endpoints::set_home_environment(cloud_env);
    let (effective, schema_service_url, expected_url) = (
        Environment::from_env(),
        endpoints::schema_service_url(),
        endpoints::schema_service_url_for(cloud_env),
    );
    if effective != cloud_env || schema_service_url != expected_url {
        tracing::warn!(
            target: "lastdb_node::cloud", %cloud_env, schema_service_env = %effective,
            %schema_service_url, expected_schema_service_url = %expected_url,
            "ENVIRONMENT MISMATCH: this home is connected to the {cloud_env} Exemem cloud \
             (cloud-credentials.json), but schema resolve/register go to {schema_service_url}. \
             An env var (ENVIRONMENT / EXEMEM_ENV / FOLD_SCHEMA_SERVICE_URL) overrides \
             the home. Unset it, or reconnect the home to the matching environment."
        );
    } else {
        tracing::info!(
            target: "lastdb_node::cloud", %cloud_env, %schema_service_url,
            "schema service environment follows the home cloud environment"
        );
    }
}

pub struct Registered {
    pub api_key: String,
    pub user_hash: String,
}

pub async fn register_native_device(
    environment: Environment,
    keypair: &Ed25519KeyPair,
    device_id: &str,
    invite_code: Option<&str>,
) -> Result<Registered, String> {
    if uuid::Uuid::parse_str(device_id)
        .map_err(|_| "invalid DEV recovery device ID")?
        .get_version()
        != Some(uuid::Version::Random)
    {
        return Err("DEV recovery requires a random device ID".into());
    }
    let (pub_hex, ts) = (
        fold_db::hex::hex_lower(keypair.public_key_bytes()),
        chrono::Utc::now().timestamp(),
    );
    let sig = fold_db::security::KeyUtils::signature_to_base64(
        &keypair.sign(format!("{pub_hex}:{ts}").as_bytes()),
    );
    let mut body = serde_json::json!({ "public_key": pub_hex, "timestamp": ts, "signature": sig, "device_id": device_id });
    if let Some(code) = invite_code {
        body["invite_code"] = serde_json::Value::String(code.to_string());
    }
    let url = reqwest::Url::parse(&format!(
        "{}/api/auth/cli/register",
        exemem_api_url_for(environment)
    ))
    .map_err(|_| "Invalid Exemem API URL for register".to_string())?;
    if url.scheme() != "https" || !url.username().is_empty() || url.password().is_some() {
        return Err("Register requires an HTTPS endpoint without embedded credentials".into());
    }
    let expected_hash = crate::identity::user_hash_from_pubkey(&keypair.public_key_base64())
        .map_err(|_| "Cannot derive native account identity".to_string())?;
    let mut resp = crate::cloud_backup_client::build_shared_http_client()
        .map_err(|_| "cannot construct DEV recovery register client")?
        .post(url.clone())
        .timeout(Duration::from_secs(30))
        .json(&body)
        .send()
        .await
        .map_err(|_| "DEV recovery register request failed")?;
    if resp.status().is_redirection() || resp.url() != &url {
        return Err("DEV recovery register redirected".into());
    }
    if !resp.status().is_success() {
        return Err(format!(
            "DEV recovery register HTTP status {}",
            resp.status().as_u16()
        ));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|_| "cannot read DEV recovery response")?
    {
        if chunk.len() > 65_536usize.saturating_sub(bytes.len()) {
            return Err("DEV recovery response exceeds supported size".into());
        }
        bytes.extend_from_slice(&chunk);
    }
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
    let reg = Registered {
        api_key: field("api_key")?,
        user_hash: field("user_hash")?,
    };
    (reg.user_hash == expected_hash)
        .then_some(reg)
        .ok_or_else(|| "Register returned credentials for another signed account".into())
}
