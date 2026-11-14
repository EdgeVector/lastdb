//! Account identity, saved cloud/developer credentials, and explicit backup enrollment.
use crate::cloud_backup_client::{bounded, build_shared_http_client, https, AuthClient};
use crate::identity::{user_hash_from_pubkey, IDENTITY_KEY_FILE};
use crate::profile::endpoints::{self, exemem_api_url_for, Environment};
use cloud_backup_protocol::{limits::REQUEST_JSON_MAX_BYTES, AccountId, Id};
use fold_db::security::Ed25519KeyPair;
use rustix::fs::{self as rfs, AtFlags, Mode, OFlags};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fs::{self, File, Metadata, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;
use zeroize::Zeroizing;

pub const CLOUD_CREDENTIALS_FILE: &str = "cloud-credentials.json";
pub const DEVELOPER_API_KEY_FILE: &str = "developer-api-key";

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
    pub fn parse(bytes: &[u8]) -> serde_json::Result<Self> {
        serde_json::from_slice(bytes)
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

fn read_private_named(
    path: &Path,
    max_bytes: u64,
    priv_err: &'static str,
    lim_err: &'static str,
) -> Result<Option<Vec<u8>>, &'static str> {
    let meta = match fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(priv_err),
    };
    if !meta.is_file() {
        return Err(priv_err);
    }
    if meta.len() > max_bytes {
        return Err(lim_err);
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|_| priv_err)?;
    if !is_private_file(&file.metadata().map_err(|_| priv_err)?) {
        return Err(priv_err);
    }
    let mut bytes = Vec::new();
    file.take(max_bytes + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| priv_err)?;
    (bytes.len() as u64 <= max_bytes)
        .then_some(Some(bytes))
        .ok_or(lim_err)
}

pub fn load_saved_credentials(home: &Path) -> Result<Option<SavedCredentials>, String> {
    const PRIV_ERR: &str = "native backup credentials must be a private owner file";
    const LIM_ERR: &str = "native backup credential file exceeds its byte limit";
    let Some(raw) = read_private_named(
        &home.join(CLOUD_CREDENTIALS_FILE),
        REQUEST_JSON_MAX_BYTES,
        PRIV_ERR,
        LIM_ERR,
    )?
    else {
        return Ok(None);
    };
    let creds = SavedCredentials::parse(&Zeroizing::new(raw))
        .map_err(|_| "invalid native backup credential file")?;
    (!creds.api_key.is_empty())
        .then_some(Some(creds))
        .ok_or_else(|| "native backup credential is empty".into())
}

pub fn is_valid_developer_key(key: &str) -> bool {
    key.strip_prefix("em_")
        .is_some_and(|h| h.len() == 48 && h.bytes().all(|b| b.is_ascii_hexdigit()))
}

pub fn developer_key_prefix(key: &str) -> Option<String> {
    let hex_part = key.strip_prefix("em_")?;
    if hex_part.len() < 10 || !hex_part.bytes().take(10).all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let short: String = hex_part.chars().take(10).collect();
    Some(format!("em_{short}..."))
}

pub fn is_valid_key_prefix(prefix: &str) -> bool {
    prefix
        .strip_prefix("em_")
        .and_then(|s| s.strip_suffix("..."))
        .is_some_and(|h| h.len() == 10 && h.bytes().all(|b| b.is_ascii_hexdigit()))
}

pub fn load_saved_developer_key(home: &Path) -> Result<Option<Zeroizing<String>>, String> {
    const PRIV_ERR: &str = "developer API key file must be a private 0600 owner file";
    const LIM_ERR: &str = "developer API key file exceeds its byte limit";
    let Some(raw) = read_private_named(&home.join(DEVELOPER_API_KEY_FILE), 256, PRIV_ERR, LIM_ERR)?
    else {
        return Ok(None);
    };
    let raw = Zeroizing::new(raw);
    let trimmed = std::str::from_utf8(&raw)
        .map_err(|_| "saved developer API key file is not UTF-8")?
        .trim();
    is_valid_developer_key(trimmed)
        .then(|| Some(Zeroizing::new(trimmed.to_owned())))
        .ok_or_else(|| "saved developer API key file has an invalid key format".into())
}

fn open_owned_home_dir(home: &Path) -> Result<File, String> {
    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let dir = File::from(
        rfs::open(home, flags, Mode::empty())
            .map_err(|_| format!("cannot open home directory {}", home.display()))?,
    );
    let meta = dir
        .metadata()
        .map_err(|_| "cannot inspect home directory")?;
    (meta.uid() == unsafe { libc::geteuid() } && meta.mode() & 0o022 == 0)
        .then_some(dir)
        .ok_or_else(|| "home directory must be owner-held and not writable by others".into())
}

pub fn save_developer_key(home: &Path, key: &str) -> Result<PathBuf, String> {
    if !is_valid_developer_key(key) {
        return Err("developer API key has an invalid format".into());
    }
    let dir = open_owned_home_dir(home)?;
    match rfs::statat(&dir, DEVELOPER_API_KEY_FILE, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(st) => {
            let reg = rfs::FileType::from_raw_mode(st.st_mode) == rfs::FileType::RegularFile;
            if !reg || st.st_uid != unsafe { libc::geteuid() } || st.st_nlink != 1 {
                return Err("refusing to replace non-regular developer-api-key entry".into());
            }
        }
        Err(rustix::io::Errno::NOENT) => {}
        Err(_) => return Err("cannot inspect existing developer-api-key entry".into()),
    }
    let temp = format!(".developer-api-key.{}.pending", uuid::Uuid::new_v4());
    let flags = OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let mut file = File::from(
        rfs::openat(&dir, temp.as_str(), flags, Mode::from_raw_mode(0o600))
            .map_err(|_| "cannot create staged developer-api-key file")?,
    );
    let write_res = (|| -> Result<(), String> {
        rfs::fchmod(&file, Mode::from_raw_mode(0o600))
            .map_err(|_| "cannot set 0600 mode on developer-api-key")?;
        let line = Zeroizing::new(format!("{key}\n").into_bytes());
        file.write_all(&line)
            .and_then(|()| file.sync_all())
            .map_err(|_| "cannot write developer-api-key")?;
        rfs::renameat(&dir, temp.as_str(), &dir, DEVELOPER_API_KEY_FILE)
            .map_err(|_| "cannot publish developer-api-key".to_owned())
    })();
    if write_res.is_err() {
        let _ = rfs::unlinkat(&dir, temp.as_str(), AtFlags::empty());
    }
    write_res?;
    dir.sync_all()
        .map_err(|_| "cannot sync home directory after saving developer-api-key".to_owned())?;
    Ok(home.join(DEVELOPER_API_KEY_FILE))
}

pub fn remove_saved_developer_key_if_prefix(home: &Path, prefix: &str) -> Result<bool, String> {
    let Some(saved) = load_saved_developer_key(home)? else {
        return Ok(false);
    };
    if developer_key_prefix(&saved).as_deref() != Some(prefix) {
        return Ok(false);
    }
    let dir = open_owned_home_dir(home)?;
    match rfs::unlinkat(&dir, DEVELOPER_API_KEY_FILE, AtFlags::empty()) {
        Ok(()) => {}
        Err(rustix::io::Errno::NOENT) => return Ok(false),
        Err(_) => return Err("cannot remove revoked developer-api-key file".into()),
    }
    dir.sync_all()
        .map_err(|_| "cannot sync home directory after removing developer-api-key".to_owned())?;
    Ok(true)
}

pub fn load_home_account_id(home: &Path) -> Result<Option<String>, String> {
    const PRIV_ERR: &str = "home identity.key must be a private owner file";
    const LIM_ERR: &str = "home identity.key exceeds 32 bytes";
    let Some(raw) = read_private_named(&home.join(IDENTITY_KEY_FILE), 32, PRIV_ERR, LIM_ERR)?
    else {
        return Ok(None);
    };
    let seed: Zeroizing<[u8; 32]> = raw
        .as_slice()
        .try_into()
        .map(Zeroizing::new)
        .map_err(|_| "home identity.key must contain exactly 32 bytes".to_owned())?;
    let pair =
        Ed25519KeyPair::from_secret_key(seed.as_ref()).map_err(|_| "invalid home identity.key")?;
    user_hash_from_pubkey(&pair.public_key_base64()).map(Some)
}

pub fn client_from_home(
    home: &Path,
) -> Result<crate::cloud_backup_client::byte_backup::ByteBackupClient, String> {
    let mut c =
        load_saved_credentials(home)?.ok_or("cloud sync needs existing account credentials")?;
    let auth = AuthClient::new(c.api_url, std::mem::take(&mut *c.api_key));
    crate::cloud_backup_client::byte_backup::ByteBackupClient::from_auth(auth)
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
    invite_code: Option<&str>,
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
    let mut body = serde_json::json!({ "public_key": pub_hex, "timestamp": ts, "signature": sig, "device_id": device_id });
    if let Some(code) = invite_code {
        body["invite_code"] = serde_json::Value::String(code.to_owned());
    }
    let url = https(&format!(
        "{}/api/auth/cli/register",
        exemem_api_url_for(environment)
    ))
    .map_err(|_| "Invalid Exemem API URL for register".to_owned())?;
    let expected = user_hash_from_pubkey(&keypair.public_key_base64())
        .map_err(|_| "Cannot derive native account identity".to_owned())?;
    let req = build_shared_http_client()
        .map_err(|_| "cannot construct native registration client")?
        .post(url.clone())
        .timeout(Duration::from_secs(30))
        .json(&body);
    let resp = observability::propagation::inject_w3c(req)
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
    let bytes = bounded(resp, 65_536)
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
