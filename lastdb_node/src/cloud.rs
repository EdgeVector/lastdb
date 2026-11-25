//! Account identity, saved cloud/developer credentials, and explicit backup enrollment.
mod enrollment;

pub use enrollment::{
    account_from_phrase, app_publisher_signing_key, load_verified_cloud_credentials,
    pin_schema_service_environment_from_home, register_native_device, seed_from_phrase,
    VerifiedHomeCredentials,
};

use crate::offline_home::read_private_named;
use crate::profile::endpoints::{environment_for_exemem_api_url, Environment};
use cloud_backup_protocol::v2::limits::CONTROL_JSON_MAX_BYTES;
use cloud_backup_protocol::{AccountId, Id, ProtocolError};
use rustix::fs::{self as rfs, AtFlags, Mode, OFlags};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fs::File;
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use zeroize::Zeroizing;

pub const CLOUD_CREDENTIALS_FILE: &str = "cloud-credentials.json";
pub const DEVELOPER_API_KEY_FILE: &str = "developer-api-key";
pub const PAYMENT_REQUIRED_MESSAGE: &str =
    "active paid cloud subscription required — run 'lastdb cloud payment checkout' first";
#[doc(hidden)]
pub const SIGNED_HISTORY_INTENT_FILE: &str = ".signed-history-intent";
#[doc(hidden)]
/// Marker bytes: this home should bootstrap signed history on next sync.
pub const SIGNED_HISTORY_INTENT: &[u8] = b"signed-history-v1\n";

#[must_use]
pub fn is_payment_subscription_error(status: u16, body: &[u8]) -> bool {
    status == 403
        && serde_json::from_slice::<ProtocolError>(body)
            .is_ok_and(|err| err.is_payment_subscription())
}

/// The relay that `LASTDB_RELAY_URL` names, if it names one.
#[must_use]
pub fn explicit_relay_url() -> Option<String> {
    let url = std::env::var("LASTDB_RELAY_URL").ok()?;
    let trimmed = url.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

/// The relay this node syncs with, if any.
///
/// `LASTDB_RELAY_URL` names a relay explicitly, for a reference relay or a
/// deploy under test. Otherwise a node with saved cloud credentials uses the
/// Exemem API it is connected to, which serves `/relay/v1`. A node with
/// neither has no relay, and the sync loop idles without erroring.
///
/// A home that has a relay captures every write of its personal database into
/// the spool of accepted writes, which is the queue the sync loop drains. A home
/// with no relay captures nothing, so no spool fills that nothing sends.
#[must_use]
pub fn relay_base_url(home: &Path) -> Option<String> {
    if let Some(url) = explicit_relay_url() {
        return Some(url);
    }
    match load_saved_credentials(home) {
        Ok(Some(saved)) => {
            let url = saved.api_url.trim().trim_end_matches('/');
            (!url.is_empty()).then(|| url.to_owned())
        }
        _ => None,
    }
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

impl SavedCredentials {
    pub fn new(api_url: impl Into<String>, api_key: String) -> Self {
        Self {
            api_url: api_url.into(),
            api_key: Zeroizing::new(api_key),
            connection: None,
        }
    }

    pub fn parse(bytes: &[u8]) -> serde_json::Result<Self> {
        serde_json::from_slice(bytes)
    }

    pub fn to_json(&self) -> serde_json::Result<Zeroizing<Vec<u8>>> {
        serde_json::to_vec(self).map(Zeroizing::new)
    }

    #[must_use]
    pub(crate) fn environment(&self) -> Option<Environment> {
        environment_for_exemem_api_url(&self.api_url).or_else(|| {
            self.connection
                .as_ref()
                .and_then(|c| match c.environment.as_str() {
                    "dev" => Some(Environment::Dev),
                    "prod" => Some(Environment::Prod),
                    _ => None,
                })
        })
    }
}

pub fn saved_cloud_environment(home: &Path) -> Result<Option<Environment>, String> {
    Ok(load_saved_credentials(home)?.and_then(|creds| creds.environment()))
}

fn write_key<S: Serializer>(key: &Zeroizing<String>, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(key.as_str())
}

fn read_key<'de, D: Deserializer<'de>>(d: D) -> Result<Zeroizing<String>, D::Error> {
    String::deserialize(d).map(Zeroizing::new)
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

pub fn load_saved_credentials(home: &Path) -> Result<Option<SavedCredentials>, String> {
    const PRIV_ERR: &str = "native backup credentials must be a private owner file";
    const LIM_ERR: &str = "native backup credential file exceeds its byte limit";
    let Some(raw) = read_private_named(
        &home.join(CLOUD_CREDENTIALS_FILE),
        CONTROL_JSON_MAX_BYTES,
        PRIV_ERR,
        LIM_ERR,
    )?
    else {
        return Ok(None);
    };
    let creds =
        SavedCredentials::parse(&raw).map_err(|_| "invalid native backup credential file")?;
    (!creds.api_key.is_empty())
        .then_some(Some(creds))
        .ok_or_else(|| "native backup credential is empty".into())
}

#[must_use]
pub fn is_valid_developer_key(key: &str) -> bool {
    key.strip_prefix("em_")
        .is_some_and(|h| h.len() == 48 && h.bytes().all(|b| b.is_ascii_hexdigit()))
}

#[must_use]
pub fn developer_key_prefix(key: &str) -> Option<String> {
    let hex_part = key.strip_prefix("em_")?;
    if hex_part.len() < 10 || !hex_part.bytes().take(10).all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let short: String = hex_part.chars().take(10).collect();
    Some(format!("em_{short}..."))
}

#[must_use]
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
    let trimmed = std::str::from_utf8(&raw)
        .map_err(|_| "saved developer API key file is not UTF-8")?
        .trim();
    is_valid_developer_key(trimmed)
        .then(|| Some(Zeroizing::new(trimmed.to_owned())))
        .ok_or_else(|| "saved developer API key file has an invalid key format".into())
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
