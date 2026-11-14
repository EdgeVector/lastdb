//! Account identity, saved cloud/developer credentials, and explicit backup enrollment.
mod dev_key;
mod enrollment;

pub use dev_key::{
    developer_key_prefix, is_valid_developer_key, is_valid_key_prefix, load_saved_developer_key,
    remove_saved_developer_key_if_prefix, save_developer_key, DEVELOPER_API_KEY_FILE,
};
pub(crate) use enrollment::load_home_keypair;
pub use enrollment::{
    account_from_phrase, client_from_home, load_home_account_id,
    pin_schema_service_environment_from_home, register_native_device, seed_from_phrase,
};

use cloud_backup_protocol::{limits::REQUEST_JSON_MAX_BYTES, AccountId, Id};
use rustix::fs::{self as rfs, Mode, OFlags};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fs::{self, File, Metadata, OpenOptions};
use std::io::{self, Read};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;
use zeroize::Zeroizing;

pub const CLOUD_CREDENTIALS_FILE: &str = "cloud-credentials.json";
#[doc(hidden)]
pub const FIRST_BACKUP_INTENT_FILE: &str = ".native-first-backup";
#[doc(hidden)]
pub const FIRST_BACKUP_INTENT: &[u8] = b"snapshot-v1\n";

pub(crate) fn first_backup_intent(home: &Path) -> bool {
    read_private_named(
        &home.join(FIRST_BACKUP_INTENT_FILE),
        FIRST_BACKUP_INTENT.len() as u64,
        "invalid first backup intent file",
        "invalid first backup intent length",
    )
    .is_ok_and(|bytes| bytes.as_deref() == Some(FIRST_BACKUP_INTENT))
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

pub(super) fn read_private_named(
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

pub(super) fn open_owned_home_dir(home: &Path) -> Result<File, String> {
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
