//! The saved cloud credentials (`cloud_sync.json.paused`): one shape, one
//! parse, and one private owner-file loader. Readers that already hold a
//! descriptor-checked byte read parse those bytes here.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fs::{self, OpenOptions};
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::Path;
use zeroize::Zeroizing;

/// `{"api_url","api_key"}`. The key zeroizes on drop. Do not derive `Debug`:
/// `Zeroizing` prints its contents.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SavedCredentials {
    pub api_url: String,
    #[serde(serialize_with = "write_key", deserialize_with = "read_key")]
    pub api_key: Zeroizing<String>,
}

/// The saved bytes are not the credential JSON. Carries no file content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidCredentials;

impl SavedCredentials {
    pub fn new(api_url: impl Into<String>, api_key: String) -> Self {
        Self {
            api_url: api_url.into(),
            api_key: Zeroizing::new(api_key),
        }
    }

    /// The serde error can quote a value, so the error drops it.
    pub fn parse(bytes: &[u8]) -> Result<Self, InvalidCredentials> {
        serde_json::from_slice(bytes).map_err(|_| InvalidCredentials)
    }

    /// Compact JSON: the form that enrollment and native registration write.
    pub fn to_json(&self) -> serde_json::Result<Zeroizing<Vec<u8>>> {
        serde_json::to_vec(self).map(Zeroizing::new)
    }

    /// Pretty JSON: the form that recovery homes write.
    pub fn to_json_pretty(&self) -> serde_json::Result<Zeroizing<Vec<u8>>> {
        serde_json::to_vec_pretty(self).map(Zeroizing::new)
    }
}

fn write_key<S: Serializer>(key: &Zeroizing<String>, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(key.as_str())
}

fn read_key<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Zeroizing<String>, D::Error> {
    String::deserialize(deserializer).map(Zeroizing::new)
}

/// Load the home's saved credentials from a private owner file: a regular
/// file opened without following a symlink, owned by this user, with no
/// group or other mode bits and one link, inside the request byte limit,
/// with a non-empty key. `Ok(None)` when the home has no saved credentials.
pub fn load_saved_credentials(home: &Path) -> Result<Option<SavedCredentials>, String> {
    let path = super::cloud_credentials_path(home);
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
