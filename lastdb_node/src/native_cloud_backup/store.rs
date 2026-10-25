//! Bounded read-only access to retired job records. No old job is created or changed.
use super::{BackupError, Result};
use cloud_backup_protocol::Id;
use serde::de::DeserializeOwned;
use std::fs::{self, OpenOptions};
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

const MAX_JSON: u64 = 256 * 1024;
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Active {
    request_id: Id,
}

pub(super) struct Store;
impl Store {
    pub fn base(home: &Path) -> PathBuf {
        home.join("data/native-cloud-backup-v1")
    }
    pub fn active(home: &Path) -> Result<Option<Id>> {
        Ok(read::<Active>(&Self::base(home).join("active.json"))?.map(|value| value.request_id))
    }
}

pub(super) fn read<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    match fs::symlink_metadata(path) {
        Ok(meta)
            if meta.is_file()
                && meta.uid() == unsafe { libc::geteuid() }
                && meta.mode() & 0o077 == 0
                && meta.len() <= MAX_JSON => {}
        Ok(_) => return Err(BackupError::Invalid),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let mut bytes = Vec::new();
    file.take(MAX_JSON + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_JSON {
        return Err(BackupError::Limit);
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| BackupError::Invalid)
}
