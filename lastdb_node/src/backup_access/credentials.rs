use cloud_backup_client::AuthClient;
use serde::Deserialize;
use std::fs::{self, OpenOptions};
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::Path;

#[derive(Deserialize)]
struct Credentials {
    api_url: String,
    api_key: String,
}

/// Shared credential read; the caller chooses its separately versioned protocol.
pub(crate) fn auth_for_recovery(home: &Path) -> Result<AuthClient, String> {
    let path = crate::cloud::cloud_credentials_path(home);
    if !present(&path)? {
        return Err("native backup needs existing account credentials".into());
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
    let mut bytes = zeroize::Zeroizing::new(Vec::new());
    file.take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read native backup credentials".to_owned())?;
    if bytes.len() as u64 > limit {
        return Err("native backup credential file exceeds its byte limit".into());
    }
    let credentials: Credentials = serde_json::from_slice(&bytes)
        .map_err(|_| "invalid native backup credential file".to_owned())?;
    if credentials.api_key.is_empty() {
        return Err("native backup credential is empty".into());
    }
    let auth = AuthClient::new(credentials.api_url, credentials.api_key);
    Ok(auth)
}

fn present(path: &Path) -> Result<bool, String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(true),
        Ok(_) => Err("native backup credential path must be a regular file".into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err("cannot inspect native backup credential path".into()),
    }
}
