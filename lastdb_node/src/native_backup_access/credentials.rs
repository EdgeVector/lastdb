use super::enabled;
use fold_db::sync::auth::native_backup::NativeBackupClient;
use fold_db::sync::auth::{AuthClient, SyncAuth};
use serde::Deserialize;
use std::fs::{self, OpenOptions};
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::sync::Arc;

#[derive(Deserialize)]
struct Credentials {
    api_url: String,
    api_key: String,
}

/// Load only an existing explicit credential file. Construction makes no request.
/// Callers bind context to their independently derived account before any effect.
pub fn client_from_home(home: &Path) -> Result<NativeBackupClient, String> {
    enabled()?;
    client_for_recovery(home)
}

/// Exact saved-job recovery only, after the owner validates its durable identity.
/// This does not authorize a new capture, upload, generation, or stream.
pub(crate) fn client_for_recovery(home: &Path) -> Result<NativeBackupClient, String> {
    let active = home.join("cloud_sync.json");
    let paused = home.join("cloud_sync.json.paused");
    let active_present = present(&active)?;
    let paused_present = present(&paused)?;
    let path = match (active_present, paused_present) {
        (true, false) => active,
        (false, true) => paused,
        (false, false) => return Err("native backup needs existing account credentials".into()),
        (true, true) => return Err("native backup credential selection is ambiguous".into()),
    };
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
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
    let http = fold_db::sync::build_shared_http_client()
        .map_err(|_| "cannot construct native backup transport".to_owned())?;
    let auth = AuthClient::new(
        Arc::new(http),
        credentials.api_url,
        SyncAuth::ApiKey(credentials.api_key),
    )
    .without_db_auto_claim();
    NativeBackupClient::from_auth(auth).map_err(|error| error.to_string())
}

fn present(path: &Path) -> Result<bool, String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(true),
        Ok(_) => Err("native backup credential path must be a regular file".into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err("cannot inspect native backup credential path".into()),
    }
}
