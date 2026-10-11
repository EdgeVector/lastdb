//! Private durable intents and receipts.

use super::*;

pub(super) fn save_intent(host: &Host, intent: &Intent) -> Result<(), String> {
    let directory = host
        .home
        .join("cloud-purge-resync/jobs")
        .join(&intent.operation_id);
    atomic(&directory.join("intent.json"), intent)?;
    atomic(&host.home.join("cloud-purge-resync/active.json"), intent)
}

pub(crate) fn read<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, String> {
    if fs::metadata(path).map_err(|error| error.to_string())?.len() > 64 * 1024 * 1024 {
        return Err("cloud purge receipt is too large".into());
    }
    serde_json::from_slice(&fs::read(path).map_err(|error| error.to_string())?)
        .map_err(|error| error.to_string())
}

pub(crate) fn atomic(path: &Path, value: &impl Serialize) -> Result<(), String> {
    use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
    let parent = path.parent().ok_or("cloud purge receipt lacks a parent")?;
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(parent)
        .map_err(|error| error.to_string())?;
    let temporary = parent.join(format!(".intent-{}", uuid::Uuid::new_v4()));
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&temporary)
        .map_err(|error| error.to_string())?;
    file.write_all(&serde_json::to_vec(value).map_err(|error| error.to_string())?)
        .map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    fs::rename(&temporary, path).map_err(|error| error.to_string())?;
    File::open(parent)
        .and_then(|file| file.sync_all())
        .map_err(|error| error.to_string())
}
