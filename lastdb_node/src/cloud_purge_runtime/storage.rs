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

fn cancel_path(host: &Host, id: &str) -> Result<PathBuf, String> {
    Ok(crate::cloud_purge_job::directory(&host.home, id)?.join("cancel-request.json"))
}

pub(super) fn request_cancel(host: &Host, intent: &Intent) -> Result<(), String> {
    atomic(
        &cancel_path(host, &intent.operation_id)?,
        &serde_json::json!({"operation_id":intent.operation_id,"account":intent.account}),
    )
}

pub(super) fn cancel_requested(host: &Host, intent: &Intent) -> Result<bool, String> {
    let path = cancel_path(host, &intent.operation_id)?;
    if !path.exists() {
        return Ok(false);
    }
    let request: Value = read(&path)?;
    if request.get("operation_id").and_then(Value::as_str) != Some(&intent.operation_id)
        || request.get("account")
            != Some(&serde_json::to_value(&intent.account).map_err(|e| e.to_string())?)
    {
        return Err("the durable cancellation request belongs to another operation".into());
    }
    Ok(true)
}

pub(super) fn clear_cancel_for_resume(host: &Host, intent: &Intent) -> Result<(), String> {
    if cancel_requested(host, intent)? {
        let path = cancel_path(host, &intent.operation_id)?;
        fs::remove_file(&path).map_err(|error| error.to_string())?;
        File::open(path.parent().ok_or("cancel request has no parent")?)
            .and_then(|file| file.sync_all())
            .map_err(|error| error.to_string())?;
    }
    Ok(())
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
