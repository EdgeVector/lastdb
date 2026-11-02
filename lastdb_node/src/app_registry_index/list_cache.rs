//! Persist verified lists and reject sequence rollback across processes.

use app_identity_crypto::VerifyingKey;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use super::interface_list::InterfaceList;
use super::IndexLocation;

#[derive(Serialize, Deserialize)]
struct StoredList {
    payload_sha256: String,
    bytes: Vec<u8>,
    signature: Vec<u8>,
}

fn cache_path(
    location: &IndexLocation,
    channel: &str,
    api_version: u32,
) -> Result<PathBuf, String> {
    let state = std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/state")))
        .ok_or("HOME or XDG_STATE_HOME is required for the registry cache")?;
    let root = state.join("lastdb-app-registry");
    let root = if matches!(location, IndexLocation::Url(base) if base == super::DEFAULT_INDEX_BASE_URL)
    {
        root
    } else {
        root.join("sources").join(fold_db::hex::sha256_hex(
            location.describe_interface(channel, api_version).as_bytes(),
        ))
    };
    Ok(root
        .join(channel)
        .join("api")
        .join(format!("{api_version}.json")))
}

fn stored(
    path: &Path,
    trust: &VerifyingKey,
    channel: &str,
    api: u32,
) -> Result<Option<(StoredList, InterfaceList)>, String> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(path).map_err(|e| e.to_string())?;
    let value: StoredList =
        serde_json::from_slice(&bytes).map_err(|e| format!("invalid registry cache: {e}"))?;
    if value.payload_sha256 != fold_db::hex::sha256_hex(&value.bytes) {
        return Err("registry cache digest mismatch".into());
    }
    let list = InterfaceList::load(&value.bytes, &value.signature, trust, channel, api)?;
    Ok(Some((value, list)))
}

/// Fetch a signed interface list, keeping the prior verified copy on any failed download.
///
/// # Errors
/// A failed download with no verified cached list, or an inability to persist rollback state.
pub async fn fetch_list(
    location: &IndexLocation,
    channel: &str,
    api: u32,
    trust: &VerifyingKey,
) -> Result<InterfaceList, String> {
    super::validate_channel(channel)?;
    let download = location
        .fetch_interface(channel, api)
        .await
        .and_then(|(bytes, signature)| {
            let list = InterfaceList::load(&bytes, &signature, trust, channel, api)?;
            Ok((
                StoredList {
                    payload_sha256: fold_db::hex::sha256_hex(&bytes),
                    bytes,
                    signature,
                },
                list,
            ))
        });
    let path = cache_path(location, channel, api)?;
    let parent = path.parent().ok_or("registry cache parent missing")?;
    std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path.with_extension("lock"))
        .map_err(|e| e.to_string())?;
    lock.lock().map_err(|e| e.to_string())?;
    let cached = stored(&path, trust, channel, api)?;
    let accepted = accept(download, cached, &path)?;
    if let Ok(generated) = chrono::DateTime::parse_from_rfc3339(&accepted.generated_at) {
        if chrono::Utc::now()
            .signed_duration_since(generated)
            .num_days()
            >= 7
        {
            eprintln!(
                "note: registry list was generated at {} (at least 7 days ago)",
                accepted.generated_at
            );
        }
    }
    Ok(accepted)
}

fn accept(
    download: Result<(StoredList, InterfaceList), String>,
    cached: Option<(StoredList, InterfaceList)>,
    path: &Path,
) -> Result<InterfaceList, String> {
    let (value, list) = match download {
        Ok(value) => value,
        Err(error) => {
            return cached.map_or_else(
                || Err(error.clone()),
                |(_, prior)| {
                    eprintln!(
                        "note: registry download rejected; using stored seq {}: {error}",
                        prior.seq
                    );
                    Ok(prior)
                },
            )
        }
    };
    if let Some((prior, old)) = cached {
        if list.seq < old.seq
            || (list.seq == old.seq && value.payload_sha256 != prior.payload_sha256)
        {
            eprintln!(
                "note: registry sequence rejected; retaining stored seq {}",
                old.seq
            );
            return Ok(old);
        }
        if list.seq == old.seq {
            return Ok(old);
        }
    }
    let temp = path.with_extension(format!("{}.tmp", std::process::id()));
    let bytes = serde_json::to_vec(&value).map_err(|e| e.to_string())?;
    std::fs::write(&temp, bytes).map_err(|e| e.to_string())?;
    let file = std::fs::File::open(&temp).map_err(|e| e.to_string())?;
    file.sync_all().map_err(|e| e.to_string())?;
    std::fs::rename(&temp, path).map_err(|e| e.to_string())?;
    std::fs::File::open(path.parent().ok_or("cache parent missing")?)
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())?;
    Ok(list)
}
