//! Rollback-safe signed registry list caching and supported app API range merge.

use std::path::PathBuf;

use app_identity_crypto::VerifyingKey;
use serde::{Deserialize, Serialize};

use super::{
    str_err, validate_channel, IndexLocation, InterfaceList, ABSENT, NO_LIST_PREFIX,
    PROD_INDEX_BASE_URL,
};

#[derive(Serialize, Deserialize)]
struct StoredList {
    payload_sha256: String,
    bytes: Vec<u8>,
    signature: Vec<u8>,
}

fn cache_file(location: &IndexLocation, channel: &str, api: u32) -> Result<PathBuf, String> {
    let state = std::env::var_os("XDG_STATE_HOME").map(PathBuf::from);
    let state =
        state.or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/state")));
    let mut root = state
        .ok_or("HOME or XDG_STATE_HOME is required for the registry cache")?
        .join("lastdb-app-registry");
    if !matches!(location, IndexLocation::Url { base, .. } if base == PROD_INDEX_BASE_URL) {
        let key = fold_db::hex::sha256_hex(location.fetch_address(channel, api).as_bytes());
        root = root.join("sources").join(key);
    }
    Ok(root.join(channel).join("api").join(format!("{api}.json")))
}

async fn fetch_list(
    location: &IndexLocation,
    channel: &str,
    api: u32,
    trust: &VerifyingKey,
) -> Result<InterfaceList, String> {
    validate_channel(channel)?;
    let download = match location.fetch_interface(channel, api).await {
        Ok((bytes, signature)) => {
            let list = InterfaceList::load(&bytes, &signature, trust, channel, api)?;
            let payload_sha256 = fold_db::hex::sha256_hex(&bytes);
            Ok((
                StoredList {
                    payload_sha256,
                    bytes,
                    signature,
                },
                list,
            ))
        }
        Err(e) => Err(e),
    };
    let path = cache_file(location, channel, api)?;
    let parent = path.parent().ok_or("registry cache parent missing")?;
    std::fs::create_dir_all(parent).map_err(str_err)?;
    let mut opts = std::fs::OpenOptions::new();
    opts.create(true).truncate(false).read(true).write(true);
    let lock = opts.open(path.with_extension("lock")).map_err(str_err)?;
    lock.lock().map_err(str_err)?;
    let cached = if path.exists() {
        let raw = std::fs::read(&path).map_err(str_err)?;
        let v: StoredList =
            serde_json::from_slice(&raw).map_err(|e| format!("invalid registry cache: {e}"))?;
        if v.payload_sha256 != fold_db::hex::sha256_hex(&v.bytes) {
            return Err("registry cache digest mismatch".into());
        }
        Some((
            v.payload_sha256,
            InterfaceList::load(&v.bytes, &v.signature, trust, channel, api)?,
        ))
    } else {
        None
    };
    let accepted = match download {
        Ok((val, list)) => {
            if let Some((sha, old)) = cached.filter(|(_, old)| list.seq <= old.seq) {
                if list.seq < old.seq || val.payload_sha256 != sha {
                    eprintln!(
                        "note: registry sequence rejected; retaining stored seq {}",
                        old.seq
                    );
                }
                old
            } else {
                let tmp = path.with_extension(format!("{}.tmp", std::process::id()));
                std::fs::write(&tmp, serde_json::to_vec(&val).map_err(str_err)?)
                    .map_err(str_err)?;
                std::fs::File::open(&tmp)
                    .and_then(|f| f.sync_all())
                    .map_err(str_err)?;
                std::fs::rename(&tmp, &path).map_err(str_err)?;
                std::fs::File::open(parent)
                    .and_then(|f| f.sync_all())
                    .map_err(str_err)?;
                list
            }
        }
        Err(err) => {
            let Some((_, old)) = cached else {
                return Err(err);
            };
            eprintln!(
                "note: registry download rejected; using stored seq {}: {err}",
                old.seq
            );
            old
        }
    };
    Ok(accepted)
}

/// The apps a node can install on one channel: for each app, its entry from the
/// highest list between the node's minimum supported app API version and its own.
pub async fn fetch_supported(
    location: &IndexLocation,
    channel: &str,
    (min_api, api): (u32, u32),
    trust: &VerifyingKey,
) -> Result<InterfaceList, String> {
    let mut merged: Option<InterfaceList> = None;
    let mut newest: Option<String> = None;
    for level in (min_api.min(api)..=api).rev() {
        let list = match fetch_list(location, channel, level, trust).await {
            Ok(list) => list,
            Err(e) if e.starts_with(ABSENT) => continue,
            Err(e) => return Err(e),
        };
        newest = newer_generated_at(newest, &list.generated_at);
        if let Some(top) = merged.as_mut() {
            for entry in list.apps {
                if !top.apps.iter().any(|e| e.app_id == entry.app_id) {
                    top.apps.push(entry);
                }
            }
        } else {
            merged = Some(list);
        }
    }
    if let Some(generated_at) = newest.as_deref() {
        note_stale_list(generated_at);
    }
    merged.ok_or_else(|| {
        format!("{NO_LIST_PREFIX}{channel} list for lastdb app API {min_api} to {api}")
    })
}

fn newer_generated_at(current: Option<String>, candidate: &str) -> Option<String> {
    let Ok(at) = chrono::DateTime::parse_from_rfc3339(candidate) else {
        return current;
    };
    let keep = current
        .as_deref()
        .is_some_and(|prev| chrono::DateTime::parse_from_rfc3339(prev).is_ok_and(|old| old >= at));
    if keep {
        current
    } else {
        Some(candidate.to_owned())
    }
}

fn note_stale_list(generated_at: &str) {
    let stale = chrono::DateTime::parse_from_rfc3339(generated_at)
        .is_ok_and(|stamp| chrono::Utc::now().signed_duration_since(stamp).num_days() >= 7);
    if stale {
        eprintln!("note: registry list was generated at {generated_at} (at least 7 days ago)");
    }
}
