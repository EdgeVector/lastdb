//! A bounded retry cache. A hit still requires matching current record values.
use crate::storage::error::StorageResult;
use crate::storage::{KvStore, StorageError, TypedKvStore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const PREFIX: &str = "retry-cache:v1:";
const DRAINED: &[u8] = b"retry-cache:v1:legacy-drained";
const CURSOR: &[u8] = b"retry-cache:v1:legacy-cursor";
const LEGACY_END: &[u8] = "\u{10ffff}".as_bytes();
const BUCKETS: usize = 256;
const ENTRIES: usize = 32;
const BUCKET_BYTES: usize = 8 * 1024;
const ID_BYTES: usize = 128;
const LEGACY_PAGE: usize = BUCKETS * 2;
type Bucket = Vec<(String, String)>;

#[derive(Default, Deserialize, Serialize)]
struct Progress {
    after: Vec<u8>,
    retired: u64,
    examined: u64,
}

pub(super) async fn status(store: &TypedKvStore<dyn KvStore>) -> StorageResult<serde_json::Value> {
    let rows = store
        .inner()
        .get_many(vec![DRAINED.to_vec(), CURSOR.to_vec()])
        .await?;
    if rows.len() != 2 {
        return Err(StorageError::BackendError(
            "retry-cache status is incomplete".into(),
        ));
    }
    let progress = rows[0]
        .as_ref()
        .or(rows[1].as_ref())
        .and_then(|bytes| serde_json::from_slice::<Progress>(bytes).ok());
    Ok(serde_json::json!({
        "receipt_cap": BUCKETS * ENTRIES,
        "bucket_cap": BUCKETS,
        "bucket_byte_cap": BUCKET_BYTES,
        "legacy_complete": rows[0].is_some(),
        "legacy_pending": rows[1].is_some(),
        "legacy_retired": progress.as_ref().map(|progress| progress.retired),
        "legacy_examined": progress.as_ref().map(|progress| progress.examined),
    }))
}

fn coordinate(key: &str) -> (String, String) {
    let digest = Sha256::digest(key.as_bytes());
    (format!("{PREFIX}{:02x}", digest[0]), format!("{digest:x}"))
}

fn decode(bytes: Option<&[u8]>) -> Bucket {
    bytes
        .filter(|bytes| bytes.len() <= BUCKET_BYTES)
        .and_then(|bytes| serde_json::from_slice::<Bucket>(bytes).ok())
        .filter(|entries| {
            entries.len() <= ENTRIES
                && entries
                    .iter()
                    .all(|(hash, id)| hash.len() == 64 && id.len() <= ID_BYTES)
        })
        .unwrap_or_default()
}

pub(super) async fn get_many(
    store: &TypedKvStore<dyn KvStore>,
    keys: &[String],
) -> StorageResult<Vec<Option<String>>> {
    let coordinates: Vec<_> = keys.iter().map(|key| coordinate(key)).collect();
    let mut buckets: BTreeMap<String, Bucket> = coordinates
        .iter()
        .map(|(key, _)| (key.clone(), Vec::new()))
        .collect();
    let selected = buckets.keys().map(|key| key.as_bytes().to_vec()).collect();
    let bytes = store.inner().get_many(selected).await?;
    if bytes.len() != buckets.len() {
        return Err(StorageError::BackendError(
            "retry-cache read is incomplete".into(),
        ));
    }
    for (bucket, bytes) in buckets.values_mut().zip(bytes) {
        *bucket = decode(bytes.as_deref());
    }
    Ok(coordinates
        .into_iter()
        .map(|(key, fingerprint)| {
            buckets[&key]
                .iter()
                .find_map(|(hash, id)| (hash == &fingerprint).then(|| id.clone()))
        })
        .collect())
}

/// Native requests serialize these stages with their complete record write.
/// Other stores may lose a cache entry to a concurrent bucket replacement;
/// that is a safe miss, never permission to skip the current-value check.
pub(super) async fn put(
    store: &TypedKvStore<dyn KvStore>,
    entries: Vec<(String, String)>,
) -> StorageResult<()> {
    let mut grouped: BTreeMap<String, Bucket> = BTreeMap::new();
    for (key, id) in entries {
        if id.len() <= ID_BYTES {
            let (bucket, fingerprint) = coordinate(&key);
            grouped.entry(bucket).or_default().push((fingerprint, id));
        }
    }
    if grouped.is_empty() {
        return Ok(());
    }
    retire_legacy(store).await?;
    let keys: Vec<_> = grouped.keys().map(|key| key.as_bytes().to_vec()).collect();
    let old = store.inner().get_many(keys).await?;
    if old.len() != grouped.len() {
        return Err(StorageError::BackendError(
            "retry-cache bucket read is incomplete".into(),
        ));
    }
    let rows = grouped
        .into_iter()
        .zip(old)
        .map(|((key, added), bytes)| {
            let mut bucket = decode(bytes.as_deref());
            for (fingerprint, id) in added {
                bucket.retain(|(hash, _)| *hash != fingerprint);
                bucket.push((fingerprint, id));
                if bucket.len() > ENTRIES {
                    bucket.remove(0);
                }
            }
            let bytes = encode(bucket)?;
            Ok((key.into_bytes(), bytes))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    store.inner().batch_put(rows).await
}

fn encode(mut bucket: Bucket) -> StorageResult<Vec<u8>> {
    loop {
        let bytes = serde_json::to_vec(&bucket)
            .map_err(|error| StorageError::SerializationError(error.to_string()))?;
        if bytes.len() <= BUCKET_BYTES {
            return Ok(bytes);
        }
        bucket.remove(0);
    }
}

/// Retire the old unbounded cache in finite metadata pages. The marker and
/// erasures share the native request's private snapshot. No product row is read.
async fn retire_legacy(store: &TypedKvStore<dyn KvStore>) -> StorageResult<()> {
    if store.inner().get(DRAINED).await?.is_some() {
        return Ok(());
    }
    let mut progress = match store.inner().get(CURSOR).await? {
        // Earlier PR revisions saved only the next inclusive key, ending in NUL.
        // Preserve that position if an operator upgrades an unfinished cleanup.
        Some(bytes) if bytes.last() == Some(&0) => Progress {
            after: bytes,
            ..Progress::default()
        },
        Some(bytes) => serde_json::from_slice::<Progress>(&bytes)
            .map_err(|error| StorageError::SerializationError(error.to_string()))?,
        None => Progress::default(),
    };
    let page = store
        .inner()
        .scan_range_yielding(&progress.after, LEGACY_END, LEGACY_PAGE, 0)
        .await?;
    progress.examined = progress.examined.saturating_add(page.rows.len() as u64);
    let old = page
        .rows
        .into_iter()
        .filter_map(|(key, _)| is_legacy(&key).then_some(key))
        .collect::<Vec<_>>();
    if !old.is_empty() {
        progress.retired = progress.retired.saturating_add(old.len() as u64);
        store.inner().batch_delete(old).await?;
    }
    if let Some(mut last) = page.resume {
        // The API includes start. Appending NUL resumes strictly after the
        // last examined key, including a deleted key named by a budget stop.
        // Unknown families advance the cursor even though we preserve them.
        last.push(0);
        progress.after = last;
        store
            .inner()
            .put(CURSOR, progress_bytes(&progress)?)
            .await?;
    } else {
        progress.after.clear();
        store
            .inner()
            .put(DRAINED, progress_bytes(&progress)?)
            .await?;
        store.inner().delete(CURSOR).await?;
    }
    Ok(())
}

fn progress_bytes(progress: &Progress) -> StorageResult<Vec<u8>> {
    serde_json::to_vec(progress)
        .map_err(|error| StorageError::SerializationError(error.to_string()))
}

fn is_legacy(key: &[u8]) -> bool {
    std::str::from_utf8(key)
        .ok()
        .and_then(|key| key.rsplit_once("idem:"))
        .is_some_and(|(scope, hash)| {
            (scope.is_empty() || scope.ends_with(':'))
                && hash.len() == 64
                && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
}
