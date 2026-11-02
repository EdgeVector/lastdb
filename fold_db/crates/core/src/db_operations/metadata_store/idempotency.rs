//! A bounded retry cache. A hit still requires matching current record values.
use crate::storage::error::StorageResult;
use crate::storage::{KvStore, StorageError, TypedKvStore};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const PREFIX: &str = "retry-cache:";
const BUCKETS: usize = 256;
const ENTRIES: usize = 32;
const BUCKET_BYTES: usize = 8 * 1024;
const ID_BYTES: usize = 128;
type Bucket = Vec<(String, String)>;

/// The caps of the cache. The status names no key or value.
pub(super) fn status() -> serde_json::Value {
    serde_json::json!({
        "receipt_cap": BUCKETS * ENTRIES,
        "bucket_cap": BUCKETS,
        "bucket_byte_cap": BUCKET_BYTES,
    })
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
