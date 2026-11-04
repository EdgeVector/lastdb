//! Bounded complete-request receipts; eviction makes a lookup a cache miss.

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

use super::super::{
    RETRY_CACHE_BUCKETS, RETRY_CACHE_BUCKET_BYTES, RETRY_CACHE_ENTRIES, RETRY_CACHE_PREFIX,
};
use super::EncodedMetadata;
use crate::storage::error::StorageResult;
use crate::storage::StorageError;

use lastdb_model::record_replacement::RetryReceipt;
type Bucket = Vec<(String, RetryReceipt)>;

pub(super) fn status() -> serde_json::Value {
    serde_json::json!({
        "receipt_cap": RETRY_CACHE_BUCKETS * RETRY_CACHE_ENTRIES,
        "bucket_cap": RETRY_CACHE_BUCKETS,
        "bucket_byte_cap": RETRY_CACHE_BUCKET_BYTES,
    })
}

fn coordinate(key: &str) -> (String, String) {
    let digest = Sha256::digest(key.as_bytes());
    (
        format!("{RETRY_CACHE_PREFIX}{:02x}", digest[0]),
        format!("{digest:x}"),
    )
}

fn decode(bytes: Option<&[u8]>) -> StorageResult<Bucket> {
    let Some(bytes) = bytes else {
        return Ok(Vec::new());
    };
    let entries = (bytes.len() <= RETRY_CACHE_BUCKET_BYTES)
        .then(|| serde_json::from_slice::<Bucket>(bytes).ok())
        .flatten()
        .filter(|entries| {
            entries.len() <= RETRY_CACHE_ENTRIES
                && entries
                    .iter()
                    .all(|(hash, id)| hash.len() == 64 && valid(id))
        });
    entries.ok_or_else(|| StorageError::SerializationError("invalid retry-cache bucket".into()))
}

pub(super) struct Lookup {
    coordinates: Vec<(String, String)>,
    buckets: BTreeMap<String, Bucket>,
}

impl Lookup {
    pub(super) fn new(keys: &[String]) -> Self {
        let coordinates: Vec<_> = keys.iter().map(|key| coordinate(key)).collect();
        let buckets = coordinates
            .iter()
            .map(|(key, _)| (key.clone(), Vec::new()))
            .collect();
        Self {
            coordinates,
            buckets,
        }
    }

    pub(super) fn keys(&self) -> Vec<Vec<u8>> {
        self.buckets
            .keys()
            .map(|key| key.as_bytes().to_vec())
            .collect()
    }

    pub(super) fn decode(
        mut self,
        bytes: Vec<Option<Vec<u8>>>,
    ) -> StorageResult<Vec<Option<RetryReceipt>>> {
        if bytes.len() != self.buckets.len() {
            return Err(StorageError::BackendError(
                "retry-cache read is incomplete".into(),
            ));
        }
        for (bucket, bytes) in self.buckets.values_mut().zip(bytes) {
            *bucket = decode(bytes.as_deref())?;
        }
        Ok(self
            .coordinates
            .into_iter()
            .map(|(key, fingerprint)| {
                self.buckets[&key]
                    .iter()
                    .find_map(|(hash, id)| (hash == &fingerprint).then(|| id.clone()))
            })
            .collect())
    }
}

pub(super) struct Batch {
    grouped: BTreeMap<String, Bucket>,
}

impl Batch {
    pub(super) fn new(entries: Vec<(String, RetryReceipt)>) -> Self {
        let mut grouped: BTreeMap<String, Bucket> = BTreeMap::new();
        for (key, id) in entries {
            // Invalid receipts are a safe miss; no unbounded value enters a bucket.
            if valid(&id) {
                let (bucket, fingerprint) = coordinate(&key);
                grouped.entry(bucket).or_default().push((fingerprint, id));
            }
        }
        Self { grouped }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.grouped.is_empty()
    }

    pub(super) fn keys(&self) -> Vec<Vec<u8>> {
        self.grouped
            .keys()
            .map(|key| key.as_bytes().to_vec())
            .collect()
    }

    pub(super) fn encode(self, old: Vec<Option<Vec<u8>>>) -> StorageResult<EncodedMetadata> {
        if old.len() != self.grouped.len() {
            return Err(StorageError::BackendError(
                "retry-cache bucket read is incomplete".into(),
            ));
        }
        let rows = self
            .grouped
            .into_iter()
            .zip(old)
            .map(|((key, added), bytes)| {
                let mut bucket = decode(bytes.as_deref())?;
                for (fingerprint, id) in added {
                    bucket.retain(|(hash, _)| *hash != fingerprint);
                    bucket.push((fingerprint, id));
                    if bucket.len() > RETRY_CACHE_ENTRIES {
                        bucket.remove(0);
                    }
                }
                Ok((key.into_bytes(), encode(bucket)?))
            })
            .collect::<StorageResult<Vec<_>>>()?;
        Ok(EncodedMetadata::Retry(rows))
    }
}

fn encode(mut bucket: Bucket) -> StorageResult<Vec<u8>> {
    loop {
        let bytes = serde_json::to_vec(&bucket)
            .map_err(|error| StorageError::SerializationError(error.to_string()))?;
        if bytes.len() <= RETRY_CACHE_BUCKET_BYTES {
            return Ok(bytes);
        }
        bucket.remove(0);
    }
}

fn valid(receipt: &RetryReceipt) -> bool {
    [&receipt.intent_hash, &receipt.revision]
        .iter()
        .all(|value| {
            value.len() == 64
                && value
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
}
