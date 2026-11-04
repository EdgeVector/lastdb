//! Bounded complete-request receipts; eviction makes a lookup a cache miss.

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

use super::super::{
    RETRY_CACHE_BUCKETS, RETRY_CACHE_BUCKET_BYTES, RETRY_CACHE_DIGEST_BYTES, RETRY_CACHE_ENTRIES,
    RETRY_CACHE_ENTRY_BYTES, RETRY_CACHE_PREFIX,
};
use super::EncodedMetadata;
use crate::storage::error::StorageResult;
use crate::storage::StorageError;

use lastdb_model::record_replacement::RetryReceipt;
type DigestBytes = [u8; RETRY_CACHE_DIGEST_BYTES];
type Entry = [u8; RETRY_CACHE_ENTRY_BYTES];
type Bucket = Vec<Entry>;

pub(super) fn status() -> serde_json::Value {
    serde_json::json!({
        "receipt_cap": RETRY_CACHE_BUCKETS * RETRY_CACHE_ENTRIES,
        "bucket_cap": RETRY_CACHE_BUCKETS,
        "bucket_byte_cap": RETRY_CACHE_BUCKET_BYTES,
    })
}

fn coordinate(key: &str) -> (String, DigestBytes) {
    let digest = Sha256::digest(key.as_bytes());
    (
        format!("{RETRY_CACHE_PREFIX}{:02x}", digest[0]),
        digest.into(),
    )
}

fn decode(bytes: Option<&[u8]>) -> StorageResult<Bucket> {
    let Some(bytes) = bytes else {
        return Ok(Vec::new());
    };
    if bytes.len() > RETRY_CACHE_BUCKET_BYTES
        || !bytes.len().is_multiple_of(RETRY_CACHE_ENTRY_BYTES)
        || bytes.len() / RETRY_CACHE_ENTRY_BYTES > RETRY_CACHE_ENTRIES
    {
        return Err(StorageError::SerializationError(
            "invalid retry-cache bucket".into(),
        ));
    }
    Ok(bytes
        .chunks_exact(RETRY_CACHE_ENTRY_BYTES)
        .map(|entry| entry.try_into().expect("complete retry entry"))
        .collect())
}

pub(super) struct Lookup {
    coordinates: Vec<(String, DigestBytes)>,
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
                    .find(|entry| entry[..RETRY_CACHE_DIGEST_BYTES] == fingerprint)
                    .map(|entry| RetryReceipt {
                        intent_hash: hex(
                            &entry[RETRY_CACHE_DIGEST_BYTES..2 * RETRY_CACHE_DIGEST_BYTES]
                        ),
                        revision: hex(&entry[2 * RETRY_CACHE_DIGEST_BYTES..]),
                    })
            })
            .collect())
    }
}

pub(super) struct Batch {
    grouped: BTreeMap<String, Bucket>,
}

impl Batch {
    pub(super) fn new(entries: Vec<(String, RetryReceipt)>) -> StorageResult<Self> {
        let mut grouped: BTreeMap<String, Bucket> = BTreeMap::new();
        for (key, id) in entries {
            let (bucket, fingerprint) = coordinate(&key);
            let mut entry = [0; RETRY_CACHE_ENTRY_BYTES];
            entry[..RETRY_CACHE_DIGEST_BYTES].copy_from_slice(&fingerprint);
            entry[RETRY_CACHE_DIGEST_BYTES..2 * RETRY_CACHE_DIGEST_BYTES]
                .copy_from_slice(&parse_digest(&id.intent_hash)?);
            entry[2 * RETRY_CACHE_DIGEST_BYTES..].copy_from_slice(&parse_digest(&id.revision)?);
            grouped.entry(bucket).or_default().push(entry);
        }
        Ok(Self { grouped })
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
                for entry in added {
                    bucket.retain(|old| {
                        old[..RETRY_CACHE_DIGEST_BYTES] != entry[..RETRY_CACHE_DIGEST_BYTES]
                    });
                    bucket.push(entry);
                    if bucket.len()
                        > RETRY_CACHE_ENTRIES
                            .min(RETRY_CACHE_BUCKET_BYTES / RETRY_CACHE_ENTRY_BYTES)
                    {
                        bucket.remove(0);
                    }
                }
                Ok((key.into_bytes(), bucket.into_iter().flatten().collect()))
            })
            .collect::<StorageResult<Vec<_>>>()?;
        Ok(EncodedMetadata::Retry(rows))
    }
}

fn parse_digest(value: &str) -> StorageResult<DigestBytes> {
    let invalid = || StorageError::SerializationError("invalid retry receipt digest".into());
    if value.len() != 2 * RETRY_CACHE_DIGEST_BYTES {
        return Err(invalid());
    }
    let nibble = |byte| match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(invalid()),
    };
    let mut digest = [0; RETRY_CACHE_DIGEST_BYTES];
    for (out, pair) in digest.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        *out = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    Ok(digest)
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    bytes
        .iter()
        .flat_map(|byte| {
            [
                HEX[(byte >> 4) as usize] as char,
                HEX[(byte & 15) as usize] as char,
            ]
        })
        .collect()
}
