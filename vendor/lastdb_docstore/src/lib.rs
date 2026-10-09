//! Storage v2 document store — three collections + CAS blobs (design:
//! `fold/docs/designs/storage-v2-three-collection-document-store.md`).
//!
//! P1 scope: the `DocumentStore` trait and a hash-sharded segment-file
//! backend for the `atoms` / `tips` (and small `schemas`) collections with
//! per-shard compaction that deletes old segment files so disk shrinks.
//! Body encryption and compression live behind [`BodyCodec`].
//! Default write durability is [`Durability::Grouped`] (batched fsync);
//! use [`Durability::Strict`] for per-put `sync_data`.

mod codec;
mod envelope;
mod error;
mod laststore_backend;
mod segment;

pub use codec::{AesGcmCodec, COMPRESS_LEVEL, COMPRESS_MAGIC, COMPRESS_THRESHOLD};
pub use envelope::{decrypt_envelope, encrypt_envelope};
pub use error::{Error, Result};
pub use laststore_backend::LastStoreEngine;
pub use segment::{Durability, SegmentStore, SegmentStoreOptions};

/// Canonical collection names from the storage-v2 design.
pub mod collections {
    pub const SCHEMAS: &str = "schemas";
    pub const ATOMS: &str = "atoms";
    pub const TIPS: &str = "tips";
}

/// Stats returned by a shard compaction.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CompactStats {
    pub collection: String,
    pub shard: u16,
    pub live_docs: u64,
    pub segments_removed: u64,
    pub bytes_before: u64,
    pub bytes_after: u64,
}

impl CompactStats {
    pub fn merge(&mut self, other: &CompactStats) {
        self.live_docs += other.live_docs;
        self.segments_removed += other.segments_removed;
        self.bytes_before += other.bytes_before;
        self.bytes_after += other.bytes_after;
    }
}

/// One operation inside a multi-document transaction. Bodies are logical
/// (pre-codec) bytes, same as [`DocumentStore::put`].
#[derive(Debug, Clone)]
pub enum TxnOp {
    Put {
        collection: String,
        id: String,
        body: Vec<u8>,
    },
    Delete {
        collection: String,
        id: String,
    },
}

impl TxnOp {
    pub fn put(collection: &str, id: &str, body: impl Into<Vec<u8>>) -> Self {
        Self::Put {
            collection: collection.to_string(),
            id: id.to_string(),
            body: body.into(),
        }
    }
    pub fn delete(collection: &str, id: &str) -> Self {
        Self::Delete {
            collection: collection.to_string(),
            id: id.to_string(),
        }
    }
}

/// Storage v2 document API (internal store trait from the design doc).
///
/// Bodies are opaque bytes: logical JSON before the codec, ciphertext on
/// disk once P2 plugs an encrypting [`BodyCodec`] in.
pub trait DocumentStore {
    fn get(&self, collection: &str, id: &str) -> Result<Option<Vec<u8>>>;
    fn put(&self, collection: &str, id: &str, body: &[u8]) -> Result<()>;
    /// Bulk put (import path). Default: sequential `put`. Engines may batch fsync.
    fn put_many(&self, collection: &str, items: Vec<(String, Vec<u8>)>) -> Result<()> {
        for (id, body) in items {
            self.put(collection, &id, &body)?;
        }
        Ok(())
    }
    fn delete(&self, collection: &str, id: &str) -> Result<()>;
    /// True if id exists (default: `get(...).is_some()`).
    fn exists(&self, collection: &str, id: &str) -> Result<bool> {
        Ok(self.get(collection, id)?.is_some())
    }
    /// Ids (sorted) and bodies of every live document whose id starts with
    /// `prefix`. An empty prefix lists the whole collection.
    fn list_prefix(&self, collection: &str, prefix: &str) -> Result<Vec<(String, Vec<u8>)>>;
    /// Ids only under `prefix` (default: map `list_prefix` and drop bodies).
    fn list_prefix_keys(&self, collection: &str, prefix: &str) -> Result<Vec<String>> {
        Ok(self
            .list_prefix(collection, prefix)?
            .into_iter()
            .map(|(id, _)| id)
            .collect())
    }
    /// At most `limit` docs under `prefix`. `after` is an exclusive keyset cursor.
    /// `limit == 0` → empty. Default: full prefix then truncate (override for lazy).
    fn list_prefix_paged(
        &self,
        collection: &str,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut rows = self.list_prefix(collection, prefix)?;
        if let Some(a) = after {
            rows.retain(|(id, _)| id.as_str() > a);
        }
        rows.truncate(limit);
        Ok(rows)
    }
    /// Keys-only paged prefix walk.
    fn list_prefix_keys_paged(
        &self,
        collection: &str,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<String>> {
        Ok(self
            .list_prefix_paged(collection, prefix, after, limit)?
            .into_iter()
            .map(|(id, _)| id)
            .collect())
    }
    /// Half-open id range `[start, end)` with bodies. Empty if `start >= end`.
    fn list_range(
        &self,
        collection: &str,
        start: &str,
        end: &str,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        self.list_range_paged(collection, start, end, usize::MAX)
    }
    /// Paged half-open range. Default: full prefix of common start/end then filter.
    fn list_range_paged(
        &self,
        collection: &str,
        start: &str,
        end: &str,
        limit: usize,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        if limit == 0 || start >= end {
            return Ok(Vec::new());
        }
        let common: String = start
            .chars()
            .zip(end.chars())
            .take_while(|(a, b)| a == b)
            .map(|(a, _)| a)
            .collect();
        let mut rows = self.list_prefix(collection, &common)?;
        rows.retain(|(id, _)| id.as_str() >= start && id.as_str() < end);
        rows.truncate(limit);
        Ok(rows)
    }
    /// Keys-only half-open range page.
    fn list_range_keys_paged(
        &self,
        collection: &str,
        start: &str,
        end: &str,
        limit: usize,
    ) -> Result<Vec<String>> {
        Ok(self
            .list_range_paged(collection, start, end, limit)?
            .into_iter()
            .map(|(id, _)| id)
            .collect())
    }
    /// Apply all ops, then durable as a batch (WAL and/or group-commit flush).
    fn transaction(&self, ops: Vec<TxnOp>) -> Result<()>;
    /// Store `bytes` in the CAS; returns `sha256:<hex>` of the **plaintext**
    /// bytes (blob identity is stable across re-encryption). Idempotent.
    fn put_blob(&self, bytes: &[u8]) -> Result<String>;
    /// Fetch a CAS blob by `sha256:<hex>` ref. `None` if absent.
    fn get_blob(&self, blob_ref: &str) -> Result<Option<Vec<u8>>>;
    /// Rewrite one shard to a single sealed segment holding only live docs,
    /// then unlink the old segment files (space returns to the OS).
    fn compact_shard(&self, collection: &str, shard: u16) -> Result<CompactStats>;
    /// Compact every shard of a collection; merged stats.
    fn compact(&self, collection: &str) -> Result<CompactStats>;
}

/// Body encode/decode hook. P1 ships [`PlainCodec`]; P2 replaces it with the
/// AES-256-GCM at-rest envelope without touching segment layout.
pub trait BodyCodec: Send + Sync {
    /// Manifest label; opening a store checks it matches.
    fn name(&self) -> &'static str;
    fn encode(&self, plain: &[u8]) -> Result<Vec<u8>>;
    fn decode(&self, stored: &[u8]) -> Result<Vec<u8>>;
}

/// Identity codec — bodies stored as given.
#[derive(Debug, Default, Clone, Copy)]
pub struct PlainCodec;

impl BodyCodec for PlainCodec {
    fn name(&self) -> &'static str {
        "plain"
    }
    fn encode(&self, plain: &[u8]) -> Result<Vec<u8>> {
        Ok(plain.to_vec())
    }
    fn decode(&self, stored: &[u8]) -> Result<Vec<u8>> {
        Ok(stored.to_vec())
    }
}
