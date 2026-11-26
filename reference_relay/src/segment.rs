//! Log segments (`docs/designs/relay-protocol.md`, "Log segments").
//!
//! A segment is a run of consecutive signed entries of one device stream whose bytes live in one
//! blob. The relay stores one item for it, in the stream directory at `{last_seq}.json`, the same
//! place an entry at `last_seq` would have. The item holds `{"segment": {...}}` and nothing of
//! the entries. A read returns the item in its place in the stream, with the presigned URL of the
//! blob, and the reader checks the entries it fetches.

use std::path::PathBuf;

use relay_protocol::SegmentAppend;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::error::{ApiError, ApiResult};
use crate::store::{atomic_write, is_hex64, EntryKey, Store};

/// Check the fields of the `segment` of a `log.append`, each with its own error field.
pub(crate) fn check(segment: &SegmentAppend) -> ApiResult<()> {
    if !is_hex64(&segment.device_id) {
        return Err(ApiError::invalid("segment.device_id"));
    }
    if segment.first_seq == 0 {
        return Err(ApiError::invalid("segment.first_seq"));
    }
    if segment.last_seq < segment.first_seq {
        return Err(ApiError::invalid("segment.last_seq"));
    }
    if !is_hex64(&segment.last_hash) {
        return Err(ApiError::invalid("segment.last_hash"));
    }
    if !is_hex64(&segment.sha256) {
        return Err(ApiError::invalid("segment.sha256"));
    }
    if segment.bytes == 0 {
        return Err(ApiError::invalid("segment.bytes"));
    }
    Ok(())
}

/// The `segment` of a stored segment item (storage, not the wire): the appended segment and its
/// writer. A read answers it as a `relay_protocol::SegmentRef` with the presigned URL of its blob.
#[derive(Serialize, Deserialize)]
pub(crate) struct StoredSegment {
    pub(crate) node_key: String,
    pub(crate) device_id: String,
    pub(crate) first_seq: u64,
    pub(crate) last_seq: u64,
    pub(crate) last_hash: String,
    pub(crate) sha256: String,
    pub(crate) bytes: u64,
}

impl StoredSegment {
    fn new(node_key: &str, segment: &SegmentAppend) -> Self {
        Self {
            node_key: node_key.to_owned(),
            device_id: segment.device_id.clone(),
            first_seq: segment.first_seq,
            last_seq: segment.last_seq,
            last_hash: segment.last_hash.clone(),
            sha256: segment.sha256.clone(),
            bytes: segment.bytes,
        }
    }
}

/// The span and hash of a stored segment item, or `None` for an entry.
pub(crate) struct Span {
    pub(crate) first_seq: u64,
    pub(crate) last_seq: u64,
    pub(crate) last_hash: String,
}

/// The span of a stored item when it is a segment.
pub(crate) fn span(item: &Value) -> ApiResult<Option<Span>> {
    let Some(segment) = item.get("segment") else {
        return Ok(None);
    };
    let num = |name: &str| segment.get(name).and_then(Value::as_u64);
    let (Some(first_seq), Some(last_seq), Some(last_hash)) = (
        num("first_seq"),
        num("last_seq"),
        segment.get("last_hash").and_then(Value::as_str),
    ) else {
        return Err(ApiError::invalid("segment"));
    };
    Ok(Some(Span {
        first_seq,
        last_seq,
        last_hash: last_hash.to_owned(),
    }))
}

/// The first and last sequence number a stored item covers: its own for an entry.
pub(crate) fn covers(key: u64, item: &Value) -> ApiResult<(u64, u64)> {
    Ok(match span(item)? {
        Some(span) => (span.first_seq, span.last_seq),
        None => (key, key),
    })
}

/// The hash of the last entry a stored item covers.
pub(crate) fn last_hash(item: &Value) -> ApiResult<String> {
    match span(item)? {
        Some(span) => Ok(span.last_hash),
        None => crate::heads::hash_of(item),
    }
}

/// What an append of a segment did.
pub(crate) enum Appended {
    Stored,
    Duplicate,
}

impl Store {
    /// Store a segment of `node_key` (the caller, already checked to be the publisher). The
    /// caller holds the locks that a `blob.drop` takes, so the blob cannot go between the check
    /// and the write.
    pub(crate) async fn append_segment(
        &self,
        database_id: &str,
        node_key: &str,
        segment: &SegmentAppend,
    ) -> ApiResult<Appended> {
        let device = segment.device_id.as_str();
        let head = self
            .heads
            .head_seq(database_id, node_key, device)
            .unwrap_or(0);
        let floor = self.heads.floor(database_id, node_key, device);
        if head >= segment.last_seq || floor >= segment.last_seq {
            return Ok(Appended::Duplicate);
        }
        let path = self.item_path(database_id, node_key, device, segment.last_seq);
        // An entry stored beyond the gap that the segment would fill: the items would overlap.
        let dir = path.parent().unwrap_or(&path);
        let overlaps = crate::store_read::stored_keys(dir, segment.first_seq)
            .await?
            .first()
            .is_some_and(|key| *key <= segment.last_seq);
        if head + 1 != segment.first_seq || floor >= segment.first_seq || overlaps {
            return Err(ApiError::conflict("segment.first_seq"));
        }
        let (bytes, _) = self
            .blob_meta(database_id, &segment.sha256)
            .await
            .map_err(|_| ApiError::not_found("segment.sha256"))?;
        if bytes != segment.bytes {
            return Err(ApiError::not_found("segment.sha256"));
        }
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|_| ApiError::invalid("data_dir"))?;
        }
        let item = json!({ "segment": StoredSegment::new(node_key, segment) });
        let item = serde_json::to_vec(&item).map_err(|_| ApiError::invalid("segment"))?;
        atomic_write(&path, &item).await?;
        let span = Span {
            first_seq: segment.first_seq,
            last_seq: segment.last_seq,
            last_hash: segment.last_hash.clone(),
        };
        self.heads
            .record_segment(database_id, node_key, device, span);
        Ok(Appended::Stored)
    }

    /// The file of the item stored at `seq`.
    pub(crate) fn item_path(
        &self,
        database_id: &str,
        node_key: &str,
        device: &str,
        seq: u64,
    ) -> PathBuf {
        self.entry_path(&EntryKey {
            database_id: database_id.to_owned(),
            node_key: node_key.to_owned(),
            device_id: device.to_owned(),
            writer_seq: seq,
        })
    }
}
