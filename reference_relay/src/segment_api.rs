//! The routes' handling of log segments: `log.append` of a segment, and what `log.read` adds to
//! the segments it returns (`docs/designs/relay-protocol.md`, "Log segments").

use std::collections::BTreeMap;

use axum::Json;
use mutation_log::{DatabaseId, NodeKey};
use relay_protocol::{LogAppendAnswer, ReadItem, SegmentAppend, SegmentRef};
use serde::Deserialize;
use serde_json::Value;

use crate::error::{ApiError, ApiResult};
use crate::handlers::AppState;
use crate::presign;
use crate::segment::{self, Appended, StoredSegment};
use crate::store::now_ms;

/// `log.append` of a segment: only the publisher, in order, naming a committed blob.
pub(crate) async fn append_segment(
    state: &AppState,
    db: &DatabaseId,
    caller: &NodeKey,
    segment: &SegmentAppend,
) -> ApiResult<Json<LogAppendAnswer>> {
    segment::check(segment)?;
    // The locks that `blob.drop` holds for its whole call, taken in its order, so the blob stays
    // until the item is stored, and the item is stored before the drop compares the heads.
    let _blob_guard = state.blob_lock.lock().await;
    let grants = state.grants.lock().await;
    let birth = grants
        .birth(db)
        .ok_or_else(|| ApiError::forbidden("publisher"))?;
    if birth.owner != *caller {
        return Err(ApiError::forbidden("publisher"));
    }
    let appended = state
        .store
        .append_segment(&db.to_hex(), &caller.to_hex(), segment)
        .await?;
    drop(grants);
    Ok(Json(LogAppendAnswer {
        stored: segment.count(),
        duplicate: matches!(appended, Appended::Duplicate),
    }))
}

/// The stored items of a read as the answer carries them: an entry as stored, and a segment with
/// the presigned URL of its blob, as `blob.get` answers it.
pub(crate) fn read_items(
    state: &AppState,
    database_id: &str,
    stored: Vec<Value>,
) -> ApiResult<Vec<ReadItem>> {
    stored
        .into_iter()
        .map(|item| read_item(state, database_id, item))
        .collect()
}

fn read_item(state: &AppState, database_id: &str, item: Value) -> ApiResult<ReadItem> {
    let Some(stored) = item.get("segment") else {
        return Ok(ReadItem::Entry(item));
    };
    let stored = StoredSegment::deserialize(stored).map_err(|_| ApiError::invalid("segment"))?;
    let (url, expires_at_ms) = presign::make_get_url(
        &state.public_base,
        &state.secret,
        database_id,
        &stored.sha256,
        now_ms(),
    );
    Ok(ReadItem::Segment {
        segment: SegmentRef {
            node_key: stored.node_key,
            device_id: stored.device_id,
            first_seq: stored.first_seq,
            last_seq: stored.last_seq,
            last_hash: stored.last_hash,
            sha256: stored.sha256,
            bytes: stored.bytes,
            url,
            headers: BTreeMap::new(),
            expires_at_ms,
        },
    })
}

/// The entries the items of a read hold: one for an entry, every entry of a segment.
pub(crate) fn entries_held(items: &[ReadItem]) -> usize {
    items
        .iter()
        .map(|item| match item {
            ReadItem::Segment { segment } => (segment.last_seq - segment.first_seq + 1) as usize,
            ReadItem::Entry(_) => 1,
        })
        .sum()
}
