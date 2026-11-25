//! The routes' handling of log segments: `log.append` of a segment, and what `log.read` adds to
//! the segments it returns (`docs/designs/relay-protocol.md`, "Log segments").

use axum::Json;
use mutation_log::{DatabaseId, NodeKey};
use serde_json::{json, Value};

use crate::error::{ApiError, ApiResult};
use crate::handlers::AppState;
use crate::presign;
use crate::segment::{span, Appended, SegmentBody};
use crate::store::now_ms;

/// `log.append` of a segment: only the publisher, in order, naming a committed blob.
pub(crate) async fn append_segment(
    state: &AppState,
    db: &DatabaseId,
    caller: &NodeKey,
    segment: &SegmentBody,
) -> ApiResult<Json<Value>> {
    segment.check()?;
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
    let mut out = json!({ "stored": segment.count() });
    if matches!(appended, Appended::Duplicate) {
        out["duplicate"] = Value::Bool(true);
    }
    Ok(Json(out))
}

/// Give each segment of a read the presigned URL of its blob, as `blob.get` answers it.
pub(crate) fn present_segments(state: &AppState, database_id: &str, items: &mut [Value]) {
    for item in items {
        let Some(segment) = item.get_mut("segment").and_then(Value::as_object_mut) else {
            continue;
        };
        let Some(sha256) = segment.get("sha256").and_then(Value::as_str) else {
            continue;
        };
        let (url, expires_at_ms) = presign::make_get_url(
            &state.public_base,
            &state.secret,
            database_id,
            sha256,
            now_ms(),
        );
        segment.insert("url".into(), Value::String(url));
        segment.insert("headers".into(), json!({}));
        segment.insert("expires_at_ms".into(), json!(expires_at_ms));
    }
}

/// The entries the items of a read hold: one for an entry, every entry of a segment.
pub(crate) fn entries_held(items: &[Value]) -> usize {
    items
        .iter()
        .map(|item| match span(item) {
            Ok(Some(span)) => (span.last_seq - span.first_seq + 1) as usize,
            _ => 1,
        })
        .sum()
}
