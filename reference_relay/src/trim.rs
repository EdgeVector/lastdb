//! `log/trim`: drop the entries that a snapshot covers on a personal database.
//!
//! Serializes trim calls through `AppState::trim_lock`, reads and verifies the named `snapshot`
//! entry, checks its manifest blob, rejects shared databases or regressed `through` positions,
//! re-verifies that no grants were added concurrently, and applies the per-stream trim plans.
//! The answer always carries what the call dropped: a failure after entries went answers 200
//! with `done: false` and the counts so far.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::Json;
use mutation_log::{verify_entry_signature, DatabaseId, NodeKey, SignedEntry, SnapshotBody};
use relay_protocol::{LogTrim, LogTrimAnswer};

use crate::error::{ApiError, ApiResult};
use crate::gate;
use crate::grant_index::GrantIndex;
use crate::handlers::{authenticate, AppState};
use crate::store::{read_json, require_hex64, Store};
use crate::trim_plan::{apply, permits_entry, plan, TrimScope};

pub(crate) async fn log_trim(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<LogTrimAnswer>> {
    let (verified, req) = authenticate::<LogTrim>(&headers, &body)?;
    require_hex64(&req.database_id, "database_id")?;
    let db = gate::parse_id(&req.database_id, "database_id")?;
    require_hex64(&req.device_id, "device_id")?;
    if req.writer_seq == 0 {
        return Err(ApiError::invalid("writer_seq"));
    }
    let _trim_guard = state.trim_lock.lock().await;
    let grants = state.grants.lock().await;
    require_publisher(&grants, &db, &verified.node_key)?;
    let publisher = verified.node_key;
    let writer = publisher.to_hex();
    let scope = TrimScope {
        store: &state.store,
        database_id: &req.database_id,
        writer: &writer,
    };
    let snap_floor = scope.floor(&req.device_id);
    if req.writer_seq <= snap_floor {
        let retained = scope
            .stream_dir(&req.device_id)
            .join(format!("{}.json", snap_floor + 1));
        let entry = read_json(&retained)
            .await
            .map_err(|_| ApiError::invalid("kind"))?;
        if entry.get("segment").is_some() {
            return Err(ApiError::forbidden("namespace"));
        }
        if !permits_entry(true, &entry) {
            return Err(ApiError::invalid("kind"));
        }
        return Ok(Json(LogTrimAnswer {
            done: true,
            entries_deleted: 0,
            released_bytes: 0,
        }));
    }
    let named = SnapshotName {
        database_id: &req.database_id,
        device_id: &req.device_id,
        writer_seq: req.writer_seq,
    };
    let snapshot = snapshot_entry(scope.store, &named, &writer).await?;
    verify_manifest_blob(scope.store, &req.database_id, &snapshot).await?;
    check_through_monotonic(&scope, publisher, &req.device_id, &snapshot)?;
    let older = req.writer_seq - 1;
    let mut plans = Vec::with_capacity(snapshot.through.len() + 1);
    for pos in &snapshot.through {
        let id = pos.stream_id(publisher);
        let writer = id.node_key.to_hex();
        let stream = TrimScope {
            writer: &writer,
            ..scope
        };
        let device = id.device_id.to_hex();
        let hash = pos.entry_hash.to_hex();
        plans.push(plan(&stream, &device, pos.writer_seq, Some(&hash), false).await?);
    }
    plans.push(plan(&scope, &req.device_id, older, None, true).await?);

    // What a call dropped is always in its answer. A failure after entries went answers with
    // the counts so far and `done: false`, and the caller calls again: a repeat is safe.
    let mut dropped = (0, 0);
    let mut done = true;
    for each in &plans {
        match apply(&scope, each, &mut dropped).await {
            Ok(()) => {}
            Err(error) if dropped == (0, 0) => return Err(error),
            Err(_) => {
                done = false;
                break;
            }
        }
    }
    drop(grants);
    Ok(Json(LogTrimAnswer {
        done,
        entries_deleted: dropped.0,
        released_bytes: dropped.1,
    }))
}

/// Only the verified birth publisher may retire stored entries or blobs.
pub(crate) fn require_publisher(
    grants: &GrantIndex,
    db: &DatabaseId,
    caller: &NodeKey,
) -> ApiResult<()> {
    let birth = grants
        .birth(db)
        .ok_or_else(|| ApiError::forbidden("publisher"))?;
    if birth.owner != *caller {
        return Err(ApiError::forbidden("publisher"));
    }
    Ok(())
}

async fn verify_manifest_blob(
    store: &Store,
    database_id: &str,
    snapshot: &SnapshotBody,
) -> ApiResult<()> {
    let hex = snapshot.manifest_sha256.to_hex();
    let (bytes, _) = store
        .blob_meta(database_id, &hex)
        .await
        .map_err(|_| ApiError::not_found("manifest_sha256"))?;
    if bytes == snapshot.manifest_bytes {
        Ok(())
    } else {
        Err(ApiError::invalid("manifest_bytes"))
    }
}

fn check_through_monotonic(
    scope: &TrimScope<'_>,
    publisher: NodeKey,
    snapshot_device: &str,
    snapshot: &SnapshotBody,
) -> ApiResult<()> {
    let mut through = std::collections::BTreeMap::new();
    for pos in &snapshot.through {
        let id = pos.stream_id(publisher);
        let (writer, device) = (id.node_key.to_hex(), id.device_id.to_hex());
        if pos.writer_seq == 0
            || (id.node_key == publisher && device == snapshot_device)
            || through.insert((writer, device), pos.writer_seq).is_some()
        {
            return Err(ApiError::invalid("through"));
        }
    }
    for (writer, device) in scope.store.heads.stored_streams(scope.database_id) {
        if writer == scope.writer && device == snapshot_device {
            continue;
        }
        let floor = scope.store.heads.floor(scope.database_id, &writer, &device);
        if floor > through.get(&(writer, device)).copied().unwrap_or(0) {
            return Err(ApiError::invalid("through"));
        }
    }
    Ok(())
}

/// One entry of a snapshot stream, by its place.
pub(crate) struct SnapshotName<'a> {
    pub(crate) database_id: &'a str,
    pub(crate) device_id: &'a str,
    pub(crate) writer_seq: u64,
}

/// The stored `snapshot` entry that `req` names, after a check of its signature, its kind and
/// its writer.
pub(crate) async fn snapshot_entry(
    store: &Store,
    req: &SnapshotName<'_>,
    writer: &str,
) -> ApiResult<SnapshotBody> {
    let (entries, _) = store
        .read_entries(req.database_id, writer, req.device_id, req.writer_seq, 1)
        .await?;
    let value = entries
        .into_iter()
        .next()
        .ok_or_else(|| ApiError::not_found("writer_seq"))?;
    if value.get("segment").is_some() {
        return Err(ApiError::forbidden("namespace"));
    }
    let entry: SignedEntry =
        serde_json::from_value(value).map_err(|_| ApiError::invalid("entries"))?;
    verify_entry_signature(&entry).map_err(|_| ApiError::invalid("signature"))?;
    let SignedEntry::Snapshot { header, body, .. } = entry else {
        return Err(ApiError::invalid("kind"));
    };
    if header.writer_seq != req.writer_seq
        || header.node_key.to_hex() != writer
        || header.database_id.to_hex() != req.database_id
        || header.device_id.to_hex() != req.device_id
    {
        return Err(ApiError::invalid("kind"));
    }
    Ok(body)
}
