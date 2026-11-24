//! `log/trim`: drop the entries that a snapshot covers on a personal database.
//!
//! Serializes trim calls through `AppState::trim_lock`, reads and verifies the named `snapshot`
//! entry, checks its manifest blob, rejects shared databases or regressed `through` positions,
//! re-verifies that no grants were added concurrently, and applies the per-stream trim plans.
//! The answer always carries what the call dropped: a failure after entries went answers 200
//! with `done: false` and the counts so far.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::Json;
use mutation_log::{verify_entry_signature, DatabaseId, NodeKey, SignedEntry, SnapshotBody};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::auth;
use crate::error::{ApiError, ApiResult};
use crate::gate;
use crate::grant_index::GrantIndex;
use crate::handlers::{parse_body, AppState, SMALL_BODY_BYTES};
use crate::store::{read_json, require_hex64, Store};
use crate::trim_plan::{apply, ensure_personal_logs, permits_entry, plan, TrimScope};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LogTrimBody {
    database_id: String,
    device_id: String,
    writer_seq: u64,
}

pub(crate) async fn log_trim(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let verified = auth::verify_request(&headers, "/relay/v1/log/trim", &body)?;
    let req: LogTrimBody = parse_body(&body, SMALL_BODY_BYTES)?;
    verified.check_database(&req.database_id)?;
    require_hex64(&req.database_id, "database_id")?;
    let db = gate::parse_id(&req.database_id, "database_id")?;
    require_hex64(&req.device_id, "device_id")?;
    if req.writer_seq == 0 {
        return Err(ApiError::invalid("writer_seq"));
    }
    let _trim_guard = state.trim_lock.lock().await;
    check_personal_owner(&state, &db, &verified.node_key).await?;
    let writer = verified.node_key.to_hex();
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
        if !permits_entry(true, &entry) {
            return Err(ApiError::invalid("kind"));
        }
        ensure_personal_logs(&scope, &req.device_id, snap_floor, &BTreeMap::new()).await?;
        return Ok(Json(
            json!({ "done": true, "entries_deleted": 0, "released_bytes": 0 }),
        ));
    }
    let named = SnapshotName {
        database_id: &req.database_id,
        device_id: &req.device_id,
        writer_seq: req.writer_seq,
    };
    let snapshot = snapshot_entry(scope.store, &named, &writer).await?;
    verify_manifest_blob(scope.store, &req.database_id, &snapshot).await?;
    let through_map = check_through_monotonic(&scope, &req.device_id, &snapshot)?;
    let older = req.writer_seq - 1;
    ensure_personal_logs(&scope, &req.device_id, older, &through_map).await?;

    let mut plans = Vec::with_capacity(snapshot.through.len() + 1);
    for pos in &snapshot.through {
        let (dev, hash) = (pos.device_id.to_hex(), pos.entry_hash.to_hex());
        plans.push(plan(&scope, &dev, pos.writer_seq, Some(&hash), false).await?);
    }
    plans.push(plan(&scope, &req.device_id, older, None, true).await?);
    check_personal_owner(&state, &db, &verified.node_key).await?;

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
    Ok(Json(
        json!({ "done": done, "entries_deleted": dropped.0, "released_bytes": dropped.1 }),
    ))
}

async fn check_personal_owner(
    state: &AppState,
    db: &DatabaseId,
    caller: &NodeKey,
) -> ApiResult<()> {
    let grants = state.grants.lock().await;
    personal_owner(&grants, db, caller)
}

/// The caller is the publisher of `db`, and `db` has no grant: it is a personal database.
pub(crate) fn personal_owner(
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
    if grants.any_read(db) || grants.has_grants(db) {
        return Err(ApiError::forbidden("grant"));
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
    snapshot_device: &str,
    snapshot: &SnapshotBody,
) -> ApiResult<BTreeMap<String, u64>> {
    let mut through_map = BTreeMap::new();
    for pos in &snapshot.through {
        let dev = pos.device_id.to_hex();
        if dev == snapshot_device || through_map.insert(dev, pos.writer_seq).is_some() {
            return Err(ApiError::invalid("through"));
        }
    }
    for listed in scope.store.list_streams(scope.database_id, scope.writer) {
        let dev = &listed.device_id;
        let floor = scope.floor(dev);
        if dev != snapshot_device && floor > 0 && through_map.get(dev).copied().unwrap_or(0) < floor
        {
            return Err(ApiError::invalid("through"));
        }
    }
    Ok(through_map)
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
    let entry: SignedEntry =
        serde_json::from_value(value).map_err(|_| ApiError::invalid("entries"))?;
    verify_entry_signature(&entry).map_err(|_| ApiError::invalid("signature"))?;
    let SignedEntry::Snapshot { header, body, .. } = entry else {
        return Err(ApiError::invalid("kind"));
    };
    if header.writer_seq != req.writer_seq || header.node_key.to_hex() != writer {
        return Err(ApiError::invalid("kind"));
    }
    Ok(body)
}
