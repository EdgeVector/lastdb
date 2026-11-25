//! `blob/list` and `blob/drop`: what the relay holds of a database, and the drop of what the
//! publisher says nothing names.
//!
//! The relay reads no manifest and no file. It lists the committed blobs of a database, and it
//! drops only the digests the publisher names, and only while the streams the publisher saw are
//! as it saw them. A drop is one step for the appends: `log/append` holds the grant index for its
//! whole call, and so does the drop while it compares the heads and removes the blobs, so an
//! entry is appended before the drop (a head moved, nothing is dropped) or after it.

use std::collections::BTreeSet;
use std::io::ErrorKind;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::fs;

use crate::auth;
use crate::error::{ApiError, ApiResult};
use crate::gate;
use crate::handlers::{parse_body, AppState};
use crate::store::{is_hex64, require_hex64, Store};
use crate::trim::{personal_owner, snapshot_entry, SnapshotName};

/// Blobs in one page of `blob/list`.
const LIST_PAGE: usize = 1_000;
/// The most stream heads one `blob/drop` may name as conditions.
const MAX_HEADS: usize = 32;
/// The most digests one `blob/drop` may name.
const MAX_DROPS: usize = 25;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BlobListBody {
    database_id: String,
    #[serde(default)]
    after: Option<String>,
}

/// `blob/list`: the committed blobs of a database, ascending by digest, for any node that may
/// read the database.
pub(crate) async fn blob_list(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let verified = auth::verify_request(&headers, "/relay/v1/blob/list", &body)?;
    let req: BlobListBody = parse_body(&body)?;
    verified.check_database(&req.database_id)?;
    require_hex64(&req.database_id, "database_id")?;
    let db = gate::parse_id(&req.database_id, "database_id")?;
    state.check_read(&db, &verified.node_key).await?;
    if let Some(after) = req.after.as_deref() {
        require_hex64(after, "after")?;
    }
    let mut listed = {
        let _blob_guard = state.blob_lock.lock().await;
        state.store.blobs_of(&req.database_id).await?
    };
    if let Some(after) = req.after.as_deref() {
        listed.retain(|(sha256, _)| sha256.as_str() > after);
    }
    let more = listed.len() > LIST_PAGE;
    listed.truncate(LIST_PAGE);
    let blobs: Vec<Value> = listed
        .iter()
        .map(|(sha256, bytes)| json!({ "sha256": sha256, "bytes": bytes }))
        .collect();
    let mut out = json!({ "blobs": blobs });
    if let Some((last, _)) = listed.last().filter(|_| more) {
        out["next"] = Value::String(last.clone());
    }
    Ok(Json(out))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StreamAt {
    device_id: String,
    writer_seq: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BlobDropBody {
    database_id: String,
    snapshot: StreamAt,
    heads: Vec<StreamAt>,
    sha256: Vec<String>,
}

/// The shape of a drop: at most [`MAX_HEADS`] heads, each stream once, none of them the snapshot
/// stream, each at an entry; 1 to [`MAX_DROPS`] digests, each once.
fn check_drop_body(req: &BlobDropBody) -> ApiResult<()> {
    require_hex64(&req.database_id, "database_id")?;
    require_hex64(&req.snapshot.device_id, "snapshot")?;
    if req.snapshot.writer_seq == 0 {
        return Err(ApiError::invalid("snapshot"));
    }
    let mut devices = BTreeSet::new();
    let heads_fit = req.heads.len() <= MAX_HEADS
        && req.heads.iter().all(|head| {
            is_hex64(&head.device_id)
                && head.writer_seq > 0
                && head.device_id != req.snapshot.device_id
                && devices.insert(head.device_id.as_str())
        });
    if !heads_fit {
        return Err(ApiError::invalid("heads"));
    }
    let mut digests = BTreeSet::new();
    let digests_fit = !req.sha256.is_empty()
        && req.sha256.len() <= MAX_DROPS
        && req
            .sha256
            .iter()
            .all(|digest| is_hex64(digest) && digests.insert(digest.as_str()));
    if !digests_fit {
        return Err(ApiError::invalid("sha256"));
    }
    Ok(())
}

/// `blob/drop`: the publisher of a personal database drops the blobs it names, on the condition
/// that the head of its snapshot stream and every head it lists are still what it saw.
///
/// The order is the order of the Exemem relay. (1) Read the head of the snapshot stream and of
/// each listed stream once: no head, or another head than named, is 409 `heads`. (2) A stream
/// that holds an entry above its head is 409 `heads` too, because such an entry may name a file
/// and the reader of the stream cannot see it. So is a stream of the publisher that the call
/// names neither as the snapshot stream nor in `heads` (field `heads`): a device that appeared
/// after the caller listed the streams. (3) In one step, every head is still as read
/// (sequence and the count of entries stored beyond a gap), each blob the database holds goes
/// with its pending record, and a digest it does not hold is counted as absent and its pending
/// record is left alone. Here the step is one because `log/append` holds the grant index for
/// its whole call, and so does this call from (1) on.
pub(crate) async fn blob_drop(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let verified = auth::verify_request(&headers, "/relay/v1/blob/drop", &body)?;
    let req: BlobDropBody = parse_body(&body)?;
    verified.check_database(&req.database_id)?;
    check_drop_body(&req)?;
    let db = gate::parse_id(&req.database_id, "database_id")?;
    let writer = verified.node_key.to_hex();
    // Not beside a trim, and not beside a commit of a blob.
    let _trim_guard = state.trim_lock.lock().await;
    let _blob_guard = state.blob_lock.lock().await;
    let grants = state.grants.lock().await;
    personal_owner(&grants, &db, &verified.node_key)?;
    let read = |at: &StreamAt| {
        state
            .store
            .heads
            .token(&req.database_id, &writer, &at.device_id)
    };
    let named = std::iter::once((&req.snapshot, "snapshot"))
        .chain(req.heads.iter().map(|at| (at, "heads")));
    let mut tokens = Vec::with_capacity(req.heads.len() + 1);
    for (at, field) in named {
        let token = read(at)
            .filter(|token| token.seq == at.writer_seq && !token.beyond)
            .ok_or_else(|| ApiError::heads_moved(field))?;
        tokens.push((at, field, token));
    }
    // A stream of the publisher that the caller did not name: an entry on it may name a file of
    // the list, and no condition would see it.
    let unnamed = state
        .store
        .heads
        .devices(&req.database_id, &writer)
        .iter()
        .any(|device| tokens.iter().all(|(at, _, _)| at.device_id != *device));
    if unnamed {
        return Err(ApiError::heads_moved("heads"));
    }
    let snapshot = snapshot_entry(
        &state.store,
        &SnapshotName {
            database_id: &req.database_id,
            device_id: &req.snapshot.device_id,
            writer_seq: req.snapshot.writer_seq,
        },
        &writer,
    )
    .await
    .map_err(|_| ApiError::invalid("snapshot"))?;
    if req.sha256.contains(&snapshot.manifest_sha256.to_hex()) {
        return Err(ApiError::invalid("sha256"));
    }
    for (at, field, token) in tokens {
        if read(at) != Some(token) {
            return Err(ApiError::heads_moved(field));
        }
    }
    let (mut dropped, mut released, mut absent) = (0u64, 0u64, 0u64);
    for digest in &req.sha256 {
        match state.store.drop_blob(&req.database_id, digest).await? {
            Some(bytes) => {
                dropped += 1;
                released += bytes;
            }
            None => absent += 1,
        }
    }
    Ok(Json(
        json!({ "dropped": dropped, "released_bytes": released, "absent": absent }),
    ))
}

impl Store {
    /// Every committed blob of `database_id` with its length, ascending by digest.
    pub(crate) async fn blobs_of(&self, database_id: &str) -> ApiResult<Vec<(String, u64)>> {
        let mut rd = match fs::read_dir(self.root.join("blobs").join(database_id)).await {
            Ok(rd) => rd,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(_) => return Err(ApiError::invalid("data_dir")),
        };
        let mut found = Vec::new();
        while let Some(ent) = rd
            .next_entry()
            .await
            .map_err(|_| ApiError::invalid("data_dir"))?
        {
            let name = ent.file_name().to_string_lossy().into_owned();
            if !is_hex64(&name) {
                continue;
            }
            if let Ok(meta) = fs::metadata(self.blob_path(&name)).await {
                found.push((name, meta.len()));
            }
        }
        found.sort();
        Ok(found)
    }

    /// Drop the blob `sha256` of `database_id`: its ownership, its pending records, and the
    /// stored bytes unless another database owns the same digest. Returns its length, or `None`
    /// when the database does not hold it.
    pub(crate) async fn drop_blob(
        &self,
        database_id: &str,
        sha256: &str,
    ) -> ApiResult<Option<u64>> {
        let marker = self.db_blob_marker(database_id, sha256);
        if fs::metadata(&marker).await.is_err() {
            return Ok(None);
        }
        let object = self.blob_path(sha256);
        let bytes = fs::metadata(&object).await.map_or(0, |meta| meta.len());
        fs::remove_file(&marker)
            .await
            .map_err(|_| ApiError::invalid("data_dir"))?;
        self.clear_pending(database_id, sha256).await;
        if !self.owned_elsewhere(sha256).await {
            // The blob is no longer owned, so it is no longer counted, whatever the delete says.
            if let Err(error) = fs::remove_file(&object).await {
                eprintln!("reference_relay: the blob {sha256} could not be deleted: {error}");
            }
        }
        Ok(Some(bytes))
    }

    /// Remove every pending record of `sha256`: the claims that `blob.put` begin recorded and
    /// the uploads that were not committed. An upload that is still arriving keeps its part file.
    pub(crate) async fn clear_pending(&self, database_id: &str, sha256: &str) {
        let Ok(mut rd) = fs::read_dir(self.staging_dir(database_id)).await else {
            return;
        };
        let prefix = format!("{sha256}-");
        while let Ok(Some(ent)) = rd.next_entry().await {
            let name = ent.file_name().to_string_lossy().into_owned();
            if name.starts_with(&prefix) && !name.contains(".part.") {
                let _ = fs::remove_file(ent.path()).await;
            }
        }
    }

    /// True when a database still owns the digest `sha256`.
    async fn owned_elsewhere(&self, sha256: &str) -> bool {
        let Ok(mut rd) = fs::read_dir(self.root.join("blobs")).await else {
            return false;
        };
        while let Ok(Some(ent)) = rd.next_entry().await {
            let is_dir = ent.file_type().await.is_ok_and(|kind| kind.is_dir());
            if is_dir && fs::metadata(ent.path().join(sha256)).await.is_ok() {
                return true;
            }
        }
        false
    }
}
