//! `log/trim`: drop the entries that a snapshot covers on a personal database.
//!
//! Reads and verifies the named `snapshot` entry, checks its manifest blob, rejects shared
//! databases or regressed `through` positions, atomically updates `floor.json` via a unique
//! temp file, and removes covered stream and older snapshot entries while retaining all blobs.

use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::Json;
use mutation_log::{verify_entry_signature, SignedEntry};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::fs;

use crate::auth;
use crate::error::{ApiError, ApiResult};
use crate::gate;
use crate::handlers::{parse_body, AppState, SMALL_BODY_BYTES};
use crate::heads::{self, FloorFile, FLOOR_FILE};
use crate::store::{atomic_write, read_json, require_hex64, Store};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LogTrimBody {
    database_id: String,
    device_id: String,
    writer_seq: u64,
}

fn permits_entry(is_snapshot: bool, entry: &Value) -> bool {
    let kind = entry.get("kind").and_then(Value::as_str);
    if is_snapshot {
        kind == Some("snapshot")
    } else {
        matches!(kind, Some("data" | "schema"))
            && entry.get("namespace").and_then(Value::as_str) == Some("sealed")
    }
}

struct TrimScope<'a> {
    store: &'a Store,
    database_id: &'a str,
    writer: &'a str,
}

impl TrimScope<'_> {
    fn stream_dir(&self, device: &str) -> PathBuf {
        let mut p = self.store.root.join("logs");
        p.extend([self.database_id, self.writer, device]);
        p
    }

    fn floor(&self, device: &str) -> u64 {
        self.store
            .heads
            .floor(self.database_id, self.writer, device)
    }
}

struct Plan {
    device: String,
    dir: PathBuf,
    floor: Option<(u64, String)>,
    covered: Vec<u64>,
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
    {
        let grants = state.grants.lock().await;
        let birth = grants
            .birth(&db)
            .ok_or_else(|| ApiError::forbidden("publisher"))?;
        if birth.owner != verified.node_key {
            return Err(ApiError::forbidden("publisher"));
        }
        if grants.any_read(&db) {
            return Err(ApiError::forbidden("grant"));
        }
    }
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
    let snapshot = snapshot_entry(scope.store, &req, &writer).await?;
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

    let (mut deleted, mut released) = (0, 0);
    for each in &plans {
        let (count, bytes) = apply(&scope, each).await?;
        deleted += count;
        released += bytes;
    }
    Ok(Json(
        json!({ "done": true, "entries_deleted": deleted, "released_bytes": released }),
    ))
}

async fn verify_manifest_blob(
    store: &Store,
    database_id: &str,
    snapshot: &mutation_log::SnapshotBody,
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
    snapshot: &mutation_log::SnapshotBody,
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

async fn ensure_personal_logs(
    scope: &TrimScope<'_>,
    snapshot_device: &str,
    snap_covered: u64,
    through_map: &BTreeMap<String, u64>,
) -> ApiResult<()> {
    let db_dir = scope.store.root.join("logs").join(scope.database_id);
    for (writer_name, writer_path) in subdirs(&db_dir).await {
        if writer_name != scope.writer {
            return Err(ApiError::forbidden("grant"));
        }
        for (dev, dev_path) in subdirs(&writer_path).await {
            let skip = if dev == snapshot_device {
                snap_covered
            } else {
                through_map.get(&dev).copied().unwrap_or(0)
            };
            check_stream_tail(&dev_path, &dev, snapshot_device, skip, through_map).await?;
        }
    }
    Ok(())
}

async fn check_stream_tail(
    dir: &Path,
    dev: &str,
    snapshot_device: &str,
    skip: u64,
    through_map: &BTreeMap<String, u64>,
) -> ApiResult<()> {
    let is_snap = dev == snapshot_device;
    for seq in stored_seqs(dir, skip + 1, u64::MAX).await {
        let entry = read_json(&dir.join(format!("{seq}.json"))).await?;
        let kind = entry.get("kind").and_then(Value::as_str);
        if matches!(kind, Some("grant" | "lease" | "bundle")) {
            return Err(ApiError::forbidden("grant"));
        }
        let is_valid_birth =
            kind == Some("birth") && seq == 1 && !is_snap && !through_map.contains_key(dev);
        if !is_valid_birth && !permits_entry(is_snap, &entry) {
            return Err(ApiError::forbidden("namespace"));
        }
    }
    Ok(())
}

async fn snapshot_entry(
    store: &Store,
    req: &LogTrimBody,
    writer: &str,
) -> ApiResult<mutation_log::SnapshotBody> {
    let (entries, _) = store
        .read_entries(&req.database_id, writer, &req.device_id, req.writer_seq, 1)
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

async fn plan(
    scope: &TrimScope<'_>,
    device: &str,
    through: u64,
    expect: Option<&str>,
    is_snapshot: bool,
) -> ApiResult<Plan> {
    let dir = scope.stream_dir(device);
    let current_floor = scope.floor(device);
    if through < current_floor {
        return Err(ApiError::invalid("through"));
    }
    if through == 0 {
        return Ok(Plan {
            device: device.to_owned(),
            dir,
            floor: None,
            covered: Vec::new(),
        });
    }
    let head = scope
        .store
        .heads
        .head_seq(scope.database_id, scope.writer, device);
    if head.is_none_or(|h| h < through) {
        return Err(ApiError::invalid("through"));
    }
    let floor = if through > current_floor {
        let last = read_json(&dir.join(format!("{through}.json")))
            .await
            .map_err(|_| ApiError::invalid("through"))?;
        let hash = heads::hash_of(&last)?;
        if expect.is_some_and(|want| want != hash.as_str()) {
            return Err(ApiError::invalid("through"));
        }
        Some((through, hash))
    } else if let Some(want) = expect {
        let raw = fs::read(dir.join(FLOOR_FILE))
            .await
            .map_err(|_| ApiError::invalid("through"))?;
        let rec: FloorFile =
            serde_json::from_slice(&raw).map_err(|_| ApiError::invalid("through"))?;
        if rec.seq != through || rec.hash != want {
            return Err(ApiError::invalid("through"));
        }
        None
    } else {
        None
    };
    let covered = stored_seqs(&dir, 1, through).await;
    for seq in &covered {
        if !permits_entry(
            is_snapshot,
            &read_json(&dir.join(format!("{seq}.json"))).await?,
        ) {
            return Err(ApiError::forbidden("namespace"));
        }
    }
    Ok(Plan {
        device: device.to_owned(),
        dir,
        floor,
        covered,
    })
}

/// Drop what a plan names. Returns how many entries went and the length of their files.
async fn apply(scope: &TrimScope<'_>, plan: &Plan) -> ApiResult<(u64, u64)> {
    if let Some((seq, hash)) = &plan.floor {
        let rec = FloorFile {
            seq: *seq,
            hash: hash.clone(),
        };
        let bytes = serde_json::to_vec(&rec).map_err(|_| ApiError::invalid("floor"))?;
        atomic_write(&plan.dir.join(FLOOR_FILE), &bytes).await?;
        let heads = &scope.store.heads;
        heads.set_floor(
            scope.database_id,
            scope.writer,
            &plan.device,
            *seq,
            hash.clone(),
        );
    }
    let covered = if let Some((through, _)) = &plan.floor {
        stored_seqs(&plan.dir, 1, *through).await
    } else {
        plan.covered.clone()
    };
    let (mut dropped, mut released) = (0, 0);
    for seq in &covered {
        let file = plan.dir.join(format!("{seq}.json"));
        let length = fs::metadata(&file).await.map_or(0, |meta| meta.len());
        match fs::remove_file(&file).await {
            Ok(()) => {
                dropped += 1;
                released += length;
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(_) => return Err(ApiError::invalid("data_dir")),
        }
    }
    Ok((dropped, released))
}

async fn subdirs(dir: &Path) -> Vec<(String, PathBuf)> {
    let mut out = Vec::new();
    if let Ok(mut rd) = fs::read_dir(dir).await {
        while let Ok(Some(ent)) = rd.next_entry().await {
            if ent.file_type().await.is_ok_and(|t| t.is_dir()) {
                if let Some(name) = ent.file_name().to_str() {
                    out.push((name.to_owned(), ent.path()));
                }
            }
        }
    }
    out
}

async fn stored_seqs(dir: &Path, min: u64, max: u64) -> Vec<u64> {
    let mut found = Vec::new();
    if let Ok(mut files) = fs::read_dir(dir).await {
        while let Ok(Some(file)) = files.next_entry().await {
            let seq = file
                .file_name()
                .to_str()
                .and_then(|name| name.strip_suffix(".json"))
                .and_then(|name| name.parse::<u64>().ok());
            if let Some(seq) = seq.filter(|s| *s >= min && *s <= max) {
                found.push(seq);
            }
        }
    }
    found.sort_unstable();
    found
}
