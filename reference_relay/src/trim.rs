//! `log/trim`: drop the entries that a snapshot covers.
//!
//! The owner names one `snapshot` entry on the snapshot stream. The relay reads that stored
//! entry itself, checks its signature, and trusts nothing else in the request: the
//! positions to drop are the `through` list of that entry. For each stream it drops the
//! entries up to the position and keeps a `floor.json` file (the last dropped seq and its
//! hash), so the head of the stream, `log/streams` and the gapless append go on as before.
//! It then drops the snapshot entries older than the named one. It never drops a blob: the
//! manifests and files of a dropped snapshot entry stay on the relay. The named snapshot,
//! the newer ones and everything after the positions stay, so a restore from any retained
//! snapshot still has all it reads.
//!
//! Only a personal database is trimmed: every entry that would be dropped must carry the
//! namespace `sealed` (data and schema entries) or be a `snapshot` entry. A database whose
//! members read the publisher's streams from the start is refused. The `birth` entry is
//! never dropped, because it is on a stream of its own that no snapshot covers.
//!
//! The call is safe to repeat. The floor is written before the entry files go, and each
//! step skips what is already gone.

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

/// What an entry of a stream may be for the trim to drop it.
#[derive(Clone, Copy)]
enum Allowed {
    /// A sealed `data` or `schema` entry of a personal database.
    Sealed,
    /// A `snapshot` entry.
    Snapshot,
}

impl Allowed {
    fn permits(self, entry: &Value) -> bool {
        let kind = entry.get("kind").and_then(Value::as_str);
        match self {
            Self::Sealed => {
                matches!(kind, Some("data" | "schema"))
                    && entry.get("namespace").and_then(Value::as_str) == Some("sealed")
            }
            Self::Snapshot => kind == Some("snapshot"),
        }
    }
}

/// The entries of one stream that the trim drops, checked and not yet dropped.
struct Plan {
    device: String,
    dir: PathBuf,
    /// Seq of the last entry to drop, and its hash. `None` when it is already gone.
    floor: Option<(u64, String)>,
    /// Every stored seq up to the floor, ascending.
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
    let caller = verified.node_key;
    let db = gate::parse_id(&req.database_id, "database_id")?;
    require_hex64(&req.device_id, "device_id")?;
    if req.writer_seq == 0 {
        return Err(ApiError::invalid("writer_seq"));
    }
    {
        // Only the owner's node key trims, and the owner's key is the publisher.
        let grants = state.grants.lock().await;
        let birth = grants
            .birth(&db)
            .ok_or_else(|| ApiError::forbidden("publisher"))?;
        if birth.owner != caller {
            return Err(ApiError::forbidden("publisher"));
        }
    }
    let writer = caller.to_hex();
    let store = &state.store;
    let snapshot = snapshot_entry(store, &req, &writer).await?;

    // Check everything first. Nothing is dropped until every stream passed.
    let mut plans = Vec::new();
    for position in &snapshot.through {
        let device = position.device_id.to_hex();
        let hash = position.entry_hash.to_hex();
        plans.push(
            plan(
                store,
                &req.database_id,
                &writer,
                &device,
                position.writer_seq,
                Some(hash.as_str()),
                Allowed::Sealed,
            )
            .await?,
        );
    }
    let older = req.writer_seq - 1;
    let snapshot_plan = plan(
        store,
        &req.database_id,
        &writer,
        &req.device_id,
        older,
        None,
        Allowed::Snapshot,
    )
    .await?;

    // The streams first, then the older snapshot entries. No file or manifest is dropped.
    let mut entries_deleted = 0;
    for each in &plans {
        entries_deleted += apply(store, &req.database_id, &writer, each).await?;
    }
    entries_deleted += apply(store, &req.database_id, &writer, &snapshot_plan).await?;
    Ok(Json(json!({
        "done": true,
        "entries_deleted": entries_deleted,
    })))
}

/// The stored, signed `snapshot` entry that the request names.
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

/// Check the entries of one stream up to `through`, and say what dropping them means.
async fn plan(
    store: &Store,
    database_id: &str,
    writer: &str,
    device: &str,
    through: u64,
    expect: Option<&str>,
    allowed: Allowed,
) -> ApiResult<Plan> {
    let dir = store
        .root
        .join("logs")
        .join(database_id)
        .join(writer)
        .join(device);
    if through == 0 {
        return Ok(Plan {
            device: device.to_owned(),
            dir,
            floor: None,
            covered: Vec::new(),
        });
    }
    let head = store
        .heads
        .head_seq(database_id, writer, device)
        .ok_or_else(|| ApiError::invalid("through"))?;
    if head < through {
        return Err(ApiError::invalid("through"));
    }
    let covered = stored_up_to(&dir, through).await;
    let mut floor = None;
    if through > store.heads.floor(database_id, writer, device) {
        // The last entry is still here, so it must be the one the snapshot names.
        let last = read_json(&dir.join(format!("{through}.json")))
            .await
            .map_err(|_| ApiError::invalid("through"))?;
        let hash = heads::hash_of(&last)?;
        if expect.is_some_and(|want| want != hash.as_str()) {
            return Err(ApiError::invalid("through"));
        }
        floor = Some((through, hash));
    }
    for seq in &covered {
        let entry = read_json(&dir.join(format!("{seq}.json"))).await?;
        if !allowed.permits(&entry) {
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

/// Drop what a plan names. Returns how many entries went.
async fn apply(store: &Store, database_id: &str, writer: &str, plan: &Plan) -> ApiResult<u64> {
    if let Some((seq, hash)) = &plan.floor {
        let record = FloorFile {
            seq: *seq,
            hash: hash.clone(),
        };
        let bytes = serde_json::to_vec(&record).map_err(|_| ApiError::invalid("floor"))?;
        atomic_write(&plan.dir.join(FLOOR_FILE), &bytes).await?;
        store
            .heads
            .set_floor(database_id, writer, &plan.device, *seq, hash.clone());
    }
    let mut dropped = 0;
    // Listed again after the floor is set: an append that raced the plan may have written an
    // entry at or below the floor, and it must go too (see `Store::append_one`).
    let covered = if let Some((through, _)) = &plan.floor {
        stored_up_to(&plan.dir, *through).await
    } else {
        plan.covered.clone()
    };
    for seq in &covered {
        match fs::remove_file(plan.dir.join(format!("{seq}.json"))).await {
            Ok(()) => dropped += 1,
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(_) => return Err(ApiError::invalid("data_dir")),
        }
    }
    Ok(dropped)
}

/// The seq of every entry file of a stream that is at most `through`, ascending.
async fn stored_up_to(dir: &Path, through: u64) -> Vec<u64> {
    let mut found = Vec::new();
    if let Ok(mut files) = fs::read_dir(dir).await {
        while let Ok(Some(file)) = files.next_entry().await {
            let seq = file
                .file_name()
                .to_str()
                .and_then(|name| name.strip_suffix(".json"))
                .and_then(|name| name.parse::<u64>().ok());
            if let Some(seq) = seq.filter(|seq| *seq <= through) {
                found.push(seq);
            }
        }
    }
    found.sort_unstable();
    found
}
