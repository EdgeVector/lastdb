//! Durable local segmented ledger and bounded cloud reclamation of personal file blobs.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use cloud_backup_protocol::v2::blobs::BlobPath;
use cloud_backup_protocol::{CloudScope, DecimalU64, Digest};
use mutation_log::StoredEntry;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::database_replay::AcceptedBody;
use crate::host::Host;
use crate::log_envelope::{self, DatabaseContentKey, LogBinding};

use super::{cloud::CloudLog, disk, retention::ProviderCleanup, snapshot::transport};

const MAX_BLOBS_PER_REQUEST: usize = 16;
const MAX_SEGMENT_ENTRIES: usize = 512;
const FILE_MAX_BYTES: usize = 1024 * 1024;
const RETRY_DELAY: Duration = Duration::from_secs(30);
const PAUSED_DELAY: Duration = Duration::from_secs(60);

type StepOut = Result<Option<ProviderCleanup>, String>;
type PaceOut = Result<Option<Duration>, String>;
type FlagOut = Result<bool, String>;
type UnitOut = Result<(), String>;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TrackedBlob {
    seq: u64,
    path: BlobPath,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Segment {
    version: u8,
    scope: CloudScope,
    environment: String,
    database_id: String,
    entries: Vec<TrackedBlob>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReclaimedBlobs {
    snapshot_id: Digest,
    reclaimed_through: DecimalU64,
    reclaimed_blobs: Vec<BlobPath>,
    state: ProviderCleanup,
}

fn lock_ledger() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn ledger_dir(home: &Path, db: &str) -> PathBuf {
    home.join("log-sync").join(db).join("blob-retention")
}

fn sync_dir(dir: &Path) -> UnitOut {
    match File::open(dir) {
        Ok(d) => d
            .sync_all()
            .map_err(|_| "blob retention sync failed".into()),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(_) => Err("blob retention sync failed".into()),
    }
}

fn remove_file(path: &Path) -> FlagOut {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
        Err(_) => Err("cannot remove completed blob retention segment".into()),
    }
}

fn list_segment_ids(home: &Path, db: &str) -> Result<Vec<u64>, String> {
    let entries = match fs::read_dir(ledger_dir(home, db)) {
        Ok(entries) => entries,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(_) => return Err("cannot inspect local blob retention directory".into()),
    };
    let mut ids = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|_| "cannot inspect blob retention dir")?;
        let fname = entry.file_name();
        if let Some(d) = fname.to_str().and_then(|s| s.strip_suffix(".json")) {
            if d.len() == 16 && d.bytes().all(|b| b.is_ascii_digit()) {
                ids.extend(d.parse::<u64>());
            }
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

impl Segment {
    fn new(log: &CloudLog, entries: Vec<TrackedBlob>) -> Self {
        Self {
            version: 1,
            scope: log.scope.clone(),
            environment: log.environment().to_owned(),
            database_id: log.database_id().to_owned(),
            entries,
        }
    }

    fn matches(&self, log: &CloudLog) -> bool {
        self.scope == log.scope
            && self.environment == log.environment()
            && self.database_id == log.database_id()
    }

    fn load(home: &Path, db: &str, id: u64) -> Result<Option<Self>, String> {
        let path = ledger_dir(home, db).join(format!("{id:016}.json"));
        let Some(b) =
            disk::read_limited(&path, FILE_MAX_BYTES).map_err(|_| "segment read failed")?
        else {
            return Ok(None);
        };
        let seg: Self = serde_json::from_slice(&b).map_err(|_| "invalid segment")?;
        let ok = |e: &TrackedBlob| {
            e.seq > 0 && e.path.database.as_str() == db && e.path.validate().is_ok()
        };
        if seg.version != 1
            || seg.database_id != db
            || !(1..=MAX_SEGMENT_ENTRIES).contains(&seg.entries.len())
            || !seg.entries.iter().all(ok)
        {
            return Err("invalid segment".into());
        }
        Ok(Some(seg))
    }

    fn save(&self, home: &Path, id: u64) -> UnitOut {
        let bytes = serde_json::to_vec(self).map_err(|_| "cannot encode blob retention segment")?;
        if bytes.len() > FILE_MAX_BYTES {
            return Err("encoded local blob retention segment exceeds file limit".into());
        }
        let path = ledger_dir(home, &self.database_id).join(format!("{id:016}.json"));
        disk::write_private(&path, &bytes).map_err(|_| "cannot store local blob retention segment")
    }
}

pub(super) fn record_personal(home: &Path, log: &CloudLog, items: &[(u64, BlobPath)]) -> UnitOut {
    if items.is_empty() {
        return Ok(());
    }
    append_items(home, log, items)
}

pub(super) fn record_pulled_personal(host: &Host, log: &CloudLog, s: &[StoredEntry]) -> UnitOut {
    if s.is_empty() {
        return Ok(());
    }
    let (_, key) = DatabaseContentKey::local_store(host, log.database_id())
        .map_err(|_| "local database or content key unavailable".to_owned())?
        .ok_or_else(|| "local database or content key unavailable".to_owned())?;
    let items: Vec<_> = s.iter().filter_map(|e| extract(log, &key, e)).collect();
    record_personal(&host.home, log, &items)
}

fn extract(log: &CloudLog, key: &DatabaseContentKey, s: &StoredEntry) -> Option<(u64, BlobPath)> {
    let e = &s.entry;
    if s.delivery_seq == 0
        || e.database_id.as_str() != log.database_id()
        || e.incorporated_from.is_some()
    {
        return None;
    }
    let b = LogBinding {
        database_id: log.database_id(),
        writer_key: e.writer.key.as_str(),
        writer_device: e.writer.device.as_str(),
        writer_seq: e.writer_seq,
        authored_at_ms: e.authored_at_ms,
        namespace: e.namespace.as_str(),
        incorporated_from: None,
    };
    let plain = log_envelope::open(key, &b, &e.payload).ok()?;
    let body: AcceptedBody = serde_json::from_slice(&plain).ok()?;
    let obj = body.file?.object?;
    (obj.scope == log.scope
        && obj.path.database.as_str() == log.database_id()
        && obj.path.validate().is_ok())
    .then_some((s.delivery_seq, obj.path))
}

fn is_stale(home: &Path, log: &CloudLog, id: u64) -> FlagOut {
    Ok(Segment::load(home, log.database_id(), id)?.is_some_and(|s| !s.matches(log)))
}

fn prune_stale(home: &Path, log: &CloudLog, ids: &mut Vec<u64>) -> UnitOut {
    let Some((&first, &last)) = ids.first().zip(ids.last()) else {
        return Ok(());
    };
    if is_stale(home, log, first)? || is_stale(home, log, last)? {
        let dir = ledger_dir(home, log.database_id());
        let mut removed = false;
        for id in ids.drain(..) {
            if is_stale(home, log, id)? {
                removed |= remove_file(&dir.join(format!("{id:016}.json")))?;
            }
        }
        if removed {
            sync_dir(&dir)?;
        }
        *ids = list_segment_ids(home, log.database_id())?;
    }
    Ok(())
}

fn collect_pending(db: &str, items: &[(u64, BlobPath)]) -> BTreeMap<String, TrackedBlob> {
    let mut pending: BTreeMap<String, TrackedBlob> = BTreeMap::new();
    for (seq, path) in items {
        if *seq > 0 && path.database.as_str() == db {
            if let Ok(key) = path.key_tail() {
                pending
                    .entry(key)
                    .and_modify(|e| e.seq = e.seq.max(*seq))
                    .or_insert_with(|| TrackedBlob {
                        seq: *seq,
                        path: path.clone(),
                    });
            }
        }
    }
    pending
}

fn append_items(home: &Path, log: &CloudLog, items: &[(u64, BlobPath)]) -> UnitOut {
    let db = log.database_id();
    let mut pending = collect_pending(db, items);
    if pending.is_empty() {
        return Ok(());
    }
    let _guard = lock_ledger();
    let mut ids = list_segment_ids(home, db)?;
    prune_stale(home, log, &mut ids)?;
    let mut tail: Option<(u64, Segment, bool)> = None;
    for &id in &ids {
        let Some(mut seg) = Segment::load(home, db, id)?.filter(|s| s.matches(log)) else {
            continue;
        };
        let mut dirty = false;
        for entry in &mut seg.entries {
            if let Ok(key) = entry.path.key_tail() {
                if let Some(inc) = pending.remove(&key) {
                    if inc.seq > entry.seq {
                        entry.seq = inc.seq;
                        dirty = true;
                    }
                }
            }
        }
        if Some(&id) == ids.last() {
            tail = Some((id, seg, dirty));
        } else if dirty {
            seg.entries.sort();
            seg.save(home, id)?;
        }
    }
    let mut fresh: Vec<_> = pending.into_values().collect();
    fresh.sort();
    let start_id = ids
        .last()
        .map_or(Ok(0), |id| id.checked_add(1).ok_or("segment id overflow"))?;
    if let Some((tail_id, mut seg, dirty)) = tail {
        let take = MAX_SEGMENT_ENTRIES
            .saturating_sub(seg.entries.len())
            .min(fresh.len());
        if take > 0 {
            seg.entries.extend(fresh.drain(..take));
        }
        if dirty || take > 0 {
            seg.entries.sort();
            seg.save(home, tail_id)?;
        }
    }
    for (idx, chunk) in fresh.chunks(MAX_SEGMENT_ENTRIES).enumerate() {
        let id = start_id.checked_add(idx as u64).ok_or("id overflow")?;
        Segment::new(log, chunk.to_vec()).save(home, id)?;
    }
    Ok(())
}

pub(super) fn has_any(home: &Path, log: &CloudLog) -> FlagOut {
    Ok(!covered(home, log, u64::MAX, 1)?.is_empty())
}

pub(super) fn has_covered(home: &Path, log: &CloudLog, floor: u64) -> FlagOut {
    Ok(!covered(home, log, floor, 1)?.is_empty())
}

fn covered(home: &Path, log: &CloudLog, floor: u64, n: usize) -> Result<Vec<BlobPath>, String> {
    if floor == 0 || n == 0 {
        return Ok(Vec::new());
    }
    let _guard = lock_ledger();
    let db = log.database_id();
    let mut batch = Vec::new();
    for id in list_segment_ids(home, db)? {
        let Some(seg) = Segment::load(home, db, id)?.filter(|s| s.matches(log)) else {
            continue;
        };
        for e in seg.entries {
            if e.seq <= floor && !batch.contains(&e.path) {
                batch.push(e.path);
                if batch.len() == n {
                    return Ok(batch);
                }
            }
        }
    }
    Ok(batch)
}

fn mark_done(home: &Path, log: &CloudLog, fl: (u64, u64), done: &[BlobPath]) -> FlagOut {
    let _guard = lock_ledger();
    let db = log.database_id();
    let dir = ledger_dir(home, db);
    let (mut more, mut removed, mut live) = (false, false, 0usize);
    for id in list_segment_ids(home, db)? {
        let Some(mut seg) = Segment::load(home, db, id)?.filter(|s| s.matches(log)) else {
            continue;
        };
        let before = seg.entries.len();
        seg.entries
            .retain(|e| !(e.seq <= fl.0 && done.contains(&e.path)));
        if seg.entries.is_empty() {
            removed |= remove_file(&dir.join(format!("{id:016}.json")))?;
        } else {
            live += 1;
            more |= seg.entries.iter().any(|e| e.seq <= fl.1);
            if seg.entries.len() != before {
                seg.save(home, id)?;
            }
        }
    }
    if removed {
        let target = if live == 0 && fs::remove_dir(&dir).is_ok() {
            dir.parent().unwrap_or(&dir)
        } else {
            &dir
        };
        sync_dir(target)?;
    }
    Ok(more)
}

pub(super) async fn reclaim_step(home: &Path, log: &CloudLog, id: &Digest, floor: u64) -> StepOut {
    let batch = covered(home, log, floor, MAX_BLOBS_PER_REQUEST)?;
    if batch.is_empty() {
        return Ok(Some(ProviderCleanup::Complete));
    }
    let req = json!({ "action": "reclaim-blobs", "database_id": log.database_id(), "snapshot_id": id, "expected_floor": DecimalU64::new(floor), "blobs": batch });
    let Some(raw) = transport::call_if_supported(log, req).await? else {
        return Ok(None);
    };
    let res: ReclaimedBlobs =
        serde_json::from_value(raw).map_err(|_| "invalid signed blob reclaim response")?;
    let ok = res.reclaimed_through.get();
    if &res.snapshot_id != id
        || ok < floor
        || res.reclaimed_blobs.iter().any(|p| !batch.contains(p))
        || (res.state == ProviderCleanup::Complete && res.reclaimed_blobs.len() != batch.len())
    {
        return Err("cloud blob reclaim did not confirm the requested snapshot and batch".into());
    }
    let more = mark_done(home, log, (ok, floor), &res.reclaimed_blobs)?;
    let state = match (res.state, more) {
        (ProviderCleanup::Complete, true) => ProviderCleanup::Progress,
        (other, _) => other,
    };
    if state == ProviderCleanup::Complete {
        tracing::info!(target: "lastdbd::log_sync", through = floor, "covered personal file blob cleanup completed");
    }
    Ok(Some(state))
}

pub(super) async fn pace_step(home: &Path, log: &CloudLog, id: &Digest, floor: u64) -> PaceOut {
    match reclaim_step(home, log, id, floor).await? {
        Some(ProviderCleanup::Complete | ProviderCleanup::Progress) => Ok(Some(Duration::ZERO)),
        Some(ProviderCleanup::Waiting) => Ok(Some(RETRY_DELAY)),
        None => Ok(Some(PAUSED_DELAY)),
    }
}
