//! Durable local ledger and bounded cloud reclamation of covered personal file blobs.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
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
const LEDGER_MAX_BYTES: usize = 8 * 1024 * 1024;
const MAX_TRACKED_ITEMS: usize = 32_768;
const RETRY_DELAY: Duration = Duration::from_secs(30);
const PAUSED_DELAY: Duration = Duration::from_secs(60);

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TrackedBlob {
    delivery_seq: u64,
    path: BlobPath,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Ledger {
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

fn ledger_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn ledger_path(home: &Path, database_id: &str) -> PathBuf {
    home.join("log-sync")
        .join(database_id)
        .join("blob-retention.json")
}

impl Ledger {
    fn empty(log: &CloudLog) -> Self {
        Self {
            version: 1,
            scope: log.scope.clone(),
            environment: log.environment().to_owned(),
            database_id: log.database_id().to_owned(),
            entries: Vec::new(),
        }
    }

    fn matches(&self, log: &CloudLog) -> bool {
        self.scope == log.scope
            && self.environment == log.environment()
            && self.database_id == log.database_id()
    }

    fn load(home: &Path, log: &CloudLog) -> Result<Option<Self>, String> {
        let path = ledger_path(home, log.database_id());
        let Some(bytes) = disk::read_limited(&path, LEDGER_MAX_BYTES)
            .map_err(|_| "cannot read local blob retention state")?
        else {
            return Ok(None);
        };
        let ledger: Self =
            serde_json::from_slice(&bytes).map_err(|_| "local blob retention state is invalid")?;
        if ledger.version != 1
            || ledger.entries.len() > MAX_TRACKED_ITEMS
            || ledger.entries.iter().any(|entry| {
                entry.delivery_seq == 0
                    || entry.path.database.as_str() != ledger.database_id
                    || entry.path.validate().is_err()
            })
        {
            return Err("local blob retention state is invalid".into());
        }
        Ok(ledger.matches(log).then_some(ledger))
    }

    fn save(&mut self, home: &Path) -> Result<(), String> {
        normalize(&mut self.entries);
        let path = ledger_path(home, &self.database_id);
        let parent = path
            .parent()
            .ok_or("blob retention state has no directory")?;
        if self.entries.is_empty() {
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
                Err(_) => {
                    return Err("cannot remove completed local blob retention state".into());
                }
            }
        } else {
            let bytes =
                serde_json::to_vec(self).map_err(|_| "cannot encode local blob retention state")?;
            disk::write_private(&path, &bytes)
                .map_err(|_| "cannot store local blob retention state")?;
        }
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| "cannot sync local blob retention directory".into())
    }
}

fn normalize(entries: &mut Vec<TrackedBlob>) {
    let mut deduped: BTreeMap<String, TrackedBlob> = BTreeMap::new();
    for entry in entries.drain(..) {
        let Ok(key) = entry.path.key_tail() else {
            continue;
        };
        deduped
            .entry(key)
            .and_modify(|existing| {
                existing.delivery_seq = existing.delivery_seq.max(entry.delivery_seq);
            })
            .or_insert(entry);
    }
    let mut merged: Vec<TrackedBlob> = deduped.into_values().collect();
    merged.sort_by(|left, right| {
        left.delivery_seq
            .cmp(&right.delivery_seq)
            .then_with(|| left.path.space.as_str().cmp(right.path.space.as_str()))
            .then_with(|| left.path.record.as_str().cmp(right.path.record.as_str()))
            .then_with(|| {
                left.path
                    .write_id
                    .as_str()
                    .cmp(right.path.write_id.as_str())
            })
    });
    if merged.len() > MAX_TRACKED_ITEMS {
        merged.truncate(MAX_TRACKED_ITEMS);
    }
    *entries = merged;
}

/// Record self-authored personal file attachments once the cloud assigns `delivery_seq`.
pub(super) fn record_personal(home: &Path, log: &CloudLog, items: &[(u64, BlobPath)]) {
    if items.is_empty() {
        return;
    }
    if let Err(error) = append_items(home, log, items) {
        tracing::warn!(
            target: "lastdbd::log_sync",
            database_id = %log.database_id(),
            %error,
            "personal blob retention ledger could not record uploaded attachments"
        );
    }
}

/// Record personal file attachments observed on a pulled personal delivery page.
pub(super) fn record_pulled_personal(host: &Host, log: &CloudLog, entries: &[StoredEntry]) {
    if entries.is_empty() {
        return;
    }
    let Ok(Some((_, key))) = DatabaseContentKey::local_store(host, log.database_id()) else {
        return;
    };
    let mut discovered = Vec::new();
    for stored in entries {
        let entry = &stored.entry;
        if stored.delivery_seq == 0
            || entry.database_id.as_str() != log.database_id()
            || entry.incorporated_from.is_some()
        {
            continue;
        }
        let binding = LogBinding {
            database_id: log.database_id(),
            writer_key: entry.writer.key.as_str(),
            writer_device: entry.writer.device.as_str(),
            writer_seq: entry.writer_seq,
            authored_at_ms: entry.authored_at_ms,
            namespace: entry.namespace.as_str(),
            incorporated_from: None,
        };
        let Ok(plain) = log_envelope::open(&key, &binding, &entry.payload) else {
            continue;
        };
        let Ok(body) = serde_json::from_slice::<AcceptedBody>(&plain) else {
            continue;
        };
        let Some(object) = body.file.and_then(|file| file.object) else {
            continue;
        };
        if object.scope == log.scope
            && object.path.database.as_str() == log.database_id()
            && object.path.validate().is_ok()
        {
            discovered.push((stored.delivery_seq, object.path));
        }
    }
    record_personal(&host.home, log, &discovered);
}

fn append_items(home: &Path, log: &CloudLog, items: &[(u64, BlobPath)]) -> Result<(), String> {
    let _guard = ledger_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut ledger = Ledger::load(home, log)?.unwrap_or_else(|| Ledger::empty(log));
    let before = ledger.entries.clone();
    for (delivery_seq, path) in items {
        if *delivery_seq == 0
            || path.database.as_str() != log.database_id()
            || path.validate().is_err()
        {
            continue;
        }
        ledger.entries.push(TrackedBlob {
            delivery_seq: *delivery_seq,
            path: path.clone(),
        });
    }
    normalize(&mut ledger.entries);
    if ledger.entries == before {
        return Ok(());
    }
    ledger.save(home)
}

pub(super) fn has_any(home: &Path, log: &CloudLog) -> Result<bool, String> {
    let _guard = ledger_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Ok(Ledger::load(home, log)?.is_some_and(|ledger| !ledger.entries.is_empty()))
}

pub(super) fn has_covered(home: &Path, log: &CloudLog, floor: u64) -> Result<bool, String> {
    if floor == 0 {
        return Ok(false);
    }
    let _guard = ledger_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Ok(Ledger::load(home, log)?.is_some_and(|ledger| {
        ledger
            .entries
            .iter()
            .any(|entry| entry.delivery_seq <= floor)
    }))
}

fn next_covered_batch(home: &Path, log: &CloudLog, floor: u64) -> Result<Vec<BlobPath>, String> {
    if floor == 0 {
        return Ok(Vec::new());
    }
    let _guard = ledger_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(ledger) = Ledger::load(home, log)? else {
        return Ok(Vec::new());
    };
    Ok(ledger
        .entries
        .into_iter()
        .filter_map(|entry| (entry.delivery_seq <= floor).then_some(entry.path))
        .take(MAX_BLOBS_PER_REQUEST)
        .collect())
}

fn remove_confirmed(
    home: &Path,
    log: &CloudLog,
    confirmed_floor: u64,
    floor: u64,
    reclaimed: &[BlobPath],
) -> Result<bool, String> {
    let _guard = ledger_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(mut ledger) = Ledger::load(home, log)? else {
        return Ok(false);
    };
    if !reclaimed.is_empty() {
        let before = ledger.entries.len();
        ledger.entries.retain(|entry| {
            !(entry.delivery_seq <= confirmed_floor && reclaimed.contains(&entry.path))
        });
        if ledger.entries.len() != before {
            ledger.save(home)?;
        }
    }
    Ok(ledger
        .entries
        .iter()
        .any(|entry| entry.delivery_seq <= floor))
}

/// Request physical deletion of one bounded batch of covered personal file blobs.
pub(super) async fn reclaim_step(
    home: &Path,
    log: &CloudLog,
    snapshot_id: &Digest,
    floor: u64,
) -> Result<Option<ProviderCleanup>, String> {
    let batch = next_covered_batch(home, log, floor)?;
    if batch.is_empty() {
        return Ok(Some(ProviderCleanup::Complete));
    }
    let response = transport::call_if_supported(
        log,
        json!({
            "action": "reclaim-blobs",
            "database_id": log.database_id(),
            "snapshot_id": snapshot_id,
            "expected_floor": DecimalU64::new(floor),
            "blobs": batch,
        }),
    )
    .await?;
    let Some(response) = response else {
        return Ok(None);
    };
    let response: ReclaimedBlobs =
        serde_json::from_value(response).map_err(|_| "invalid signed blob reclaim response")?;
    let confirmed_floor = response.reclaimed_through.get();
    if &response.snapshot_id != snapshot_id
        || confirmed_floor < floor
        || response
            .reclaimed_blobs
            .iter()
            .any(|path| !batch.contains(path))
        || (response.state == ProviderCleanup::Complete
            && response.reclaimed_blobs.len() != batch.len())
    {
        return Err("cloud blob reclaim did not confirm the requested snapshot and batch".into());
    }
    let more_covered =
        remove_confirmed(home, log, confirmed_floor, floor, &response.reclaimed_blobs)?;
    let state = match response.state {
        ProviderCleanup::Complete if more_covered => ProviderCleanup::Progress,
        other => other,
    };
    if state == ProviderCleanup::Complete {
        tracing::info!(
            target: "lastdbd::log_sync",
            through = floor,
            "covered personal file blob cleanup completed"
        );
    }
    Ok(Some(state))
}

pub(super) async fn pace_step(
    home: &Path,
    log: &CloudLog,
    snapshot_id: &Digest,
    floor: u64,
) -> Result<Option<Duration>, String> {
    match reclaim_step(home, log, snapshot_id, floor).await? {
        Some(ProviderCleanup::Complete | ProviderCleanup::Progress) => Ok(Some(Duration::ZERO)),
        Some(ProviderCleanup::Waiting) => Ok(Some(RETRY_DELAY)),
        None => Ok(Some(PAUSED_DELAY)),
    }
}
