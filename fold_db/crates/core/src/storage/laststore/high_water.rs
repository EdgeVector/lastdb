use super::{StorageError, StorageResult};
use crate::clock::unix_secs;
use crate::hex::sha256_hex;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use uuid::Uuid;

const VERSION: u32 = 1;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct LastStoreHighWater {
    pub version: u32,
    pub store_uuid: String,
    pub csn_high_water: u64,
    pub backup_epoch: u64,
    pub backup_manifest_counter: u64,
    pub updated_at_unix_secs: u64,
    /// Unix seconds when a backup manifest was last **committed** — the only
    /// event that proves data reached the cloud.
    ///
    /// Deliberately separate from `updated_at_unix_secs`, which every writer of
    /// this file bumps: an ordinary local write (`record_csn_high_water`) and a
    /// CAS epoch bump both move it, so it answers "when was this marker
    /// touched", never "when were we last backed up". Reading durability age
    /// off `updated_at_unix_secs` reports a store that has not backed up in
    /// weeks as fresh.
    ///
    /// `None` on a marker written before this field existed. That is
    /// "unknown", not "never" — see [`BackupDurability::backup_manifest_counter`],
    /// which still proves whether any backup ever committed.
    #[serde(default)]
    pub last_backup_commit_unix_secs: Option<u64>,
    /// Unix seconds when a held backup cut was abandoned as provably
    /// unpublishable (residual `source_missing` after cloud-presence heal on a
    /// demoted continuous sealed home).
    ///
    /// Durable because the abandon is otherwise a one-shot in-process event:
    /// the publisher logs it once, releases the hold, and every later demoted
    /// cycle short-circuits to idle without producing a progress sample. A
    /// restart then re-read `last_backup_commit_unix_secs`, found an old
    /// commit, and reported `complete: true` for a home whose sealed base can
    /// no longer be published. Measured on the primary 2026-08-24T07:09:34Z
    /// (`source_missing=233`, `generation=521`).
    ///
    /// Cleared by the two events that prove a publishable base again:
    /// `record_backup_manifest_commit` and `record_restored_backup_manifest`.
    /// `None` means no abandon is outstanding.
    #[serde(default)]
    pub sealed_base_abandoned_unix_secs: Option<u64>,
}

/// Durable, engine-independent answer to "when did a backup last commit on this
/// home?".
///
/// Read straight off the on-disk marker so it is available with **no sync
/// engine running** — a disabled uploader and a failing one are the same
/// outcome for the data, and only a durable fact covers both.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackupDurability {
    /// Highest committed backup manifest counter. `0` means no backup has ever
    /// committed on this home.
    pub backup_manifest_counter: u64,
    /// When the last manifest committed, if the marker records it.
    pub last_backup_commit_unix_secs: Option<u64>,
    /// When any writer last touched the marker. Diagnostic only — never use it
    /// as a backup age.
    pub marker_updated_at_unix_secs: u64,
    /// When the sealed-home base was last abandoned as unpublishable, if the
    /// marker records it.
    pub sealed_base_abandoned_unix_secs: Option<u64>,
}

impl BackupDurability {
    /// Whether the recorded abandon is still outstanding — no commit has landed
    /// since the sealed base was given up.
    ///
    /// A commit clears the abandon field outright, so a surviving abandon
    /// already means no commit landed after it. The timestamp comparison is a
    /// guard against a marker some other writer left inconsistent, and it keeps
    /// a stale abandon from pinning a recovered home to "degraded" forever.
    ///
    /// The tie goes to the abandon. Both stamps have one-second resolution, so
    /// a commit and an abandon in the same second are indistinguishable here —
    /// and only one order is reachable, because the other one would have
    /// cleared the field. Resolving a tie toward "degraded" is also the safe
    /// direction for a durability report.
    #[must_use]
    pub fn sealed_base_abandoned_outstanding(&self) -> bool {
        match self.sealed_base_abandoned_unix_secs {
            None => false,
            Some(abandoned) => self
                .last_backup_commit_unix_secs
                .is_none_or(|committed| abandoned >= committed),
        }
    }
}

/// Resolve the high-water marker path for a store root.
///
/// Mini layout: `$HOME/data` store root → `$HOME/laststore_high_water.json`.
/// Tests / ad-hoc roots keep the marker beside the store path.
pub fn high_water_path_for_store_root(store_root: &Path) -> PathBuf {
    if store_root.file_name().and_then(|s| s.to_str()) == Some("data") {
        store_root
            .parent()
            .unwrap_or(store_root)
            .join("laststore_high_water.json")
    } else {
        store_root.join("laststore_high_water.json")
    }
}

/// Read backup durability facts for a store root without opening the store.
///
/// Never creates or repairs the marker: a status read must not manufacture the
/// very state it reports on. Returns `None` when the marker is absent or
/// unreadable, which callers must treat as unknown rather than healthy.
pub fn read_backup_durability(store_root: &Path) -> Option<BackupDurability> {
    let bytes = fs::read(high_water_path_for_store_root(store_root)).ok()?;
    let state: LastStoreHighWater = serde_json::from_slice(&bytes).ok()?;
    Some(BackupDurability {
        backup_manifest_counter: state.backup_manifest_counter,
        last_backup_commit_unix_secs: state.last_backup_commit_unix_secs,
        marker_updated_at_unix_secs: state.updated_at_unix_secs,
        sealed_base_abandoned_unix_secs: state.sealed_base_abandoned_unix_secs,
    })
}

/// Cloud object root for a LastStore `store_uuid`.
///
/// Matches [`super::LastStoreNamespacedStore::cloud_db_hash`]: SHA-256 of
/// `laststore-db:{store_uuid}` as 64 lowercase hex. Restore and daemon publish
/// must use the same function or `backup_latest_get` looks under the wrong
/// prefix.
pub fn cloud_db_hash_for_store_uuid(store_uuid: &str) -> String {
    sha256_hex(format!("laststore-db:{store_uuid}"))
}

/// Read the source home's cloud `db_hash` from its high-water marker.
///
/// Never creates a marker: a missing/unreadable file returns `None` so restore
/// can fail closed instead of inventing a new store identity.
pub fn read_cloud_db_hash(store_root: &Path) -> Option<String> {
    let bytes = fs::read(high_water_path_for_store_root(store_root)).ok()?;
    let state: LastStoreHighWater = serde_json::from_slice(&bytes).ok()?;
    if state.store_uuid.trim().is_empty() {
        return None;
    }
    Some(cloud_db_hash_for_store_uuid(&state.store_uuid))
}

#[derive(Clone, Debug)]
pub(crate) struct LastStoreHighWaterFile {
    path: PathBuf,
    state_lock: std::sync::Arc<std::sync::Mutex<()>>,
}

impl LastStoreHighWaterFile {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            state_lock: std::sync::Arc::new(std::sync::Mutex::new(())),
        }
    }

    /// Directory holding the durable marker — the store's sidecar home for
    /// other small durable files (e.g. the chunk-sha memo).
    pub(crate) fn sidecar_dir(&self) -> Option<&Path> {
        self.path.parent()
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, ()> {
        self.state_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn load_or_init(&self) -> StorageResult<LastStoreHighWater> {
        let _guard = self.lock_state();
        self.load_or_init_unlocked()
    }

    fn load_or_init_unlocked(&self) -> StorageResult<LastStoreHighWater> {
        match fs::read(&self.path) {
            Ok(bytes) => {
                let state: LastStoreHighWater =
                    serde_json::from_slice(&bytes).map_err(|e| self.error(format_args!("{e}")))?;
                if state.version != VERSION {
                    return Err(self.error(format_args!(
                        "unsupported laststore high-water version {}",
                        state.version
                    )));
                }
                Ok(state)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let state = LastStoreHighWater {
                    version: VERSION,
                    store_uuid: Uuid::new_v4().to_string(),
                    csn_high_water: 0,
                    backup_epoch: 1,
                    backup_manifest_counter: 0,
                    updated_at_unix_secs: unix_secs(),
                    last_backup_commit_unix_secs: None,
                    sealed_base_abandoned_unix_secs: None,
                };
                self.write(&state)?;
                Ok(state)
            }
            Err(e) => Err(self.error(format_args!("read failed: {e}"))),
        }
    }

    pub fn csn_floor(&self) -> StorageResult<u64> {
        self.load_or_init().map(|state| state.csn_high_water)
    }

    pub(crate) fn backup_durability(&self) -> StorageResult<BackupDurability> {
        let state = self.load_or_init()?;
        Ok(BackupDurability {
            backup_manifest_counter: state.backup_manifest_counter,
            last_backup_commit_unix_secs: state.last_backup_commit_unix_secs,
            marker_updated_at_unix_secs: state.updated_at_unix_secs,
            sealed_base_abandoned_unix_secs: state.sealed_base_abandoned_unix_secs,
        })
    }

    pub fn record_csn_high_water(&self, csn: u64) -> StorageResult<LastStoreHighWater> {
        let _guard = self.lock_state();
        let mut state = self.load_or_init_unlocked()?;
        if csn > state.csn_high_water {
            state.csn_high_water = csn;
            state.updated_at_unix_secs = unix_secs();
            self.write(&state)?;
        }
        Ok(state)
    }

    pub fn reserve_backup_manifest(&self, cut_csn: u64) -> StorageResult<LastStoreHighWater> {
        let _guard = self.lock_state();
        let mut state = self.load_or_init_unlocked()?;
        state.backup_manifest_counter = state.backup_manifest_counter.saturating_add(1);
        state.csn_high_water = state.csn_high_water.max(cut_csn);
        state.updated_at_unix_secs = unix_secs();
        Ok(state)
    }

    pub fn record_backup_manifest_commit(
        &self,
        counter: u64,
        cut_csn: u64,
    ) -> StorageResult<LastStoreHighWater> {
        let _guard = self.lock_state();
        let mut state = self.load_or_init_unlocked()?;
        if counter > state.backup_manifest_counter {
            state.backup_manifest_counter = counter;
        }
        if cut_csn > state.csn_high_water {
            state.csn_high_water = cut_csn;
        }
        let now = unix_secs();
        state.updated_at_unix_secs = now;
        // The one event that proves data reached the cloud. Stamped here and in
        // `record_restored_backup_manifest` only — never on a local write or an
        // epoch bump, or the age this feeds becomes a lie.
        state.last_backup_commit_unix_secs = Some(now);
        // A landed commit is the proof that a publishable base exists again.
        state.sealed_base_abandoned_unix_secs = None;
        self.write(&state)?;
        Ok(state)
    }

    /// Record that the held sealed-home cut was abandoned as unpublishable.
    ///
    /// Stamped by the demoted continuous publisher when residual
    /// `source_missing` survives the cloud-presence heal and the hold is
    /// released. Deliberately durable: the abandon is otherwise invisible to
    /// the next process, which reads only the (older) commit timestamp and
    /// reports the home as backed up.
    pub fn record_sealed_base_abandoned(&self) -> StorageResult<LastStoreHighWater> {
        let _guard = self.lock_state();
        let mut state = self.load_or_init_unlocked()?;
        let now = unix_secs();
        state.sealed_base_abandoned_unix_secs = Some(now);
        state.updated_at_unix_secs = now;
        self.write(&state)?;
        Ok(state)
    }

    /// Raise `backup_manifest_counter` to at least `counter` after observing
    /// cloud `backup/latest`. Does **not** stamp `last_backup_commit_unix_secs`
    /// — that field is only for a land we committed or a restore we proved.
    ///
    /// Used when a CoW of a lower local high-water is aimed at a bucket that
    /// already holds a different tip at the next local counter (`stale_counter`).
    pub fn observe_cloud_backup_counter(&self, counter: u64) -> StorageResult<LastStoreHighWater> {
        let _guard = self.lock_state();
        let mut state = self.load_or_init_unlocked()?;
        if counter > state.backup_manifest_counter {
            state.backup_manifest_counter = counter;
            state.updated_at_unix_secs = unix_secs();
            self.write(&state)?;
        }
        Ok(state)
    }

    /// Raise the publisher epoch so a subsequent cut can CAS-rebind `latest`
    /// after a cloud `store_uuid_mismatch` (server allows rebind only when
    /// candidate.epoch > cloud.epoch). Does not change store_uuid or counter.
    pub fn ensure_backup_epoch_at_least(
        &self,
        min_epoch: u64,
    ) -> StorageResult<LastStoreHighWater> {
        let _guard = self.lock_state();
        let mut state = self.load_or_init_unlocked()?;
        let target = min_epoch.max(1);
        if target > state.backup_epoch {
            state.backup_epoch = target;
            state.updated_at_unix_secs = unix_secs();
            self.write(&state)?;
        }
        Ok(state)
    }

    pub fn validate_restore_candidate(
        &self,
        store_uuid: &str,
        epoch: u64,
        counter: u64,
        cut_csn: u64,
    ) -> StorageResult<LastStoreHighWater> {
        let _guard = self.lock_state();
        let state = self.load_or_init_unlocked()?;
        self.validate_restore_candidate_state(&state, store_uuid, epoch, counter, cut_csn)?;
        Ok(state)
    }

    fn validate_restore_candidate_state(
        &self,
        state: &LastStoreHighWater,
        store_uuid: &str,
        epoch: u64,
        counter: u64,
        cut_csn: u64,
    ) -> StorageResult<()> {
        if counter < state.backup_manifest_counter {
            return Err(self.error(format_args!(
                "backup restore rollback refused: manifest counter {counter} < local high-water {}",
                state.backup_manifest_counter
            )));
        }
        if cut_csn < state.csn_high_water {
            return Err(self.error(format_args!(
                "backup restore CSN gap refused: manifest cut_csn {cut_csn} < local high-water {}",
                state.csn_high_water
            )));
        }
        if state.backup_manifest_counter > 0 && state.store_uuid != store_uuid {
            return Err(self.error(format_args!(
                "backup restore fork fence refused: manifest store_uuid {store_uuid} != local {}",
                state.store_uuid
            )));
        }
        if state.backup_manifest_counter > 0 && epoch > state.backup_epoch {
            return Err(self.error(format_args!(
                "backup restore foreign epoch refused: manifest epoch {epoch} > local {}",
                state.backup_epoch
            )));
        }
        Ok(())
    }

    pub fn record_restored_backup_manifest(
        &self,
        store_uuid: &str,
        epoch: u64,
        counter: u64,
        cut_csn: u64,
    ) -> StorageResult<LastStoreHighWater> {
        let _guard = self.lock_state();
        let mut state = self.load_or_init_unlocked()?;
        self.validate_restore_candidate_state(&state, store_uuid, epoch, counter, cut_csn)?;
        state.store_uuid = store_uuid.to_string();
        state.backup_manifest_counter = counter;
        state.csn_high_water = state.csn_high_water.max(cut_csn);
        state.backup_epoch = epoch.saturating_add(1).max(1);
        let now = unix_secs();
        state.updated_at_unix_secs = now;
        // A restore just proved the cloud copy round-trips, so the home is
        // covered as of now. Without this, a restored home reports "never
        // backed up" until its first fresh publish cycle.
        state.last_backup_commit_unix_secs = Some(now);
        // The restored copy round-trips, so any earlier abandon is history.
        state.sealed_base_abandoned_unix_secs = None;
        self.write(&state)?;
        Ok(state)
    }

    fn write(&self, state: &LastStoreHighWater) -> StorageResult<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| self.error(format_args!("create dir failed: {e}")))?;
        }
        let bytes =
            serde_json::to_vec_pretty(state).map_err(|e| self.error(format_args!("{e}")))?;
        let tmp = self.path.with_file_name(format!(
            ".{}.tmp-{}",
            self.path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("laststore_high_water.json"),
            std::process::id()
        ));
        let _ = fs::remove_file(&tmp);
        write_owner_only(&tmp, &bytes)
            .map_err(|e| self.error(format_args!("write tmp failed: {e}")))?;
        fs::rename(&tmp, &self.path).map_err(|e| self.error(format_args!("rename failed: {e}")))?;
        Ok(())
    }

    fn error(&self, message: std::fmt::Arguments<'_>) -> StorageError {
        StorageError::BackendError(format!(
            "laststore high-water {}: {message}",
            self.path.display()
        ))
    }
}

fn write_owner_only(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}
