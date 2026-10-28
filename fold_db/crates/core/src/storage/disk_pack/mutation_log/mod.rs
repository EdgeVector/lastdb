//! Optional native mutation log. Durable commit bytes remain native encrypted files.
mod capture;
mod model;
mod recording;
mod retention;
mod store;

use super::{physical::V2Home, PackRuntime, StorageError, StorageResult};
pub use capture::{
    log_path, FrozenLogCapture, FrozenLogFile, MutationLogCapture, MutationLogReader,
};
use cloud_backup_protocol::Id;
pub use model::{CommitRole, Frontiers, LogAnchor, LogEntry, LogPart, LogPosition};
use model::{EpochState, Index};
pub(super) use recording::Recording;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};

fn invalid(reason: &str) -> StorageError {
    StorageError::InvalidOperation(reason.to_owned())
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct MutationLogStatus {
    pub enabled: bool,
    pub anchor: Option<LogAnchor>,
    pub head: Option<LogPosition>,
    pub acknowledged_through: u64,
    pub retained_bytes: u64,
    pub metadata_spool_bytes: u64,
    pub snapshot_required: bool,
    pub gap: Option<String>,
}

#[derive(Default)]
struct Cache {
    loaded: bool,
    index: Option<Index>,
}
pub(super) struct MutationLog {
    root: PathBuf,
    main_home: Id,
    cas_home: Id,
    cache: Mutex<Cache>,
}
impl MutationLog {
    pub fn new(v2: &V2Home) -> Self {
        Self {
            root: v2.home.path().join("mutation-log-v1"),
            main_home: Id::try_from(v2.home.identity().to_string()).expect("native main UUID"),
            cas_home: Id::try_from(v2.cas.identity().to_string()).expect("native CAS UUID"),
            cache: Mutex::default(),
        }
    }

    fn lock(&self) -> StorageResult<MutexGuard<'_, Cache>> {
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| invalid("mutation log owner needs attention"))?;
        if !cache.loaded {
            cache.index = store::read(&self.root.join("index.json"))?;
            if let Some(index) = &cache.index {
                self.validate_index(index)?;
            }
            cache.loaded = true;
        }
        Ok(cache)
    }

    fn validate_index(&self, index: &Index) -> StorageResult<()> {
        if index.version != 1
            || index.main_home != self.main_home
            || index.cas_home != self.cas_home
            || index.epochs.len() + index.cleanup.len() > store::MAX_EPOCHS
            || index
                .active
                .as_ref()
                .is_some_and(|id| !index.epochs.contains(id))
            || index
                .epochs
                .iter()
                .enumerate()
                .any(|(n, id)| index.epochs[..n].contains(id))
            || index
                .cleanup
                .iter()
                .enumerate()
                .any(|(n, id)| index.epochs.contains(id) || index.cleanup[..n].contains(id))
        {
            return Err(invalid("mutation log owner identities or resource bounds"));
        }
        Ok(())
    }

    fn state(&self, id: &Id) -> StorageResult<EpochState> {
        let state: EpochState = store::read(&store::epoch(&self.root, id).join("state.json"))?
            .ok_or_else(|| invalid("mutation log epoch is absent"))?;
        state.validate()?;
        if state.anchor.epoch != *id
            || state.anchor.main_home != self.main_home
            || state.anchor.cas_home != self.cas_home
        {
            return Err(invalid("mutation log epoch owner"));
        }
        Ok(state)
    }

    fn totals(&self, index: &Index) -> StorageResult<(u64, u64)> {
        index
            .epochs
            .iter()
            .try_fold((0u64, 0u64), |(payload, metadata), id| {
                let state = self.state(id)?;
                let pending: Option<LogEntry> = if state.head.sequence < store::MAX_ENTRIES {
                    store::read(
                        &store::entry(&self.root, id, state.head.sequence + 1)?
                            .join("pending.json"),
                    )?
                } else {
                    None
                };
                let extra = pending
                    .as_ref()
                    .map(LogEntry::bytes)
                    .transpose()?
                    .unwrap_or(0);
                let payload = payload
                    .checked_add(state.retained_bytes)
                    .and_then(|n| n.checked_add(extra))
                    .ok_or_else(|| invalid("mutation log retained size overflow"))?;
                let extra_metadata = pending
                    .map(|entry| {
                        cloud_backup_protocol::v2::hash::canonical_bytes(&entry)
                            .map(|bytes| bytes.len() as u64)
                            .map_err(|_| invalid("mutation log metadata encoding"))
                    })
                    .transpose()?
                    .unwrap_or(0);
                let metadata = metadata
                    .checked_add(state.metadata_bytes)
                    .and_then(|n| n.checked_add(extra_metadata))
                    .ok_or_else(|| invalid("mutation log metadata size overflow"))?;
                Ok((payload, metadata))
            })
    }

    fn charge(&self, index: &Index) -> StorageResult<u64> {
        let (payload, metadata) = self.totals(index)?;
        payload
            .checked_add(metadata)
            .ok_or_else(|| invalid("mutation log total size overflow"))
    }

    pub fn configure(&self, enabled: bool) -> StorageResult<()> {
        let mut cache = self.lock()?;
        if cache.index.is_none() {
            if !enabled {
                return Ok(());
            }
            if !self.root.try_exists()? {
                store::create(&self.root)?;
            }
            cache.index = Some(Index {
                version: 1,
                enabled: false,
                main_home: self.main_home.clone(),
                cas_home: self.cas_home.clone(),
                epochs: Vec::new(),
                active: None,
                cleanup: Vec::new(),
            });
        }
        let index = cache
            .index
            .as_mut()
            .ok_or_else(|| invalid("mutation log policy"))?;
        let mut saved = index.clone();
        saved.enabled = enabled;
        store::write(&self.root.join("index.json"), &saved)?;
        *index = saved;
        Ok(())
    }

    /// The caller holds the native write gate across this anchor and the paired snapshot.
    pub fn snapshot_anchor(&self, v2: &V2Home) -> StorageResult<Option<LogAnchor>> {
        let mut cache = self.lock()?;
        let Some(index) = cache.index.as_mut() else {
            return Ok(None);
        };
        if !index.enabled {
            return Ok(None);
        }
        if index.epochs.len() + index.cleanup.len() >= store::MAX_EPOCHS {
            return Err(invalid("mutation log retained epoch limit"));
        }
        let epoch = Id::try_from(uuid::Uuid::new_v4().to_string())
            .map_err(|_| invalid("mutation log UUID"))?;
        let anchor = LogAnchor {
            version: 1,
            epoch: epoch.clone(),
            main_home: self.main_home.clone(),
            cas_home: self.cas_home.clone(),
            frontiers: frontiers(v2),
        };
        let state = EpochState {
            head: anchor.position()?,
            released: anchor.position()?,
            anchor: anchor.clone(),
            acknowledged: 0,
            retained_bytes: 0,
            metadata_bytes: 0,
            gap: None,
        };
        let directory = store::epoch(&self.root, &epoch);
        store::create(&directory)?;
        store::write(&directory.join("state.json"), &state)?;
        let mut saved = index.clone();
        saved.epochs.push(epoch.clone());
        saved.active = Some(epoch);
        store::write(&self.root.join("index.json"), &saved)?;
        *index = saved;
        Ok(Some(anchor))
    }

    pub fn status(&self, v2: &V2Home) -> StorageResult<MutationLogStatus> {
        let cache = self.lock()?;
        let Some(index) = &cache.index else {
            return Ok(MutationLogStatus {
                enabled: false,
                anchor: None,
                head: None,
                acknowledged_through: 0,
                retained_bytes: 0,
                metadata_spool_bytes: 0,
                snapshot_required: true,
                gap: None,
            });
        };
        let state = index.active.as_ref().map(|id| self.state(id)).transpose()?;
        let gap = state
            .as_ref()
            .and_then(|state| state.gap.clone())
            .or_else(|| {
                state
                    .as_ref()
                    .filter(|state| state.head.frontiers != frontiers(v2))
                    .map(|_| "native frontier exceeds the complete mutation log".to_owned())
            });
        let (retained_bytes, metadata_spool_bytes) = self.totals(index)?;
        Ok(MutationLogStatus {
            enabled: index.enabled,
            anchor: state.as_ref().map(|s| s.anchor.clone()),
            head: state.as_ref().map(|s| s.head.clone()),
            acknowledged_through: state.as_ref().map_or(0, |s| s.acknowledged),
            retained_bytes,
            metadata_spool_bytes,
            snapshot_required: state.is_none() || gap.is_some(),
            gap,
        })
    }
}

fn frontiers(v2: &V2Home) -> Frontiers {
    Frontiers {
        main: v2.home.current_root().frontier(),
        cas: v2.cas.current_root().frontier(),
    }
}

impl PackRuntime {
    pub fn configure_mutation_log(&self, enabled: bool) -> StorageResult<()> {
        self.mutation_log.configure(enabled)
    }
    pub fn mutation_log_status(&self) -> StorageResult<MutationLogStatus> {
        self.mutation_log.status(&self.physical)
    }
    pub fn mutation_log_entry(&self, epoch: &Id, sequence: u64) -> StorageResult<LogEntry> {
        let entry: LogEntry = store::read(
            &store::entry(&self.mutation_log.root, epoch, sequence)?.join("entry.json"),
        )?
        .ok_or_else(|| invalid("complete mutation log entry is absent"))?;
        if entry.epoch != *epoch || entry.sequence != sequence {
            return Err(invalid("mutation log entry identity"));
        }
        Ok(entry)
    }
}
