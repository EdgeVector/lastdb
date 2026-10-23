//! Exact private output protection. No registry mutex spans filesystem I/O.
use super::corrupt;
use crate::Result;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub(super) struct FileRegistry(Mutex<State>);

#[derive(Default)]
struct State {
    epoch: u64,
    protected: BTreeMap<PathBuf, usize>,
    deleting: BTreeSet<PathBuf>,
}

struct Protection {
    registry: Arc<FileRegistry>,
    path: PathBuf,
}

/// A private operation retains exact files and the directories its allocators use.
#[derive(Clone, Default)]
pub(super) struct OutputFiles(Option<Arc<Owner>>);

struct Owner {
    registry: Arc<FileRegistry>,
    paths: Mutex<OwnedPaths>,
}

#[derive(Default)]
struct OwnedPaths {
    entries: BTreeMap<PathBuf, Protection>,
    charged: usize,
}

/// Transient path metadata per copy; cache and body budgets remain separate.
const MAX_PATH_BYTES: usize = 64 * 1024 * 1024;
pub(super) const ACTIVE_DELETION: &str =
    "private output path has an active deletion; retry allocation";

#[derive(Clone)]
pub(super) struct DeletionEpoch {
    registry: Arc<FileRegistry>,
    number: u64,
}

pub(super) struct Deletion {
    registry: Arc<FileRegistry>,
    path: PathBuf,
}

impl OutputFiles {
    pub(super) fn new(registry: Arc<FileRegistry>) -> Self {
        Self(Some(Arc::new(Owner {
            registry,
            paths: Mutex::default(),
        })))
    }

    pub(super) fn protect_directory(&self, path: &Path) -> Result<()> {
        self.protect_path(path)
    }

    pub(super) fn protect_path(&self, path: &Path) -> Result<()> {
        if let Some(owner) = &self.0 {
            let mut protection = Some(owner.registry.protect(path)?);
            owner.keep(&mut protection)?;
        }
        Ok(())
    }

    pub(super) fn create_new(&self, path: &Path) -> Result<File> {
        let mut protection = self
            .0
            .as_ref()
            .map(|owner| owner.registry.protect(path))
            .transpose()?;
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        if let Some(owner) = &self.0 {
            if let Err(error) = owner.keep(&mut protection) {
                // The local guard stays active through close and exact unlink.
                // Never leave an empty file after a path-budget refusal.
                drop(file);
                std::fs::remove_file(path)?;
                return Err(error);
            }
        }
        Ok(file)
    }
}

impl Owner {
    fn keep(&self, protection: &mut Option<Protection>) -> Result<()> {
        let held = protection.as_ref().expect("private path protection");
        let mut paths = self
            .paths
            .lock()
            .map_err(|_| corrupt("private output owner is poisoned"))?;
        if !paths.entries.contains_key(&held.path) {
            // Count duplicated path storage and conservative tree-node overhead.
            let charge = held
                .path
                .as_os_str()
                .len()
                .saturating_mul(3)
                .saturating_add(256);
            let total = paths.charged.checked_add(charge).filter(|n| *n <= MAX_PATH_BYTES)
                .ok_or_else(|| crate::Error::WriteCapacity("compaction_behind: private path metadata budget exhausted; retry maintenance".into()))?;
            paths.charged = total;
        }
        paths.entries.insert(
            held.path.clone(),
            protection.take().expect("private path protection"),
        );
        Ok(())
    }
}

impl FileRegistry {
    pub(super) fn epoch(self: &Arc<Self>) -> Result<DeletionEpoch> {
        let number = self
            .0
            .lock()
            .map_err(|_| corrupt("file protection is poisoned"))?
            .epoch;
        Ok(DeletionEpoch {
            registry: Arc::clone(self),
            number,
        })
    }

    /// Invalidate captured as well as executing plans before a root can change.
    pub(super) fn invalidate(&self) -> Result<()> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| corrupt("file protection is poisoned"))?;
        state.epoch = state
            .epoch
            .checked_add(1)
            .ok_or_else(|| corrupt("cleanup epoch exhaustion"))?;
        Ok(())
    }

    fn protect(self: &Arc<Self>, path: &Path) -> Result<Protection> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| corrupt("file protection is poisoned"))?;
        if path
            .ancestors()
            .any(|parent| state.deleting.contains(parent))
        {
            return Err(crate::Error::WriteCapacity(ACTIVE_DELETION.into()));
        }
        let count = state.protected.entry(path.to_path_buf()).or_default();
        *count = count.checked_add(1).ok_or_else(super::resource)?;
        Ok(Protection {
            registry: Arc::clone(self),
            path: path.to_path_buf(),
        })
    }

    fn delete(
        self: &Arc<Self>,
        path: &Path,
        directory: bool,
        epoch: u64,
    ) -> Result<Option<Deletion>> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| corrupt("file protection is poisoned"))?;
        let protected = state.protected.contains_key(path)
            || (directory
                && state
                    .protected
                    .range(path.to_path_buf()..)
                    .next()
                    .is_some_and(|(held, _)| held.starts_with(path)));
        if epoch != state.epoch
            || protected
            || path
                .ancestors()
                .any(|parent| state.deleting.contains(parent))
        {
            return Ok(None);
        }
        state.deleting.insert(path.to_path_buf());
        Ok(Some(Deletion {
            registry: Arc::clone(self),
            path: path.to_path_buf(),
        }))
    }
}

impl DeletionEpoch {
    pub(super) fn valid(&self) -> Result<bool> {
        Ok(self
            .registry
            .0
            .lock()
            .map_err(|_| corrupt("file protection is poisoned"))?
            .epoch
            == self.number)
    }

    pub(super) fn delete(&self, path: &Path, directory: bool) -> Result<Option<Deletion>> {
        self.registry.delete(path, directory, self.number)
    }
}

impl Drop for Protection {
    fn drop(&mut self) {
        if let Ok(mut state) = self.registry.0.lock() {
            if let Some(count) = state.protected.get_mut(&self.path) {
                *count -= 1;
                if *count == 0 {
                    state.protected.remove(&self.path);
                }
            }
        }
    }
}

impl Drop for Deletion {
    fn drop(&mut self) {
        if let Ok(mut state) = self.registry.0.lock() {
            state.deleting.remove(&self.path);
        }
    }
}
