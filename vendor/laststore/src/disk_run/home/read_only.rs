//! A stopped-home snapshot with no writer, creation, or crash cleanup.

use super::{manifest::Manifest, read_descriptor, RunRead, RunReadCosts};
use crate::disk_pack::ReadBudget;
use crate::{Error, Result};
use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::Arc;

pub(super) type ReadSlot = (
    Arc<super::manifest::RunFile>,
    Option<crate::disk_run::RunReader>,
);

pub(super) fn slots(manifest: &Manifest, maximum: u64) -> Result<Vec<ReadSlot>> {
    let count = manifest.newest_first().count();
    let bytes = count
        .checked_mul(std::mem::size_of::<ReadSlot>())
        .ok_or(Error::ReadBudgetExceeded)?;
    if bytes as u64 > maximum {
        return Err(Error::ReadBudgetExceeded);
    }
    let mut runs = Vec::new();
    runs.try_reserve_exact(count)
        .map_err(|_| Error::ReadBudgetExceeded)?;
    if (runs.capacity() * std::mem::size_of::<ReadSlot>()) as u64 > maximum {
        return Err(Error::ReadBudgetExceeded);
    }
    for run in manifest.newest_first() {
        runs.push((Arc::clone(run), None));
    }
    Ok(runs)
}

impl RunRead {
    /// A new bounded call at this same held CURRENT, without another open or
    /// cleanup. The owner lock stays held across both calls.
    pub fn fork(&self, budget: ReadBudget) -> Self {
        Self {
            bounded_streams: self.bounded_streams,
            shared_metadata_bytes: self
                .shared_metadata_bytes
                .max(self.retained_bytes())
                .saturating_add(
                    (self.runs.len()
                        * std::mem::size_of::<(
                            Arc<super::manifest::RunFile>,
                            Option<crate::disk_run::RunReader>,
                        )>()) as u64,
                ),
            manifest: Arc::clone(&self.manifest),
            runs: self
                .manifest
                .newest_first()
                .map(|run| (Arc::clone(run), None))
                .collect(),
            key: self.key,
            budget,
            costs: RunReadCosts::default(),
            _owner_lock: Arc::clone(&self._owner_lock),
        }
    }
    /// Fork the exact held root with preallocation-bounded backup reads enabled.
    pub fn fork_bounded(&self, budget: ReadBudget) -> Result<Self> {
        let retained = self.shared_metadata_bytes.max(self.retained_bytes());
        let available = lastdb_storage_policy::BACKUP_RESOURCES
            .working_memory_bytes
            .checked_sub(retained)
            .ok_or(Error::ReadBudgetExceeded)?;
        let runs = slots(&self.manifest, available)?;
        let extra = (runs.capacity() * std::mem::size_of::<ReadSlot>()) as u64;
        Ok(Self {
            bounded_streams: true,
            shared_metadata_bytes: retained + extra,
            manifest: Arc::clone(&self.manifest),
            runs,
            key: self.key,
            budget,
            costs: RunReadCosts::default(),
            _owner_lock: Arc::clone(&self._owner_lock),
        })
    }

    /// Open one existing CURRENT without the cleanup performed by RunHome::open.
    /// The existing owner lock excludes writers for the lifetime of this read.
    /// Missing descriptor, CURRENT, or LOCK is an error, never initialization.
    pub fn open_existing(root: &Path, key: &[u8; 32], budget: ReadBudget) -> Result<Self> {
        Self::open_existing_inner(root, key, budget, None)
    }

    /// Open an inactive root with explicit combined metadata-memory admission.
    pub fn open_existing_bounded(
        root: &Path,
        key: &[u8; 32],
        budget: ReadBudget,
        maximum: u64,
    ) -> Result<Self> {
        Self::open_existing_inner(root, key, budget, Some(maximum))
    }
    fn open_existing_inner(
        root: &Path,
        key: &[u8; 32],
        budget: ReadBudget,
        maximum: Option<u64>,
    ) -> Result<Self> {
        let root = root.canonicalize()?;
        let lock = OpenOptions::new()
            .read(true)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
            .open(root.join("LOCK"))?;
        if !lock.metadata()?.is_file() {
            return Err(Error::Config(
                "disk run snapshot: LOCK is not a file".into(),
            ));
        }
        rustix::fs::flock(&lock, rustix::fs::FlockOperation::NonBlockingLockExclusive)
            .map_err(|_| Error::Config("disk run snapshot: another owner holds the home".into()))?;
        let identity = read_descriptor(&root)?;
        let manifest = Arc::new(match maximum {
            Some(bytes) => Manifest::load_paths_bounded(
                &root.join("CURRENT"),
                &root.join("runs"),
                key,
                identity,
                bytes,
            )?,
            None => Manifest::load(&root, key, identity)?,
        });
        let available = maximum
            .unwrap_or(u64::MAX)
            .checked_sub(manifest.retained_bytes())
            .ok_or(Error::ReadBudgetExceeded)?;
        let runs = slots(&manifest, available)?;
        Ok(Self {
            bounded_streams: maximum.is_some(),
            shared_metadata_bytes: 0,
            manifest,
            runs,
            key: *key,
            budget,
            costs: RunReadCosts::default(),
            _owner_lock: Arc::new(lock),
        })
    }
}
