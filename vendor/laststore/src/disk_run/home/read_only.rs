//! A stopped-home snapshot with no writer, creation, or crash cleanup.

use super::{manifest::Manifest, read_descriptor, RunRead, RunReadCosts};
use crate::disk_pack::ReadBudget;
use crate::{Error, Result};
use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::Arc;

impl RunRead {
    /// A new bounded call at this same held CURRENT, without another open or
    /// cleanup. The owner lock stays held across both calls.
    pub fn fork(&self, budget: ReadBudget) -> Self {
        Self {
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

    /// Open one existing CURRENT without the cleanup performed by RunHome::open.
    /// The existing owner lock excludes writers for the lifetime of this read.
    /// Missing descriptor, CURRENT, or LOCK is an error, never initialization.
    pub fn open_existing(root: &Path, key: &[u8; 32], budget: ReadBudget) -> Result<Self> {
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
        let manifest = Arc::new(Manifest::load(&root, key, identity)?);
        let runs = manifest
            .newest_first()
            .map(|run| (Arc::clone(run), None))
            .collect();
        Ok(Self {
            manifest,
            runs,
            key: *key,
            budget,
            costs: RunReadCosts::default(),
            _owner_lock: Arc::new(lock),
        })
    }
}
