//! Commit and CURRENT publication locks fail closed when poisoned.
//!
//! A poisoned mutex means its holder panicked inside the update. The guard is
//! not recovered: installing or deleting from that state can publish a
//! half-applied CURRENT. The home is marked poisoned so a later error does
//! not delete runs the rename may already name.

use super::manifest::Manifest;
use super::RunHome;
use crate::{Error, Result};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, MutexGuard};

impl RunHome {
    /// Commit gate. The critical section runs only when no commit holder panicked.
    pub(super) fn commit_lock(&self) -> Result<MutexGuard<'_, ()>> {
        self.exclusive(&self.commit)
    }

    /// Publication gate. The critical section runs only when no publisher panicked.
    pub(super) fn publish_lock(&self) -> Result<MutexGuard<'_, ()>> {
        self.exclusive(&self.publish)
    }

    fn exclusive<'a>(&self, lock: &'a Mutex<()>) -> Result<MutexGuard<'a, ()>> {
        lock.lock().map_err(|_| self.poisoned_lock())
    }

    /// Install the manifest whose durable CURRENT rename already succeeded.
    pub(super) fn install_current(&self, next: &Arc<Manifest>) -> Result<()> {
        let mut current = self.current.write().map_err(|_| self.poisoned_lock())?;
        *current = Arc::clone(next);
        Ok(())
    }

    fn poisoned_lock(&self) -> Error {
        self.poisoned.store(true, Ordering::Release);
        Error::WriteCapacity(
            "disk run home: a lock was poisoned; restart the process to reopen the home".into(),
        )
    }
}
