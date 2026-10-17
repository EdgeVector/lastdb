use crate::disk_pack::write_lock::WriteLock;
use crate::disk_pack::{corrupt, Current};
use crate::Result;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// A read-only capability from the exclusive writer. It retains that writer's
/// operating-system lease, including when a read outlives the runtime owner.
#[derive(Clone)]
pub struct WriterReadLease {
    lock: Arc<WriteLock>,
}

#[derive(Default)]
pub(in crate::disk_pack) struct WriterReadState {
    readers: usize,
    reclaiming: bool,
    minimum_generation: u64,
}

impl WriterReadState {
    pub(in crate::disk_pack) fn new(minimum_generation: u64) -> Self {
        Self {
            minimum_generation,
            ..Self::default()
        }
    }
}

pub(super) struct WriterReadGuard {
    lock: Arc<WriteLock>,
}

/// Excludes new in-process reads until every possible deletion ends. The floor
/// remains advanced on error because an interrupted reclaim can remove files.
pub(in crate::disk_pack) struct ReclaimExclusion<'a> {
    state: &'a Mutex<WriterReadState>,
    minimum_generation: u64,
}

impl WriterReadLease {
    pub(in crate::disk_pack) fn new(lock: Arc<WriteLock>) -> Self {
        Self { lock }
    }

    pub(super) fn hold(&self, root: &Path, current: &Current) -> Result<WriterReadGuard> {
        if self.lock.root() != root {
            return Err(corrupt("writer read lease belongs to another root"));
        }
        current.encode()?;
        let mut state = self
            .lock
            .read_state
            .lock()
            .map_err(|_| corrupt("writer read lifetime lock is poisoned"))?;
        if state.reclaiming || current.generation < state.minimum_generation {
            return Err(corrupt("writer read root is retired or reclaim is active"));
        }
        state.readers = state
            .readers
            .checked_add(1)
            .ok_or_else(super::super::resource)?;
        Ok(WriterReadGuard {
            lock: Arc::clone(&self.lock),
        })
    }
}

impl WriteLock {
    pub(in crate::disk_pack) fn exclude_local_reads(
        &self,
        current: &Current,
    ) -> Result<Option<ReclaimExclusion<'_>>> {
        let mut state = self
            .read_state
            .lock()
            .map_err(|_| corrupt("writer read lifetime lock is poisoned"))?;
        if state.readers != 0 || state.reclaiming {
            return Ok(None);
        }
        state.reclaiming = true;
        Ok(Some(ReclaimExclusion {
            state: &self.read_state,
            minimum_generation: current.generation,
        }))
    }
}

impl Drop for WriterReadGuard {
    fn drop(&mut self) {
        // Poisoning leaves the writer fail-closed: all later holds/reclaims fail.
        if let Ok(mut state) = self.lock.read_state.lock() {
            state.readers -= 1;
        }
    }
}

impl Drop for ReclaimExclusion<'_> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock() {
            state.minimum_generation = state.minimum_generation.max(self.minimum_generation);
            state.reclaiming = false;
        }
    }
}
