use crate::disk_pack::write_lock::WriteLock;
use crate::disk_pack::{corrupt, Current};
use crate::Result;
use std::collections::{BTreeMap, BTreeSet};
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
    /// Active local reads, counted for each generation that a read holds.
    readers: BTreeMap<u64, usize>,
    reclaiming: bool,
    allow_current_reads: bool,
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
    generation: u64,
}

/// Excludes new in-process reads until every possible deletion ends. The floor
/// remains advanced on error because an interrupted reclaim can remove files.
///
/// A read that already runs does not block reclaim. It holds one generation,
/// and `held` names every such generation. Reclaim keeps the whole closure of
/// each one, so the read finds every file that its own receipt names.
pub(in crate::disk_pack) struct ReclaimExclusion<'a> {
    state: &'a Mutex<WriterReadState>,
    minimum_generation: u64,
    held: BTreeSet<u64>,
}

impl ReclaimExclusion<'_> {
    /// Generations that active local reads hold now.
    pub(in crate::disk_pack) fn held(&self) -> &BTreeSet<u64> {
        &self.held
    }
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
        if (state.reclaiming && !state.allow_current_reads)
            || current.generation < state.minimum_generation
        {
            return Err(corrupt("writer read root is retired or reclaim is active"));
        }
        let count = state.readers.entry(current.generation).or_insert(0);
        *count = count.checked_add(1).ok_or_else(super::super::resource)?;
        Ok(WriterReadGuard {
            lock: Arc::clone(&self.lock),
            generation: current.generation,
        })
    }

    pub(super) fn hold_reference(
        &self,
        reference: &super::DurableReference,
    ) -> Result<WriterReadGuard> {
        if self.lock.root() != reference.home_path() {
            return Err(corrupt("writer read lease belongs to another root"));
        }
        // File protection bridges reference validation and local registration.
        // Never hold the state mutex across this filesystem work.
        let retained = reference.hold()?;
        let generation = retained.current().generation;
        let mut state = self
            .lock
            .read_state
            .lock()
            .map_err(|_| corrupt("writer read lifetime lock is poisoned"))?;
        if state.reclaiming {
            return Err(corrupt("writer read root is retired or reclaim is active"));
        }
        // Only the verified durable reference permits a root below the floor.
        let count = state.readers.entry(generation).or_insert(0);
        *count = count.checked_add(1).ok_or_else(super::super::resource)?;
        let guard = WriterReadGuard {
            lock: Arc::clone(&self.lock),
            generation,
        };
        drop(state);
        drop(retained);
        Ok(guard)
    }
}

impl WriteLock {
    /// Freeze the old-read set, then admit only installed or newer roots.
    /// Older active reads retain their complete generation through `held`.
    pub(in crate::disk_pack) fn retire_before(
        &self,
        current: &Current,
    ) -> Result<Option<ReclaimExclusion<'_>>> {
        let mut state = self
            .read_state
            .lock()
            .map_err(|_| corrupt("writer read lifetime lock is poisoned"))?;
        if state.reclaiming || current.generation < state.minimum_generation {
            return Ok(None);
        }
        state.minimum_generation = current.generation;
        state.reclaiming = true;
        state.allow_current_reads = true;
        Ok(Some(ReclaimExclusion {
            state: &self.read_state,
            minimum_generation: current.generation,
            held: state.readers.keys().copied().collect(),
        }))
    }

    pub(in crate::disk_pack) fn exclude_local_reads(
        &self,
        current: &Current,
    ) -> Result<Option<ReclaimExclusion<'_>>> {
        let mut state = self
            .read_state
            .lock()
            .map_err(|_| corrupt("writer read lifetime lock is poisoned"))?;
        if state.reclaiming {
            return Ok(None);
        }
        state.reclaiming = true;
        Ok(Some(ReclaimExclusion {
            state: &self.read_state,
            minimum_generation: current.generation,
            held: state.readers.keys().copied().collect(),
        }))
    }
}

impl Drop for WriterReadGuard {
    fn drop(&mut self) {
        // Poisoning leaves the writer fail-closed: all later holds/reclaims fail.
        if let Ok(mut state) = self.lock.read_state.lock() {
            if let Some(count) = state.readers.get_mut(&self.generation) {
                *count -= 1;
                if *count == 0 {
                    state.readers.remove(&self.generation);
                }
            }
        }
    }
}

impl Drop for ReclaimExclusion<'_> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock() {
            state.minimum_generation = state.minimum_generation.max(self.minimum_generation);
            state.reclaiming = false;
            state.allow_current_reads = false;
        }
    }
}
