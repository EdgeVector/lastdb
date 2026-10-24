//! Temporary exact locator changes while an off-gate replacement prepares.
//! No payload, file descriptor, page cache, or disk recovery log is retained.
use super::PreparedMutation;
use crate::disk_pack::lifetime::{GenerationHold, WriterReadLease};
use crate::disk_pack::{Current, PublishedGeneration};
use crate::Result;
use std::path::Path;
use std::sync::{Arc, Mutex};

const MAX_CAPTURES: usize = 2;
const MAX_OPERATIONS: usize = 4096;
const MAX_BYTES: usize = 16 * 1024 * 1024;

#[derive(Default)]
pub(super) struct ChangeCaptures(Arc<Mutex<Vec<Arc<ChangeCapture>>>>);

/// Invoke outside the publication boundary. The registry remains a strong
/// owner so an in-gate temporary reference can never free a capture's arrays.
#[derive(Clone)]
pub struct CaptureCleanup(Arc<Mutex<Vec<Arc<ChangeCapture>>>>);

/// A registration returns idle predecessors instead of dropping them in-gate.
pub struct RetiredCaptures {
    _captures: Vec<Arc<ChangeCapture>>,
}

pub(in crate::disk_pack) struct ChangeCapture {
    start: Current,
    state: Mutex<CaptureState>,
}

struct CaptureState {
    last: Current,
    batches: Vec<Arc<CapturedBatch>>,
    operations: usize,
    bytes: usize,
    failure: Option<String>,
    sealed: bool,
}

pub(in crate::disk_pack) struct CapturedBatch {
    mutations: Vec<PreparedMutation>,
    // A later physical rewrite can move a location without changing its value.
    // Keep its original complete source until the offered output drops.
    _hold: GenerationHold,
}

pub(in crate::disk_pack) struct CapturedChanges {
    pub(in crate::disk_pack) mutations: Vec<PreparedMutation>,
    pub(in crate::disk_pack) batches: Vec<Arc<CapturedBatch>>,
}

pub(super) struct PreparedChanges {
    mutations: Vec<PreparedMutation>,
    bytes: usize,
}

pub(super) struct CapturedPublication {
    pub(super) publication: PublishedGeneration,
    pub(super) changes: Option<PreparedChanges>,
}

impl ChangeCaptures {
    pub(super) fn register(
        &self,
        current: &Current,
    ) -> Result<(Arc<ChangeCapture>, RetiredCaptures)> {
        let mut entries = self
            .0
            .lock()
            .map_err(|_| behind("capture registry unavailable"))?;
        if entries
            .iter()
            .filter(|entry| Arc::strong_count(entry) > 1)
            .count()
            >= MAX_CAPTURES
        {
            return Err(behind("two exact change captures already exist"));
        }
        let retired = take_idle(&mut entries);
        let capture = Arc::new(ChangeCapture {
            start: current.clone(),
            state: Mutex::new(CaptureState {
                last: current.clone(),
                batches: Vec::new(),
                operations: 0,
                bytes: 0,
                failure: None,
                sealed: false,
            }),
        });
        entries.push(Arc::clone(&capture));
        Ok((capture, RetiredCaptures { _captures: retired }))
    }

    pub(super) fn cleanup_handle(&self) -> CaptureCleanup {
        CaptureCleanup(Arc::clone(&self.0))
    }

    fn active(&self) -> Vec<Arc<ChangeCapture>> {
        let Ok(entries) = self.0.lock() else {
            return Vec::new();
        };
        entries
            .iter()
            .filter(|entry| Arc::strong_count(entry) > 1)
            .cloned()
            .collect()
    }

    /// Capture only actual incoming locations, before the publisher consumes
    /// them. Optional tracking failure never refuses a client's durable write.
    pub(super) fn prepare(
        &self,
        previous: &Current,
        root: &Path,
        mutations: &[PreparedMutation],
    ) -> Option<PreparedChanges> {
        let active = self.active();
        if active.is_empty() {
            return None;
        }
        let bytes = mutations.iter().fold(
            root.as_os_str()
                .len()
                .saturating_mul(2)
                .saturating_add(1024),
            |size, mutation| {
                size.saturating_add(mutation.collection.len())
                    .saturating_add(mutation.key.len())
                    .saturating_add(512)
            },
        );
        let mut needed = false;
        for capture in &active {
            if let Ok(mut state) = capture.state.lock() {
                if state.failure.is_some() || state.sealed {
                    continue;
                }
                if state.last != *previous {
                    state.fail("missed a complete root", mutations.len(), bytes);
                } else if state.operations.saturating_add(mutations.len()) > MAX_OPERATIONS
                    || state.bytes.saturating_add(bytes) > MAX_BYTES
                {
                    state.fail("exact change capacity exceeded", mutations.len(), bytes);
                } else {
                    needed = true;
                }
            }
        }
        if !needed {
            return None;
        }
        match copy_mutations(mutations.iter(), mutations.len()) {
            Ok(mutations) => Some(PreparedChanges { mutations, bytes }),
            Err(_) => {
                for capture in active {
                    if let Ok(mut state) = capture.state.lock() {
                        state.fail("change metadata allocation refused", mutations.len(), bytes);
                    }
                }
                None
            }
        }
    }

    /// Run only after the complete client durability barrier succeeds.
    pub(super) fn committed(
        &self,
        previous: &Current,
        current: &Current,
        root: &Path,
        lease: &WriterReadLease,
        prepared: Option<PreparedChanges>,
    ) {
        let active = self.active();
        if active.is_empty() {
            return;
        }
        let batch = prepared.and_then(|prepared| {
            GenerationHold::from_writer(lease, root, current)
                .ok()
                .map(|hold| {
                    let bytes = prepared.bytes;
                    (
                        Arc::new(CapturedBatch {
                            mutations: prepared.mutations,
                            _hold: hold,
                        }),
                        bytes,
                    )
                })
        });
        for capture in active {
            let Ok(mut state) = capture.state.lock() else {
                continue;
            };
            if state.failure.is_some() || state.sealed {
                continue;
            }
            let Some((batch, bytes)) = &batch else {
                state.fail("complete change batch unavailable", 0, 0);
                continue;
            };
            if state.last != *previous || previous.frontier.checked_add(1) != Some(current.frontier)
            {
                state.fail(
                    "complete change frontier differs",
                    batch.mutations.len(),
                    *bytes,
                );
            } else if state.batches.try_reserve(1).is_err() {
                state.fail(
                    "change batch allocation refused",
                    batch.mutations.len(),
                    *bytes,
                );
            } else {
                state.operations += batch.mutations.len();
                state.bytes += *bytes;
                state.last = current.clone();
                state.batches.push(Arc::clone(batch));
            }
        }
    }

    /// Rewrites preserve values. Existing batch holds retain the prior physical
    /// locations even if this root chooses different copies of those bytes.
    pub(super) fn rewritten(&self, previous: &Current, current: &Current) {
        for capture in self.active() {
            if let Ok(mut state) = capture.state.lock() {
                if state.failure.is_some() || state.sealed {
                    continue;
                }
                if state.last != *previous || current.frontier != previous.frontier {
                    state.fail("physical rewrite frontier differs", 0, 0);
                } else {
                    state.last = current.clone();
                }
            }
        }
    }
}

impl CaptureCleanup {
    /// A live owner or writer reference excludes retirement. No registry lock
    /// spans destruction of key arrays or generation holds.
    pub fn release_idle(&self) {
        let retired = match self.0.lock() {
            Ok(mut entries) => take_idle(&mut entries),
            Err(_) => return,
        };
        drop(retired);
    }
}

fn take_idle(entries: &mut Vec<Arc<ChangeCapture>>) -> Vec<Arc<ChangeCapture>> {
    let mut retired = Vec::new();
    let mut index = 0;
    while index < entries.len() {
        if Arc::strong_count(&entries[index]) == 1 {
            retired.push(entries.swap_remove(index));
        } else {
            index += 1;
        }
    }
    retired
}

impl ChangeCapture {
    pub(in crate::disk_pack) fn snapshot(
        &self,
        start: &Current,
        current: &Current,
        limit: usize,
    ) -> Result<CapturedChanges> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| behind("change capture unavailable"))?;
        if let Some(failure) = &state.failure {
            return Err(behind(failure));
        }
        if state.sealed || self.start != *start || state.last != *current {
            return Err(behind("change capture root or ownership differs"));
        }
        if state.operations > limit {
            return Err(behind(&format!(
                "{} captured operations exceed remaining {limit}",
                state.operations
            )));
        }
        let mutations = copy_mutations(
            state
                .batches
                .iter()
                .flat_map(|batch| batch.mutations.iter()),
            state.operations,
        )?;
        let mut batches = Vec::new();
        batches
            .try_reserve_exact(state.batches.len())
            .map_err(|_| behind("capture hold allocation refused"))?;
        batches.extend(state.batches.iter().cloned());
        // Keep the originals in the output owner until its off-gate drop.
        state.sealed = true;
        crate::commit_timing::count("compaction_captured_operations", state.operations as u64);
        crate::commit_timing::count("compaction_captured_metadata_bytes", state.bytes as u64);
        Ok(CapturedChanges { mutations, batches })
    }
}

impl CaptureState {
    fn fail(&mut self, reason: &str, operations: usize, bytes: usize) {
        if self.failure.is_none() {
            self.failure = Some(format!("{reason}: retained={}, incoming={operations}, metadata_bytes={}, incoming_bytes={bytes}, operation_limit={MAX_OPERATIONS}, byte_limit={MAX_BYTES}", self.operations, self.bytes));
        }
    }
}

fn copy_mutations<'a>(
    mutations: impl Iterator<Item = &'a PreparedMutation>,
    count: usize,
) -> Result<Vec<PreparedMutation>> {
    let mut copied = Vec::new();
    copied
        .try_reserve_exact(count)
        .map_err(|_| behind("change vector allocation refused"))?;
    for mutation in mutations {
        let mut collection = String::new();
        collection
            .try_reserve_exact(mutation.collection.len())
            .map_err(|_| behind("change collection allocation refused"))?;
        collection.push_str(&mutation.collection);
        let mut key = String::new();
        key.try_reserve_exact(mutation.key.len())
            .map_err(|_| behind("change key allocation refused"))?;
        key.push_str(&mutation.key);
        copied.push(PreparedMutation {
            collection,
            key,
            group: mutation.group,
            location: mutation.location.clone(),
        });
    }
    Ok(copied)
}

fn behind(reason: &str) -> crate::Error {
    crate::Error::WriteCapacity(format!("compaction_behind: exact change capture {reason}"))
}
