//! One durable batch's written groups, carried across the async boundary.
//!
//! LastStore notes a placement on the thread that wrote. `spawn_blocking` and
//! the persist lane do not keep that thread or a task-local, so the batch
//! holds an `Arc` and the worker appends into it.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use serde::Serialize;

tokio::task_local! {
    static BATCH_LOG: Arc<BatchPlacementLog>;
}

/// Placements for one mutation batch. Duplicates are kept; the flush dedupes.
pub struct BatchPlacementLog {
    placements: Mutex<Vec<laststore::PlacedWrite>>,
}

impl BatchPlacementLog {
    /// An empty log shared by the request task and the persist lane.
    #[must_use]
    pub fn shared() -> Arc<Self> {
        Arc::new(Self {
            placements: Mutex::new(Vec::new()),
        })
    }

    /// Append placements from one synchronous storage call.
    pub fn record(&self, writes: Vec<laststore::PlacedWrite>) {
        if writes.is_empty() {
            return;
        }
        self.placements
            .lock()
            .expect("batch placement log")
            .extend(writes);
    }

    /// Each recorded write, mapped to the flush record.
    #[must_use]
    pub fn dirty_writes(&self) -> Vec<DirtyWrite> {
        self.placements
            .lock()
            .expect("batch placement log")
            .iter()
            .map(|placed| DirtyWrite {
                id: placed.id.clone(),
                written: placed.written.clone(),
            })
            .collect()
    }

    /// Deduped groups the batch wrote. This is the flush argument.
    #[must_use]
    pub fn written_keys(&self) -> Vec<laststore::ShardKey> {
        let mut keys: Vec<_> = self
            .dirty_writes()
            .into_iter()
            .map(|write| write.written)
            .collect();
        keys.sort_unstable();
        keys.dedup();
        keys
    }

    /// Fanout slots of partitions this batch wrote. Not a flush argument.
    #[must_use]
    pub fn touched_group_ids(&self) -> Vec<TouchedGroupId> {
        let mut groups = Vec::new();
        for placed in self.placements.lock().expect("batch placement log").iter() {
            for (collection, shard, group) in &placed.touched {
                let Some(group) = group else {
                    continue;
                };
                groups.push(TouchedGroupId {
                    collection: collection.clone(),
                    shard: *shard,
                    group: *group,
                });
            }
        }
        groups.sort_unstable();
        groups.dedup();
        groups
    }
}

/// One id and the single group `group_of` assigned it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirtyWrite {
    /// Document id that was written.
    pub id: String,
    /// The group that write landed in.
    pub written: laststore::ShardKey,
}

/// One fanout slot the server already resolved. The client does not rehash.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct TouchedGroupId {
    /// LastStore collection.
    pub collection: String,
    /// Shard index. Product homes use shard 0.
    pub shard: u16,
    /// Hash-group index inside that shard.
    pub group: u32,
}

/// The log installed on this task, if a mutation batch owns the call.
#[must_use]
pub fn current() -> Option<Arc<BatchPlacementLog>> {
    BATCH_LOG.try_with(Arc::clone).ok()
}

/// Written groups for the current batch. Empty when no batch is installed.
#[must_use]
pub fn current_written_keys() -> Vec<laststore::ShardKey> {
    current().map(|log| log.written_keys()).unwrap_or_default()
}

/// Touched fanout slots for the current batch. Empty when no batch is installed.
#[must_use]
pub fn current_touched_groups() -> Vec<TouchedGroupId> {
    current()
        .map(|log| log.touched_group_ids())
        .unwrap_or_default()
}

/// Run `future` with `log` visible to storage calls on this task.
pub async fn scope<F>(log: Arc<BatchPlacementLog>, future: F) -> F::Output
where
    F: std::future::Future,
{
    BATCH_LOG.scope(log, future).await
}

/// How many scoped keys are absent from the written set.
#[must_use]
pub fn foreign_group_count(scope: &[laststore::ShardKey], written: &[laststore::ShardKey]) -> u64 {
    let written: HashSet<&laststore::ShardKey> = written.iter().collect();
    scope.iter().filter(|key| !written.contains(key)).count() as u64
}

/// A foreign group is the warn. Width is not.
#[must_use]
pub fn flush_foreign_should_warn(foreign: u64) -> bool {
    foreign > 0
}

/// Log groups the barrier synced that this batch did not write.
///
/// The mutation still succeeds. The line names the count, not key contents.
pub fn warn_if_flush_foreign(foreign: u64) {
    if flush_foreign_should_warn(foreign) {
        tracing::warn!(
            flush_foreign_groups = foreign,
            "durable flush synced groups outside the written set"
        );
    }
}
