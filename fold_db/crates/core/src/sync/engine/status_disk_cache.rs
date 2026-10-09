//! TTL cache for per-collection disk usage on the sync status path.
//!
//! `SyncEngine::status` used to call [`NamespacedStore::collection_disk_usage`]
//! inline for every automatic-compaction plane plus `tips` plus locators.
//! LastStore implements that as a recursive `read_dir` (`dir_size_bytes`).
//! On the primary, `tips` alone is multi-GiB, so `/api/status` — the cheap
//! health check — spent tens of seconds in `status_sync`.
//!
//! This cache is the same contract as the node-home `DATA_DIR_SIZE` gauge:
//! never walk on the caller thread, `None` until a background walk completes,
//! TTL refresh off-runtime. Compaction workers keep calling
//! `collection_disk_usage` directly when they need a fresh trigger sample.

use crate::storage::traits::{CollectionDiskUsage, NamespacedStore};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// How long a measured collection size stays fresh before a background
/// refresh is kicked off. This is a display gauge on `/api/status`, not a
/// compaction trigger, so a minute of staleness is cheaper than re-walking
/// `tips` on every health poll.
pub const DEFAULT_TTL: Duration = Duration::from_secs(60);

struct Entry {
    usage: Option<CollectionDiskUsage>,
    measured_at: Option<Instant>,
    refreshing: bool,
}

/// Per-collection disk-usage cache. One instance per [`super::SyncEngine`].
pub(crate) struct StatusDiskUsageCache {
    state: Mutex<HashMap<String, Entry>>,
    walks: AtomicU64,
}

impl StatusDiskUsageCache {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(HashMap::new()),
            walks: AtomicU64::new(0),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Entry>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn abandon_refresh(&self, collection: &str) {
        if let Some(entry) = self.lock().get_mut(collection) {
            entry.refreshing = false;
        }
    }

    fn store(&self, collection: String, usage: Option<CollectionDiskUsage>) {
        self.walks.fetch_add(1, Ordering::Relaxed);
        let mut state = self.lock();
        let entry = state.entry(collection).or_insert(Entry {
            usage: None,
            measured_at: None,
            refreshing: false,
        });
        entry.usage = usage;
        entry.measured_at = Some(Instant::now());
        entry.refreshing = false;
    }

    /// Cached usage for `collection`, or `None` when no walk has completed.
    ///
    /// Never walks inline. Every call is O(1): it serves a cached number
    /// (fresh or stale) or reports "not measured yet", and arranges for at
    /// most one background `collection_disk_usage` per collection.
    pub(crate) fn get(
        self: &Arc<Self>,
        store: Arc<dyn NamespacedStore>,
        collection: &str,
        ttl: Duration,
    ) -> Option<CollectionDiskUsage> {
        let mut state = self.lock();
        let entry = state.entry(collection.to_string()).or_insert(Entry {
            usage: None,
            measured_at: None,
            refreshing: false,
        });
        if let Some(at) = entry.measured_at {
            if at.elapsed() < ttl {
                return entry.usage;
            }
        }
        if !entry.refreshing {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                entry.refreshing = true;
                let cache = Arc::clone(self);
                let collection = collection.to_string();
                handle.spawn(async move {
                    let walked = collection.clone();
                    match tokio::task::spawn_blocking(move || {
                        store.collection_disk_usage(&collection)
                    })
                    .await
                    {
                        Ok(usage) => cache.store(walked, usage),
                        Err(_) => cache.abandon_refresh(&walked),
                    }
                });
            }
        }
        entry.measured_at.and(entry.usage)
    }
}
