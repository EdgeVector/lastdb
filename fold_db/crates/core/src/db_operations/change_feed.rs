//! Durable, ordered local mutation feed for authorized LastDB apps.
//!
//! The feed is node-local coordination metadata. Product rows remain the
//! source of truth; consumers point-read them after receiving a change.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, Mutex};

use crate::schema::SchemaError;
use crate::storage::{KvStore, StorageError, TypedKvStore};

const TIP_KEY: &str = "tip";
const EVENT_PREFIX: &str = "event:";
const EVENT_END: &str = "event;";
/// Bounded node-local retention. A lagging consumer receives `gap=true` and
/// must rescan its declared product keys before advancing its durable cursor.
pub const MAX_EVENTS: u64 = 250_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChangeFeedEvent {
    pub seq: u64,
    pub mutation_id: String,
    pub schema: String,
    pub operation: String,
    pub hash: Option<String>,
    pub range: Option<String>,
    pub committed_at_ms: u64,
    pub background_tasks_drained: bool,
    pub convergence_pending: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeFeedPage {
    pub events: Vec<ChangeFeedEvent>,
    pub gap: bool,
}

#[derive(Clone)]
pub struct ChangeFeedStore {
    store: Arc<TypedKvStore<dyn KvStore>>,
    tip: Arc<Mutex<u64>>,
    subscribers: broadcast::Sender<ChangeFeedEvent>,
}

impl ChangeFeedStore {
    pub(crate) async fn new(store: Arc<dyn KvStore>) -> Result<Self, StorageError> {
        let store = Arc::new(TypedKvStore::new(store));
        let tip = store.get_item::<u64>(TIP_KEY).await?.unwrap_or(0);
        let (subscribers, _) = broadcast::channel(1024);
        Ok(Self {
            store,
            tip: Arc::new(Mutex::new(tip)),
            subscribers,
        })
    }

    /// Subscribe to committed feed events from this node process.
    ///
    /// The durable feed remains the replay source. This receiver is the
    /// low-latency path for live watches, so a watcher can sleep without
    /// polling the change-feed namespace.
    pub fn subscribe(&self) -> broadcast::Receiver<ChangeFeedEvent> {
        self.subscribers.subscribe()
    }

    pub async fn append(&self, mut event: ChangeFeedEvent) -> Result<u64, SchemaError> {
        // This mutex is the one lock on the node NOT scoped to a schema, key,
        // or molecule — every successful mutation passes through it. Measured
        // 2026-08-03: queueing here was 33% of all mutation wall time, 132x
        // the cost of `persist`. Split lock-wait from the write itself
        // (`crate::request_phases::RequestPhase::ChangeRecordWrite`) so a
        // caller narrowing this gate can tell contention from storage IO —
        // same reasoning as `LockWait` vs `MoleculeGate`.
        let lock_wait_started = std::time::Instant::now();
        let mut tip = self.tip.lock().await;
        crate::request_phases::add_phase(
            crate::request_phases::RequestPhase::ChangeRecordLockWait,
            lock_wait_started.elapsed(),
        );

        let write_started = std::time::Instant::now();
        let seq = tip.saturating_add(1);
        event.seq = seq;
        let event_bytes =
            serde_json::to_vec(&event).map_err(|e| SchemaError::InvalidData(e.to_string()))?;
        let tip_bytes =
            serde_json::to_vec(&seq).map_err(|e| SchemaError::InvalidData(e.to_string()))?;
        self.store
            .inner()
            .batch_put(vec![
                (event_key(seq).into_bytes(), event_bytes),
                (TIP_KEY.as_bytes().to_vec(), tip_bytes),
            ])
            .await?;
        *tip = seq;
        let _ = self.subscribers.send(event.clone());
        // Drop the mutex before retention runs below: retention is best-effort
        // and independent of tip ordering, so holding the node-wide lock
        // across a second sequential storage round trip only serializes every
        // other writer behind work that does not need serializing.
        drop(tip);
        crate::request_phases::add_phase(
            crate::request_phases::RequestPhase::ChangeRecordWrite,
            write_started.elapsed(),
        );

        if seq > MAX_EVENTS {
            // Off the mutex AND off the request's critical path entirely: a
            // crash before this delete runs retains one extra event, never
            // loses a newly acknowledged one, exactly as when this ran
            // synchronously under the lock.
            let store = Arc::clone(&self.store);
            let stale_key = event_key(seq - MAX_EVENTS);
            tokio::spawn(async move {
                let _ = store.delete_item(&stale_key).await;
            });
        }
        Ok(seq)
    }

    pub async fn list_after(
        &self,
        after: u64,
        limit: usize,
    ) -> Result<ChangeFeedPage, SchemaError> {
        if limit == 0 {
            return Ok(ChangeFeedPage {
                events: Vec::new(),
                gap: false,
            });
        }
        let tip = *self.tip.lock().await;
        let oldest = tip.saturating_sub(MAX_EVENTS).saturating_add(1).max(1);
        let gap = after > 0 && after.saturating_add(1) < oldest;
        let effective_after = if gap { oldest.saturating_sub(1) } else { after };
        let start = event_key(effective_after.saturating_add(1));
        let events = self
            .store
            .scan_items_in_range_paged::<ChangeFeedEvent>(&start, EVENT_END, limit)
            .await?
            .into_iter()
            .map(|(_, event)| event)
            .collect();
        Ok(ChangeFeedPage { events, gap })
    }

    pub async fn tip(&self) -> u64 {
        *self.tip.lock().await
    }
}

fn event_key(seq: u64) -> String {
    format!("{EVENT_PREFIX}{seq:020}")
}
