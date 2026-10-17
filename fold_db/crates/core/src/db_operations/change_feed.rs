//! Durable, ordered local mutation feed for authorized LastDB apps.
//!
//! The feed is node-local coordination metadata. Product rows remain the
//! source of truth; consumers point-read them after receiving a change.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, Mutex};
use tracing::Instrument;

use crate::schema::SchemaError;
use crate::storage::{KvStore, StorageError, TypedKvStore};

const TIP_KEY: &str = "tip";
const EVENT_PREFIX: &str = "event:";
const EVENT_END: &str = "event;";
const APPEND_EVENTS: usize = 1_024;
const APPEND_BYTES: usize = 32 * 1024 * 1024;
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
    native: Option<Arc<crate::storage::disk_pack::PackRuntime>>,
    tip: Arc<Mutex<u64>>,
    subscribers: broadcast::Sender<ChangeFeedEvent>,
}

impl ChangeFeedStore {
    pub(crate) async fn new(
        store: Arc<dyn KvStore>,
        native: Option<Arc<crate::storage::disk_pack::PackRuntime>>,
    ) -> Result<Self, StorageError> {
        let store = Arc::new(TypedKvStore::new(store));
        let tip = store.get_item::<u64>(TIP_KEY).await?.unwrap_or(0);
        let (subscribers, _) = broadcast::channel(1024);
        Ok(Self {
            store,
            native,
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

    pub async fn append(&self, event: ChangeFeedEvent) -> Result<u64, SchemaError> {
        self.append_many(vec![event]).await
    }

    /// Preserve caller order with one event/tip write per bounded chunk.
    /// Native feed batches can share the bounded product flush deadline.
    pub async fn append_many(&self, events: Vec<ChangeFeedEvent>) -> Result<u64, SchemaError> {
        if events.is_empty() {
            return Ok(*self.tip.lock().await);
        }
        // One feed lock keeps every caller's sequence range contiguous. Hold
        // it through each bounded event/tip write and subscriber publication.
        let lock_wait_started = std::time::Instant::now();
        let mut tip = self.tip.lock().await;
        crate::request_phases::add_phase(
            crate::request_phases::RequestPhase::ChangeRecordLockWait,
            lock_wait_started.elapsed(),
        );

        let write_started = std::time::Instant::now();
        let count = u64::try_from(events.len())
            .map_err(|error| SchemaError::InvalidData(error.to_string()))?;
        let end = tip.checked_add(count).ok_or_else(|| {
            SchemaError::InvalidData("change-feed sequence exhausted".to_string())
        })?;
        let first = *tip + 1;
        let mut sequence = *tip;
        let mut rows = Vec::new();
        let mut ready = Vec::new();
        let mut bytes = 0usize;
        for mut event in events {
            sequence += 1;
            event.seq = sequence;
            let key = event_key(sequence).into_bytes();
            let body = serde_json::to_vec(&event)
                .map_err(|error| SchemaError::InvalidData(error.to_string()))?;
            let length = key.len().saturating_add(body.len()).saturating_add(256);
            if length > APPEND_BYTES {
                return Err(SchemaError::InvalidData(
                    "change-feed event exceeds its byte limit".to_string(),
                ));
            }
            if !ready.is_empty() && (ready.len() == APPEND_EVENTS || bytes + length > APPEND_BYTES)
            {
                self.append_ready(
                    &mut tip,
                    std::mem::take(&mut rows),
                    std::mem::take(&mut ready),
                )
                .await?;
                bytes = 0;
            }
            rows.push((key, body));
            ready.push(event);
            bytes += length;
        }
        if !ready.is_empty() {
            self.append_ready(&mut tip, rows, ready).await?;
        }
        // Drop the mutex before retention runs below: retention is best-effort
        // and independent of tip ordering, so holding the node-wide lock
        // across a second sequential storage round trip only serializes every
        // other writer behind work that does not need serializing.
        drop(tip);
        crate::request_phases::add_phase(
            crate::request_phases::RequestPhase::ChangeRecordWrite,
            write_started.elapsed(),
        );

        self.prune(first, end);
        Ok(end)
    }

    async fn append_ready(
        &self,
        tip: &mut u64,
        mut rows: Vec<(Vec<u8>, Vec<u8>)>,
        events: Vec<ChangeFeedEvent>,
    ) -> Result<(), SchemaError> {
        let sequence = events.last().expect("nonempty change-feed batch").seq;
        let body = serde_json::to_vec(&sequence)
            .map_err(|error| SchemaError::InvalidData(error.to_string()))?;
        rows.push((TIP_KEY.as_bytes().to_vec(), body));
        crate::storage::disk_pack::transaction::background_scope(
            self.native.clone(),
            self.store.inner().batch_put(rows),
        )
        .await?;
        *tip = sequence;
        for event in events {
            let _ = self.subscribers.send(event);
        }
        Ok(())
    }

    fn prune(&self, first: u64, end: u64) {
        if end <= MAX_EVENTS || first > end {
            return;
        }
        let store = Arc::clone(&self.store);
        let mut first = first.saturating_sub(MAX_EVENTS).max(1);
        let end = end - MAX_EVENTS;
        // Best-effort retention stays outside the tip lock. A crash retains
        // surplus events; it never removes a newly acknowledged event.
        tokio::spawn(
            async move {
                while first <= end {
                    let last = first.saturating_add(APPEND_EVENTS as u64 - 1).min(end);
                    let keys = (first..=last)
                        .map(|sequence| event_key(sequence).into_bytes())
                        .collect();
                    let _ = store.inner().batch_delete(keys).await;
                    first = last + 1;
                }
            }
            .in_current_span(),
        );
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
