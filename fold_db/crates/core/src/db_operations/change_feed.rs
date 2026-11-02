//! Durable, ordered local mutation feed for authorized LastDB apps.
//!
//! The feed is node-local coordination metadata. Product rows remain the
//! source of truth; consumers point-read them after receiving a change.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, Mutex};

use crate::schema::SchemaError;
use crate::storage::{KvStore, StorageError, TypedKvStore};

pub(crate) mod native;

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
    native: Arc<crate::storage::disk_pack::PackRuntime>,
    tip: Arc<Mutex<u64>>,
    subscribers: broadcast::Sender<ChangeFeedEvent>,
}

impl ChangeFeedStore {
    pub(crate) async fn new(
        store: Arc<dyn KvStore>,
        native: Arc<crate::storage::disk_pack::PackRuntime>,
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

    /// Preserve caller order in bounded chunks. Each native chunk acquires the
    /// writer before the tip and publishes subscribers only after durability.
    pub async fn append_many(&self, events: Vec<ChangeFeedEvent>) -> Result<u64, SchemaError> {
        if events.is_empty() {
            return Ok(*self.tip.lock().await);
        }
        let mut chunk = Vec::new();
        let mut bytes = 0usize;
        let mut end = 0;
        for event in events {
            let length = serde_json::to_vec(&event)
                .map_err(|e| SchemaError::InvalidData(e.to_string()))?
                .len()
                .saturating_add(512);
            if length > APPEND_BYTES {
                return Err(SchemaError::InvalidData(
                    "change-feed event exceeds its byte limit".into(),
                ));
            }
            if !chunk.is_empty() && (chunk.len() == APPEND_EVENTS || bytes + length > APPEND_BYTES)
            {
                end = self.append_chunk(std::mem::take(&mut chunk)).await?;
                bytes = 0;
            }
            bytes += length;
            chunk.push(event);
        }
        if !chunk.is_empty() {
            end = self.append_chunk(chunk).await?;
        }
        Ok(end)
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
