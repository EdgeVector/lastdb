//! Durable, ordered local mutation feed for authorized LastDB apps.
//!
//! Event rows and their sequence tip join the native product transaction.
//! A notice describes an accepted logical mutation, not later request success
//! or background convergence. An absent or spared Delete can still emit a
//! hint; consumers must read the current product values.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::Instrument;

use crate::schema::types::{KeyValue, Mutation, MutationType};
use crate::schema::SchemaError;
use crate::storage::disk_pack::transaction;
use crate::storage::{KvStore, StorageError, TypedKvStore};

const TIP_KEY: &str = "tip";
const EVENT_PREFIX: &str = "event:";
const EVENT_END: &str = "event;";
const PRUNE_BATCH_EVENTS: u64 = 1_024;
const STAGE_BYTES: usize = 32 * 1024 * 1024;
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

impl ChangeFeedEvent {
    /// Caller-key coordinates stay in query form, independent of disk encoding.
    pub(crate) fn from_mutation(mutation: &Mutation, key: &KeyValue) -> Option<Self> {
        if mutation.aggregate_derived_internal {
            return None;
        }
        let operation = match mutation.mutation_type {
            MutationType::Create => "create",
            MutationType::Update => "update",
            MutationType::Delete => "delete",
            MutationType::Purge => "purge",
        };
        Some(Self {
            seq: 0,
            mutation_id: mutation.uuid.clone(),
            schema: mutation.schema_name.clone(),
            operation: operation.to_string(),
            hash: key.hash.clone(),
            range: key.range.clone(),
            committed_at_ms: 0,
            background_tasks_drained: false,
            convergence_pending: true,
        })
    }
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
    tip: Arc<AtomicU64>,
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
            tip: Arc::new(AtomicU64::new(tip)),
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

    /// Stage notices beside their data; the native gate owns sequence allocation.
    /// The caller reserves complete preparation memory after this method returns.
    pub(crate) async fn stage(&self, events: Vec<ChangeFeedEvent>) -> Result<(), SchemaError> {
        let runtime = self.native.as_ref().ok_or_else(|| {
            SchemaError::InvalidData("change-feed events require native disk storage".into())
        })?;
        if !transaction::publication::active(self.native.clone()) {
            return Err(SchemaError::InvalidData(
                "change-feed events require a complete native write scope".into(),
            ));
        }
        if events.is_empty() {
            return Ok(());
        }
        let write_started = std::time::Instant::now();
        // Read the private transaction's tip, including an earlier stage in
        // this same scope. The public atomic contains committed state only.
        let tip = self
            .store
            .get_item::<u64>(TIP_KEY)
            .await
            .map_err(SchemaError::from)
            .map_err(SchemaError::read_failure)?
            .unwrap_or(0);
        let (rows, ready, end) = prepare_events(events, tip)?;
        self.store.inner().batch_put(rows).await?;
        let committed_tip = Arc::clone(&self.tip);
        transaction::publication::publish(
            self.native.clone(),
            Arc::as_ptr(&self.tip) as usize,
            TIP_KEY.to_string(),
            end,
            move || {
                committed_tip.store(end, Ordering::Release);
                Ok(())
            },
        )?;
        let feed = self.clone();
        transaction::after_commit(runtime, async move {
            for event in ready {
                let _ = feed.subscribers.send(event);
            }
            feed.prune(tip + 1, end);
            Ok(())
        })?;
        crate::request_phases::add_phase(
            crate::request_phases::RequestPhase::ChangeNoticePrepare,
            write_started.elapsed(),
        );
        Ok(())
    }

    fn prune(&self, first: u64, end: u64) {
        if end <= MAX_EVENTS || first > end {
            return;
        }
        let store = Arc::clone(&self.store);
        let mut first = first.saturating_sub(MAX_EVENTS).max(1);
        let end = end - MAX_EVENTS;
        // Retention runs after durable delivery. A crash retains
        // surplus events; it never removes a newly acknowledged event.
        tokio::spawn(
            async move {
                while first <= end {
                    let last = first.saturating_add(PRUNE_BATCH_EVENTS - 1).min(end);
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
        let tip = self.tip.load(Ordering::Acquire);
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

    pub fn tip(&self) -> u64 {
        self.tip.load(Ordering::Acquire)
    }
}

fn event_key(seq: u64) -> String {
    format!("{EVENT_PREFIX}{seq:020}")
}

type EventRows = Vec<(Vec<u8>, Vec<u8>)>;

fn prepare_events(
    mut events: Vec<ChangeFeedEvent>,
    tip: u64,
) -> Result<(EventRows, Vec<ChangeFeedEvent>, u64), SchemaError> {
    let count =
        u64::try_from(events.len()).map_err(|error| SchemaError::InvalidData(error.to_string()))?;
    let end = tip
        .checked_add(count)
        .ok_or_else(|| SchemaError::InvalidData("change-feed sequence exhausted".to_string()))?;
    let committed_at_ms = crate::clock::unix_millis();
    let mut rows = Vec::with_capacity(events.len() + 1);
    let mut bytes = 256usize;
    for (offset, event) in events.iter_mut().enumerate() {
        event.seq = tip + offset as u64 + 1;
        event.committed_at_ms = committed_at_ms;
        event.background_tasks_drained = false;
        event.convergence_pending = true;
        let key = event_key(event.seq).into_bytes();
        let body = serde_json::to_vec(event)
            .map_err(|error| SchemaError::InvalidData(error.to_string()))?;
        bytes = bytes
            .saturating_add(key.len())
            .saturating_add(body.len())
            .saturating_add(256);
        if bytes > STAGE_BYTES {
            return Err(SchemaError::WriteResourceLimit {
                detail: "complete change-feed stage exceeds 32 MiB".into(),
            });
        }
        rows.push((key, body));
    }
    let body =
        serde_json::to_vec(&end).map_err(|error| SchemaError::InvalidData(error.to_string()))?;
    rows.push((TIP_KEY.as_bytes().to_vec(), body));
    Ok((rows, events, end))
}
