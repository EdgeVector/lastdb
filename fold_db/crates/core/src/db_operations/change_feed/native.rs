//! Notifications share their product snapshot; subscribers see only durable events.

use super::*;
use crate::schema::types::{Mutation, MutationType};
use crate::storage::disk_pack::transaction;
use std::collections::HashSet;
use std::sync::Mutex as StdMutex;

pub(crate) struct CaptureState {
    hints: Vec<(String, ChangeFeedEvent)>,
    committed: StdMutex<HashSet<String>>,
}

pub(crate) type Captured = Arc<CaptureState>;

tokio::task_local! {
    static CAPTURE: Captured;
}

impl ChangeFeedStore {
    /// Capture notifications in native product commits. Return committed IDs so
    /// the host queues only events that did not share a native transaction.
    pub async fn capture_native<T>(
        hints: Vec<(String, ChangeFeedEvent)>,
        future: impl std::future::Future<Output = T>,
    ) -> (T, HashSet<String>) {
        let captured = Arc::new(CaptureState {
            hints,
            committed: StdMutex::new(HashSet::new()),
        });
        let result = CAPTURE.scope(Arc::clone(&captured), future).await;
        let ids = captured
            .committed
            .lock()
            .expect("native notification capture")
            .clone();
        (result, ids)
    }

    pub(crate) fn native_events(mutations: &[Mutation]) -> Vec<ChangeFeedEvent> {
        CAPTURE
            .try_with(|capture| {
                let mut used = HashSet::new();
                mutations
                    .iter()
                    .filter_map(|mutation| {
                        let operation = match mutation.mutation_type {
                            MutationType::Create => "create",
                            MutationType::Update => "update",
                            MutationType::Delete => "delete",
                            MutationType::Purge => "purge",
                        };
                        if mutation.aggregate_derived_internal {
                            return None;
                        }
                        // Match the resolved schema while preserving the host's
                        // original spelling in the durable public event.
                        let (position, (_, hint)) = capture.hints.iter().enumerate().find(
                            |(position, (canonical, hint))| {
                                !used.contains(position)
                                    && *canonical == mutation.schema_name
                                    && hint.operation == operation
                                    && hint.hash == mutation.key_value.hash
                                    && hint.range == mutation.key_value.range
                            },
                        )?;
                        used.insert(position);
                        let mut event = hint.clone();
                        event.mutation_id = mutation.uuid.clone();
                        event.committed_at_ms = crate::clock::unix_millis();
                        // This event describes the commit boundary, before convergence waits.
                        event.background_tasks_drained = false;
                        event.convergence_pending = true;
                        Some(event)
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) async fn stage_native_events(
        &self,
        events: Vec<ChangeFeedEvent>,
    ) -> Result<(), SchemaError> {
        if events.is_empty() {
            return Ok(());
        }
        let captured = CAPTURE.try_with(Arc::clone).map_err(|_| {
            SchemaError::InvalidData("native notification capture is absent".into())
        })?;
        self.stage_chunk(events, Some(captured)).await.map(|_| ())
    }

    pub(super) async fn append_chunk(
        &self,
        events: Vec<ChangeFeedEvent>,
    ) -> Result<u64, SchemaError> {
        transaction::background_scope(
            Some(Arc::clone(&self.native)),
            self.stage_chunk(events, None),
        )
        .await
    }

    async fn stage_chunk(
        &self,
        events: Vec<ChangeFeedEvent>,
        captured: Option<Captured>,
    ) -> Result<u64, SchemaError> {
        // Native preparation has one FIFO owner. The private prefix contains
        // earlier accepted tips; ordinary readers still see the durable tip.
        let tip = self.prepared_tip().await?;
        let first = tip
            .checked_add(1)
            .ok_or_else(|| SchemaError::InvalidData("change-feed sequence exhausted".into()))?;
        let end = tip
            .checked_add(events.len() as u64)
            .ok_or_else(|| SchemaError::InvalidData("change-feed sequence exhausted".into()))?;
        let mut ready = Vec::with_capacity(events.len());
        let mut rows = Vec::with_capacity(events.len().min(APPEND_EVENTS) + 1);
        let mut bytes = 0usize;
        for (offset, mut event) in events.into_iter().enumerate() {
            event.seq = first + offset as u64;
            let key = event_key(event.seq).into_bytes();
            let body =
                serde_json::to_vec(&event).map_err(|e| SchemaError::InvalidData(e.to_string()))?;
            bytes = bytes
                .saturating_add(key.len())
                .saturating_add(body.len())
                .saturating_add(256);
            if bytes > APPEND_BYTES {
                return Err(SchemaError::InvalidData(
                    "change-feed batch exceeds its byte limit".into(),
                ));
            }
            rows.push((key, body));
            ready.push(event);
            if rows.len() == APPEND_EVENTS {
                self.store
                    .inner()
                    .batch_put(std::mem::take(&mut rows))
                    .await?;
            }
        }
        rows.push((
            TIP_KEY.as_bytes().to_vec(),
            serde_json::to_vec(&end).map_err(|e| SchemaError::InvalidData(e.to_string()))?,
        ));
        self.store.inner().batch_put(rows).await?;
        if end > MAX_EVENTS {
            let expired = (first.saturating_sub(MAX_EVENTS).max(1)..=end - MAX_EVENTS)
                .map(|sequence| event_key(sequence).into_bytes())
                .collect();
            // Expiry joins the same bounded snapshot rather than starting a
            // second writer after an acknowledged event reaches retention.
            self.store.inner().batch_delete(expired).await?;
        }
        self.stage_prepared_tip(end)?;
        let store = self.clone();
        let deliver = async move {
            *store.tip.lock().await = end;
            for event in ready {
                if let Some(captured) = &captured {
                    captured
                        .committed
                        .lock()
                        .expect("committed notification capture")
                        .insert(event.mutation_id.clone());
                }
                let _ = store.subscribers.send(event);
            }
            Ok::<_, StorageError>(())
        };
        transaction::after_commit(&self.native, deliver)?;
        Ok(end)
    }

    async fn prepared_tip(&self) -> Result<u64, SchemaError> {
        let cache = Arc::as_ptr(&self.tip) as usize;
        match transaction::publication::get::<u64>(Some(Arc::clone(&self.native)), cache, TIP_KEY)?
        {
            Some(tip) => Ok(*tip),
            None => Ok(*self.tip.lock().await),
        }
    }

    fn stage_prepared_tip(&self, end: u64) -> Result<(), StorageError> {
        // The existing private metadata prefix covers earlier queued calls and
        // repeated stages in this call. The fallback is the durable memory tip.
        transaction::publication::publish(
            Some(Arc::clone(&self.native)),
            Arc::as_ptr(&self.tip) as usize,
            TIP_KEY.into(),
            end,
            || Ok(()),
        )
    }
}
