//! Captured notifications record only durable events from the product snapshot.

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
        transaction::background_scope(Arc::clone(&self.native), self.stage_chunk(events, None))
            .await
    }

    async fn stage_chunk(
        &self,
        events: Vec<ChangeFeedEvent>,
        captured: Option<Captured>,
    ) -> Result<u64, SchemaError> {
        // Native preparation has one FIFO owner. The private prefix contains
        // earlier accepted sequences; ordinary readers still see the durable one.
        let last_seq = self.prepared_last_seq().await?;
        let prepared = self.format.stage_events(last_seq, events).await?;
        let end = prepared.end;
        let ready = prepared.events;
        self.stage_prepared_last_seq(end)?;
        let store = self.clone();
        let deliver = async move {
            *store.last_seq.lock().await = end;
            for event in ready {
                if let Some(captured) = &captured {
                    captured
                        .committed
                        .lock()
                        .expect("committed notification capture")
                        .insert(event.mutation_id.clone());
                }
            }
            Ok::<_, StorageError>(())
        };
        transaction::after_commit(&self.native, deliver)?;
        Ok(end)
    }

    async fn prepared_last_seq(&self) -> Result<u64, SchemaError> {
        let cache = Arc::as_ptr(&self.last_seq) as usize;
        match transaction::publication::get::<u64>(&self.native, cache, LAST_SEQ_KEY)? {
            Some(last_seq) => Ok(*last_seq),
            None => Ok(*self.last_seq.lock().await),
        }
    }

    fn stage_prepared_last_seq(&self, end: u64) -> Result<(), StorageError> {
        // The existing private metadata prefix covers earlier queued calls and
        // repeated stages in this call. The fallback is the durable last sequence.
        transaction::publication::publish(
            &self.native,
            Arc::as_ptr(&self.last_seq) as usize,
            LAST_SEQ_KEY.into(),
            end,
            || Ok(()),
        )
    }
}
