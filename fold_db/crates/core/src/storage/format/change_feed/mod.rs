//! Typed change-feed storage. The nested encoder has no storage capability.

mod events;

use crate::schema::SchemaError;
use crate::storage::disk_pack::{transaction, PackRuntime};
use crate::storage::error::StorageResult;
use crate::storage::{KvStore, StorageError, TypedKvStore};
use std::sync::Arc;

pub(crate) use super::{
    CHANGE_FEED_APPEND_BYTES as APPEND_BYTES, CHANGE_FEED_APPEND_EVENTS as APPEND_EVENTS,
    CHANGE_FEED_LAST_SEQ_KEY as LAST_SEQ_KEY,
};
pub use events::ChangeFeedEvent;

#[derive(Clone)]
pub(crate) struct ChangeFeed {
    feed_store: Arc<TypedKvStore<dyn KvStore>>,
    runtime: Arc<PackRuntime>,
}

enum EncodedFeed {
    Put(Vec<(Vec<u8>, Vec<u8>)>),
    Delete(Vec<Vec<u8>>),
}

pub(crate) struct PreparedEvents {
    pub(crate) end: u64,
    pub(crate) events: Vec<ChangeFeedEvent>,
}

impl ChangeFeed {
    pub(in crate::storage::format) fn new(
        store: Arc<dyn KvStore>,
        runtime: Arc<PackRuntime>,
    ) -> Self {
        Self {
            feed_store: Arc::new(TypedKvStore::new(store)),
            runtime,
        }
    }

    pub(crate) fn runtime(&self) -> Arc<PackRuntime> {
        Arc::clone(&self.runtime)
    }

    pub(crate) async fn last_seq(&self) -> StorageResult<u64> {
        Ok(self
            .feed_store
            .get_item::<u64>(LAST_SEQ_KEY)
            .await?
            .unwrap_or(0))
    }

    pub(crate) async fn events_after(
        &self,
        after: u64,
        limit: usize,
    ) -> StorageResult<Vec<ChangeFeedEvent>> {
        let start = events::event_key(after.saturating_add(1));
        Ok(self
            .feed_store
            .scan_items_in_range_paged::<ChangeFeedEvent>(
                &start,
                super::CHANGE_FEED_EVENT_END,
                limit,
            )
            .await?
            .into_iter()
            .map(|(_, event)| event)
            .collect())
    }

    /// Retain the original row batch sizes and encode each event as it arrives.
    /// Only this format component selects or stages physical feed destinations.
    pub(crate) async fn stage_events(
        &self,
        last_seq: u64,
        events: Vec<ChangeFeedEvent>,
    ) -> Result<PreparedEvents, SchemaError> {
        self.require_scope()?;
        let mut encoder = events::Encoder::new(last_seq, events.len())?;
        for event in events {
            if let Some(batch) = encoder.push(event)? {
                self.stage(batch).await?;
            }
        }
        let (last, expired, ready) = encoder.finish()?;
        self.stage(last).await?;
        if let Some(expired) = expired {
            self.stage(expired).await?;
        }
        Ok(ready)
    }

    fn require_scope(&self) -> StorageResult<()> {
        if !transaction::publication::active(&self.runtime) {
            return Err(StorageError::InvalidOperation(
                "a change-feed write requires a complete native write scope".into(),
            ));
        }
        Ok(())
    }

    async fn stage(&self, batch: EncodedFeed) -> StorageResult<()> {
        self.require_scope()?;
        match batch {
            EncodedFeed::Put(rows) => self.feed_store.inner().batch_put(rows).await,
            EncodedFeed::Delete(keys) => self.feed_store.inner().batch_delete(keys).await,
        }
    }
}

/// Keep append admission's existing conservative wire-size charge.
pub(crate) fn event_charge(event: &ChangeFeedEvent) -> Result<usize, SchemaError> {
    Ok(serde_json::to_vec(event)
        .map_err(|error| SchemaError::InvalidData(error.to_string()))?
        .len()
        .saturating_add(512))
}
