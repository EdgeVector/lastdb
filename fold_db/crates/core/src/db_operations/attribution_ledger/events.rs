//! Attribution source events and pending write-intent scopes.

use super::{
    hash, AttributionLedger, ATTRIBUTION_EVENT_END, ATTRIBUTION_EVENT_MUTATION_PREFIX,
    ATTRIBUTION_EVENT_PREFIX, ATTRIBUTION_EVENT_TIP_KEY, ATTRIBUTION_PENDING_PREFIX,
    ATTRIBUTION_ROOT_DOMAIN,
};
use crate::schema::SchemaError;
use serde::{Deserialize, Serialize};

/// One ordered mutation source row for the attribution projector.
///
/// The projector uses this event only to find canonical rows. It never treats
/// this compact descriptor as user data or as proof of an object path.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AttributionEvent {
    pub seq: u64,
    pub mutation_id: String,
    pub schema: String,
    pub operation: String,
    #[serde(default)]
    pub storage_prefix: Option<String>,
    pub hash: Option<String>,
    pub range: Option<String>,
}

impl AttributionEvent {
    #[must_use]
    pub fn new(
        mutation_id: impl Into<String>,
        schema: impl Into<String>,
        operation: impl Into<String>,
        hash: Option<String>,
        range: Option<String>,
    ) -> Self {
        Self {
            seq: 0,
            mutation_id: mutation_id.into(),
            schema: schema.into(),
            operation: operation.into(),
            storage_prefix: None,
            hash,
            range,
        }
    }

    #[must_use]
    pub fn with_storage_prefix(mut self, storage_prefix: Option<String>) -> Self {
        self.storage_prefix = storage_prefix;
        self
    }

    fn validate(&self) -> Result<(), SchemaError> {
        if self.mutation_id.trim().is_empty()
            || self.schema.trim().is_empty()
            || self.operation.trim().is_empty()
        {
            return Err(SchemaError::InvalidData(
                "attribution event requires mutation, schema, and operation".to_string(),
            ));
        }
        Ok(())
    }
}

/// A bounded page from the durable attribution event source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttributionEventPage {
    pub events: Vec<AttributionEvent>,
}

/// A fail-closed write-intent marker. A marker without a matching event means
/// the attribution projector must classify its scope as unknown.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AttributionPendingScope {
    pub mutation_id: String,
    pub schema: String,
    pub operation: String,
    #[serde(default)]
    pub storage_prefix: Option<String>,
    pub hash: Option<String>,
    pub range: Option<String>,
}

impl AttributionPendingScope {
    #[must_use]
    pub fn new(
        mutation_id: impl Into<String>,
        schema: impl Into<String>,
        operation: impl Into<String>,
        hash: Option<String>,
        range: Option<String>,
    ) -> Self {
        Self {
            mutation_id: mutation_id.into(),
            schema: schema.into(),
            operation: operation.into(),
            storage_prefix: None,
            hash,
            range,
        }
    }

    #[must_use]
    pub fn with_storage_prefix(mut self, storage_prefix: Option<String>) -> Self {
        self.storage_prefix = storage_prefix;
        self
    }

    fn validate(&self) -> Result<(), SchemaError> {
        if self.mutation_id.trim().is_empty()
            || self.schema.trim().is_empty()
            || self.operation.trim().is_empty()
        {
            return Err(SchemaError::InvalidData(
                "attribution pending scope requires mutation, schema, and operation".to_string(),
            ));
        }
        Ok(())
    }

    fn storage_key(&self) -> String {
        pending_key(&self.mutation_id)
    }
}

impl AttributionLedger {
    /// Append a contiguous page of source events before a caller reports the
    /// write as attribution-complete. This method does not prune old events;
    /// an active epoch can pin any sequence between H0 and H1.
    pub async fn append_events(
        &self,
        events: Vec<AttributionEvent>,
    ) -> Result<Vec<u64>, SchemaError> {
        self.append_events_and_clear_pending_scopes(events, &[])
            .await
    }

    /// Delete the pending scopes for `mutation_ids`; a no-op when empty.
    async fn delete_pending_scopes(&self, mutation_ids: &[String]) -> Result<(), SchemaError> {
        if mutation_ids.is_empty() {
            return Ok(());
        }
        self.store
            .batch_delete_keys(
                mutation_ids
                    .iter()
                    .map(|mutation_id| pending_key(mutation_id))
                    .collect(),
            )
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("delete attribution pending scopes: {error}"))
            })
    }

    /// Delete the pending scopes and flush, only when there are any to delete.
    async fn clear_pending_and_flush(&self, mutation_ids: &[String]) -> Result<(), SchemaError> {
        if mutation_ids.is_empty() {
            return Ok(());
        }
        self.delete_pending_scopes(mutation_ids).await?;
        self.store.inner().flush().await.map_err(SchemaError::from)
    }

    /// Append source events and clear their pending scopes in one durable batch.
    ///
    /// The event is the recovery record. A pending scope stays conservative
    /// until this batch flushes both the event and the scope deletion.
    pub async fn append_events_and_clear_pending_scopes(
        &self,
        mut events: Vec<AttributionEvent>,
        mutation_ids: &[String],
    ) -> Result<Vec<u64>, SchemaError> {
        if events.is_empty() {
            self.clear_pending_and_flush(mutation_ids).await?;
            return Ok(Vec::new());
        }
        for event in &events {
            event.validate()?;
        }
        let mut tip = self.event_tip.lock().await;
        let mut writes = Vec::with_capacity(events.len() + 1);
        let mut sequences = Vec::with_capacity(events.len());
        for event in &mut events {
            let mutation_key = event_mutation_key(&event.mutation_id);
            if let Some(sequence) =
                self.store
                    .get_item::<u64>(&mutation_key)
                    .await
                    .map_err(|error| {
                        SchemaError::InvalidData(format!(
                            "read attribution event mutation: {error}"
                        ))
                    })?
            {
                sequences.push(sequence);
                continue;
            }
            let seq = tip.saturating_add(1);
            event.seq = seq;
            let value = serde_json::to_vec(event).map_err(|error| {
                SchemaError::InvalidData(format!("encode attribution event: {error}"))
            })?;
            writes.push((event_key(seq).into_bytes(), value));
            let mutation_value = serde_json::to_vec(&seq).map_err(|error| {
                SchemaError::InvalidData(format!("encode attribution event mutation: {error}"))
            })?;
            writes.push((mutation_key.into_bytes(), mutation_value));
            *tip = seq;
            sequences.push(seq);
        }
        if sequences.is_empty() {
            self.clear_pending_and_flush(mutation_ids).await?;
            return Ok(sequences);
        }
        let tip_value = serde_json::to_vec(&*tip).map_err(|error| {
            SchemaError::InvalidData(format!("encode attribution event tip: {error}"))
        })?;
        writes.push((ATTRIBUTION_EVENT_TIP_KEY.as_bytes().to_vec(), tip_value));
        self.store
            .inner()
            .batch_put(writes)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("append attribution event: {error}"))
            })?;
        self.delete_pending_scopes(mutation_ids).await?;
        self.store
            .inner()
            .flush()
            .await
            .map_err(SchemaError::from)?;
        Ok(sequences)
    }

    /// Read a bounded source-event page after one exact sequence frontier.
    pub async fn attribution_events_after(
        &self,
        after: u64,
        limit: usize,
    ) -> Result<AttributionEventPage, SchemaError> {
        if limit == 0 {
            return Ok(AttributionEventPage { events: Vec::new() });
        }
        let events = self
            .store
            .scan_items_in_range_paged::<AttributionEvent>(
                &event_key(after.saturating_add(1)),
                ATTRIBUTION_EVENT_END,
                limit,
            )
            .await
            .map_err(|error| SchemaError::InvalidData(format!("read attribution events: {error}")))?
            .into_iter()
            .map(|(_, event)| event)
            .collect();
        Ok(AttributionEventPage { events })
    }

    /// The H0/H1 frontier for this node-local attribution source.
    pub async fn attribution_event_tip(&self) -> u64 {
        *self.event_tip.lock().await
    }

    /// Persist all pending scopes for one product write batch with one flush.
    pub async fn begin_pending_scopes(
        &self,
        scopes: &[AttributionPendingScope],
    ) -> Result<(), SchemaError> {
        if scopes.is_empty() {
            return Ok(());
        }
        for scope in scopes {
            scope.validate()?;
        }
        self.store
            .batch_put_items(
                scopes
                    .iter()
                    .map(|scope| (scope.storage_key(), scope))
                    .collect(),
            )
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("put attribution pending scopes: {error}"))
            })?;
        self.store.inner().flush().await.map_err(SchemaError::from)
    }

    /// Read one pending scope after restart or a failed write transition.
    pub async fn pending_scope(
        &self,
        mutation_id: &str,
    ) -> Result<Option<AttributionPendingScope>, SchemaError> {
        self.store
            .get_item(&pending_key(mutation_id))
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("get attribution pending scope: {error}"))
            })
    }
}

fn event_key(sequence: u64) -> String {
    format!("{ATTRIBUTION_EVENT_PREFIX}{sequence:020}")
}

fn event_mutation_key(mutation_id: &str) -> String {
    format!(
        "{ATTRIBUTION_EVENT_MUTATION_PREFIX}{}",
        hash(ATTRIBUTION_ROOT_DOMAIN, mutation_id)
    )
}

fn pending_key(mutation_id: &str) -> String {
    format!(
        "{ATTRIBUTION_PENDING_PREFIX}{}",
        hash(ATTRIBUTION_ROOT_DOMAIN, mutation_id)
    )
}
