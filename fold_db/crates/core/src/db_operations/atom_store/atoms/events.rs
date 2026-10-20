use super::*;

impl AtomStore {
    /// Keep imported Puts that lost to a Delete without changing a tip.
    /// The caller supplies stable keys so replay of one source mutation is
    /// idempotent. Each history row and its atom edge share one durable batch.
    pub(crate) async fn batch_store_suppressed_put_events(
        &self,
        events: Vec<(String, MutationEvent)>,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        if events.is_empty() {
            return Ok(());
        }
        let mut items = Vec::with_capacity(events.len() * 2);
        let mut keys = Vec::with_capacity(events.len());
        for (key, event) in events {
            if event.kind != crate::atom::MutationEventKind::SuppressedPut {
                return Err(SchemaError::InvalidData(
                    "suppressed Put history requires a suppressed Put event".into(),
                ));
            }
            items.extend(self.mutation_history_atom_ref_edge_items(
                &key,
                &event,
                storage_prefix,
            )?);
            items.push((
                key.clone(),
                serde_json::to_value(event).map_err(|error| {
                    SchemaError::InvalidData(format!("serialize suppressed Put history: {error}"))
                })?,
            ));
            keys.push(key);
        }
        self.batch_put_items_with_atom_ref_v2(items, storage_prefix)
            .await?;
        for key in keys {
            crate::atom::history_event_memo::invalidate_for_event_key(&key);
        }
        Ok(())
    }
}
