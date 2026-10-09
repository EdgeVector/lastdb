use super::*;

impl AtomStore {
    /// Batch store mutation events for point-in-time query support.
    /// Events are stored with zero-padded nanosecond timestamps for lexicographic ordering.
    ///
    /// When `storage_prefix` is `Some`, all keys are prefixed with `{storage_prefix}:`.
    ///
    /// Within a batch, two events on the same molecule with the same
    /// `timestamp_nanos` would naively map to the same storage key
    /// (`history:{mol}:{ts:020}`) and the second would overwrite the
    /// first in the underlying batch insert — Last Store and the
    /// in-memory backend last-write-wins on a duplicate key in a
    /// `Batch`. That dropped event then never reaches
    /// `FieldVariant::rewind_to`, so an `as_of` query crossing the lost
    /// event's timestamp silently returns a state the molecule was
    /// never in (the missing field-key flip is skipped). Collisions
    /// are reachable whenever `MutationManager` emits >1 event for the
    /// same molecule in one `apply_mutations_to_molecules` call and the
    /// per-iteration `Utc::now()` returns the same nanosecond — a real
    /// possibility on platforms with coarser-than-nanosecond
    /// `SystemTime::now()` resolution and a non-trivial one even on
    /// nanosecond clocks since consecutive calls are not guaranteed to
    /// advance.
    ///
    /// Fix: group events by `molecule_uuid`, sort each group by
    /// timestamp, and walk through bumping the storage key's
    /// `ts_nanos` by 1 whenever it would not strictly exceed the
    /// previous storage `ts_nanos` for the same molecule. Mirrors the
    /// `+i` disambiguator that
    /// `SyncEngine::store_merge_conflicts` already applies for
    /// merge-conflict-originated history rows. The event's own
    /// `timestamp` field is left untouched — only the storage key is
    /// adjusted — so the actual `MutationEvent.timestamp` that
    /// downstream rewind reads is the original wall-clock value.
    pub async fn batch_store_mutation_events(
        &self,
        events: Vec<MutationEvent>,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        if events.is_empty() {
            return Ok(());
        }

        // Sort by (molecule_uuid, timestamp_nanos) so we can detect
        // same-molecule collisions in a single linear pass — events for
        // different molecules can never collide (the key includes
        // `molecule_uuid`), so they're independent.
        let mut indexed: Vec<(i64, MutationEvent)> = events
            .into_iter()
            .map(|e| (e.timestamp.timestamp_nanos_opt().unwrap_or(0), e))
            .collect();
        indexed.sort_by(|a, b| {
            a.1.molecule_uuid
                .cmp(&b.1.molecule_uuid)
                .then_with(|| a.0.cmp(&b.0))
        });

        let mut items: Vec<(String, Value)> = Vec::with_capacity(indexed.len() * 3);
        let mut event_keys = Vec::with_capacity(indexed.len());
        let mut prev_mol: Option<String> = None;
        let mut prev_ts: i64 = 0;
        for (mut ts, event) in indexed {
            if prev_mol.as_deref() == Some(event.molecule_uuid.as_str()) && ts <= prev_ts {
                ts = prev_ts.saturating_add(1);
            }
            prev_mol = Some(event.molecule_uuid.clone());
            prev_ts = ts;
            let base_key = molecule_key_codec::history_event_key(&event.molecule_uuid, ts);
            let key = build_storage_key(storage_prefix, &base_key);
            items.extend(self.mutation_history_atom_ref_edge_items(
                &key,
                &event,
                storage_prefix,
            )?);
            items.push((
                key.clone(),
                serde_json::to_value(event).map_err(|e| {
                    SchemaError::InvalidData(format!("serialize mutation event: {e}"))
                })?,
            ));
            event_keys.push(key);
        }

        self.batch_put_items_with_atom_ref_v2(items, storage_prefix)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!("Failed to store mutation events: {e}"))
            })?;
        for key in event_keys {
            // Invalidate only after the history + edge batch returns success.
            crate::atom::legacy_history_memo::invalidate_for_event_key(&key);
        }
        Ok(())
    }

    /// Keep imported Puts that lost to a Delete without changing a tip.
    /// The caller supplies stable keys so replay of one source mutation is
    /// idempotent. Each history row and its atom edge share one durable batch.
    #[cfg_attr(not(feature = "cloud-sync"), allow(dead_code))]
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
            crate::atom::legacy_history_memo::invalidate_for_event_key(&key);
        }
        Ok(())
    }
}
