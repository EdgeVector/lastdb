//! Secondary-field steps of the co-key plan: tip matching, then atom bodies.

use super::*;

impl HashRangeQueryProcessor {
    /// Fields to query: every declared field when `fields` is empty, otherwise
    /// the requested runtime fields minus record-molecule fields.
    pub(super) fn cokey_selected_fields(schema: &Schema, fields: &[String]) -> Vec<String> {
        if fields.is_empty() {
            crate::record_molecule::declared_runtime_field_names(schema)
        } else {
            fields
                .iter()
                .filter(|f| {
                    schema.runtime_fields.contains_key(f.as_str())
                        && !crate::record_molecule::is_record_molecule_field(f)
                })
                .cloned()
                .collect()
        }
    }

    /// Match every secondary field against the page keys, collecting the atom
    /// bodies still to resolve.
    #[allow(
        clippy::too_many_arguments,
        reason = "mirrors the co-key plan inputs; bundling them into a struct would only move the argument list"
    )]
    pub(super) async fn collect_cokey_secondary_pending(
        &self,
        schema: &mut Schema,
        secondary: Vec<String>,
        fanout_keys: &[KeyValue],
        fanout_sources: &HashMap<KeyValue, KeySource>,
        as_of: Option<DateTime<Utc>>,
        include_tombstones: bool,
        request_concurrency: Option<usize>,
    ) -> Result<Vec<SecondaryPending>, SchemaError> {
        let mut pending: Vec<SecondaryPending> = Vec::new();
        if as_of.is_some() {
            for fname in &secondary {
                // History: hydrate + rewind per storage prefix, keep page_keys.
                self.secondary_matches_as_of(
                    schema,
                    fname,
                    fanout_keys,
                    fanout_sources,
                    as_of,
                    include_tombstones,
                    &mut pending,
                )
                .await?;
            }
        } else {
            // Each field has a separate molecule. Overlap its slot load, but
            // preserve field order when collecting pending atom bodies.
            let configured = std::env::var("LASTDB_QUERY_SECONDARY_CONCURRENCY").ok();
            let concurrency =
                secondary_field_concurrency(configured.as_deref(), request_concurrency);
            let schema: &Schema = schema;
            let mut fields = stream::iter(secondary.into_iter().map(|fname| async move {
                let mut field_pending = Vec::new();
                self.secondary_matches_current(
                    schema,
                    &fname,
                    fanout_keys,
                    fanout_sources,
                    include_tombstones,
                    &mut field_pending,
                )
                .await?;
                Ok::<_, SchemaError>(field_pending)
            }))
            .buffered(concurrency);
            while let Some(field_pending) = fields.next().await {
                pending.extend(field_pending?);
            }
        }
        Ok(pending)
    }

    /// Resolve the pending atom bodies in one batch and file each value under
    /// its field and key in `result`.
    pub(super) async fn fill_cokey_secondaries(
        &self,
        schema: &Schema,
        pending: Vec<SecondaryPending>,
        include_tombstones: bool,
        result: &mut HashMap<String, HashMap<KeyValue, FieldValue>>,
    ) -> Result<(), SchemaError> {
        let values = self.resolve_pending_values(schema, &pending).await?;
        for (entry, value) in pending.into_iter().zip(values) {
            if let Some(value) = value {
                if include_tombstones || !crate::atom::is_tombstone_value(&value.value) {
                    result.entry(entry.0).or_default().insert(entry.1, value);
                }
            }
        }
        Ok(())
    }
}
