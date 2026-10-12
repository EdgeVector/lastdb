use super::super::FoldDB;
use crate::schema::types::field::{FieldValue, HashRangeFilter};
use crate::schema::types::operations::MutationType;
use crate::schema::types::{KeyValue, Mutation, Query};
use std::collections::HashMap;

impl FoldDB {
    /// Publish the live rows of a legacy personal schema as self-contained
    /// mutation intents after a personal-to-org share.
    ///
    /// The local molecule and atom planes stay untouched. The org target gets
    /// current row bodies, so a friend can restore the schema without a later
    /// owner write. Exact key reads keep this path within the LastDB access
    /// model; the membership walk only returns keys.
    pub(crate) async fn republish_personal_schema_to_org_targets(
        &self,
        schema_name: &str,
    ) -> Result<usize, crate::error::FoldDbError> {
        let Some(engine) = self.sync_engine() else {
            return Ok(0);
        };
        let schema = self
            .schema_manager()
            .get_schema_metadata(schema_name)?
            .ok_or_else(|| crate::schema::SchemaError::NotFound(schema_name.to_string()))?;
        let fields: Vec<String> = schema.runtime_fields.keys().cloned().collect();
        let Some(key) = schema.key.as_ref() else {
            return Ok(0);
        };
        let has_hash = key
            .hash_field
            .as_ref()
            .is_some_and(|field| !field.is_empty());
        let has_range = key
            .range_field
            .as_ref()
            .is_some_and(|field| !field.is_empty());
        if !has_hash && !has_range {
            return Ok(0);
        }

        const KEY_PAGE_SIZE: usize = 128;
        const MUTATION_BATCH_SIZE: usize = 32;
        let mut cursor = None;
        let mut republished_rows = 0;
        loop {
            let page = self
                .list_schema_record_keys(
                    schema_name,
                    schema_name,
                    KEY_PAGE_SIZE,
                    cursor.as_deref(),
                    None,
                )
                .await?;
            let mut batch = Vec::with_capacity(MUTATION_BATCH_SIZE);
            for record_key in page.keys {
                let filter = match (has_hash, has_range) {
                    (true, true) => HashRangeFilter::HashRangeKey {
                        hash: record_key.hash.clone(),
                        range: record_key.range.clone(),
                    },
                    (true, false) => HashRangeFilter::HashKey(record_key.hash.clone()),
                    (false, true) => HashRangeFilter::RangeKey(record_key.range.clone()),
                    (false, false) => continue,
                };
                let rows = self
                    .query_executor()
                    .query(Query::new_with_filter(
                        schema_name.to_string(),
                        fields.clone(),
                        Some(filter),
                    ))
                    .await?;
                let mutations = rows_to_mutations(schema_name, rows);
                for mutation in mutations {
                    batch.push(mutation);
                    if batch.len() == MUTATION_BATCH_SIZE {
                        let envelopes =
                            crate::sync::mutation_intent::encode_mutations(&batch, None);
                        engine
                            .record_op(crate::sync::mutation_intent::mutation_intent_op(envelopes))
                            .await
                            .map_err(crate::error::FoldDbError::Database)?;
                        republished_rows += batch.len();
                        batch.clear();
                    }
                }
            }
            if !batch.is_empty() {
                let count = batch.len();
                let envelopes = crate::sync::mutation_intent::encode_mutations(&batch, None);
                engine
                    .record_op(crate::sync::mutation_intent::mutation_intent_op(envelopes))
                    .await
                    .map_err(crate::error::FoldDbError::Database)?;
                republished_rows += count;
            }
            if !page.has_more {
                break;
            }
            cursor = page.next_cursor;
        }
        Ok(republished_rows)
    }
}

/// Group queried field values by record key and rebuild one self-contained
/// update mutation per key.
fn rows_to_mutations(
    schema_name: &str,
    rows: HashMap<String, HashMap<KeyValue, FieldValue>>,
) -> Vec<Mutation> {
    let mut out = Vec::new();
    let mut by_key: HashMap<crate::schema::types::KeyValue, HashMap<String, FieldValue>> =
        HashMap::new();
    for (field, values) in rows {
        for (key_value, field_value) in values {
            by_key
                .entry(key_value)
                .or_default()
                .insert(field.clone(), field_value);
        }
    }
    for (key_value, values) in by_key {
        let fields_and_values = values
            .iter()
            .map(|(field, value)| (field.clone(), value.value.clone()))
            .collect();
        let pub_key = values
            .values()
            .find_map(|value| value.writer_pubkey.clone())
            .unwrap_or_default();
        let imported_written_at = values.values().filter_map(|value| value.written_at).max();
        let source_file_name = values
            .values()
            .find_map(|value| value.source_file_name.clone());
        let metadata = values.values().find_map(|value| value.metadata.clone());
        let mut mutation = Mutation::new(
            schema_name.to_string(),
            fields_and_values,
            key_value,
            pub_key,
            MutationType::Update,
        );
        mutation.imported_written_at = imported_written_at;
        mutation.source_file_name = source_file_name;
        mutation.metadata = metadata;
        out.push(mutation);
    }
    out
}
