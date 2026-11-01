//! The read of a format 7 home: records, not field molecules.
//!
//! One key selector reads records by key, or one range of records under one
//! hash. The conditions, the order and the window apply to whole records,
//! then the projection returns the selected fields of each record. A record
//! has no atom or molecule, so a field value carries the record metadata
//! (decision D7) and no atom, molecule or writer ID. While the node receives
//! data from other users, the read merges the received scopes by key
//! (`records/received.rs`).

mod received;
mod received_range;

use super::condition_rules::{
    condition_fields, exact_key_selected_fields, matching_field_predicate_keys,
    order_condition_keys,
};
use super::*;
use crate::db_operations::atom_store::{record_scope, KeyedRecord};
use crate::schema::types::key_comparison::KeyComparisonContext;
use crate::schema::types::KeyValuePageOrder;
use lastdb_model::record_value::StoredRecord;

type FieldMap = HashMap<String, HashMap<KeyValue, FieldValue>>;

impl HashRangeQueryProcessor {
    /// True when the node serves a format 7 home.
    pub(super) fn record_home(&self) -> bool {
        self.db_ops.atoms().record_home()
    }

    /// One captured read of records. A record is a row when it has the key
    /// field or one of the selected fields. The conditions see every
    /// candidate, as in the molecule path, so a window applies only to a read
    /// without them.
    pub(super) async fn query_records(
        &self,
        schema: &Schema,
        fields: &[String],
        filter: Option<HashRangeFilter>,
        window: Option<KeyWindow>,
        options: FieldQueryOptions<'_>,
        comparison: &KeyComparisonContext,
    ) -> Result<FieldMap, SchemaError> {
        let selected = record_fields(schema, fields);
        let needs_conditions = options.requires_condition_pass();
        if !needs_conditions {
            let rows = self
                .select_rows(schema, comparison, filter.as_ref(), window, &selected)
                .await?;
            return Ok(project(&selected, &rows));
        }
        let filter = filter.or_else(|| Some(full_span_page()));
        let rows = self
            .select_rows(schema, comparison, filter.as_ref(), None, &selected)
            .await?;

        let predicates = options.predicates.unwrap_or(&[]);
        let value_filters = options.value_filters.unwrap_or(&[]);
        let candidates = CokeyRows {
            fields: project(
                &condition_fields(schema, predicates, value_filters, options.order_by)?,
                &rows,
            ),
            page_keys: rows.iter().map(|(key, _)| key.clone()).collect(),
            key_sources: HashMap::new(),
            primary_field: String::new(),
        };
        let keys = matching_field_predicate_keys(&candidates, predicates, value_filters);
        let kept = order_condition_keys(
            keys,
            &candidates.fields,
            options.order_by,
            options.predicate_limit,
        );
        let mut by_key: HashMap<KeyValue, std::sync::Arc<StoredRecord>> =
            rows.into_iter().collect();
        let kept: Vec<KeyedRecord> = kept
            .into_iter()
            .filter_map(|key| by_key.remove(&key).map(|record| (key, record)))
            .collect();
        Ok(project(
            &exact_key_selected_fields(schema, fields, options.order_by),
            &kept,
        ))
    }

    /// The live rows that a key-restricted selector or `None` (the whole
    /// schema) names, with one of the selected fields.
    pub(super) async fn count_records(
        &self,
        schema: &Schema,
        fields: &[String],
        filter: Option<&HashRangeFilter>,
        comparison: &KeyComparisonContext,
    ) -> Result<usize, SchemaError> {
        let selected = record_fields(schema, fields);
        let filter = filter.cloned().unwrap_or_else(full_span_page);
        Ok(self
            .select_rows(schema, comparison, Some(&filter), None, &selected)
            .await?
            .len())
    }

    /// The rows of one selector in page order: the live records of the own
    /// scope with one of the selected fields, bounded by the window. While a
    /// sender is active, the received scopes join the read; a node with no
    /// sender reads only its own scope. A range under one hash reads its keys
    /// in bounded pages of each scope (`records/received_range.rs`), so the
    /// window ends the read in the own scope too.
    async fn select_rows(
        &self,
        schema: &Schema,
        comparison: &KeyComparisonContext,
        filter: Option<&HashRangeFilter>,
        window: Option<KeyWindow>,
        selected: &[String],
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        let mut scopes = vec![record_scope(schema)];
        for namespace in self
            .collect_received_from_namespaces()
            .await?
            .into_iter()
            .map(Some)
        {
            if !scopes.contains(&namespace) {
                scopes.push(namespace);
            }
        }
        if let Some((hash, bound)) = received::range_under_hash(schema, filter) {
            return self
                .received_range(schema, hash, bound, &scopes, selected, window)
                .await;
        }
        if scopes.len() > 1 {
            return self
                .select_received(schema, comparison, filter, window, selected, &scopes)
                .await;
        }
        // The own scope is the only scope.
        let own = scopes.pop().flatten();
        let mut rows = self
            .db_ops
            .atoms()
            .select_records(schema, comparison, filter, own.as_deref())
            .await?;
        rows.retain(|(_, record)| has_any(record, selected));
        Ok(match window {
            Some(window) => bound(rows, window),
            None => rows,
        })
    }
}

/// The selected fields and the declared key field. The key field sets the
/// record set and is in each row even when unprojected, as in the molecule
/// path (`field_rules::primary_field`).
fn record_fields(schema: &Schema, fields: &[String]) -> Vec<String> {
    let mut selected = field_rules::selected_fields(schema, fields);
    if !selected.is_empty() {
        let primary = field_rules::primary_field(schema, &selected);
        if !selected.contains(&primary) {
            selected.insert(0, primary);
        }
    }
    selected
}

fn has_any(record: &StoredRecord, selected: &[String]) -> bool {
    selected
        .iter()
        .any(|field| record.fields.contains_key(field))
}

/// The window over rows in page order.
fn bound(rows: Vec<KeyedRecord>, window: KeyWindow) -> Vec<KeyedRecord> {
    match window {
        KeyWindow::Offset { offset, limit } => rows.into_iter().skip(offset).take(limit).collect(),
        KeyWindow::After { after, limit } => rows
            .into_iter()
            .filter(|(key, _)| key.cmp_page_order(&after).is_gt())
            .take(limit)
            .collect(),
    }
}

/// The selected fields of each row. Every selected field has a map, as in
/// the molecule path, even when no row has a value for it.
fn project(selected: &[String], rows: &[KeyedRecord]) -> FieldMap {
    let mut out: FieldMap = selected
        .iter()
        .map(|field| (field.clone(), HashMap::new()))
        .collect();
    for (key, record) in rows {
        for (field, values) in &mut out {
            if let Some(value) = field_value(record, field) {
                values.insert(key.clone(), value);
            }
        }
    }
    out
}

/// One field of a record. The metadata belongs to the record (D7).
fn field_value(record: &StoredRecord, field: &str) -> Option<FieldValue> {
    let value = record.fields.get(field)?;
    let meta = record.meta.as_ref();
    Some(FieldValue {
        value: value.clone(),
        atom_uuid: String::new(),
        source_file_name: meta.and_then(|meta| meta.source_file_name.clone()),
        metadata: meta
            .filter(|meta| !meta.metadata.is_empty())
            .map(|meta| meta.metadata.clone().into_iter().collect()),
        molecule_uuid: None,
        writer_pubkey: None,
        written_at: record.stamps.get(field).map(|stamp| stamp.written_at),
    })
}
