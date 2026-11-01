//! The read of a format 7 home: records, not field molecules.
//!
//! One key selector reads records by key, or one range of records under one
//! hash. The conditions, the order and the window apply to whole records,
//! then the projection returns the selected fields of each record. A record
//! has no atom or molecule, so each returned field carries the record
//! metadata (decision D7) and no atom, molecule or writer ID. While the node
//! receives data from other users, the read merges the received scopes by key
//! (`records/received.rs`).

mod received;
mod received_range;

use super::condition_rules::{
    condition_fields, exact_key_selected_fields, matching_field_predicate_keys,
    order_condition_keys,
};
use super::*;
use crate::db_operations::record_store::{record_scope, KeyedRecord, WalkWindow, PAGE_READ_KEYS};
use crate::db_operations::{note_page_resume, note_walk_positions, page_can_resume, PageResume};
use crate::fold_db_core::query::{FieldMetadata, QueryRecords, Record};
use crate::schema::types::field::filter_utils::DEFAULT_UNFILTERED_PAGE_LIMIT;
use crate::schema::types::field::SAMPLE_PEEK_CAP;
use crate::schema::types::key_comparison::KeyComparisonContext;
use crate::schema::types::KeyValuePageOrder;
use lastdb_model::record_value::StoredRecord;

type FieldMap = HashMap<String, HashMap<KeyValue, FieldValue>>;

impl HashRangeQueryProcessor {
    /// One captured read of records. A record is a row when it has the key
    /// field or one of the selected fields. The conditions see every
    /// candidate, so a window applies only to a read without them.
    pub(super) async fn query_records(
        &self,
        schema: &Schema,
        fields: &[String],
        filter: Option<HashRangeFilter>,
        window: Option<KeyWindow>,
        options: FieldQueryOptions<'_>,
        comparison: &KeyComparisonContext,
    ) -> Result<QueryRecords, SchemaError> {
        let selected = record_fields(schema, fields);
        let needs_conditions = options.requires_condition_pass();
        if !needs_conditions {
            let rows = self
                .select_rows(schema, comparison, filter.as_ref(), window, &selected)
                .await?;
            return Ok(to_records(&selected, rows));
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
        Ok(to_records(
            &exact_key_selected_fields(schema, fields, options.order_by),
            kept,
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
    /// window ends the read in the own scope too. An unfiltered read walks
    /// the records of every scope in record key order, and its window is a
    /// window of that walk (`record_store/record_walk.rs`).
    async fn select_rows(
        &self,
        schema: &Schema,
        comparison: &KeyComparisonContext,
        filter: Option<&HashRangeFilter>,
        window: Option<KeyWindow>,
        selected: &[String],
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        if filter.is_some() && matches!(window, Some(KeyWindow::AfterRecord { .. })) {
            return Err(SchemaError::InvalidCursor(
                "a record walk cursor continues only an unfiltered read".into(),
            ));
        }
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
        if filter.is_none() {
            return self
                .walk_rows(schema, comparison, window.as_ref(), selected, &scopes)
                .await;
        }
        if let Some(HashRangeFilter::SampleN(count)) = filter {
            let rows = self
                .sample_rows(schema, comparison, *count, selected, &scopes)
                .await?;
            return Ok(match window {
                Some(window) => bound(rows, window),
                None => rows,
            });
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
            .records()
            .select_records(schema, comparison, filter, own.as_deref())
            .await?;
        rows.retain(|(_, record)| has_any(record, selected));
        Ok(match window {
            Some(window) => bound(rows, window),
            None => rows,
        })
    }
}

impl HashRangeQueryProcessor {
    /// One window of the unfiltered walk over `scopes`, in record key order.
    /// No window reads the first page of the default size. A cursor names a
    /// row by its API key or a record key position (`AfterRecord`). The page
    /// notes the record key position of each row for the reply, and where it
    /// stopped when a run of deleted records ended it early.
    async fn walk_rows(
        &self,
        schema: &Schema,
        comparison: &KeyComparisonContext,
        window: Option<&KeyWindow>,
        selected: &[String],
        scopes: &[Option<String>],
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        let records = self.db_ops.records();
        let (after, skip, take) = match window {
            None => (None, 0, DEFAULT_UNFILTERED_PAGE_LIMIT),
            Some(KeyWindow::Offset { offset, limit }) => (None, *offset, *limit),
            Some(KeyWindow::After { after, limit }) => {
                (Some(records.walk_position(schema, after)?), 0, *limit)
            }
            Some(KeyWindow::AfterRecord { record, limit }) => (Some(record.clone()), 0, *limit),
        };
        // Only a page that its caller continues may end early.
        let resumable = window.is_some() && page_can_resume();
        let walk = WalkWindow {
            after: after.as_deref(),
            skip,
            take,
        };
        let mut page = records
            .walk_record_window(schema, comparison, walk, scopes, |record| {
                has_any(record, selected)
            })
            .await?;
        if let Some(stopped) = page.stopped.take() {
            if !resumable {
                return Err(SchemaError::ReadResourceLimit {
                    detail: format!(
                        "an unfiltered read passed {PAGE_READ_KEYS} deleted records before its \
                         page was full; read it in pages with next_cursor"
                    ),
                });
            }
            note_page_resume(PageResume::Record(stopped));
        }
        note_walk_positions(
            page.rows
                .iter()
                .map(|row| (row.key.clone(), row.at.clone()))
                .collect(),
        );
        Ok(page.into_keyed())
    }

    /// A sample over `scopes`: the first `count` live records (at most
    /// `SAMPLE_PEEK_CAP`) with one of the selected fields, in record key
    /// order, in page order in the result. One walk merges the scopes, as the
    /// unfiltered walk does, so a received record fills the place of a record
    /// that the own scope deleted.
    ///
    /// A run of deleted records can use the read allowance of the walk before
    /// the sample is full. A caller that continues a page that ends early
    /// gets the short sample and the record key position where the walk
    /// stopped (`PageResume::Record`), so its reply says the sample is not
    /// complete. Any other caller gets a read limit error: a short sample with
    /// no sign that it is short looks like the whole schema.
    async fn sample_rows(
        &self,
        schema: &Schema,
        comparison: &KeyComparisonContext,
        count: usize,
        selected: &[String],
        scopes: &[Option<String>],
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        let walk = WalkWindow {
            after: None,
            skip: 0,
            take: count.min(SAMPLE_PEEK_CAP),
        };
        let mut page = self
            .db_ops
            .records()
            .walk_record_window(schema, comparison, walk, scopes, |record| {
                has_any(record, selected)
            })
            .await?;
        if let Some(stopped) = page.stopped.take() {
            if !page_can_resume() {
                return Err(SchemaError::ReadResourceLimit {
                    detail: format!(
                        "a sample passed {PAGE_READ_KEYS} deleted records before it was full; \
                         read the schema in pages with next_cursor"
                    ),
                });
            }
            note_page_resume(PageResume::Record(stopped));
        }
        let mut rows = page.into_keyed();
        rows.sort_by(|a, b| a.0.cmp_page_order(&b.0));
        Ok(rows)
    }
}

/// The selected fields and the declared key field. The key field sets the
/// record set and is in each row even when unprojected
/// (`field_rules::primary_field`).
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
        // `select_rows` refuses a record walk cursor on a filtered read.
        KeyWindow::AfterRecord { limit, .. } => rows.into_iter().take(limit).collect(),
    }
}

/// The selected fields of each candidate row, by field, for the conditions
/// and the value order. Every selected field has a map, even when no row has
/// a value for it.
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
        source_file_name: meta.and_then(|meta| meta.source_file_name.clone()),
        metadata: meta
            .filter(|meta| !meta.metadata.is_empty())
            .map(|meta| meta.metadata.clone().into_iter().collect()),
        molecule_uuid: None,
        writer_pubkey: None,
        written_at: record.stamps.get(field).map(|stamp| stamp.written_at),
    })
}

/// The reply rows: the selected fields of each record and the metadata of
/// each returned field. A row with none of the selected fields is not a row.
fn to_records(selected: &[String], rows: Vec<KeyedRecord>) -> QueryRecords {
    let mut out = QueryRecords::with_capacity(rows.len());
    for (key, record) in rows {
        let mut row = Record {
            fields: HashMap::new(),
            metadata: HashMap::new(),
        };
        for field in selected {
            let Some(value) = record.fields.get(field) else {
                continue;
            };
            row.fields.insert(field.clone(), value.clone());
            row.metadata.insert(field.clone(), field_metadata(&record));
        }
        if !row.fields.is_empty() {
            out.insert(key, row);
        }
    }
    out
}

/// The metadata of one returned field: the record metadata (D7).
fn field_metadata(record: &StoredRecord) -> FieldMetadata {
    let meta = record.meta.as_ref();
    FieldMetadata {
        source_file_name: meta.and_then(|meta| meta.source_file_name.clone()),
        metadata: meta
            .filter(|meta| !meta.metadata.is_empty())
            .map(|meta| meta.metadata.clone().into_iter().collect()),
        molecule_uuid: None,
        writer_pubkey: None,
    }
}
