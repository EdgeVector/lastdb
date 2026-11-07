//! The read of a home: records, not field keys.
//!
//! One key selector reads records by key, or one range of records under one
//! hash. The conditions, the order and the window apply to whole records,
//! then the reply returns every stored field of each record and its one
//! metadata block (decision D7).

mod range;

use super::condition_rules::{matching_records, order_records, reject_unknown_conditions};
use super::*;
use crate::db_operations::record_store::{KeyedRecord, WalkWindow, PAGE_READ_KEYS};
use crate::db_operations::{note_page_resume, note_walk_positions, page_can_resume, PageResume};
use crate::fold_db_core::query::{InlineFileView, QueryRecords, Record, RecordMetadata};
use crate::schema::types::field::filter_utils::DEFAULT_UNFILTERED_PAGE_LIMIT;
use crate::schema::types::field::SAMPLE_PEEK_CAP;
use crate::schema::types::key_comparison::KeyComparisonContext;
use crate::schema::types::KeyValuePageOrder;
use lastdb_model::record_value::StoredRecord;

impl HashRangeQueryProcessor {
    /// One captured read of records. A live record is a row. The conditions
    /// see every candidate, so a window applies only to a read without them.
    pub(super) async fn query_records(
        &self,
        schema: &Schema,
        filter: Option<HashRangeFilter>,
        window: Option<KeyWindow>,
        options: FieldQueryOptions<'_>,
        comparison: &KeyComparisonContext,
    ) -> Result<QueryRecords, SchemaError> {
        let needs_conditions = options.requires_condition_pass();
        if !needs_conditions {
            let rows = self
                .select_rows(schema, comparison, filter.as_ref(), window)
                .await?;
            return Ok(to_records(rows));
        }
        let predicates = options.predicates.unwrap_or(&[]);
        let value_filters = options.value_filters.unwrap_or(&[]);
        reject_unknown_conditions(schema, predicates, value_filters, options.order_by)?;
        let filter = filter.or(Some(full_span_page()));
        let rows = self
            .select_rows(schema, comparison, filter.as_ref(), None)
            .await?;
        let rows = matching_records(rows, predicates, value_filters);
        let rows = order_records(rows, options.order_by, options.predicate_limit);
        Ok(to_records(rows))
    }

    /// The live rows that a key-restricted selector or `None` (the whole
    /// schema) names.
    pub(super) async fn count_records(
        &self,
        schema: &Schema,
        filter: Option<&HashRangeFilter>,
        comparison: &KeyComparisonContext,
    ) -> Result<usize, SchemaError> {
        let filter = filter.cloned().unwrap_or_else(full_span_page);
        Ok(self
            .select_rows(schema, comparison, Some(&filter), None)
            .await?
            .len())
    }

    /// The rows of one selector in page order: the live records of the
    /// schema, bounded by the window.
    /// A range under one hash reads its keys in bounded pages
    /// (`records/range.rs`), so the window ends the read. An unfiltered read
    /// walks the records in record key order, and its window is a window of
    /// that walk (`record_store/record_walk.rs`).
    async fn select_rows(
        &self,
        schema: &Schema,
        comparison: &KeyComparisonContext,
        filter: Option<&HashRangeFilter>,
        window: Option<KeyWindow>,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        if filter.is_some() && matches!(window, Some(KeyWindow::AfterRecord { .. })) {
            return Err(SchemaError::InvalidCursor(
                "a record walk cursor continues only an unfiltered read".into(),
            ));
        }
        if filter.is_none() {
            return self.walk_rows(schema, comparison, window.as_ref()).await;
        }
        if let Some(HashRangeFilter::SampleN(count)) = filter {
            let rows = self.sample_rows(schema, comparison, *count).await?;
            return Ok(match window {
                Some(window) => bound(rows, window),
                None => rows,
            });
        }
        if let Some((hash, bound)) = range::range_under_hash(schema, filter) {
            return self.range_rows(schema, hash, bound, window).await;
        }
        let mut rows = self
            .db_ops
            .records()
            .select_records(schema, comparison, filter)
            .await?;
        rows.retain(|(_, record)| record.is_live());
        Ok(match window {
            Some(window) => bound(rows, window),
            None => rows,
        })
    }
}

impl HashRangeQueryProcessor {
    /// One window of the unfiltered walk of the schema, in record key order.
    /// No window reads the first page of the default size. A cursor names a
    /// row by its API key or a record key position (`AfterRecord`). The page
    /// notes the record key position of each row for the reply, and where it
    /// stopped when a run of deleted records ended it early.
    async fn walk_rows(
        &self,
        schema: &Schema,
        comparison: &KeyComparisonContext,
        window: Option<&KeyWindow>,
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
            .walk_record_window(schema, comparison, walk, StoredRecord::is_live)
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

    /// A sample of the schema: the first `count` live records (at most
    /// `SAMPLE_PEEK_CAP`), in record key order, in page order in the result.
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
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        let walk = WalkWindow {
            after: None,
            skip: 0,
            take: count.min(SAMPLE_PEEK_CAP),
        };
        let mut page = self
            .db_ops
            .records()
            .walk_record_window(schema, comparison, walk, StoredRecord::is_live)
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

/// The reply rows: every stored field of each record and its one metadata
/// block. An empty payload still represents a live record.
fn to_records(rows: Vec<KeyedRecord>) -> QueryRecords {
    let mut out = QueryRecords::with_capacity(rows.len());
    for (key, record) in rows {
        let fields = record.fields.clone().into_iter().collect();
        let metadata = record_metadata(&record);
        out.insert(
            key,
            Record {
                revision: record.stamp.revision(),
                alternates: record.alternates.clone(),
                fields,
                metadata,
            },
        );
    }
    out
}

/// The one metadata block of a record (D7) with its file markers, or `None`
/// when it has none of them.
fn record_metadata(record: &StoredRecord) -> Option<RecordMetadata> {
    let meta = record
        .meta
        .as_ref()
        .filter(|meta| meta.source_file_name.is_some() || !meta.metadata.is_empty());
    let file = record.file.clone();
    let inline_file = record.inline_file.as_ref().map(InlineFileView::from);
    if meta.is_none() && file.is_none() && inline_file.is_none() {
        return None;
    }
    Some(RecordMetadata {
        source_file_name: meta.and_then(|meta| meta.source_file_name.clone()),
        metadata: meta
            .filter(|meta| !meta.metadata.is_empty())
            .map(|meta| meta.metadata.clone().into_iter().collect()),
        file,
        inline_file,
    })
}
