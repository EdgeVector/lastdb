//! Keyed record query processor and paged range reads under one hash.

use super::{QueryRecords, Record};
use crate::db_operations::record_store::{
    KeyedRecord, RecordBound, RecordSpan, RecordStore, SpanKey,
};
use crate::db_operations::{note_page_resume, page_can_resume, DbOperations, PageResume};
use crate::schema::types::field::{HashRangeFilter, KeyWindow};
use crate::schema::types::key_comparison::KeyComparisonContext;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, KeyValuePageOrder, Schema, SchemaError};
use futures::FutureExt;
use std::collections::VecDeque;
use std::sync::Arc;

const PAGE_READ_KEYS: usize = 8_192;
const MIN_STEP: usize = 32;
const MAX_STEP: usize = 1024;

struct RangeKeys {
    span: Option<RecordSpan>,
    keys: VecDeque<SpanKey>,
    cursor: Option<String>,
    done: bool,
}

impl RangeKeys {
    fn new(span: Option<RecordSpan>) -> Self {
        Self {
            done: span.is_none(),
            span,
            keys: VecDeque::new(),
            cursor: None,
        }
    }

    async fn fill(
        &mut self,
        records: &RecordStore,
        wanted: usize,
        room: usize,
    ) -> Result<usize, SchemaError> {
        let Some(span) = self.span.as_ref().filter(|_| !self.done) else {
            return Ok(0);
        };
        let ask = wanted.saturating_sub(self.keys.len()).min(room);
        if ask == 0 {
            return Ok(0);
        }
        let page = records
            .record_span_page(span, self.cursor.as_deref(), ask)
            .await?;
        let read = page.keys.len();
        self.keys.extend(page.keys);
        self.done = page.next.is_none();
        if page.next.is_some() {
            self.cursor = page.next;
        }
        Ok(read)
    }

    fn examined(&self) -> Option<Option<&str>> {
        let span = self.span.as_ref().filter(|_| !self.done)?;
        Some(
            self.cursor
                .as_deref()
                .map(|cursor| span.storage_range(cursor)),
        )
    }
}

/// Processor for keyed record queries of a home.
pub struct HashRangeQueryProcessor {
    db_ops: Arc<DbOperations>,
}

impl HashRangeQueryProcessor {
    pub fn new(db_ops: Arc<DbOperations>) -> Self {
        Self { db_ops }
    }

    fn require_supported_query(
        schema: &Schema,
        filter: Option<&HashRangeFilter>,
    ) -> Result<(), SchemaError> {
        crate::schema::require_logical_layout(&schema.schema_type)?;
        crate::schema::require_logical_filter(filter)
    }

    async fn current_query_scope<T>(
        &self,
        comparison: Arc<KeyComparisonContext>,
        future: impl std::future::Future<Output = Result<T, SchemaError>>,
    ) -> Result<T, SchemaError> {
        let scope = self.db_ops.read_scope().await?;
        comparison.run(scope.run_result(future)).await
    }

    pub async fn count_rows(
        &self,
        schema: &Schema,
        filter: Option<&HashRangeFilter>,
    ) -> Result<usize, SchemaError> {
        Self::require_supported_query(schema, filter)?;
        let comparison = self.db_ops.records().key_comparison_context(schema)?;
        let filter = comparison
            .canonical_filter(filter.cloned())?
            .ok_or_else(|| {
                SchemaError::InvalidData("LastDB requires a Hash or HashRange key selector".into())
            })?;
        self.current_query_scope(
            comparison,
            async move { Ok(self.select_rows(schema, &filter, None).await?.len()) }.boxed(),
        )
        .await
    }

    pub async fn query_with_filter(
        &self,
        schema: &Schema,
        filter: Option<HashRangeFilter>,
        window: Option<KeyWindow>,
    ) -> Result<QueryRecords, SchemaError> {
        Self::require_supported_query(schema, filter.as_ref())?;
        let comparison = self.db_ops.records().key_comparison_context(schema)?;
        let filter = comparison.canonical_filter(filter)?.ok_or_else(|| {
            SchemaError::InvalidData("LastDB requires a Hash or HashRange key selector".into())
        })?;
        let window = comparison.canonical_window(window)?;
        self.current_query_scope(
            comparison,
            async move {
                let rows = self.select_rows(schema, &filter, window).await?;
                let mut out = QueryRecords::with_capacity(rows.len());
                for (key, record) in rows {
                    out.insert(key, Record::from_stored(&record));
                }
                Ok(out)
            }
            .boxed(),
        )
        .await
    }

    async fn select_rows(
        &self,
        schema: &Schema,
        filter: &HashRangeFilter,
        window: Option<KeyWindow>,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        if let Some((hash, bound)) = range_under_hash(schema, filter) {
            return self.range_rows(schema, hash, bound, window).await;
        }
        let mut rows = self.db_ops.records().select_records(schema, filter).await?;
        rows.retain(|(_, record)| record.is_live());
        Ok(match window {
            Some(KeyWindow::Offset { offset, limit }) => {
                rows.into_iter().skip(offset).take(limit).collect()
            }
            Some(KeyWindow::After { after, limit }) => rows
                .into_iter()
                .filter(|(key, _)| key.cmp_page_order(&after).is_gt())
                .take(limit)
                .collect(),
            None => rows,
        })
    }

    async fn range_rows(
        &self,
        schema: &Schema,
        hash: &str,
        bound: RecordBound<'_>,
        window: Option<KeyWindow>,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        let resumable = window.is_some() && page_can_resume();
        let (after, mut skip, limit) = match window {
            None => (None, 0, usize::MAX),
            Some(KeyWindow::Offset { offset, limit }) => (None, offset, limit),
            Some(KeyWindow::After { after, limit }) => (Some(after), 0, limit),
        };
        let records = self.db_ops.records();
        let from_range = after.as_ref().and_then(|after| after.range.as_deref());
        let mut source = RangeKeys::new(records.record_span(schema, hash, bound, from_range)?);
        let mut rows = Vec::new();
        let mut room = PAGE_READ_KEYS;
        let mut wide = 1usize;
        while rows.len() < limit {
            let step = skip
                .saturating_add(limit - rows.len())
                .saturating_mul(wide)
                .clamp(MIN_STEP, MAX_STEP);
            let read = source.fill(records, step, room).await?;
            room = room.saturating_sub(read);
            let Some(taken) = next_keys(&mut source, step) else {
                if source.done {
                    break;
                }
                let moved = |stop: &KeyValue| {
                    after
                        .as_ref()
                        .is_none_or(|after| stop.cmp_page_order(after).is_gt())
                };
                match stop_key(records, &source)? {
                    Some(stop) if resumable && skip == 0 && moved(&stop) => {
                        note_page_resume(PageResume::Key(stop));
                        break;
                    }
                    _ => return Err(range_limit_error(skip > 0)),
                }
            };
            let mark = (rows.len(), skip);
            let mut kept = Vec::new();
            for winner in taken {
                if after
                    .as_ref()
                    .is_some_and(|after| winner.key.cmp_page_order(after).is_le())
                    || !winner.record.is_live()
                {
                    continue;
                }
                if skip > 0 {
                    skip -= 1;
                    continue;
                }
                kept.push(winner);
                if rows.len() + kept.len() == limit {
                    break;
                }
            }
            room = room.saturating_sub(kept.len());
            rows.extend(read_kept(records, kept).await?);
            if (rows.len(), skip) == mark {
                wide = wide.saturating_mul(2);
            }
        }
        Ok(rows)
    }
}

fn next_keys(source: &mut RangeKeys, step: usize) -> Option<Vec<SpanKey>> {
    let bound = match source.examined() {
        Some(None) => return None,
        Some(Some(examined)) => Some(examined.to_owned()),
        None => None,
    };
    let count = source
        .keys
        .iter()
        .take(step)
        .take_while(|key| {
            bound
                .as_deref()
                .is_none_or(|bound| key.range.as_str() <= bound)
        })
        .count();
    if count == 0 {
        return None;
    }
    Some(
        source
            .keys
            .drain(..count)
            .filter(|key| key.record.is_live())
            .collect(),
    )
}

async fn read_kept(
    record_store: &RecordStore,
    kept: Vec<SpanKey>,
) -> Result<Vec<KeyedRecord>, SchemaError> {
    if kept.is_empty() {
        return Ok(Vec::new());
    }
    let (keys, physical): (Vec<KeyValue>, Vec<String>) = kept
        .into_iter()
        .map(|kept| (kept.key, kept.physical))
        .unzip();
    let records = record_store.read_records(&physical).await?;
    Ok(keys
        .into_iter()
        .zip(records)
        .filter_map(|(key, record)| {
            record
                .filter(|record| record.is_live())
                .map(|record| (key, record))
        })
        .collect())
}

fn stop_key(records: &RecordStore, source: &RangeKeys) -> Result<Option<KeyValue>, SchemaError> {
    let Some(span) = source.span.as_ref().filter(|_| !source.done) else {
        return Ok(None);
    };
    source
        .cursor
        .as_deref()
        .map(|cursor| records.span_key(span, cursor))
        .transpose()
}

fn range_limit_error(offset: bool) -> SchemaError {
    let continuation = if offset {
        "read it with next_cursor pages, not an offset"
    } else {
        "read it in pages with next_cursor; a count or a condition needs the complete range"
    };
    SchemaError::ReadResourceLimit {
        detail: format!(
            "the range under one hash holds more deleted records than one request reads \
             ({PAGE_READ_KEYS} keys); {continuation}"
        ),
    }
}

fn range_under_hash<'a>(
    schema: &Schema,
    filter: &'a HashRangeFilter,
) -> Option<(&'a str, RecordBound<'a>)> {
    if schema.schema_type != DeclarativeSchemaType::HashRange {
        return None;
    }
    match filter {
        HashRangeFilter::HashKey(hash) => Some((hash.as_str(), RecordBound::All)),
        HashRangeFilter::HashRangePrefix { hash, prefix } => {
            Some((hash.as_str(), RecordBound::Prefix(prefix)))
        }
        HashRangeFilter::HashRangeRange { hash, start, end } => {
            Some((hash.as_str(), RecordBound::Between(start, end)))
        }
        HashRangeFilter::HashRangeKey { .. } | HashRangeFilter::HashRangeKeys(_) => None,
    }
}
