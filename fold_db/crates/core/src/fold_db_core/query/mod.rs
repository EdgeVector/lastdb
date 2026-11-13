//! Query types and keyed hash/range execution.

pub mod query_executor;

pub use query_executor::{AlternatePage, QueryExecutor};

use crate::db_operations::record_store::{
    KeyedRecord, RecordBound, RecordSpan, RecordStore, SpanKey,
};
use crate::db_operations::{note_page_resume, page_can_resume, DbOperations, PageResume};
use crate::schema::types::field::{HashRangeFilter, KeyWindow};
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, KeyValuePageOrder, Query, Schema, SchemaError};
use crate::schema::SchemaCore;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;

const PAGE_READ_KEYS: usize = 8_192;
const MIN_STEP: usize = 32;
const MAX_STEP: usize = 1024;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RecordMetadata {
    pub source_file_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<lastdb_model::record_value::FileMarker>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inline_file: Option<lastdb_model::record_value::FileMarker>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub revision: String,
    pub alternates: BTreeMap<String, Option<KeyValue>>,
    pub fields: HashMap<String, Value>,
    pub metadata: Option<RecordMetadata>,
}

pub type QueryRecords = HashMap<KeyValue, Record>;

impl Record {
    pub fn from_stored(r: &lastdb_model::record_value::StoredRecord) -> Self {
        let meta = r
            .meta
            .as_ref()
            .filter(|m| m.source_file_name.is_some() || !m.metadata.is_empty());
        let metadata = (meta.is_some() || r.file.is_some() || r.inline_file.is_some()).then(|| {
            RecordMetadata {
                source_file_name: meta.and_then(|m| m.source_file_name.clone()),
                metadata: meta
                    .filter(|m| !m.metadata.is_empty())
                    .map(|m| m.metadata.clone().into_iter().collect()),
                file: r.file.clone(),
                inline_file: r.inline_file.clone(),
            }
        });
        Self {
            revision: r.stamp.revision(),
            alternates: r.alternates.clone(),
            fields: r.fields.clone().into_iter().collect(),
            metadata,
        }
    }
}

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

    fn next_live(&mut self, step: usize) -> Option<Vec<SpanKey>> {
        let bound = self
            .cursor
            .as_deref()
            .zip(self.span.as_ref().filter(|_| !self.done))
            .map(|(c, s)| s.storage_range(c).to_owned());
        let n = self
            .keys
            .iter()
            .take(step)
            .take_while(|k| bound.as_deref().is_none_or(|b| k.range.as_str() <= b))
            .count();
        (n > 0).then(|| {
            self.keys
                .drain(..n)
                .filter(|k| k.record.is_live())
                .collect()
        })
    }
}

impl QueryExecutor {
    pub fn new(db_ops: &Arc<DbOperations>, schema_manager: Arc<SchemaCore>) -> Self {
        Self {
            db_ops: Arc::clone(db_ops),
            schema_manager,
        }
    }

    pub(super) async fn in_read_scope<T>(
        &self,
        fut: impl std::future::Future<Output = Result<T, SchemaError>>,
    ) -> Result<T, SchemaError> {
        self.db_ops
            .read_scope()
            .await?
            .run_result(Box::pin(fut))
            .await
    }

    pub async fn query(&self, query: Query) -> Result<QueryRecords, SchemaError> {
        self.in_read_scope(self.query_current(query, None)).await
    }

    pub async fn query_window(
        &self,
        query: Query,
        window: KeyWindow,
    ) -> Result<QueryRecords, SchemaError> {
        self.in_read_scope(self.query_current(query, Some(window)))
            .await
    }

    pub(super) async fn query_current(
        &self,
        mut query: Query,
        window: Option<KeyWindow>,
    ) -> Result<QueryRecords, SchemaError> {
        if query.key_name.is_some() {
            return Err(SchemaError::InvalidData(
                "use the named alternate page reader".into(),
            ));
        }
        let schema = self.prepare_query_schema(&query).await?.ok_or_else(|| {
            SchemaError::InvalidData(format!("Schema '{}' not found", query.schema_name))
        })?;
        Ok(self
            .select_filtered(&schema, query.filter.take(), window)
            .await?
            .into_iter()
            .map(|(k, r)| (k, Record::from_stored(&r)))
            .collect())
    }

    pub(super) async fn prepare_query_schema(
        &self,
        query: &Query,
    ) -> Result<Option<Schema>, SchemaError> {
        let Some(schema) = self
            .schema_manager
            .resolve_schema(&query.schema_name)
            .await?
        else {
            return Ok(None);
        };
        self.schema_manager.require_queryable_schema(&schema.name)?;
        crate::schema::require_logical_layout(&schema.schema_type)?;
        crate::schema::require_logical_filter(query.filter.as_ref())?;
        Ok(Some(schema))
    }

    pub(super) async fn select_filtered(
        &self,
        schema: &Schema,
        filter: Option<HashRangeFilter>,
        window: Option<KeyWindow>,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        crate::schema::require_logical_layout(&schema.schema_type)?;
        crate::schema::require_logical_filter(filter.as_ref())?;
        let cmp = self.db_ops.records().key_comparison_context(schema)?;
        let filter = cmp.canonical_filter(filter)?.ok_or_else(|| {
            SchemaError::InvalidData("LastDB requires a Hash or HashRange key selector".into())
        })?;
        let window = cmp.canonical_window(window)?;
        cmp.run(self.select_rows(schema, &filter, window)).await
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
        let rows = self.db_ops.records().select_records(schema, filter).await?;
        Ok(match window {
            Some(KeyWindow::Offset { offset, limit }) => {
                rows.into_iter().skip(offset).take(limit).collect()
            }
            Some(KeyWindow::After { after, limit }) => rows
                .into_iter()
                .filter(|(k, _)| k.cmp_page_order(&after).is_gt())
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
        let from = after.as_ref().and_then(|a| a.range.as_deref());
        let mut source = RangeKeys::new(records.record_span(schema, hash, bound, from)?);
        let (mut rows, mut room, mut wide) = (Vec::new(), PAGE_READ_KEYS, 1usize);
        while rows.len() < limit {
            let step = skip
                .saturating_add(limit - rows.len())
                .saturating_mul(wide)
                .clamp(MIN_STEP, MAX_STEP);
            room = room.saturating_sub(source.fill(records, step, room).await?);
            let Some(taken) = source.next_live(step) else {
                if source.done {
                    break;
                }
                let stop = source
                    .span
                    .as_ref()
                    .zip(source.cursor.as_deref())
                    .map(|(span, c)| records.span_key(span, c))
                    .transpose()?;
                if let Some(stop) = stop.filter(|s| {
                    resumable
                        && skip == 0
                        && after.as_ref().is_none_or(|a| s.cmp_page_order(a).is_gt())
                }) {
                    note_page_resume(PageResume::Key(stop));
                    break;
                }
                let how = if skip > 0 {
                    "read it with next_cursor pages, not an offset"
                } else {
                    "read it in pages with next_cursor; a count or a condition needs the complete range"
                };
                return Err(SchemaError::ReadResourceLimit {
                    detail: format!(
                        "the range under one hash holds more deleted records than one request reads ({PAGE_READ_KEYS} keys); {how}"
                    ),
                });
            };
            let mark = (rows.len(), skip);
            let mut kept = Vec::new();
            for winner in taken {
                if after
                    .as_ref()
                    .is_some_and(|a| winner.key.cmp_page_order(a).is_le())
                {
                    continue;
                }
                if skip > 0 {
                    skip -= 1;
                } else {
                    kept.push(winner);
                    if rows.len() + kept.len() == limit {
                        break;
                    }
                }
            }
            room = room.saturating_sub(kept.len());
            if !kept.is_empty() {
                let (keys, phys): (Vec<_>, Vec<_>) =
                    kept.into_iter().map(|k| (k.key, k.physical)).unzip();
                rows.extend(
                    keys.into_iter()
                        .zip(records.read_records(&phys).await?)
                        .filter_map(|(k, r)| r.and_then(|r| r.is_live().then_some((k, r)))),
                );
            }
            if (rows.len(), skip) == mark {
                wide = wide.saturating_mul(2);
            }
        }
        Ok(rows)
    }
}

fn range_under_hash<'a>(
    schema: &Schema,
    filter: &'a HashRangeFilter,
) -> Option<(&'a str, RecordBound<'a>)> {
    (schema.schema_type == DeclarativeSchemaType::HashRange)
        .then_some(match filter {
            HashRangeFilter::HashKey(h) => Some((h.as_str(), RecordBound::All)),
            HashRangeFilter::HashRangePrefix { hash, prefix } => {
                Some((hash.as_str(), RecordBound::Prefix(prefix)))
            }
            HashRangeFilter::HashRangeRange { hash, start, end } => {
                Some((hash.as_str(), RecordBound::Between(start, end)))
            }
            HashRangeFilter::HashRangeKey { .. } | HashRangeFilter::HashRangeKeys(_) => None,
        })
        .flatten()
}
