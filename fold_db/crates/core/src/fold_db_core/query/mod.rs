//! Query types, executor re-export, and keyed hash/range query execution.

use crate::db_operations::record_store::{
    KeyedRecord, RecordBound, RecordSpan, RecordStore, SpanKey,
};
use crate::db_operations::{note_page_resume, page_can_resume, PageResume};
use crate::schema::types::field::{HashRangeFilter, KeyWindow};
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, KeyValuePageOrder, Schema, SchemaError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, VecDeque};

pub mod query_executor;

pub use query_executor::QueryExecutor;

const PAGE_READ_KEYS: usize = 8_192;
const MIN_STEP: usize = 32;
const MAX_STEP: usize = 1024;

/// Metadata of one stored record.
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

/// One stored record returned in [`QueryRecords`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub revision: String,
    pub alternates: BTreeMap<String, Option<KeyValue>>,
    pub fields: HashMap<String, Value>,
    pub metadata: Option<RecordMetadata>,
}

/// Map of stored records keyed by canonical API key.
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
        let bound = match self.span.as_ref().filter(|_| !self.done) {
            Some(span) => Some(span.storage_range(self.cursor.as_deref()?).to_owned()),
            None => None,
        };
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
        cmp.run(self.select_rows(schema, &filter, cmp.canonical_window(window)?))
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
                        .filter_map(|(k, r)| r.filter(|r| r.is_live()).map(|r| (k, r))),
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
