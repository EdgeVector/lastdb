//! Record selection, paged key listing, span pages, existence probes, release, and range walks.

use super::{invalid, RecordDeleteLedgerEntry, RecordSpan, RecordStore, SpanKey, SpanPage};
use crate::schema::types::field::{FilterUtils, HashRangeFilter};
use crate::schema::types::key_comparison::KeyComparisonContext;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, KeyValuePageOrder, Schema, SchemaError};
use crate::storage::format::records::{release_families, ReleaseFamily, ReleaseKind};
use lastdb_model::kind_partition::exact_prefix_bounds;
use lastdb_model::record_space::{record_hash_prefix, record_space_uuid, RECORD_PREFIX};
use lastdb_model::record_value::StoredRecord;
use std::collections::HashSet;
use std::sync::Arc;

const MIN_ROWS: usize = 16;

pub(crate) type KeyedRecord = (KeyValue, Arc<StoredRecord>);

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct SchemaRelease {
    pub(crate) records: u64,
    pub(crate) alternates: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct RangeWalk {
    todo: Vec<(Vec<u8>, Vec<u8>)>,
    limit: usize,
    asked: usize,
}

impl RangeWalk {
    pub(crate) fn new(start: &[u8], end: &[u8], limit: usize) -> Self {
        let limit = limit.max(1);
        Self {
            todo: (start < end)
                .then(|| (start.to_vec(), end.to_vec()))
                .into_iter()
                .collect(),
            limit,
            asked: limit,
        }
    }

    pub(crate) fn set_limit(&mut self, rows: usize) {
        self.limit = rows.max(1);
        self.asked = self.asked.min(self.limit);
    }

    pub(crate) fn done(&self) -> bool {
        self.todo.is_empty()
    }

    pub(crate) fn request(&self) -> Option<(Vec<u8>, Vec<u8>, usize)> {
        self.todo
            .last()
            .map(|(s, e)| (s.clone(), e.clone(), self.asked))
    }

    pub(crate) fn read_too_big(&mut self) -> bool {
        if self.asked > MIN_ROWS {
            self.asked = MIN_ROWS;
            return true;
        }
        let Some((start, end)) = self.todo.pop() else {
            return false;
        };
        let Some(middle) = midpoint(&start, &end) else {
            self.todo.push((start, end));
            return false;
        };
        self.todo.push((middle.clone(), end));
        self.todo.push((start, middle));
        true
    }

    pub(crate) fn finish_yield(&mut self, rows: usize, resume: Option<&[u8]>) {
        let Some((_, end)) = self.todo.pop() else {
            return;
        };
        if let Some(last) = resume {
            let mut next = last.to_vec();
            next.push(0);
            if next < end {
                self.todo.push((next, end));
            }
            if rows > 0 {
                self.asked = (self.asked * 2).min(self.limit);
            }
        }
    }
}

impl RecordStore {
    pub async fn list_record_delete_ledger(
        &self,
        limit: usize,
    ) -> Result<Vec<RecordDeleteLedgerEntry>, SchemaError> {
        let prefix =
            lastdb_model::kind_partition::anchored(crate::storage::format::DELETE_AUDIT_FAMILY, "");
        let mut rows = self
            .reader()
            .scan_prefix(prefix.as_bytes())
            .await
            .map_err(|e| invalid(format!("delete ledger scan: {e}")))?;
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        let take = if limit > 0 { limit } else { rows.len() };
        rows.into_iter()
            .take(take)
            .map(|(k, v)| {
                serde_json::from_slice(&v).map_err(|e| {
                    invalid(format!(
                        "delete ledger row {} is unreadable: {e}",
                        String::from_utf8_lossy(&k)
                    ))
                })
            })
            .collect()
    }

    pub(crate) async fn release_schema_rows(
        &self,
        schema: &Schema,
    ) -> Result<SchemaRelease, SchemaError> {
        let mut out = SchemaRelease::default();
        let families: Vec<ReleaseFamily> = release_families(schema)?;
        for family in families {
            let (start, end) = exact_prefix_bounds(family.prefix());
            let mut walk = RangeWalk::new(
                start.as_bytes(),
                end.as_bytes(),
                crate::storage::format::SCHEMA_RELEASE_ROWS,
            );
            let mut count = 0;
            while !walk.done() {
                let rows = self.walk_step(&mut walk).await?;
                if !rows.is_empty() {
                    let keys = rows.into_iter().map(|(k, _)| k).collect();
                    count += self
                        .in_write_scope(self.format.release_schema_batch(keys, &family))
                        .await?;
                }
            }
            match family.kind() {
                ReleaseKind::Records => out.records += count,
                ReleaseKind::Alternates => out.alternates += count,
                ReleaseKind::AppliedDeletes | ReleaseKind::InlineFiles => {}
            }
        }
        Ok(out)
    }

    pub(crate) async fn record_span_page(
        &self,
        span: &RecordSpan,
        after: Option<&str>,
        limit: usize,
    ) -> Result<SpanPage, SchemaError> {
        let start = after.map_or_else(|| span.start.clone(), |c| format!("{c}\0"));
        let page = self
            .reader()
            .scan_range_yielding(start.as_bytes(), span.end.as_bytes(), limit, 2)
            .await
            .map_err(|e| invalid(format!("read record keys: {e}")))?;
        let text =
            |k: Vec<u8>| String::from_utf8(k).map_err(|_| invalid("a record key is not text"));
        let keys = page
            .rows
            .into_iter()
            .map(|(physical, value)| {
                let physical = text(physical)?;
                Ok(SpanKey {
                    key: self.span_key(span, &physical)?,
                    range: physical[span.base.len()..].to_owned(),
                    physical,
                    record: decode(&value)?,
                })
            })
            .collect::<Result<_, SchemaError>>()?;
        Ok(SpanPage {
            keys,
            next: page.resume.map(text).transpose()?,
        })
    }

    pub(crate) fn span_key(
        &self,
        span: &RecordSpan,
        physical: &str,
    ) -> Result<KeyValue, SchemaError> {
        let range = physical
            .get(span.base.len()..)
            .ok_or_else(|| invalid("a record key has no range"))?;
        Ok(KeyValue::new(
            Some(span.hash.clone()),
            Some(span.codec.api_range(range).map_err(invalid)?),
        ))
    }

    pub(crate) async fn walk_step(
        &self,
        walk: &mut RangeWalk,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, SchemaError> {
        loop {
            let Some((start, end, asked)) = walk.request() else {
                return Ok(Vec::new());
            };
            match self
                .reader()
                .scan_range_yielding(&start, &end, asked, 0)
                .await
                .map_err(SchemaError::from)
            {
                Ok(scan) => {
                    walk.finish_yield(scan.rows.len(), scan.resume.as_deref());
                    return Ok(scan.rows);
                }
                Err(SchemaError::ReadResourceLimit { .. }) if walk.read_too_big() => {}
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) async fn select_records(
        &self,
        schema: &Schema,
        filter: &HashRangeFilter,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        use HashRangeFilter as F;
        let ranged = schema.schema_type == DeclarativeSchemaType::HashRange;
        let mk = |h: &str, r: &str| KeyValue::new(Some(h.to_owned()), ranged.then(|| r.to_owned()));
        let requested: Vec<KeyValue> = match filter {
            F::HashRangeKey { hash, range } => vec![mk(hash, range)],
            F::HashRangeKeys(keys) => keys.iter().map(|(h, r)| mk(h, r)).collect(),
            F::HashKey(hash) | F::HashRangePrefix { hash, .. } | F::HashRangeRange { hash, .. }
                if !ranged =>
            {
                vec![KeyValue::new(Some(hash.clone()), None)]
            }
            F::HashKey(_) | F::HashRangePrefix { .. } | F::HashRangeRange { .. } => {
                return Err(invalid(
                    "HashRange range selectors are read through range_rows",
                ));
            }
        };
        let mut seen = HashSet::new();
        let keys: Vec<KeyValue> = requested
            .into_iter()
            .filter(|k| seen.insert(k.clone()))
            .collect();
        let storage_keys = keys
            .iter()
            .map(|k| self.record_key(schema, k))
            .collect::<Result<Vec<_>, _>>()?;
        let mut rows: Vec<KeyedRecord> = keys
            .into_iter()
            .zip(self.read_records(&storage_keys).await?)
            .filter_map(|(k, r)| r.filter(|r| r.is_live()).map(|r| (k, r)))
            .collect();
        rows.sort_by(|a, b| a.0.cmp_page_order(&b.0));
        Ok(rows)
    }

    pub(crate) async fn list_record_keys(
        &self,
        schema: &Schema,
        comparison: &KeyComparisonContext,
        limit: usize,
        after: Option<&str>,
        hash: Option<&str>,
    ) -> Result<(Vec<(String, String)>, Option<String>, bool), SchemaError> {
        if limit == 0 {
            return Ok((Vec::new(), None, false));
        }
        let space = record_space_uuid(&schema.name);
        let base = match hash {
            Some(h) => record_hash_prefix(
                &space,
                &self
                    .primary_codec(schema)?
                    .storage_hash(&space, h)
                    .map_err(invalid)?,
            ),
            None => format!("{RECORD_PREFIX}{space}:"),
        };
        let start = match after {
            Some(c) if !c.starts_with(&base) => {
                return Err(SchemaError::InvalidCursor(
                    "cursor is not in this schema's records".into(),
                ));
            }
            Some(c) => format!("{c}\0"),
            None => base.clone(),
        };
        let end = FilterUtils::create_prefix_end(&base)?;
        let rows = self
            .reader()
            .scan_range_paged(start.as_bytes(), end.as_bytes(), limit.saturating_add(1))
            .await
            .map_err(|e| invalid(format!("list record keys: {e}")))?;
        let has_more = rows.len() > limit;
        let (mut keys, mut cursor) = (Vec::with_capacity(limit), None);
        for (key, value) in rows.into_iter().take(limit) {
            let record = decode(&value)?;
            let physical =
                String::from_utf8(key).map_err(|_| invalid("a record key is not text"))?;
            if record.is_live() {
                let k = comparison.canonical_key(record.key)?;
                keys.push((k.hash.unwrap_or_default(), k.range.unwrap_or_default()));
            }
            cursor = Some(physical);
        }
        Ok((keys, cursor.filter(|_| has_more), has_more))
    }

    pub async fn schema_may_hold_records(&self, schema: &Schema) -> Result<bool, SchemaError> {
        self.record_read_scope(async {
            let prefix = format!("{RECORD_PREFIX}{}:", record_space_uuid(&schema.name));
            let end = FilterUtils::create_prefix_end(&prefix)?;
            let rows = self
                .reader()
                .scan_range_paged(prefix.as_bytes(), end.as_bytes(), MIN_ROWS + 1)
                .await
                .map_err(|e| invalid(format!("probe records: {e}")))?;
            if rows.len() > MIN_ROWS {
                return Ok(true);
            }
            for (_, v) in &rows {
                if decode(v)?.is_live() {
                    return Ok(true);
                }
            }
            Ok(false)
        })
        .await
    }
}

pub(super) fn decode(bytes: &[u8]) -> Result<StoredRecord, SchemaError> {
    StoredRecord::decode(bytes).map_err(|e| invalid(format!("a stored record: {e}")))
}

fn midpoint(low: &[u8], high: &[u8]) -> Option<Vec<u8>> {
    if low >= high || low.iter().chain(high).any(|&b| b >= 0x80) {
        return None;
    }
    let width = low.len().max(high.len()) + 1;
    let (mut sum, mut carry) = (vec![0u32; width + 1], 0u32);
    for at in (0..width).rev() {
        let total = low.get(at).map_or(0, |&b| u32::from(b))
            + high.get(at).map_or(0, |&b| u32::from(b))
            + carry;
        sum[at + 1] = total % 128;
        carry = total / 128;
    }
    sum[0] = carry;
    let (mut middle, mut rem) = (Vec::with_capacity(width), 0);
    for (at, digit) in sum.into_iter().enumerate() {
        let val = rem * 128 + digit;
        rem = val % 2;
        if at > 0 {
            middle.push(u8::try_from(val / 2).ok()?);
        }
    }
    while middle.len() > 1 && middle.last() == Some(&0) && middle[..middle.len() - 1] > *low {
        middle.pop();
    }
    (middle.as_slice() > low && middle.as_slice() < high).then_some(middle)
}
