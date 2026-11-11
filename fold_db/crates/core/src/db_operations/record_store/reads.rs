//! Record selection, paged key listing, existence probes, and bounded range walks.

use super::RecordStore;
use crate::schema::types::field::{FilterUtils, HashRangeFilter};
use crate::schema::types::key_comparison::KeyComparisonContext;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, KeyValuePageOrder, Schema, SchemaError};
use lastdb_model::record_space::{record_hash_prefix, record_space_uuid, RECORD_PREFIX};
use lastdb_model::record_value::StoredRecord;
use std::collections::HashSet;
use std::sync::Arc;

const MIN_ROWS: usize = 16;
const RECORD_PROBE_ROWS: usize = 16;

pub(crate) type KeyedRecord = (KeyValue, Arc<StoredRecord>);
pub(crate) type RecordKeyPage = (Vec<(String, String)>, Option<String>, bool);

#[derive(Debug, Default)]
pub(crate) struct Step {
    pub rows: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Bounded walk over `start..end` that halves spans if a single read hits the resource limit.
#[derive(Debug, Clone)]
pub(crate) struct RangeWalk {
    todo: Vec<(Vec<u8>, Vec<u8>)>,
    limit: usize,
    asked: usize,
}

impl RangeWalk {
    pub(crate) fn new(start: &[u8], end: &[u8], limit: usize) -> Self {
        let limit = limit.max(1);
        let todo = if start < end {
            vec![(start.to_vec(), end.to_vec())]
        } else {
            Vec::new()
        };
        Self {
            todo,
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
            .map(|(start, end)| (start.clone(), end.clone(), self.asked))
    }

    fn cut_top(&mut self) -> bool {
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

    pub(crate) fn read_too_big(&mut self) -> bool {
        if self.asked > MIN_ROWS {
            self.asked = MIN_ROWS;
            return true;
        }
        self.cut_top()
    }

    pub(crate) fn finish_yield(&mut self, rows: usize, resume: Option<&[u8]>) {
        let Some((_, end)) = self.todo.pop() else {
            return;
        };
        if let Some(last) = resume {
            let mut next = Vec::with_capacity(last.len() + 1);
            next.extend_from_slice(last);
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
    pub(crate) async fn walk_step(&self, walk: &mut RangeWalk) -> Result<Step, SchemaError> {
        loop {
            let Some((start, end, asked)) = walk.request() else {
                return Ok(Step::default());
            };
            let result = self
                .reader()
                .scan_range_yielding(&start, &end, asked, 0)
                .await;
            match result.map_err(SchemaError::from) {
                Ok(scan) => {
                    walk.finish_yield(scan.rows.len(), scan.resume.as_deref());
                    return Ok(Step { rows: scan.rows });
                }
                Err(error @ SchemaError::ReadResourceLimit { .. }) => {
                    if !walk.read_too_big() {
                        return Err(error);
                    }
                }
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
        let mut rows = match filter {
            F::HashRangeKey { hash, range } => {
                self.records_at(schema, &[api_key(ranged, hash, range)])
                    .await?
            }
            F::HashRangeKeys(keys) => {
                let keys: Vec<_> = keys
                    .iter()
                    .map(|(hash, range)| api_key(ranged, hash, range))
                    .collect();
                self.records_at(schema, &keys).await?
            }
            F::HashKey(hash) | F::HashRangePrefix { hash, .. } | F::HashRangeRange { hash, .. }
                if !ranged =>
            {
                self.records_at(schema, &[KeyValue::new(Some(hash.clone()), None)])
                    .await?
            }
            F::HashKey(_) | F::HashRangePrefix { .. } | F::HashRangeRange { .. } => {
                return Err(SchemaError::InvalidData(
                    "HashRange range selectors are read through range_rows".into(),
                ))
            }
        };
        rows.retain(|(_, record)| record.is_live());
        rows.sort_by(|a, b| a.0.cmp_page_order(&b.0));
        Ok(rows)
    }

    pub(crate) async fn records_at(
        &self,
        schema: &Schema,
        keys: &[KeyValue],
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        let mut seen = HashSet::new();
        let keys: Vec<KeyValue> = keys
            .iter()
            .filter(|key| seen.insert(*key))
            .cloned()
            .collect();
        let storage_keys = keys
            .iter()
            .map(|key| self.record_key(schema, key))
            .collect::<Result<Vec<_>, _>>()?;
        let records = self.read_records(&storage_keys).await?;
        Ok(keys
            .into_iter()
            .zip(records)
            .filter_map(|(key, record)| record.map(|record| (key, record)))
            .collect())
    }

    pub(crate) async fn list_record_keys(
        &self,
        schema: &Schema,
        comparison: &KeyComparisonContext,
        limit: usize,
        after: Option<&str>,
        hash: Option<&str>,
    ) -> Result<RecordKeyPage, SchemaError> {
        if limit == 0 {
            return Ok((Vec::new(), None, false));
        }
        let space = record_space_uuid(&schema.name);
        let base = match hash {
            Some(hash) => {
                let codec = self.primary_codec(schema)?;
                record_hash_prefix(
                    &space,
                    &codec
                        .storage_hash(&space, hash)
                        .map_err(|e| SchemaError::InvalidData(e.to_string()))?,
                )
            }
            None => space_prefix(schema),
        };
        let start = match after {
            Some(cursor) if !cursor.starts_with(&base) => {
                return Err(SchemaError::InvalidCursor(
                    "cursor is not in this schema's records".into(),
                ))
            }
            Some(cursor) => format!("{cursor}\0"),
            None => base.clone(),
        };
        let end = FilterUtils::create_prefix_end(&base)?;
        let rows = self
            .main_store
            .scan_range_paged(start.as_bytes(), end.as_bytes(), limit.saturating_add(1))
            .await
            .map_err(|error| SchemaError::InvalidData(format!("list record keys: {error}")))?;
        let has_more = rows.len() > limit;
        let mut keys = Vec::with_capacity(limit);
        let mut cursor = None;
        for (key, value) in rows.into_iter().take(limit) {
            let record = decode(&value)?;
            let physical = String::from_utf8(key)
                .map_err(|_| SchemaError::InvalidData("a record key is not text".into()))?;
            if record.is_live() {
                let key = comparison.canonical_key(record.key.clone())?;
                keys.push((key.hash.unwrap_or_default(), key.range.unwrap_or_default()));
            }
            cursor = Some(physical);
        }
        Ok((keys, cursor.filter(|_| has_more), has_more))
    }

    pub async fn schema_may_hold_records(&self, schema: &Schema) -> Result<bool, SchemaError> {
        self.record_read_scope(async {
            let prefix = space_prefix(schema);
            let end = FilterUtils::create_prefix_end(&prefix)?;
            let rows = self
                .main_store
                .scan_range_paged(prefix.as_bytes(), end.as_bytes(), RECORD_PROBE_ROWS + 1)
                .await
                .map_err(|error| SchemaError::InvalidData(format!("probe records: {error}")))?;
            if rows.len() > RECORD_PROBE_ROWS {
                return Ok(true);
            }
            for (_, value) in &rows {
                if decode(value)?.is_live() {
                    return Ok(true);
                }
            }
            Ok(false)
        })
        .await
    }
}

fn space_prefix(schema: &Schema) -> String {
    format!("{RECORD_PREFIX}{}:", record_space_uuid(&schema.name))
}

fn api_key(ranged: bool, hash: &str, range: &str) -> KeyValue {
    KeyValue::new(Some(hash.to_owned()), ranged.then(|| range.to_owned()))
}

pub(super) fn decode(bytes: &[u8]) -> Result<StoredRecord, SchemaError> {
    StoredRecord::decode(bytes)
        .map_err(|error| SchemaError::InvalidData(format!("a stored record: {error}")))
}

fn midpoint(low: &[u8], high: &[u8]) -> Option<Vec<u8>> {
    if low >= high || low.iter().chain(high).any(|byte| *byte >= 0x80) {
        return None;
    }
    let width = low.len().max(high.len()) + 1;
    let digits = |key: &[u8]| -> Vec<u32> {
        (0..width)
            .map(|at| key.get(at).map_or(0, |byte| u32::from(*byte)))
            .collect()
    };
    let (a, b) = (digits(low), digits(high));
    let mut sum = vec![0u32; width + 1];
    let mut carry = 0;
    for at in (0..width).rev() {
        let total = a[at] + b[at] + carry;
        sum[at + 1] = total % 128;
        carry = total / 128;
    }
    sum[0] = carry;
    let mut middle = Vec::with_capacity(width);
    let mut remainder = 0;
    for (at, digit) in sum.into_iter().enumerate() {
        let value = remainder * 128 + digit;
        remainder = value % 2;
        if at > 0 {
            middle.push(u8::try_from(value / 2).ok()?);
        }
    }
    while middle.len() > 1 && middle.last() == Some(&0) && middle[..middle.len() - 1] > *low {
        middle.pop();
    }
    (middle.as_slice() > low && middle.as_slice() < high).then_some(middle)
}
