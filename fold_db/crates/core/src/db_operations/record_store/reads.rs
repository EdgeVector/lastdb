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
        let mut rows = match filter {
            F::HashRangeKey { hash, range } => {
                self.records_at(schema, &[api_key(ranged, hash, range)])
                    .await?
            }
            F::HashRangeKeys(keys) => {
                let keys: Vec<_> = keys.iter().map(|(h, r)| api_key(ranged, h, r)).collect();
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
        let keys: Vec<KeyValue> = keys.iter().filter(|k| seen.insert(*k)).cloned().collect();
        let storage_keys = keys
            .iter()
            .map(|k| self.record_key(schema, k))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(keys
            .into_iter()
            .zip(self.read_records(&storage_keys).await?)
            .filter_map(|(k, r)| r.map(|r| (k, r)))
            .collect())
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
            Some(h) => {
                let storage = self
                    .primary_codec(schema)?
                    .storage_hash(&space, h)
                    .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
                record_hash_prefix(&space, &storage)
            }
            None => space_prefix(schema),
        };
        let start = match after {
            Some(c) if !c.starts_with(&base) => {
                return Err(SchemaError::InvalidCursor(
                    "cursor is not in this schema's records".into(),
                ))
            }
            Some(c) => format!("{c}\0"),
            None => base.clone(),
        };
        let end = FilterUtils::create_prefix_end(&base)?;
        let rows = self
            .main_store
            .scan_range_paged(start.as_bytes(), end.as_bytes(), limit.saturating_add(1))
            .await
            .map_err(|e| SchemaError::InvalidData(format!("list record keys: {e}")))?;
        let has_more = rows.len() > limit;
        let mut keys = Vec::with_capacity(limit);
        let mut cursor = None;
        for (key, value) in rows.into_iter().take(limit) {
            let record = decode(&value)?;
            let physical = String::from_utf8(key)
                .map_err(|_| SchemaError::InvalidData("a record key is not text".into()))?;
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
            let prefix = space_prefix(schema);
            let end = FilterUtils::create_prefix_end(&prefix)?;
            let rows = self
                .main_store
                .scan_range_paged(prefix.as_bytes(), end.as_bytes(), RECORD_PROBE_ROWS + 1)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("probe records: {e}")))?;
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
        .map_err(|e| SchemaError::InvalidData(format!("a stored record: {e}")))
}

fn midpoint(low: &[u8], high: &[u8]) -> Option<Vec<u8>> {
    if low >= high || low.iter().chain(high).any(|&b| b >= 0x80) {
        return None;
    }
    let width = low.len().max(high.len()) + 1;
    let mut sum = vec![0u32; width + 1];
    let mut carry = 0u32;
    for at in (0..width).rev() {
        let total = low.get(at).map_or(0, |&b| u32::from(b))
            + high.get(at).map_or(0, |&b| u32::from(b))
            + carry;
        sum[at + 1] = total % 128;
        carry = total / 128;
    }
    sum[0] = carry;
    let mut middle = Vec::with_capacity(width);
    let mut rem = 0;
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
