//! Owned row and page response assembly for `/api/query` and Share reads.

use std::collections::HashMap;

use fold_db::fold_db_core::query::query_executor::AlternatePage;
use fold_db::schema::types::{KeyValue, QueryCursor};
use serde_json::Value;

use crate::exec::reply::HostError;

pub(super) fn page_payload(
    page: Vec<Value>,
    total_count: Option<usize>,
    limit: usize,
    offset: usize,
    has_more: bool,
    key_form: Option<&'static str>,
) -> Value {
    let returned_count = page.len();
    Value::Object(serde_json::Map::from_iter([
        ("results".into(), Value::Array(page)),
        (
            "total_count".into(),
            total_count.map_or(Value::Null, Value::from),
        ),
        ("returned_count".into(), Value::from(returned_count)),
        ("limit".into(), Value::from(limit)),
        ("offset".into(), Value::from(offset)),
        ("has_more".into(), Value::from(has_more)),
        ("next_cursor".into(), Value::Null),
        ("key_form".into(), key_form.map_or(Value::Null, Value::from)),
    ]))
}

pub(super) fn cursor_payload(
    mut page: Vec<Value>,
    total_count: Option<usize>,
    limit: usize,
    offset: usize,
    resume: Option<KeyValue>,
    comparison: Option<String>,
) -> Value {
    let has_more = page.len() > limit || resume.is_some();
    if page.len() > limit {
        page.truncate(limit);
    }
    let next_cursor = resume
        .or_else(|| next_cursor_for(&page, has_more))
        .map(|key| QueryCursor::new(key, comparison));
    let mut payload = page_payload(page, total_count, limit, offset, has_more, None);
    payload["next_cursor"] = serde_json::to_value(next_cursor).unwrap_or(Value::Null);
    payload
}

pub(super) fn overfetched_offset_payload(
    mut page: Vec<Value>,
    total_count: usize,
    limit: usize,
    offset: usize,
) -> Value {
    let has_more = page.len() > limit;
    if page.len() > limit {
        page.truncate(limit);
    }
    page_payload(page, Some(total_count), limit, offset, has_more, None)
}

pub(super) fn materialized_page_payload(results: Vec<Value>, limit: usize, offset: usize) -> Value {
    let total_count = results.len();
    let page: Vec<_> = results.into_iter().skip(offset).take(limit).collect();
    let has_more = offset.saturating_add(page.len()) < total_count;
    page_payload(page, Some(total_count), limit, offset, has_more, None)
}

fn next_cursor_for(page: &[Value], has_more: bool) -> Option<KeyValue> {
    if !has_more {
        return None;
    }
    page.last()
        .and_then(|row| row.get("key"))
        .and_then(|key| serde_json::from_value::<KeyValue>(key.clone()).ok())
}

pub(super) fn format_rows(
    records: fold_db::fold_db_core::query::QueryRecords,
    compact: bool,
) -> Vec<Value> {
    records
        .into_iter()
        .map(|(key, record)| format_row(&key, record, compact))
        .collect()
}

fn format_row(
    key: &KeyValue,
    record: fold_db::fold_db_core::query::Record,
    compact: bool,
) -> Value {
    let mut row = serde_json::Map::new();
    row.insert("key".into(), serde_json::json!(key));
    row.insert("revision".into(), Value::String(record.revision));
    row.insert("alternates".into(), serde_json::json!(record.alternates));
    row.insert(
        "fields".into(),
        Value::Object(record.fields.into_iter().collect()),
    );
    if !compact {
        row.insert("metadata".into(), serde_json::json!(record.metadata));
    }
    Value::Object(row)
}

/// Render an already ordered page read from one verified database view.
pub(in crate::exec) fn verified_query_page(
    records: Vec<(KeyValue, fold_db::fold_db_core::query::Record)>,
    compact: bool,
    total_count: Option<usize>,
    limit: usize,
    offset: usize,
    has_more: bool,
    next_cursor: Option<QueryCursor>,
) -> Value {
    let key_form = (!records.is_empty()).then_some("api");
    let rows = records
        .into_iter()
        .map(|(key, record)| format_row(&key, record, compact))
        .collect();
    let mut payload = page_payload(rows, total_count, limit, offset, has_more, key_form);
    payload["next_cursor"] = serde_json::to_value(next_cursor).unwrap_or(Value::Null);
    payload
}

/// Format a verified alternate page with the standard primary and matched keys.
pub(in crate::exec) fn format_alternate_page(
    page: AlternatePage,
    compact: bool,
    limit: usize,
) -> Result<Value, HostError> {
    let mut rows = HashMap::new();
    for row in format_rows(page.values, compact) {
        let key: KeyValue = serde_json::from_value(row["key"].clone())
            .map_err(|_| HostError::internal("alternate response has an invalid primary key"))?;
        rows.insert(key, row);
    }
    let mut results = Vec::with_capacity(page.entries.len());
    for entry in &page.entries {
        let mut row = rows.remove(&entry.primary).unwrap_or_else(|| {
            let mut row = serde_json::json!({ "key": entry.primary, "fields": {} });
            if !compact {
                row["metadata"] = Value::Null;
            }
            row
        });
        row["matched_key"] = serde_json::to_value(&entry.matched)
            .map_err(|_| HostError::internal("invalid matched alternate key"))?;
        results.push(row);
    }
    let next = if page.has_more {
        page.entries.last().map(|entry| {
            let mut cursor = QueryCursor::new(entry.matched.clone(), Some(page.comparison));
            cursor.alternate_primary = Some(entry.primary.clone());
            cursor
        })
    } else {
        None
    };
    let mut payload = page_payload(results, None, limit, 0, page.has_more, None);
    payload["next_cursor"] =
        serde_json::to_value(next).map_err(|_| HostError::internal("invalid alternate cursor"))?;
    Ok(payload)
}
