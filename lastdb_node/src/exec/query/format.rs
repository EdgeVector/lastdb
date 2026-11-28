//! Owned row and page response assembly and ascending range sorting.

use std::cmp::Ordering;

use fold_db::alternate_keys::AlternateEntry;
use fold_db::fold_db_core::query::{AlternateKeyPage, AlternatePage, Record};
use fold_db::schema::types::key_comparison::RangeKeyComparison;
use fold_db::schema::types::{KeyValue, QueryCursor, SchemaError};
use serde_json::{json, Value};

use crate::exec::reply::HostError;

fn page_payload(
    page: Vec<Value>,
    limit: usize,
    has_more: bool,
    next_cursor: Option<QueryCursor>,
) -> Value {
    let n = page.len();
    json!({
        "results": Value::Array(page), "total_count": Value::Null, "returned_count": n,
        "limit": limit, "offset": 0, "has_more": has_more,
        "next_cursor": serde_json::to_value(next_cursor).unwrap_or(Value::Null),
    })
}

pub(super) fn cursor_payload(
    mut page: Vec<Value>,
    limit: usize,
    resume: Option<KeyValue>,
    comparison: Option<String>,
) -> Value {
    let has_more = page.len() > limit || resume.is_some();
    page.truncate(limit);
    let next = resume
        .or_else(|| {
            has_more
                .then(|| serde_json::from_value(page.last()?.get("key")?.clone()).ok())
                .flatten()
        })
        .map(|key| QueryCursor::new(key, comparison));
    page_payload(page, limit, has_more, next)
}

pub(super) fn format_rows(
    records: impl IntoIterator<Item = (KeyValue, Record)>,
    compact: bool,
) -> Vec<Value> {
    records
        .into_iter()
        .map(|(k, r)| format_row(&k, r, compact))
        .collect()
}

/// Rows that hold only their key, as `format_row` writes it.
pub(super) fn key_rows(keys: Vec<KeyValue>) -> Vec<Value> {
    keys.into_iter().map(|key| json!({ "key": key })).collect()
}

fn format_row(key: &KeyValue, record: Record, compact: bool) -> Value {
    let mut row = serde_json::Map::from_iter([
        ("key".into(), json!(key)),
        ("revision".into(), Value::String(record.revision)),
    ]);
    // Omit the key when the record has no value. Do not send null.
    if let Some(created_by) = record.created_by {
        row.insert("created_by".into(), Value::String(created_by));
    }
    if let Some(updated_by) = record.updated_by {
        row.insert("updated_by".into(), Value::String(updated_by));
    }
    row.insert("alternates".into(), json!(record.alternates));
    row.insert(
        "fields".into(),
        Value::Object(record.fields.into_iter().collect()),
    );
    if !compact {
        row.insert("metadata".into(), json!(record.metadata));
    }
    Value::Object(row)
}

pub(super) fn format_alternate_page(
    mut page: AlternatePage,
    compact: bool,
    limit: usize,
) -> Result<Value, HostError> {
    let mut results = Vec::with_capacity(page.entries.len());
    for entry in &page.entries {
        let mut row = page.values.remove(&entry.primary).map_or_else(
            || {
                let mut r = json!({ "key": entry.primary, "fields": {} });
                if !compact {
                    r["metadata"] = Value::Null;
                }
                r
            },
            |record| format_row(&entry.primary, record, compact),
        );
        row["matched_key"] = serde_json::to_value(&entry.matched)
            .map_err(|_| HostError::internal("invalid matched alternate key"))?;
        results.push(row);
    }
    let next = alternate_cursor(&page.entries, page.has_more, page.comparison);
    Ok(page_payload(results, limit, page.has_more, next))
}

/// The rows of `format_alternate_page`, holding only `key` and `matched_key`.
pub(super) fn format_alternate_keys(
    page: AlternateKeyPage,
    limit: usize,
) -> Result<Value, HostError> {
    let mut results = Vec::with_capacity(page.entries.len());
    for entry in &page.entries {
        let matched = serde_json::to_value(&entry.matched)
            .map_err(|_| HostError::internal("invalid matched alternate key"))?;
        results.push(json!({ "key": entry.primary, "matched_key": matched }));
    }
    let next = alternate_cursor(&page.entries, page.has_more, page.comparison);
    Ok(page_payload(results, limit, page.has_more, next))
}

fn alternate_cursor(
    entries: &[AlternateEntry],
    has_more: bool,
    comparison: String,
) -> Option<QueryCursor> {
    has_more.then(|| entries.last()).flatten().map(|e| {
        let mut c = QueryCursor::new(e.matched.clone(), Some(comparison));
        c.alternate_primary = Some(e.primary.clone());
        c
    })
}

pub(super) fn sort_rows(
    rows: &mut [Value],
    range_cmp: Option<&RangeKeyComparison>,
) -> Result<(), HostError> {
    if let Some(cmp) = range_cmp {
        for r in &*rows {
            cmp.validate_value(r["key"]["range"].as_str().unwrap_or(""))
                .map_err(SchemaError::InvalidData)?;
        }
    }
    let mut err = None;
    rows.sort_by(|a, b| {
        let (ar, br) = (
            a["key"]["range"].as_str().unwrap_or(""),
            b["key"]["range"].as_str().unwrap_or(""),
        );
        let ord = if let Some(cmp) = range_cmp {
            match cmp.compare(ar, br) {
                Ok(o) => o,
                Err(d) => {
                    err.get_or_insert(SchemaError::InvalidData(d));
                    return Ordering::Equal;
                }
            }
        } else {
            ar.cmp(br)
        };
        ord.then_with(|| {
            a["key"]["hash"]
                .as_str()
                .unwrap_or("")
                .cmp(b["key"]["hash"].as_str().unwrap_or(""))
        })
    });
    err.map_or(Ok(()), |e| Err(e.into()))
}
