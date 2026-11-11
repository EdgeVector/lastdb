//! Owned row/page response assembly. No host access, clocks, or telemetry.

use fold_db::schema::types::{KeyValue, QueryCursor};
use serde_json::Value;

use super::rows::PageFlags;
use crate::lastdb_host::pagination::compute_has_more;

/// The `has_more`-carrying page shape both sockets serve, before the envelope.
///
/// `next_cursor` is always `null` for offset pages. Keyed query pages use
/// [`cursor_payload`], whose continuation uses the same API-coordinate window.
/// The page flags stay present for unknown classifications.
pub(in crate::lastdb_host::handlers) fn page_payload(
    page: Vec<Value>,
    total_count: Option<usize>,
    limit: usize,
    offset: usize,
    has_more: bool,
    flags: PageFlags,
) -> Value {
    let returned_count = page.len();
    // Move the owned page directly into the response. json! serializes Value
    // expressions by reference and would copy every nested field value here.
    // The page flags stay present even when null or unknown.
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
        (
            "key_form".into(),
            flags
                .key_form
                .map_or(Value::Null, |form| Value::from(form.as_str())),
        ),
    ]))
}

pub(in crate::lastdb_host::handlers) fn cursor_payload(
    mut page: Vec<Value>,
    total_count: Option<usize>,
    limit: usize,
    offset: usize,
    flags: PageFlags,
    resume: Option<KeyValue>,
    comparison: Option<String>,
) -> Value {
    // This shape over-fetches `limit + 1` and infers `has_more` from the extra
    // row. A page that a run of deleted records ended early (`resume`) has
    // more keys after the last key it examined, and its cursor names that key.
    let has_more = page.len() > limit || resume.is_some();
    if page.len() > limit {
        page.truncate(limit);
    }
    // Derive the cursor from the truncated page: it has to name the last row the
    // caller actually received, not the `limit + 1`th probe row, or the next
    // continuation skips a row.
    let next_cursor = resume
        .or_else(|| next_cursor_for(&page, has_more))
        .map(|key| QueryCursor::new(key, comparison));
    let mut payload = page_payload(page, total_count, limit, offset, has_more, flags);
    payload["next_cursor"] = serde_json::to_value(next_cursor).unwrap_or(Value::Null);
    payload
}

/// A keyed descending window includes a probe row; trim it before the reply.
pub(in crate::lastdb_host::handlers) fn overfetched_offset_payload(
    mut page: Vec<Value>,
    total_count: usize,
    limit: usize,
    offset: usize,
    flags: PageFlags,
) -> Value {
    let has_more = page.len() > limit;
    if page.len() > limit {
        page.truncate(limit);
    }
    page_payload(page, Some(total_count), limit, offset, has_more, flags)
}

/// Slice a materialized result, whose count includes only delivered records.
/// Unknown views retain the injected-cap signal; complete sets do not.
pub(in crate::lastdb_host::handlers) fn materialized_page_payload(
    results: Vec<Value>,
    limit: usize,
    offset: usize,
    flags: PageFlags,
    cap_may_truncate: bool,
) -> Value {
    let total_count = results.len();
    let page: Vec<_> = results.into_iter().skip(offset).take(limit).collect();
    let has_more = compute_has_more(offset, page.len(), total_count, cap_may_truncate);
    page_payload(page, Some(total_count), limit, offset, has_more, flags)
}

fn next_cursor_for(page: &[Value], has_more: bool) -> Option<KeyValue> {
    if !has_more {
        return None;
    }
    page.last()
        .and_then(|row| row.get("key"))
        .and_then(|key| serde_json::from_value::<KeyValue>(key.clone()).ok())
}

/// Build `/api/query` row envelopes without clocks, host access, or telemetry.
/// Conditions already selected the keys in the core. A full row carries the
/// stored fields and the one metadata block of its
/// record (decision D7), or `null` when the record has none; a compact row
/// carries no metadata.
pub(in crate::lastdb_host::handlers) fn format_rows(
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
pub fn verified_query_page(
    records: Vec<(KeyValue, fold_db::fold_db_core::query::Record)>,
    compact: bool,
    total_count: Option<usize>,
    limit: usize,
    offset: usize,
    has_more: bool,
    next_cursor: Option<QueryCursor>,
) -> Value {
    let flags = PageFlags {
        key_form: (!records.is_empty()).then_some(fold_db::db_operations::KeyForm::Api),
    };
    let rows = records
        .into_iter()
        .map(|(key, record)| format_row(&key, record, compact))
        .collect();
    let mut payload = page_payload(rows, total_count, limit, offset, has_more, flags);
    payload["next_cursor"] = serde_json::to_value(next_cursor).unwrap_or(Value::Null);
    payload
}
