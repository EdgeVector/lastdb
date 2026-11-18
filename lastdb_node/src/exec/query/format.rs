//! Owned row and page response assembly, scan rejection, and sorting.

use std::cmp::Ordering;

use fold_db::fold_db_core::query::{AlternatePage, Record};
use fold_db::schema::types::key_comparison::RangeKeyComparison;
use fold_db::schema::types::operations::SortOrder;
use fold_db::schema::types::{KeyValue, QueryCursor, Schema, SchemaError};
use serde_json::{json, Value};

use crate::exec::reply::HostError;

const KEYED_FILTER_VARIANTS: &[&str] = &[
    "HashKey",
    "HashRangeKey",
    "HashRangePrefix",
    "HashRangeRange",
    "HashRangeKeys",
];

pub(super) fn page_payload(
    page: Vec<Value>,
    total_count: Option<usize>,
    (limit, offset): (usize, usize),
    has_more: bool,
    next_cursor: Option<QueryCursor>,
) -> Value {
    let n = page.len();
    json!({
        "results": Value::Array(page), "total_count": total_count, "returned_count": n,
        "limit": limit, "offset": offset, "has_more": has_more,
        "next_cursor": serde_json::to_value(next_cursor).unwrap_or(Value::Null),
    })
}

pub(super) fn cursor_payload(
    mut page: Vec<Value>,
    total_count: Option<usize>,
    (limit, offset): (usize, usize),
    resume: Option<KeyValue>,
    comparison: Option<String>,
    with_cursor: bool,
) -> Value {
    let has_more = page.len() > limit || resume.is_some();
    page.truncate(limit);
    let next = with_cursor
        .then(|| {
            resume.or_else(|| {
                has_more
                    .then(|| serde_json::from_value(page.last()?.get("key")?.clone()).ok())
                    .flatten()
            })
        })
        .flatten()
        .map(|key| QueryCursor::new(key, comparison));
    page_payload(page, total_count, (limit, offset), has_more, next)
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

fn format_row(key: &KeyValue, record: Record, compact: bool) -> Value {
    let mut row = serde_json::Map::from_iter([
        ("key".into(), json!(key)),
        ("revision".into(), Value::String(record.revision)),
        ("alternates".into(), json!(record.alternates)),
        (
            "fields".into(),
            Value::Object(record.fields.into_iter().collect()),
        ),
    ]);
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
    let next = page
        .has_more
        .then(|| page.entries.last())
        .flatten()
        .map(|e| {
            let mut c = QueryCursor::new(e.matched.clone(), Some(page.comparison));
            c.alternate_primary = Some(e.primary.clone());
            c
        });
    Ok(page_payload(results, None, (limit, 0), page.has_more, next))
}

pub(super) fn full_schema_scan_rejection(requested: &str, schema: Option<&Schema>) -> HostError {
    let is_id = |v: &str| v.len() == 64 && v.bytes().all(|b| b.is_ascii_hexdigit());
    let name = schema
        .and_then(|s| s.descriptive_name.clone())
        .or_else(|| (!is_id(requested)).then(|| requested.to_string()))
        .or_else(|| schema.and_then(|s| (!is_id(&s.name)).then(|| s.name.clone())))
        .or_else(|| {
            schema
                .and_then(|s| s.owner_app_id.as_deref())
                .map(|o| format!("{o}/<unnamed-schema>"))
        })
        .unwrap_or_else(|| "<unnamed-product-schema>".to_string());
    let msg = format!(
        "full_schema_scan_not_allowed: unfiltered query on product schema '{name}' is not supported; add a keyed filter using the structured remediation"
    );
    let body = json!({
        "ok": false, "kind": "full_schema_scan_not_allowed",
        "error": "product apps are keyed-access only; unfiltered schema queries are not supported",
        "message": msg, "schema_name": name,
        "schema_id": schema.map_or(requested, |s| s.name.as_str()),
        "remediation": {
            "action": "add_key_filter", "request_key": "filter",
            "record_keys": schema.and_then(|s| s.record_keys.as_ref()),
            "filter_variants": KEYED_FILTER_VARIANTS,
        }
    });
    HostError::structured(400, msg, body)
}

pub(super) fn sort_rows(
    rows: &mut [Value],
    sort_order: Option<&SortOrder>,
    range_cmp: Option<&RangeKeyComparison>,
) -> Result<(), HostError> {
    if let Some(cmp) = range_cmp {
        for r in &*rows {
            cmp.validate_value(r["key"]["range"].as_str().unwrap_or(""))
                .map_err(SchemaError::InvalidData)?;
        }
    }
    let (mut err, desc) = (None, matches!(sort_order, Some(SortOrder::Desc)));
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
        (if desc { ord.reverse() } else { ord }).then_with(|| {
            a["key"]["hash"]
                .as_str()
                .unwrap_or("")
                .cmp(b["key"]["hash"].as_str().unwrap_or(""))
        })
    });
    err.map_or(Ok(()), |e| Err(e.into()))
}
