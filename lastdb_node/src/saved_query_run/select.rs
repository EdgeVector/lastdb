//! Run the saved query against the source database. Links expand from the
//! read rows only. A link does not expand another link.

use std::collections::BTreeMap;

use crate::saved_query::{
    DeclareSchema, Definition, Link, LiveRow, Projected, RangeSelect, Read, RecordKey, MAX_RECORDS,
};
use serde_json::{json, Value};

use super::http::{DatabaseTarget, Store};

pub(super) fn select(
    store: &Store,
    source: &DatabaseTarget,
    definition: &Definition,
    declared: &[DeclareSchema],
) -> Result<Vec<Projected>, String> {
    let mut rows = BTreeMap::new();
    for read in &definition.reads {
        for row in read_one(store, source, read)? {
            let key = row_key(&read.namespace, &read.schema, &row)?;
            expect_read_key(read, &key)?;
            rows.insert(key, row);
        }
    }
    let reads = rows.clone();
    for link in &definition.links {
        expand_link(store, source, link, &reads, &mut rows)?;
    }
    if rows.len() > MAX_RECORDS {
        return Err("saved query selects more than 256 records".into());
    }
    let mut projected = Vec::new();
    for (key, row) in rows {
        let schema = declared_schema(declared, &key)?;
        let mut item = Projected::from_query(&key.namespace, &key.schema, &schema.fields, &row)
            .map_err(|error| error.as_str().to_owned())?;
        if !schema.files {
            item.blob = None;
            item.inline = None;
        }
        projected.push(item);
    }
    Ok(projected)
}

fn read_one(store: &Store, source: &DatabaseTarget, read: &Read) -> Result<Vec<Value>, String> {
    let schema = format!("{}/{}", read.namespace, read.schema);
    let filter = match &read.range {
        RangeSelect::Point | RangeSelect::All => json!({"HashKey": read.hash}),
        RangeSelect::Exact(range) => json!({"HashRangeKey": {"hash": read.hash, "range": range}}),
    };
    let rows = store.query(source, &schema, &filter)?;
    let exact = !matches!(read.range, RangeSelect::All);
    if exact && rows.len() > 1 {
        return Err("saved query read returned more than one record".into());
    }
    Ok(rows)
}

fn expect_read_key(read: &Read, key: &RecordKey) -> Result<(), String> {
    if key.hash != read.hash {
        return Err("saved query read returned an unexpected record".into());
    }
    if let RangeSelect::Exact(range) = &read.range {
        if key.range.as_deref() != Some(range.as_str()) {
            return Err("saved query read returned an unexpected record".into());
        }
    }
    Ok(())
}

fn expand_link(
    store: &Store,
    source: &DatabaseTarget,
    link: &Link,
    reads: &BTreeMap<RecordKey, Value>,
    rows: &mut BTreeMap<RecordKey, Value>,
) -> Result<(), String> {
    for (key, row) in reads {
        if key.namespace != link.from_namespace || key.schema != link.from_schema {
            continue;
        }
        let Some(hash) = link_hash(row, &link.field)? else {
            continue;
        };
        let schema = format!("{}/{}", link.namespace, link.schema);
        let found = store.query(source, &schema, &json!({ "HashKey": hash }))?;
        if found.len() > 1 {
            return Err("saved query link returned more than one record".into());
        }
        if let Some(target) = found.into_iter().next() {
            let key = row_key(&link.namespace, &link.schema, &target)?;
            if key.hash != hash {
                return Err("saved query link returned an unexpected record".into());
            }
            rows.insert(key, target);
        }
    }
    Ok(())
}

fn link_hash(row: &Value, field: &str) -> Result<Option<String>, String> {
    let value = row.pointer(&format!("/fields/{field}"));
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) if text.is_empty() => Ok(None),
        Some(Value::String(text)) => Ok(Some(text.clone())),
        Some(_) => Err("saved query link field is not a string".into()),
    }
}

fn row_key(namespace: &str, schema: &str, row: &Value) -> Result<RecordKey, String> {
    LiveRow::from_query(namespace, schema, row)
        .map(|row| row.key)
        .map_err(|error| error.as_str().to_owned())
}

fn declared_schema<'a>(
    declared: &'a [DeclareSchema],
    key: &RecordKey,
) -> Result<&'a DeclareSchema, String> {
    declared
        .iter()
        .find(|schema| schema.namespace == key.namespace && schema.name == key.schema)
        .ok_or("source schema is not installed".to_owned())
}
