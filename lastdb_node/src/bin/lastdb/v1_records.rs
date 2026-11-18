//! `lastdb` record reads and writes on `POST /v1/read` and `POST /v1/write`.
//!
//! A received database is a `database` field on `/v1/read`. A write to one,
//! and an unknown database, print the node's refusal. A file is a path.

use super::{post_json_with_timeout, DatabaseTarget, RecordListArgs};
use base64::Engine as _;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::time::Duration;

/// A file the write names by path. No bytes cross the request.
pub(super) struct LinkedFile {
    pub(super) path: String,
    pub(super) name: Option<String>,
    pub(super) media_type: Option<String>,
}

/// What `/v1/write` receives for `file`. Clear is JSON null. Absent sends no key.
pub(super) enum WrittenFile<'a> {
    Absent,
    Clear,
    Link(&'a LinkedFile),
}

pub(super) fn database(target: &DatabaseTarget) -> Option<&str> {
    target.selector.as_deref()
}

pub(super) fn record_item(
    schema: &str,
    hash: &str,
    range: Option<&str>,
    database: Option<&str>,
) -> Value {
    let mut key = Map::new();
    key.insert("hash".into(), json!(hash));
    if let Some(range) = range.map(str::trim).filter(|range| !range.is_empty()) {
        key.insert("range".into(), json!(range));
    }
    let mut item = Map::new();
    insert_database(&mut item, database);
    item.insert("schema".into(), json!(schema));
    item.insert("key".into(), Value::Object(key));
    Value::Object(item)
}

pub(super) fn key_list_item(args: &RecordListArgs, database: Option<&str>) -> Value {
    let mut item = Map::new();
    insert_database(&mut item, database);
    item.insert("schema".into(), json!(args.schema));
    item.insert("keys_only".into(), json!(true));
    item.insert("limit".into(), json!(args.limit.max(1)));
    if let Some(hash) = args.key_hash.as_deref() {
        item.insert("range".into(), json!({ "hash": hash }));
    }
    if let Some(cursor) = args.cursor.as_deref() {
        item.insert("cursor".into(), json!(cursor));
    }
    Value::Object(item)
}

pub(super) fn read_page(
    target: &DatabaseTarget,
    item: &Value,
    timeout: Duration,
) -> Result<Value, String> {
    let reply = post_v1(target, "/v1/read", &json!({ "reads": [item] }), timeout)?;
    let page = reply
        .get("results")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .ok_or("read response missing results")?;
    if page.get("error").is_some() {
        return Err(item_refusal("/v1/read", page));
    }
    Ok(page.clone())
}

pub(super) fn write_mutations(
    target: &DatabaseTarget,
    item: &Value,
    timeout: Duration,
) -> Result<Value, String> {
    post_v1(
        target,
        "/v1/write",
        &json!({ "mutations": [item] }),
        timeout,
    )
}

/// The old get page, filled from one `/v1/read` records page.
pub(super) fn query_view(page: &Value) -> Result<Value, String> {
    let records = page
        .get("records")
        .and_then(Value::as_array)
        .ok_or("read response missing records")?;
    let results: Vec<Value> = records.iter().map(query_row).collect();
    let next = page["next_cursor"].as_str();
    Ok(json!({
        "ok": true,
        "results": results,
        "returned_count": results.len(),
        "has_more": next.is_some(),
        "next_cursor": next,
    }))
}

/// The old primary key-list report, filled from one `/v1/read` keys page.
pub(super) fn key_report(
    args: &RecordListArgs,
    schema: &Value,
    page: &Value,
) -> Result<Value, String> {
    let schema_id = schema["name"]
        .as_str()
        .ok_or("schema response has no name")?;
    let key_definition = schema
        .pointer("/record_keys/primary")
        .filter(|value| !value.is_null())
        .cloned()
        .ok_or("schema has no primary key")?;
    let next = page["next_cursor"]
        .as_str()
        .filter(|cursor| !cursor.is_empty());
    let mut report = Map::new();
    report.insert("schema".into(), json!(args.schema));
    report.insert("schema_id".into(), json!(schema_id));
    report.insert("key_definition".into(), key_definition);
    if let Some(hash) = args.key_hash.as_deref() {
        report.insert("hash_filter".into(), json!(hash));
    }
    report.insert("field_key".into(), json!(record_space_id(schema_id)));
    report.insert("keys".into(), Value::Array(listed_keys(page)?));
    if let Some(cursor) = next {
        report.insert("next_cursor".into(), json!(cursor));
    }
    let more = next.is_some();
    report.insert("has_more".into(), json!(more));
    report.insert("truncated".into(), json!(more));
    Ok(Value::Object(report))
}

/// One `/v1/write` mutation.
///
/// A create expects the key to be absent. A replace or delete names the
/// revision the caller read. Anything else is an invalid mutation.
pub(super) fn record_write(
    mutation: &Value,
    database: Option<&str>,
    file: &WrittenFile<'_>,
) -> Result<Value, String> {
    let linking = matches!(file, WrittenFile::Link(_));
    if !v1_capable(mutation, linking) {
        return Err("invalid complete mutation".into());
    }
    let Some(op) = mutation["mutation_type"].as_str() else {
        return Err("invalid complete mutation".into());
    };
    let mut item = Map::new();
    insert_database(&mut item, database);
    item.insert("schema".into(), mutation["schema"].clone());
    item.insert("op".into(), json!(op));
    item.insert("key".into(), mutation["key_value"].clone());
    if op != "delete" {
        item.insert("record".into(), mutation["record"].clone());
        item.insert("alternates".into(), mutation["alternates"].clone());
    }
    if op != "create" {
        let Some(revision) = mutation["expected"]["revision"].as_str() else {
            return Err("invalid complete mutation".into());
        };
        item.insert("revision".into(), json!(revision));
    }
    match file {
        WrittenFile::Link(link) => {
            item.insert("file".into(), file_link(link));
        }
        WrittenFile::Clear => {
            item.insert("file".into(), Value::Null);
        }
        WrittenFile::Absent => {}
    }
    Ok(Value::Object(item))
}

pub(super) fn receipt(reply: &Value, local: bool) -> Value {
    let revision = reply
        .get("revisions")
        .and_then(Value::as_array)
        .and_then(|revisions| revisions.first())
        .cloned()
        .unwrap_or(Value::Null);
    let mut out = json!({
        "ok": true,
        "success": true,
        "record_revision": revision,
        "written": reply["written"],
        "revisions": reply["revisions"],
    });
    if local {
        out["local"] = json!(true);
    }
    out
}

fn v1_capable(mutation: &Value, linking: bool) -> bool {
    if kept_aside(mutation) {
        return false;
    }
    let op = mutation["mutation_type"].as_str().unwrap_or("");
    let expected = mutation["expected"]["type"].as_str().unwrap_or("");
    let revision = mutation["expected"]["revision"].as_str().unwrap_or("");
    let fields_clear = mutation["record"].as_object().is_some_and(Map::is_empty);
    match (op, expected) {
        ("create", "absent") => true,
        ("replace", "revision") => !revision.is_empty(),
        ("delete", "revision") => {
            !linking && !revision.is_empty() && fields_clear && alternates_clear(mutation)
        }
        _ => false,
    }
}

/// A file marker or definition id has no field on `/v1/write`.
fn kept_aside(mutation: &Value) -> bool {
    named(mutation, "file") || named(mutation, "definition_id")
}

fn named(mutation: &Value, field: &str) -> bool {
    mutation.get(field).is_some_and(|value| !value.is_null())
}

fn alternates_clear(mutation: &Value) -> bool {
    match mutation.get("alternates") {
        None | Some(Value::Null) => true,
        Some(Value::Object(map)) => map.is_empty(),
        Some(_) => false,
    }
}

fn file_link(link: &LinkedFile) -> Value {
    let mut file = Map::new();
    file.insert("path".into(), json!(link.path));
    if let Some(name) = &link.name {
        file.insert("name".into(), json!(name));
    }
    if let Some(media_type) = &link.media_type {
        file.insert("media_type".into(), json!(media_type));
    }
    Value::Object(file)
}

fn query_row(record: &Value) -> Value {
    let mut row = Map::new();
    row.insert("key".into(), record["key"].clone());
    row.insert("revision".into(), record["revision"].clone());
    row.insert(
        "created_by".into(),
        record.get("created_by").cloned().unwrap_or(Value::Null),
    );
    row.insert(
        "updated_by".into(),
        record.get("updated_by").cloned().unwrap_or(Value::Null),
    );
    row.insert("fields".into(), record["record"].clone());
    if let Some(file) = record.get("file").filter(|file| !file.is_null()) {
        row.insert("metadata".into(), json!({ "file": file_marker(file) }));
    }
    Value::Object(row)
}

fn file_marker(file: &Value) -> Value {
    let mut marker = Map::new();
    for name in ["name", "media_type"] {
        if let Some(value) = file.get(name).filter(|value| !value.is_null()) {
            marker.insert(name.into(), value.clone());
        }
    }
    marker.insert("size_bytes".into(), file["size"].clone());
    if let Some(local) = file.get("local").and_then(Value::as_str) {
        marker.insert("local".into(), json!(local));
    }
    Value::Object(marker)
}

fn listed_keys(page: &Value) -> Result<Vec<Value>, String> {
    let keys = page
        .get("keys")
        .and_then(Value::as_array)
        .ok_or_else(|| "read response missing keys".to_string())?;
    Ok(keys.iter().map(listed_key).collect())
}

fn listed_key(key: &Value) -> Value {
    let mut out = Map::new();
    out.insert(
        "hash".into(),
        key.get("hash").cloned().unwrap_or(Value::Null),
    );
    if let Some(range) = key
        .get("range")
        .and_then(Value::as_str)
        .filter(|range| !range.is_empty())
    {
        out.insert("range".into(), json!(range));
    }
    Value::Object(out)
}

fn insert_database(item: &mut Map<String, Value>, database: Option<&str>) {
    if let Some(database) = database {
        item.insert("database".into(), json!(database));
    }
}

fn post_v1(
    target: &DatabaseTarget,
    route: &str,
    body: &Value,
    timeout: Duration,
) -> Result<Value, String> {
    let raw = post_json_with_timeout(&target.socket, route, body, timeout)?;
    let (head, resp) = raw
        .split_once("\r\n\r\n")
        .ok_or_else(|| format!("{route} response had no body"))?;
    let line = head.lines().next().unwrap_or("<empty response>");
    if !line.starts_with("HTTP/1.1 200 ") {
        return Err(refusal(route, line, resp.trim()));
    }
    serde_json::from_str(resp.trim()).map_err(|e| format!("invalid {route} JSON: {e}"))
}

fn refusal(route: &str, line: &str, body: &str) -> String {
    let parsed = serde_json::from_str::<Value>(body).ok();
    let error = parsed.as_ref().map(|value| &value["error"]);
    let message = error
        .and_then(|err| err["message"].as_str())
        .unwrap_or(body);
    let code = error
        .and_then(|err| err["code"].as_str())
        .unwrap_or("error");
    let mut text = format!("{route} returned {line}: {message} ({code})");
    if let Some(reason) = error.and_then(|err| err["reason"].as_str()) {
        text.push_str(&format!(" reason {reason}"));
    }
    if let Some(index) = error.and_then(|err| err["index"].as_u64()) {
        text.push_str(&format!(" index {index}"));
    }
    text
}

fn item_refusal(route: &str, page: &Value) -> String {
    let error = &page["error"];
    let message = error["message"].as_str().unwrap_or("read failed");
    let code = error["code"].as_str().unwrap_or("error");
    let mut text = format!("{route}: {message} ({code})");
    if let Some(reason) = error["reason"].as_str() {
        text.push_str(&format!(" reason {reason}"));
    }
    text
}

/// Same bytes as the record-space id printed by the old key list.
fn record_space_id(schema_name: &str) -> String {
    let mut hasher = Sha256::new();
    for part in [b"lastdb:record-space:v1".as_slice(), schema_name.as_bytes()] {
        let len = u32::try_from(part.len()).expect("canonical field exceeds 4 GiB");
        hasher.update(len.to_be_bytes());
        hasher.update(part);
    }
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hasher.finalize())
}
