//! `POST /v1/read`: a list of keyed reads, each with its own result or error.

use fold_db::access::AccessContext;
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::{KeyValue, Query};
use futures::StreamExt as _;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use super::super::calling_app::active_app;
use super::{bad, database, error_value, fail, key_filter, require_exact_name, require_read};
use crate::exec::query::{execute_query, QueryResponseFormat};
use crate::exec::reply::{map_host_err, HostError};
use crate::exec::schema_routes::resolve_schema_name;
use crate::exec::wire::{decode_cursor_token, encode_cursor_token, QueryRequest};
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

mod points;

use points::Answer;

const MAX_READS: usize = 64;
const KEY_LIST_DEFAULT: usize = 100;
const KEY_LIST_MAX: usize = 1000;
const CONCURRENT_READS: usize = 4;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadBody {
    reads: Vec<ReadItem>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadItem {
    #[serde(default)]
    database: Option<String>,
    schema: String,
    #[serde(default)]
    key: Option<KeyValue>,
    #[serde(default)]
    range: Option<RangeSelector>,
    #[serde(default)]
    via: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    keys_only: bool,
    #[serde(default)]
    file: bool,
    /// Owner socket only. The folder a held file is written into.
    #[serde(default)]
    file_dir: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RangeSelector {
    hash: String,
    #[serde(default)]
    prefix: Option<String>,
    #[serde(default)]
    start: Option<String>,
    #[serde(default)]
    end: Option<String>,
}

pub(in crate::exec) async fn read_route(
    req: &UdsRequest,
    _ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let items = match parse(&req.body) {
        Ok(items) => items,
        Err(error) => return fail(&error),
    };
    // Point reads share one lookup per schema. Every other read, and every
    // point read that path does not answer, runs on its own as before.
    let found = points::read(host, &items).await;
    let reads = items
        .into_iter()
        .zip(found)
        .map(|(item, found)| async move {
            match found {
                Some(point) => Answer::Point(point),
                None => Answer::Built(
                    read_one(host, item)
                        .await
                        .unwrap_or_else(|error| error_value(&error)),
                ),
            }
        });
    let answers: Vec<Answer> = futures::stream::iter(reads)
        .buffered(CONCURRENT_READS)
        .collect()
        .await;
    points::reply(&answers)
}

fn parse(body: &[u8]) -> Result<Vec<ReadItem>, HostError> {
    let parsed: ReadBody = serde_json::from_slice(body)
        .map_err(|error| bad(format!("invalid /v1/read body: {error}")))?;
    if parsed.reads.is_empty() {
        return Err(bad("reads must hold at least one read"));
    }
    if parsed.reads.len() > MAX_READS {
        return Err(bad(format!(
            "reads holds {} reads; the limit is {MAX_READS}",
            parsed.reads.len()
        )));
    }
    Ok(parsed.reads)
}

fn filter(item: &ReadItem) -> Result<HashRangeFilter, HostError> {
    match (&item.key, &item.range) {
        (Some(key), None) => key_filter(key),
        (None, Some(range)) => {
            let hash = range.hash.clone();
            match (&range.prefix, &range.start, &range.end) {
                (None, None, None) => Ok(HashRangeFilter::HashKey(hash)),
                (Some(prefix), None, None) => Ok(HashRangeFilter::HashRangePrefix {
                    hash,
                    prefix: prefix.clone(),
                }),
                (None, Some(start), Some(end)) => Ok(HashRangeFilter::HashRangeRange {
                    hash,
                    start: start.clone(),
                    end: end.clone(),
                }),
                _ => Err(bad(
                    "range takes hash alone, hash with prefix, or hash with start and end",
                )),
            }
        }
        _ => Err(bad("a read names exactly one of key or range")),
    }
}

/// Every primary key of a schema, a page at a time. Only keys leave without a
/// key or a hash: a read of records still names one.
async fn list_keys(host: &Host, item: ReadItem) -> Result<Value, HostError> {
    if !item.keys_only {
        return Err(bad(
            "a read of records names a key or a range; only keys_only lists a schema",
        ));
    }
    if item.via.is_some() || item.file || item.file_dir.is_some() {
        return Err(bad("a key list takes no via, no file, and no file_dir"));
    }
    let limit = match item.limit {
        Some(0) => return Err(bad("limit must be at least 1")),
        Some(limit) => limit.min(KEY_LIST_MAX),
        None => KEY_LIST_DEFAULT,
    };
    let (schema, cursor) = (item.schema, item.cursor);
    database::read_in(item.database.as_deref(), host, async {
        require_exact_name(host, &schema)?;
        let canonical = resolve_schema_name(host, &schema)?
            .ok_or_else(|| HostError::new(404, format!("no schema is named {schema}")))?;
        let report = host
            .records()
            .list_schema_record_keys(&schema, &canonical, limit, cursor.as_deref(), None)
            .await
            .map_err(|error| map_host_err("key list failed", error))?;
        let keys: Vec<Value> = report
            .keys
            .iter()
            .map(|key| {
                let range = (!key.range.is_empty()).then_some(key.range.as_str());
                json!({ "hash": key.hash, "range": range })
            })
            .collect();
        Ok(json!({ "keys": keys, "next_cursor": report.next_cursor }))
    })
    .await
}

async fn read_one(host: &Host, item: ReadItem) -> Result<Value, HostError> {
    require_read(&item.schema)?;
    let owner_dir = file_dir(host, &item)?;
    if item.key.is_none() && item.range.is_none() {
        return list_keys(host, item).await;
    }
    let filter = filter(&item)?;
    let cursor = decode_cursor_token(item.cursor.as_deref(), "", "invalid cursor")?;
    let schema = item.schema.clone();
    let mut query = Query::new_with_filter(item.schema, Some(filter));
    query.key_name = item.via;
    // Only the full row form carries the file marker of a record. A key read
    // still reads and checks each record, but copies none of its fields.
    let response_format = if item.keys_only {
        QueryResponseFormat::Keys
    } else {
        QueryResponseFormat::Full
    };
    let request = QueryRequest {
        query,
        limit: item.limit,
        offset: None,
        cursor,
        response_format,
    };
    let (keys_only, with_paths) = (item.keys_only, item.file);
    database::read_in(item.database.as_deref(), host, async {
        require_exact_name(host, &schema)?;
        let mut page = execute_query(host, request).await?;
        let mut out = shape(&mut page, keys_only)?;
        if !keys_only {
            super::file_place::attach(
                host,
                &schema,
                &page,
                &mut out,
                with_paths,
                owner_dir.as_deref(),
            )
            .await?;
        }
        Ok(out)
    })
    .await
}

/// `file_dir` is an owner-socket folder. An app's folder is its grant.
fn file_dir(host: &Host, item: &ReadItem) -> Result<Option<std::path::PathBuf>, HostError> {
    let Some(dir) = item.file_dir.as_deref() else {
        return Ok(None);
    };
    if active_app().is_some() {
        return Err(bad(
            "file_dir is refused on the app socket; the app's folder is fixed by its grant",
        ));
    }
    if !item.file {
        return Err(bad("file_dir requires file"));
    }
    if item.key.is_none() && item.range.is_none() {
        return Err(bad("a key list takes no via, no file, and no file_dir"));
    }
    super::owner_file::accept(dir, &host.home).map(Some)
}

/// The name, media type, size and local path of a record's file, or null without one.
/// `local` starts null. `file_place` fills it when this device has the file.
fn file_info(marker: &Value) -> Value {
    if marker.is_null() {
        return Value::Null;
    }
    json!({
        "name": marker["name"],
        "media_type": marker["media_type"],
        "size": marker["size_bytes"],
        "local": Value::Null,
    })
}

/// Copy one author key when the row has a string. Omit it otherwise. Never send null.
fn copy_author(record: &mut Map<String, Value>, row: &Value, name: &str) {
    if let Some(value) = row.get(name).filter(|value| value.is_string()) {
        record.insert(name.to_owned(), value.clone());
    }
}

/// Moves each record's fields out of the page: they are the bulk of a reply.
/// The page keeps its keys and metadata for `file_place`.
fn shape(page: &mut Value, keys_only: bool) -> Result<Value, HostError> {
    let rows = page
        .get_mut("results")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| HostError::internal("read page is incomplete"))?;
    let mut out = Map::new();
    if keys_only {
        let keys: Vec<Value> = rows.iter().map(|row| row["key"].clone()).collect();
        out.insert("keys".into(), Value::Array(keys));
    } else {
        let records: Vec<Value> = rows
            .iter_mut()
            .map(|row| {
                let mut record = Map::new();
                record.insert("key".into(), row["key"].clone());
                record.insert("revision".into(), row["revision"].clone());
                // A missing key stays omitted. Indexing a missing key would yield null.
                copy_author(&mut record, row, "created_by");
                copy_author(&mut record, row, "updated_by");
                let fields = row.get_mut("fields").map_or(Value::Null, Value::take);
                record.insert("record".into(), fields);
                record.insert("file".into(), file_info(&row["metadata"]["file"]));
                Value::Object(record)
            })
            .collect();
        out.insert("records".into(), Value::Array(records));
    }
    let next = encode_cursor_token(&page["next_cursor"], "", "cursor encoding failed")?;
    out.insert("next_cursor".into(), json!(next));
    Ok(Value::Object(out))
}
