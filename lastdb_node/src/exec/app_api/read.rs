//! `POST /v1/read`: a list of keyed reads, each with its own result or error.

use base64::Engine as _;
use fold_db::access::AccessContext;
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::{KeyValue, Query, QueryCursor};
use futures::StreamExt as _;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use super::{bad, error_value, fail, in_database, ok};
use crate::exec::query::{execute_query, QueryResponseFormat};
use crate::exec::reply::HostError;
use crate::exec::schema_routes::resolve_schema_name;
use crate::exec::wire::QueryRequest;
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

const MAX_READS: usize = 64;
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
    let reads = items.into_iter().map(|item| async move {
        read_one(host, item)
            .await
            .unwrap_or_else(|error| error_value(&error))
    });
    let results: Vec<Value> = futures::stream::iter(reads)
        .buffered(CONCURRENT_READS)
        .collect()
        .await;
    ok(&json!({ "results": results }))
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
        (Some(key), None) => {
            let hash = key
                .hash
                .clone()
                .filter(|hash| !hash.is_empty())
                .ok_or_else(|| bad("key.hash is required"))?;
            Ok(match key.range.clone() {
                Some(range) => HashRangeFilter::HashRangeKey { hash, range },
                None => HashRangeFilter::HashKey(hash),
            })
        }
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

fn decode_cursor(raw: Option<&str>) -> Result<Option<QueryCursor>, HostError> {
    raw.map(|encoded| {
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| bad("invalid cursor"))?;
        serde_json::from_slice::<QueryCursor>(&bytes).map_err(|_| bad("invalid cursor"))
    })
    .transpose()
}

fn encode_cursor(next: &Value) -> Result<Value, HostError> {
    if next.is_null() {
        return Ok(Value::Null);
    }
    serde_json::to_vec(next)
        .map(|bytes| Value::String(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)))
        .map_err(|_| HostError::internal("cursor encoding failed"))
}

async fn read_one(host: &Host, item: ReadItem) -> Result<Value, HostError> {
    let filter = filter(&item)?;
    let cursor = decode_cursor(item.cursor.as_deref())?;
    let schema = item.schema.clone();
    let mut query = Query::new_with_filter(item.schema, Some(filter));
    query.key_name = item.via;
    // Only the full row form carries the file marker of a record.
    let response_format = if item.keys_only {
        QueryResponseFormat::Values
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
    in_database(item.database.as_deref(), host, async {
        let page = execute_query(host, request).await?;
        let mut out = shape(&page, keys_only)?;
        if with_paths && !keys_only {
            add_paths(host, &schema, &mut out).await?;
        }
        Ok(out)
    })
    .await
}

/// The name, media type and size of a record's file, or null without one.
fn file_info(marker: &Value) -> Value {
    if marker.is_null() {
        return Value::Null;
    }
    json!({
        "name": marker["name"], "media_type": marker["media_type"], "size": marker["size_bytes"],
    })
}

/// Add the path of each record's file. A file that LastDB holds as bytes and
/// not as a reference to a local file gets a null path.
async fn add_paths(host: &Host, schema: &str, out: &mut Value) -> Result<(), HostError> {
    let canonical = resolve_schema_name(host, schema)?.unwrap_or_else(|| schema.to_owned());
    let Some(records) = out.get_mut("records").and_then(Value::as_array_mut) else {
        return Ok(());
    };
    for record in records {
        if record["file"].is_null() {
            continue;
        }
        let key: KeyValue = serde_json::from_value(record["key"].clone())
            .map_err(|_| HostError::internal("read returned an invalid key"))?;
        let local = host
            .records()
            .record_file_local_path(&canonical, &key)
            .await?;
        record["file"]["path"] = match local {
            Some(file) if file.intact => Value::String(file.path.to_string_lossy().into_owned()),
            Some(file) => {
                return Err(HostError::new(
                    404,
                    format!(
                        "the file of this record is missing or changed at {}",
                        file.path.display()
                    ),
                ));
            }
            None => Value::Null,
        };
    }
    Ok(())
}

fn shape(page: &Value, keys_only: bool) -> Result<Value, HostError> {
    let rows = page["results"]
        .as_array()
        .ok_or_else(|| HostError::internal("read page is incomplete"))?;
    let mut out = Map::new();
    if keys_only {
        let keys: Vec<Value> = rows.iter().map(|row| row["key"].clone()).collect();
        out.insert("keys".into(), Value::Array(keys));
    } else {
        let records: Vec<Value> = rows
            .iter()
            .map(|row| {
                json!({
                    "key": row["key"], "revision": row["revision"], "record": row["fields"],
                    "file": file_info(&row["metadata"]["file"]),
                })
            })
            .collect();
        out.insert("records".into(), Value::Array(records));
    }
    out.insert("next_cursor".into(), encode_cursor(&page["next_cursor"])?);
    Ok(Value::Object(out))
}
