//! `GET /api/list?schema=` — paged primary or alternate API keys for live schema records.

use base64::Engine as _;
use fold_db::access::AccessContext;
use fold_db::db_operations::SchemaRecordKey;
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::{KeyValue, Query, QueryCursor, Schema};
use serde_json::Value;

use super::query::{execute_query, QueryResponseFormat};
use super::reply::{envelope, error_response, json_ok, mapped_error_response, render, HostError};
use super::schema_resolve::resolve_schema_name;
use super::wire::query_value;
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

pub(in crate::exec) struct ListRecordKeysRequest {
    pub(in crate::exec) requested: String,
    pub(in crate::exec) limit: usize,
    pub(in crate::exec) cursor: Option<String>,
    pub(in crate::exec) hash_filter: Option<String>,
    pub(in crate::exec) key_name: Option<String>,
}

impl ListRecordKeysRequest {
    pub(in crate::exec) fn parse(target: &str) -> Result<Self, &'static str> {
        let requested = query_value(target, "schema")
            .filter(|s| !s.is_empty())
            .ok_or("schema query parameter is required")?;
        let limit = match query_value(target, "limit") {
            None => 100,
            Some(raw) => match raw.parse::<usize>() {
                Ok(0) => return Err("limit must be at least 1"),
                Ok(limit) => limit.min(1000),
                Err(_) => return Err("limit must be a positive integer"),
            },
        };
        let key_name = match query_value(target, "key_name") {
            Some(name) if name.is_empty() => return Err("key_name must not be empty"),
            name => name,
        };
        Ok(Self {
            requested,
            limit,
            cursor: query_value(target, "cursor").filter(|c| !c.is_empty()),
            hash_filter: query_value(target, "hash"),
            key_name,
        })
    }
}

pub(in crate::exec) async fn execute_list_record_keys_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let request = match ListRecordKeysRequest::parse(&req.target) {
        Ok(r) => r,
        Err(msg) => return error_response(400, msg, ctx),
    };
    let canonical = match resolve_schema_name(host, &request.requested) {
        Ok(Some(c)) => c,
        Ok(None) => {
            return error_response(
                404,
                &format!("schema not found: {}", request.requested),
                ctx,
            )
        }
        Err(e) => return render(Err(e), ctx),
    };
    if request.key_name.is_some() {
        let result = match host.db.db_ops().read_scope().await {
            Ok(scope) => {
                scope
                    .run_result(Box::pin(alternate_keyed_page(host, &request, &canonical)))
                    .await
            }
            Err(error) => Err(HostError::from(error)),
        };
        return render(
            result.map(|report| serde_json::json!({ "list": report })),
            ctx,
        );
    }
    match host
        .db
        .list_schema_record_keys(
            &request.requested,
            &canonical,
            request.limit,
            request.cursor.as_deref(),
            request.hash_filter.as_deref(),
        )
        .await
    {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "list": report }),
            ctx.user_id.as_str(),
        )),
        Err(e) => mapped_error_response("list failed", e, ctx),
    }
}

async fn alternate_keyed_page(
    host: &Host,
    request: &ListRecordKeysRequest,
    canonical: &str,
) -> Result<Value, HostError> {
    let name = request.key_name.as_deref().expect("named branch");
    let hash = request
        .hash_filter
        .as_deref()
        .ok_or_else(|| HostError::new(400, "alternate key lists require hash="))?;
    let schema = host
        .db
        .schema_manager()
        .get_schema_metadata(canonical)
        .map_err(HostError::from)?
        .ok_or_else(|| HostError::new(404, "schema is absent"))?;
    let mut query = Query::new(canonical.to_owned());
    query.key_name = Some(name.to_owned());
    query.filter = Some(HashRangeFilter::HashKey(hash.to_owned()));
    let cursor = decode_alternate_cursor(request.cursor.as_deref())?;
    let page = execute_query(
        host,
        query,
        Some(request.limit),
        None,
        cursor,
        QueryResponseFormat::Full,
    )
    .await?;
    alternate_list_report(request, &schema, &page)
}

pub(in crate::exec) fn decode_alternate_cursor(
    raw: Option<&str>,
) -> Result<Option<QueryCursor>, HostError> {
    raw.map(|encoded| {
        let encoded = encoded
            .strip_prefix("ak2_")
            .ok_or_else(|| HostError::new(400, "invalid alternate key-list cursor"))?;
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| HostError::new(400, "invalid alternate key-list cursor"))?;
        serde_json::from_slice::<QueryCursor>(&bytes)
            .map_err(|_| HostError::new(400, "invalid alternate key-list cursor"))
    })
    .transpose()
}

pub(in crate::exec) fn alternate_list_report(
    request: &ListRecordKeysRequest,
    schema: &Schema,
    page: &Value,
) -> Result<Value, HostError> {
    let name = request.key_name.as_deref().expect("named branch");
    let definition = schema
        .require_record_keys()?
        .alternates
        .get(name)
        .ok_or_else(|| HostError::new(400, "alternate key is not declared"))?;
    let rows = page["results"]
        .as_array()
        .ok_or_else(|| HostError::new(500, "alternate key-list page is incomplete"))?;
    let keys: Vec<_> = rows
        .iter()
        .map(|row| record_key(&row["matched_key"]))
        .collect::<Result<_, _>>()?;
    let primary_keys: Vec<_> = rows
        .iter()
        .map(|row| record_key(&row["key"]))
        .collect::<Result<_, _>>()?;
    let next = if page["next_cursor"].is_null() {
        None
    } else {
        Some(format!(
            "ak2_{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
                serde_json::to_vec(&page["next_cursor"])
                    .map_err(|_| HostError::new(500, "invalid alternate key-list cursor"))?
            )
        ))
    };
    Ok(serde_json::json!({
        "schema": request.requested,
        "schema_id": schema.name,
        "key_name": name,
        "key_definition": definition,
        "hash_filter": request.hash_filter,
        "field_key": fold_db::alternate_keys::field_key(schema, name),
        "keys": keys,
        "primary_keys": primary_keys,
        "has_more": page["has_more"],
        "truncated": page["has_more"],
        "next_cursor": next,
    }))
}

fn record_key(value: &Value) -> Result<SchemaRecordKey, HostError> {
    let key: KeyValue = serde_json::from_value(value.clone())
        .map_err(|_| HostError::new(500, "key-list returned invalid coordinates"))?;
    Ok(SchemaRecordKey {
        hash: key
            .hash
            .ok_or_else(|| HostError::new(500, "key-list hash is absent"))?,
        range: key.range.unwrap_or_default(),
    })
}
