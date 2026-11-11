//! `GET /api/list?schema=` — paged primary or alternate API keys for live schema records.

use base64::Engine as _;
use fold_db::access::AccessContext;
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::{Query, QueryCursor, Schema};
use serde_json::Value;

use super::query::{execute_query, QueryResponseFormat};
use super::reply::{envelope, error_response, json_ok, mapped_error_response, render, HostError};
use super::schema_resolve::resolve_schema_name;
use super::wire::query_value;
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

const LIST_RECORD_KEYS_DEFAULT_LIMIT: usize = 100;
const LIST_RECORD_KEYS_MAX_LIMIT: usize = 1000;

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
            .filter(|schema| !schema.is_empty())
            .ok_or("schema query parameter is required")?;
        let limit = match query_value(target, "limit") {
            None => LIST_RECORD_KEYS_DEFAULT_LIMIT,
            Some(raw) => match raw.parse::<usize>() {
                Ok(0) => return Err("limit must be at least 1"),
                Ok(limit) => limit.min(LIST_RECORD_KEYS_MAX_LIMIT),
                Err(_) => return Err("limit must be a positive integer"),
            },
        };
        Ok(Self {
            requested,
            limit,
            cursor: query_value(target, "cursor").filter(|cursor| !cursor.is_empty()),
            hash_filter: query_value(target, "hash"),
            key_name: match query_value(target, "key_name") {
                Some(name) if name.is_empty() => return Err("key_name must not be empty"),
                name => name,
            },
        })
    }
}

pub(in crate::exec) async fn execute_list_record_keys_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let request = match ListRecordKeysRequest::parse(&req.target) {
        Ok(request) => request,
        Err(message) => return error_response(400, message, ctx),
    };

    let resolved = match resolve_schema_name(host, &request.requested) {
        Ok(v) => v,
        Err(e) => return render(Err(e), ctx),
    };
    let Some(canonical) = resolved else {
        return error_response(
            404,
            &format!("schema not found: {}", request.requested),
            ctx,
        );
    };

    if request.key_name.is_some() {
        return execute_alternate_list(host, ctx, &request, &canonical).await;
    }

    match host
        .db
        .list_schema_record_keys(fold_db::db_operations::SchemaRecordKeysRequest {
            requested: &request.requested,
            schema_id: &canonical,
            limit: request.limit,
            cursor: request.cursor.as_deref(),
            hash_filter: request.hash_filter.as_deref(),
        })
        .await
    {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "list": report }),
            ctx.user_id.as_str(),
        )),
        Err(e) => mapped_error_response("list failed", e, ctx),
    }
}

async fn execute_alternate_list(
    host: &Host,
    ctx: &AccessContext,
    request: &ListRecordKeysRequest,
    canonical: &str,
) -> UdsResponse {
    let result = match host.db.db_ops().read_scope().await {
        Ok(scope) => {
            scope
                .run_result(Box::pin(alternate_keyed_page(host, request, canonical)))
                .await
        }
        Err(error) => Err(HostError::from(error)),
    };
    match result {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "list": report }),
            ctx.user_id.as_str(),
        )),
        Err(error) => render(Err(error), ctx),
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

fn record_key(value: &Value) -> Result<fold_db::db_operations::SchemaRecordKey, HostError> {
    let key: fold_db::schema::types::KeyValue = serde_json::from_value(value.clone())
        .map_err(|_| HostError::new(500, "key-list returned invalid coordinates"))?;
    Ok(fold_db::db_operations::SchemaRecordKey {
        hash: key
            .hash
            .ok_or_else(|| HostError::new(500, "key-list hash is absent"))?,
        range: key.range.unwrap_or_default(),
    })
}
