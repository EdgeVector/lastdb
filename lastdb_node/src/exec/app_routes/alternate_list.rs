//! Named key lists share the captured alternate query and its cursor checks.

use super::*;
use base64::Engine as _;
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::{Query, QueryCursor};

pub(super) async fn execute(
    host: &Host,
    ctx: &AccessContext,
    request: &ListRecordKeysRequest,
    canonical: &str,
) -> UdsResponse {
    let result = match host.db.db_ops().read_scope().await {
        Ok(scope) => {
            scope
                .run_result(Box::pin(keyed_page(host, ctx, request, canonical)))
                .await
        }
        Err(error) => Err(HostError::from(error)),
    };
    match result {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({"list": report}),
            ctx.user_id.as_str(),
        )),
        Err(error) => render(Err(error), ctx),
    }
}

async fn keyed_page(
    host: &Host,
    ctx: &AccessContext,
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
    let definition = schema
        .alternate_keys
        .get(name)
        .ok_or_else(|| HostError::new(400, "alternate key is not declared"))?;
    let primary = schema
        .key
        .as_ref()
        .and_then(|key| key.hash_field.clone())
        .ok_or_else(|| HostError::new(400, "primary hash field is absent"))?;
    let mut query = Query::new(canonical.to_owned(), vec![primary]);
    query.key_name = Some(name.to_owned());
    query.filter = Some(HashRangeFilter::HashKey(hash.to_owned()));
    let cursor = request
        .cursor
        .as_deref()
        .map(|encoded| {
            let encoded = encoded
                .strip_prefix("ak1_")
                .ok_or_else(|| HostError::new(400, "invalid alternate key-list cursor"))?;
            let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(encoded)
                .map_err(|_| HostError::new(400, "invalid alternate key-list cursor"))?;
            serde_json::from_slice::<QueryCursor>(&bytes)
                .map_err(|_| HostError::new(400, "invalid alternate key-list cursor"))
        })
        .transpose()?;
    let page =
        handlers::execute_query(host, query, Some(request.limit), None, cursor, ctx, false).await?;
    let rows = page["results"]
        .as_array()
        .ok_or_else(|| HostError::internal("alternate key-list page is incomplete"))?;
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
            "ak1_{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
                serde_json::to_vec(&page["next_cursor"])
                    .map_err(|_| HostError::internal("invalid alternate key-list cursor"))?
            )
        ))
    };
    Ok(
        serde_json::json!({"schema": request.requested, "schema_id": canonical, "key_name": name,
        "key_field": definition.hash_field, "hash_field": definition.hash_field, "range_field": definition.range_field,
        "hash_filter": hash, "molecule": fold_db::alternate_keys::molecule(&schema, name),
        "keys": keys, "primary_keys": primary_keys, "has_more": page["has_more"], "truncated": page["has_more"], "next_cursor": next}),
    )
}

fn record_key(value: &Value) -> Result<fold_db::db_operations::SchemaRecordKey, HostError> {
    let key: fold_db::schema::types::KeyValue = serde_json::from_value(value.clone())
        .map_err(|_| HostError::internal("key-list returned invalid coordinates"))?;
    Ok(fold_db::db_operations::SchemaRecordKey {
        hash: key
            .hash
            .ok_or_else(|| HostError::internal("key-list hash is absent"))?,
        range: key.range.unwrap_or_default(),
    })
}
