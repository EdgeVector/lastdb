//! Owner-socket delete-ledger and schema-retention routes.

use fold_db::access::AccessContext;
use serde::Deserialize;

use super::reply::{envelope, error_response, json_ok};
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

/// `GET /api/db/delete-ledger` — read the durable record hard-delete audit trail.
pub(in crate::exec) async fn execute_db_delete_ledger_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let limit = query_u64(&req.target, "limit").unwrap_or(0) as usize;
    match host.db.list_record_delete_ledger(limit).await {
        Ok(entries) => json_ok(&envelope(
            &serde_json::json!({
                "delete_ledger": { "count": entries.len(), "entries": entries }
            }),
            ctx.user_id.as_str(),
        )),
        Err(e) => error_response(500, &format!("delete-ledger failed: {e}"), ctx),
    }
}

fn query_u64(target: &str, key: &str) -> Option<u64> {
    let (_, q) = target.split_once('?')?;
    for pair in q.split('&') {
        let (k, v) = pair.split_once('=')?;
        if k == key {
            return v.parse().ok();
        }
    }
    None
}

#[derive(Deserialize)]
struct RetentionBody {
    action: String,
    schema: String,
    #[serde(default)]
    ttl_seconds: Option<u64>,
    #[serde(default)]
    hash_partitions: Vec<String>,
    #[serde(default)]
    confirm_expire_existing: bool,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

fn clear_may_expire(
    body: &RetentionBody,
    previous: &Option<fold_db::schema::SchemaRetentionPolicy>,
) -> Result<bool, String> {
    use fold_db::schema::SchemaRetentionPolicy as Policy;
    if body.action != "clear" {
        return Ok(false);
    }
    let (Some(operator), Some(restored)) = (
        previous.as_ref(),
        Policy::after_operator_clear(previous.as_ref()),
    ) else {
        return Ok(false);
    };
    if restored.ttl_seconds >= operator.ttl_seconds {
        return Ok(false);
    }
    if body.confirm_expire_existing {
        return Ok(true);
    }
    Err(format!(
        "clearing the operator policy of {}s puts the manifest TTL of {}s in force. \
         existing_records_may_expire: every record older than {}s is deleted for good, a Range \
         schema's at the next sweep. Review it, then run \
         `lastdb admin retention clear --schema {} --confirm-expire-existing`",
        operator.ttl_seconds, restored.ttl_seconds, restored.ttl_seconds, body.schema
    ))
}

/// `POST /api/db/schema-retention` — owner read/set/clear/backfill/partitions of node-local retention.
pub(in crate::exec) async fn execute_db_schema_retention_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(403, "schema retention is owner-only", ctx);
    }
    let body: RetentionBody = match serde_json::from_slice(&req.body) {
        Ok(body) => body,
        Err(error) => {
            return error_response(
                400,
                &format!("invalid /api/db/schema-retention body: {error}"),
                ctx,
            )
        }
    };
    if body.schema.is_empty() {
        return error_response(400, "schema is required", ctx);
    }
    if body.action == "backfill" {
        return execute_retention_backfill(ctx, host, &body).await;
    }
    let store = host.db.db_ops().schemas();
    let previous = match store.get_schema_retention_policy(&body.schema).await {
        Ok(previous) => previous,
        Err(error) => {
            return error_response(400, &format!("schema retention failed: {error}"), ctx)
        }
    };
    if body.action == "partitions" {
        return execute_retention_partitions(&body, ctx, host).await;
    }
    let may_expire = match clear_may_expire(&body, &previous) {
        Ok(may_expire) => may_expire,
        Err(message) => return error_response(409, &message, ctx),
    };
    let result = match body.action.as_str() {
        "get" => store.get_schema_retention_policy(&body.schema).await,
        "set" => {
            let Some(ttl_seconds) = body.ttl_seconds else {
                return error_response(400, "ttl_seconds is required for set", ctx);
            };
            match store
                .set_schema_retention_policy(
                    &body.schema,
                    fold_db::schema::SchemaRetentionPolicy::operator(
                        ttl_seconds,
                        body.hash_partitions.clone(),
                    ),
                )
                .await
            {
                Ok(()) => store.get_schema_retention_policy(&body.schema).await,
                Err(error) => Err(error),
            }
        }
        "clear" => match store.clear_schema_retention_policy(&body.schema).await {
            Ok(()) => store.get_schema_retention_policy(&body.schema).await,
            Err(error) => Err(error),
        },
        _ => return error_response(400, "action must be get, set, or clear", ctx),
    };
    match result {
        Ok(policy) => json_ok(&envelope(
            &serde_json::json!({
                "schema": body.schema,
                "retention_policy": policy,
                "anchor": body.schema,
                "existing_records_may_expire": may_expire,
            }),
            ctx.user_id.as_str(),
        )),
        Err(error) => error_response(400, &format!("schema retention failed: {error}"), ctx),
    }
}

async fn execute_retention_backfill(
    ctx: &AccessContext,
    host: &Host,
    body: &RetentionBody,
) -> UdsResponse {
    if !body.confirm_expire_existing {
        return error_response(
            409,
            "retention backfill can expire existing records; confirm_expire_existing is required",
            ctx,
        );
    }
    match host
        .db
        .backfill_schema_retention_page(
            &body.schema,
            body.cursor.as_deref(),
            body.limit.unwrap_or(16),
        )
        .await
    {
        Ok(value) => json_ok(&envelope(&value, ctx.user_id.as_str())),
        Err(error) => error_response(400, &format!("retention backfill failed: {error}"), ctx),
    }
}

async fn execute_retention_partitions(
    body: &RetentionBody,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    match host
        .db
        .db_ops()
        .schemas()
        .schema_retention_hash_partitions_page(
            &body.schema,
            body.cursor.as_deref(),
            body.limit.unwrap_or(256),
        )
        .await
    {
        Ok((partitions, next_cursor)) => json_ok(&envelope(
            &serde_json::json!({
                "schema": body.schema,
                "observed_hash_partitions": partitions,
                "next_cursor": next_cursor,
            }),
            ctx.user_id.as_str(),
        )),
        Err(error) => error_response(400, &format!("schema retention failed: {error}"), ctx),
    }
}
