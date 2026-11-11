//! Owner-socket route that reads and changes schema retention.

use super::*;

mod retention_backfill;
mod retention_partitions;

#[derive(Deserialize)]
struct RetentionBody {
    action: String,
    schema: String,
    #[serde(default)]
    ttl_seconds: Option<u64>,
    #[serde(default)]
    hash_partitions: Vec<String>,
    /// Confirms a `clear` that puts a shorter manifest TTL in force.
    #[serde(default)]
    confirm_expire_existing: bool,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

/// Whether clearing `previous` puts a SHORTER manifest TTL in force, so that
/// records between the two TTLs expire. That needs an explicit confirmation,
/// like a manifest TTL that deletes records. `Err` is the refusal.
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

/// `POST /api/db/schema-retention` — owner read/set/clear of one installed
/// schema's node-local retention policy.
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
        return retention_backfill::execute(ctx, host, &body).await;
    }
    let store = host.db.db_ops().schemas();
    let previous = match store.get_schema_retention_policy(&body.schema).await {
        Ok(previous) => previous,
        Err(error) => {
            return error_response(400, &format!("schema retention failed: {error}"), ctx)
        }
    };
    if body.action == "partitions" {
        return retention_partitions::execute(body, ctx, host).await;
    }
    let may_expire = match clear_may_expire(&body, &previous) {
        Ok(may_expire) => may_expire,
        Err(message) => return error_response(409, &message, ctx),
    };
    let result = match body.action.as_str() {
        "get" => store.get_schema_retention_policy(&body.schema).await,
        "set" => match body.ttl_seconds {
            Some(ttl_seconds) => match store
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
            },
            None => return error_response(400, "ttl_seconds is required for set", ctx),
        },
        "clear" => match store.clear_schema_retention_policy(&body.schema).await {
            Ok(()) => store.get_schema_retention_policy(&body.schema).await,
            Err(error) => Err(error),
        },
        _ => return error_response(400, "action must be get, set, or clear", ctx),
    };
    match result {
        Ok(policy) => retention_response(&body.schema, policy, may_expire, ctx),
        Err(error) => error_response(400, &format!("schema retention failed: {error}"), ctx),
    }
}

fn retention_response(
    schema: &str,
    policy: Option<fold_db::schema::SchemaRetentionPolicy>,
    may_expire: bool,
    ctx: &AccessContext,
) -> UdsResponse {
    json_ok(&envelope(
        &serde_json::json!({
            "schema": schema,
            "retention_policy": policy,
            "anchor": schema,
            "existing_records_may_expire": may_expire,
        }),
        ctx.user_id.as_str(),
    ))
}
