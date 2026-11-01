//! Owner-socket route that reads and changes schema retention.

use super::*;

mod retention_backfill;

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
         `lastdb schema-retention clear --schema {} --confirm-expire-existing`",
        operator.ttl_seconds, restored.ttl_seconds, restored.ttl_seconds, body.schema
    ))
}

/// Write the audit row that precedes an operator `set` or `clear`. `Ok(None)`
/// means the call changes nothing. An `Err` means the row could not be written,
/// so the policy must not change.
async fn open_retention_audit(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
    body: &RetentionBody,
    previous: &Option<fold_db::schema::SchemaRetentionPolicy>,
) -> Result<Option<crate::retention_audit::RetentionAuditEvent>, String> {
    use fold_db::schema::SchemaRetentionPolicy as Policy;
    let expected = match (body.action.as_str(), body.ttl_seconds) {
        ("set", Some(ttl_seconds)) if ttl_seconds > 0 => Some(Policy::operator_over(
            previous.as_ref(),
            ttl_seconds,
            body.hash_partitions.clone(),
        )),
        ("clear", _) => Policy::after_operator_clear(previous.as_ref()),
        _ => return Ok(None),
    };
    if &expected == previous {
        return Ok(None);
    }
    let anchor = host
        .db
        .db_ops()
        .schemas()
        .retention_anchor(&body.schema)
        .await
        .unwrap_or_else(|_| body.schema.clone());
    let event = crate::retention_audit::RetentionAuditEvent::new(
        "db_schema_retention",
        &crate::request_telemetry::client_from_request(req),
        ctx.user_id.as_str(),
        &format!("operator_{}", body.action),
        &body.schema,
        &anchor,
    )
    .with_policies(previous.clone(), expected);
    let mut event = event;
    event.confirmed_expire_existing = body.confirm_expire_existing;
    crate::retention_audit::append(&host.home, &event).map_err(|error| {
        format!("no policy change was made: its audit row could not be written: {error}")
    })?;
    Ok(Some(event))
}

/// `POST /api/db/schema-retention` — owner read/set/clear of one installed
/// schema's node-local retention policy. Body: `{ action: "get"|"set"|"clear",
/// schema: string, ttl_seconds?: u64, hash_partitions?: string[] }`.
///
/// `set` writes an operator policy, which wins over a manifest policy until
/// `clear` removes it. The response names the `anchor` schema that holds the
/// policy for `schema`: an app schema's two installed names share one anchor.
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
        return retention_backfill::execute(req, ctx, host, &body).await;
    }
    let store = host.db.db_ops().schemas();
    // A failed read is an error, never "no policy": the audit row and the
    // confirmation below both depend on what is in force now.
    let previous = match store.get_schema_retention_policy(&body.schema).await {
        Ok(previous) => previous,
        Err(error) => {
            return error_response(400, &format!("schema retention failed: {error}"), ctx)
        }
    };
    let may_expire = match clear_may_expire(&body, &previous) {
        Ok(may_expire) => may_expire,
        Err(message) => return error_response(409, &message, ctx),
    };
    // Every change to a policy leaves a durable row, written before the change.
    let audit = match open_retention_audit(req, ctx, host, &body, &previous).await {
        Ok(audit) => audit,
        Err(message) => return error_response(503, &message, ctx),
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
        // Read back: clearing an operator policy over a manifest policy puts
        // the manifest policy in force.
        "clear" => match store.clear_schema_retention_policy(&body.schema).await {
            Ok(()) => store.get_schema_retention_policy(&body.schema).await,
            Err(error) => Err(error),
        },
        _ => return error_response(400, "action must be get, set, or clear", ctx),
    };
    if let Some(event) = &audit {
        let error = result.as_ref().err().map(ToString::to_string);
        let closed = event.closed(error.as_deref().map_or(Ok(()), Err));
        if let Err(error) = crate::retention_audit::append(&host.home, &closed) {
            return error_response(
                503,
                &format!("the audit row that closes the change failed: {error}"),
                ctx,
            );
        }
    }
    match result {
        Ok(policy) => {
            // The schema that holds the state for both installed names.
            let anchor = store
                .retention_anchor(&body.schema)
                .await
                .unwrap_or_else(|_| body.schema.clone());
            json_ok(&envelope(
                &serde_json::json!({
                    "schema": body.schema,
                    "retention_policy": policy,
                    "anchor": anchor,
                    "existing_records_may_expire": may_expire,
                }),
                ctx.user_id.as_str(),
            ))
        }
        Err(error) => error_response(400, &format!("schema retention failed: {error}"), ctx),
    }
}
