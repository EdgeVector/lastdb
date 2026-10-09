//! Owner-socket routes that read molecule keys and change schema retention or HashRange key fields.

use super::*;

/// `POST /api/db/molecule-keys` — list one molecule's live `mk:` keys.
/// Read-only. Body: `{ molecule: string, max_keys?: usize }`.
///
/// An isolated development node can also pass `api_hash` to request a raw,
/// partition-scoped dump with opened atom values. This mode requires
/// `LASTDB_ISOLATED_COPY=1` and `LASTDB_DEV_FIELD_INDEX_DEBUG=1`. The node also
/// verifies that this debug build opened a marked `lastdb-dev` home whose
/// canonical path does not overlap the live primary.
pub(in crate::exec) async fn execute_db_molecule_keys_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct Body {
        molecule: String,
        #[serde(default)]
        max_keys: Option<usize>,
        #[serde(default)]
        api_hash: Option<String>,
    }
    let body: Body = match serde_json::from_slice(&req.body) {
        Ok(b) => b,
        Err(e) => {
            return error_response(
                400,
                &format!("invalid /api/db/molecule-keys body: {e}"),
                ctx,
            )
        }
    };
    if body.molecule.is_empty() {
        return error_response(400, "molecule is required", ctx);
    }
    if let Some(api_hash) = body.api_hash.as_deref() {
        let isolated = std::env::var("LASTDB_ISOLATED_COPY").as_deref() == Ok("1");
        let enabled = std::env::var("LASTDB_DEV_FIELD_INDEX_DEBUG").as_deref() == Ok("1");
        if !isolated || !enabled {
            return error_response(
                403,
                "api_hash debug mode requires LASTDB_ISOLATED_COPY=1 and LASTDB_DEV_FIELD_INDEX_DEBUG=1",
                ctx,
            );
        }
        if let Err(error) =
            crate::ephemeral::verify_dev_field_index_debug_home(&host.home, &host.data_dir)
        {
            return error_response(403, &error, ctx);
        }
        if api_hash.is_empty() {
            return error_response(400, "api_hash must not be empty", ctx);
        }
        let max_keys = body.max_keys.unwrap_or(10_000).min(10_000);
        if max_keys == 0 {
            return error_response(400, "max_keys must be greater than zero", ctx);
        }
        return match host
            .db
            .debug_molecule_hash_bucket(&body.molecule, api_hash, max_keys)
            .await
        {
            Ok(report) => json_ok(&envelope(
                &serde_json::json!({ "molecule_hash_bucket": report }),
                ctx.user_id.as_str(),
            )),
            Err(e) => mapped_error_response("molecule hash bucket debug failed", e, ctx),
        };
    }
    match host
        .db
        .list_molecule_keys(&body.molecule, body.max_keys)
        .await
    {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "molecule_keys": report }),
            ctx.user_id.as_str(),
        )),
        Err(e) => mapped_error_response("molecule-keys failed", e, ctx),
    }
}

/// `POST /api/db/schema-retention` — owner read/set/clear of one installed
/// schema's node-local retention policy. Body: `{ action: "get"|"set"|"clear",
/// schema: string, ttl_seconds?: u64, hash_partitions?: string[] }`.
pub(in crate::exec) async fn execute_db_schema_retention_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct Body {
        action: String,
        schema: String,
        #[serde(default)]
        ttl_seconds: Option<u64>,
        #[serde(default)]
        hash_partitions: Vec<String>,
    }
    if !ctx.is_owner {
        return error_response(403, "schema retention is owner-only", ctx);
    }
    let body: Body = match serde_json::from_slice(&req.body) {
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
    let store = host.db.db_ops().schemas();
    let result = match body.action.as_str() {
        "get" => store.get_schema_retention_policy(&body.schema).await,
        "set" => match body.ttl_seconds {
            Some(ttl_seconds) => match store
                .set_schema_retention_policy(
                    &body.schema,
                    fold_db::schema::SchemaRetentionPolicy {
                        ttl_seconds,
                        hash_partitions: body.hash_partitions.clone(),
                    },
                )
                .await
            {
                Ok(()) => store.get_schema_retention_policy(&body.schema).await,
                Err(error) => Err(error),
            },
            None => return error_response(400, "ttl_seconds is required for set", ctx),
        },
        "clear" => store
            .clear_schema_retention_policy(&body.schema)
            .await
            .map(|()| None),
        _ => return error_response(400, "action must be get, set, or clear", ctx),
    };
    match result {
        Ok(policy) => json_ok(&envelope(
            &serde_json::json!({ "schema": body.schema, "retention_policy": policy }),
            ctx.user_id.as_str(),
        )),
        Err(error) => error_response(400, &format!("schema retention failed: {error}"), ctx),
    }
}

/// `POST /api/db/repair-hashrange-key-fields` — owner-only repair of one
/// named HashRange schema and one API hash partition. Default is dry-run.
pub(in crate::exec) async fn execute_db_repair_hashrange_key_fields_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct Body {
        schema: String,
        api_hash: String,
        #[serde(default)]
        execute: bool,
    }
    if !ctx.is_owner {
        return error_response(403, "HashRange key-field repair is owner-only", ctx);
    }
    let body: Body = match serde_json::from_slice(&req.body) {
        Ok(body) => body,
        Err(error) => {
            return error_response(
                400,
                &format!("invalid /api/db/repair-hashrange-key-fields body: {error}"),
                ctx,
            )
        }
    };
    let canonical = match handlers::resolve_schema_name(host, &body.schema) {
        Ok(Some(canonical)) => canonical,
        Ok(None) => return error_response(404, "schema not found", ctx),
        Err(error) => return render(Err(error), ctx),
    };
    match host
        .db
        .repair_hashrange_key_fields(&canonical, &body.api_hash, body.execute)
        .await
    {
        Ok(report) => json_ok(&envelope(&report, ctx.user_id.as_str())),
        Err(error) => error_response(
            400,
            &format!("HashRange key-field repair failed: {error}"),
            ctx,
        ),
    }
}
