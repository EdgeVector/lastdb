//! Database catalog routes: get, put, delete, reclaim and share, with the catalog identity and domain wrap-key parsers.

use super::*;

pub(super) fn catalog_identity_from_query(req: &UdsRequest) -> Result<(String, String), String> {
    let db_locator = query_value(&req.target, "db_locator")
        .ok_or_else(|| "db_locator query param is required".to_string())?;
    let schema_name = query_value(&req.target, "schema_name")
        .ok_or_else(|| "schema_name query param is required".to_string())?;
    canonicalize_catalog_identity(&db_locator, &schema_name)
}

pub(super) fn canonicalize_catalog_identity(
    db_locator: &str,
    schema_name: &str,
) -> Result<(String, String), String> {
    let schema_name = schema_name.trim();
    if schema_name.is_empty() {
        return Err("schema_name must be non-empty".to_string());
    }
    let parsed = parse_db_locator(db_locator)?;
    match parsed {
        DbLocator::Personal => {
            Err("db_catalog requires a named locator (lastdb://org/…), not personal".to_string())
        }
        other => Ok((other.canonical(), schema_name.to_string())),
    }
}

/// `GET /api/db/catalog?db_locator=…&schema_name=…` — point-get one membership.
pub(super) async fn execute_db_catalog_get_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let (db_locator, schema_name) = match catalog_identity_from_query(req) {
        Ok(pair) => pair,
        Err(e) => return error_response(400, &e, ctx),
    };
    match host
        .db
        .db_ops()
        .db_catalog()
        .get(&db_locator, &schema_name)
        .await
    {
        Ok(Some(entry)) => json_ok(&envelope(
            &serde_json::to_value(&entry).unwrap_or_else(|_| serde_json::json!({})),
            ctx.user_id.as_str(),
        )),
        Ok(None) => error_response(404, "database catalog entry not found", ctx),
        Err(e) => mapped_error_response("db catalog get failed", e.into(), ctx),
    }
}

/// `POST /api/db/catalog` — insert or replace one membership row.
pub(super) async fn execute_db_catalog_put_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if crate::ephemeral::is_ephemeral() {
        return error_response(
            403,
            "catalog entry insertion is not permitted on ephemeral nodes; \
             catalog operations are restricted to primary nodes only",
            ctx,
        );
    }

    #[derive(Deserialize)]
    struct Body {
        db_locator: String,
        schema_name: String,
        #[serde(default)]
        instance_id: Option<String>,
        #[serde(default)]
        key_selection: Option<DbCatalogKeySelection>,
    }
    let body: Body = match serde_json::from_slice(&req.body) {
        Ok(b) => b,
        Err(e) => return error_response(400, &format!("invalid /api/db/catalog body: {e}"), ctx),
    };
    let (db_locator, schema_name) =
        match canonicalize_catalog_identity(&body.db_locator, &body.schema_name) {
            Ok(pair) => pair,
            Err(e) => return error_response(400, &e, ctx),
        };
    let instance_id = match body.instance_id {
        Some(id) if id.trim().is_empty() => {
            return error_response(400, "instance_id must be absent or non-empty", ctx)
        }
        Some(id) => Some(id),
        None => match parse_db_locator(&db_locator) {
            Ok(loc) => storage_prefix_for(&loc),
            Err(e) => return error_response(400, &e, ctx),
        },
    };
    let entry = DbCatalogEntry {
        db_locator,
        schema_name,
        instance_id,
        key_selection: body.key_selection.unwrap_or_default(),
    };
    match host.db.db_ops().db_catalog().put(&entry).await {
        Ok(()) => json_ok(&envelope(
            &serde_json::to_value(&entry).unwrap_or_else(|_| serde_json::json!({})),
            ctx.user_id.as_str(),
        )),
        Err(e) => mapped_error_response("db catalog put failed", e.into(), ctx),
    }
}

/// `DELETE /api/db/catalog?db_locator=…&schema_name=…`
pub(super) async fn execute_db_catalog_delete_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if crate::ephemeral::is_ephemeral() {
        return error_response(
            403,
            "catalog entry deletion is not permitted on ephemeral nodes; \
             catalog operations are restricted to primary nodes only",
            ctx,
        );
    }

    let (db_locator, schema_name) = match catalog_identity_from_query(req) {
        Ok(pair) => pair,
        Err(e) => return error_response(400, &e, ctx),
    };
    match host
        .db
        .delete_catalog_membership(&db_locator, &schema_name)
        .await
    {
        Ok(result) => json_ok(&envelope(
            &serde_json::to_value(&result).unwrap_or_else(|_| {
                serde_json::json!({
                    "ok": true,
                    "deleted": result.deleted,
                    "db_locator": db_locator,
                    "schema_name": schema_name,
                    "reclaimed_rows": result.reclaimed_rows,
                })
            }),
            ctx.user_id.as_str(),
        )),
        Err(e) => mapped_error_response("db catalog delete failed", e, ctx),
    }
}

/// `POST /api/db/catalog/reclaim` — drop leftover `{64hex}:` rows not in catalog.
pub(super) async fn execute_db_catalog_reclaim_route(
    _req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if crate::ephemeral::is_ephemeral() {
        return error_response(
            403,
            "catalog reclamation is not permitted on ephemeral nodes; \
             catalog operations are restricted to primary nodes only",
            ctx,
        );
    }

    match host.db.reclaim_unreferenced_prefixed_rows().await {
        Ok(result) => json_ok(&envelope(
            &serde_json::to_value(&result).unwrap_or_else(|_| serde_json::json!({})),
            ctx.user_id.as_str(),
        )),
        Err(e) => mapped_error_response("db catalog reclaim failed", e, ctx),
    }
}

/// `POST /api/db/catalog/share` — zero-copy share into the named target locator.
///
/// Target is `X-LastDB-Db` (or `target_db_locator`). Source defaults to personal.
pub(super) async fn execute_db_catalog_share_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if crate::ephemeral::is_ephemeral() {
        return error_response(
            403,
            "catalog sharing is not permitted on ephemeral nodes; \
             catalog operations are restricted to primary nodes only",
            ctx,
        );
    }

    #[derive(Deserialize)]
    struct Body {
        #[serde(default)]
        source_db_locator: Option<String>,
        #[serde(default)]
        target_db_locator: Option<String>,
        schema_name: String,
        access_domain: String,
        domain_wrap_key_hex: String,
    }
    let body: Body = match serde_json::from_slice(&req.body) {
        Ok(b) => b,
        Err(e) => {
            return error_response(
                400,
                &format!("invalid /api/db/catalog/share body: {e}"),
                ctx,
            )
        }
    };
    let target_raw = body
        .target_db_locator
        .as_deref()
        .or(ctx.db_locator.as_deref())
        .unwrap_or("");
    if target_raw.trim().is_empty() {
        return error_response(400, "share requires X-LastDB-Db or target_db_locator", ctx);
    }
    let source_raw = body
        .source_db_locator
        .as_deref()
        .unwrap_or("lastdb://personal");
    let wrap_key = match parse_domain_wrap_key_hex(&body.domain_wrap_key_hex) {
        Ok(key) => key,
        Err(e) => return error_response(400, &e, ctx),
    };
    if body.access_domain.trim().is_empty() {
        return error_response(400, "access_domain must be non-empty", ctx);
    }
    match host
        .db
        .share_schema(
            source_raw,
            target_raw,
            body.schema_name.trim(),
            body.access_domain.trim(),
            &wrap_key,
        )
        .await
    {
        Ok(result) => json_ok(&envelope(
            &serde_json::to_value(&result).unwrap_or_else(|_| serde_json::json!({})),
            ctx.user_id.as_str(),
        )),
        Err(e) => mapped_error_response("db catalog share failed", e, ctx),
    }
}

pub(super) fn parse_domain_wrap_key_hex(raw: &str) -> Result<[u8; 32], String> {
    let hex = raw.trim();
    let bytes = hex::decode(hex).map_err(|e| format!("domain_wrap_key_hex: {e}"))?;
    bytes
        .try_into()
        .map_err(|_: Vec<u8>| "domain_wrap_key_hex must be 32 bytes (64 hex chars)".to_string())
}
