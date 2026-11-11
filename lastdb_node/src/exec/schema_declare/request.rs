//! Declaration entry points. Decode, normalize, and select check or sync.

use super::*;

/// `POST /api/apps/declare-schema` — obtain canonical definitions and load
/// one schema that owns its values.
pub async fn execute_apps_declare_schema_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let embedded_retention = body_has_embedded_retention(&req.body);
    let body: AppsDeclareRequest = match serde_json::from_slice(&req.body) {
        Ok(b) => b,
        Err(e) => {
            return error_response(
                400,
                &format!("invalid /api/apps/declare-schema body: {e}"),
                ctx,
            )
        }
    };
    let retention = match prepare_declare_retention(ctx, embedded_retention, body.retention) {
        Ok(retention) => retention,
        Err((status, e)) => return error_response(status, &e, ctx),
    };
    let mut declare = match normalize_direct_declare_proposal("app_id", &body.app_id, body.schema) {
        Ok(declare) => declare,
        Err(e) => return error_response(400, &e, ctx),
    };
    declare.retention = retention;
    match body.intent {
        SchemaSyncIntent::CatalogSync => execute_catalog_schema_sync(ctx, host, declare).await,
        SchemaSyncIntent::Check => execute_catalog_schema_check(ctx, host, declare).await,
    }
}

pub(crate) async fn execute_declare_schema_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let body = match serde_json::from_slice::<SchemaDeclareRequest>(&req.body) {
        Ok(body) => body,
        Err(e) => {
            return error_response(400, &format!("invalid /api/schemas/declare body: {e}"), ctx)
        }
    };
    let declare = match normalize_direct_declare_proposal("namespace", &body.namespace, body.schema)
    {
        Ok(declare) => declare,
        Err(e) => return error_response(400, &e, ctx),
    };
    match body.intent {
        SchemaSyncIntent::CatalogSync => execute_catalog_schema_sync(ctx, host, declare).await,
        SchemaSyncIntent::Check => execute_catalog_schema_check(ctx, host, declare).await,
    }
}
