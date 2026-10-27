//! Declaration entry points. Decode, normalize, and select check or sync.

use super::*;

/// `POST /api/apps/declare-schema` — resolve an app-owned schema proposal to
/// the global catalog and load the reused catalog identity locally.
///
/// Request: `{ "app_id": "<namespace>", "schema": <DeclarativeSchemaDefinition>,
/// "intent"?: "catalog_sync" | "check" }`
/// Response envelope: `{ app_id, schema, canonical, resolution: "reuse", adapter }`
///
/// `intent` defaults to `catalog_sync` (described below). `check` is the
/// read-only plan: see [`execute_catalog_schema_check`].
///
/// This is the one catalog registration/bind/load route for app schemas.
/// Single-schema **reuse** and multi-schema **compose** (embedding-beam
/// UseComponents / candidate_equivalent with ≥2 catalog hashes) both bind after
/// loading catalog identities. Novel and under-covered proposals register once
/// with Schema Service, anchored to any same-name predecessor's mapper sources,
/// then load the exact returned identity. If live resolution is unavailable,
/// the already-loaded catalog predecessor supplies the same expansion anchor
/// before Schema Service registration. No local identity mint exists.
///
/// Served on the owner data socket AND the full-surface setup socket so Mini
/// first-run init can load exact catalog identities without storing a local
/// schema dialect.
pub async fn execute_apps_declare_schema_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let facts = DeclareRequestFacts::from_body(&req.body);
    let body: AppsDeclareRequest = match serde_json::from_slice(&req.body) {
        Ok(b) => b,
        Err(e) => {
            return reject_schema_sync_request(
                req,
                &facts,
                ctx,
                host,
                "apps_declare_schema",
                400,
                &format!("invalid /api/apps/declare-schema body: {e}"),
            )
        }
    };
    let retention = match prepare_declare_retention(ctx, &facts, body.retention) {
        Ok(retention) => retention,
        Err((status, e)) => {
            return reject_schema_sync_request(
                req,
                &facts,
                ctx,
                host,
                "apps_declare_schema",
                status,
                &e,
            )
        }
    };
    let mut declare = match normalize_direct_declare_proposal("app_id", &body.app_id, body.schema) {
        Ok(declare) => declare,
        Err(e) => {
            return reject_schema_sync_request(
                req,
                &facts,
                ctx,
                host,
                "apps_declare_schema",
                400,
                &e,
            )
        }
    };
    declare.retention = retention;
    match body.intent {
        SchemaSyncIntent::CatalogSync => {
            execute_catalog_schema_sync(req, ctx, host, "apps_declare_schema", declare).await
        }
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
            return reject_schema_sync_request(
                req,
                &DeclareRequestFacts::from_body(&req.body),
                ctx,
                host,
                "schemas_declare_compat",
                400,
                &format!("invalid /api/schemas/declare body: {e}"),
            )
        }
    };
    let declare = match normalize_direct_declare_proposal("namespace", &body.namespace, body.schema)
    {
        Ok(declare) => declare,
        Err(e) => {
            return reject_schema_sync_request(
                req,
                &DeclareRequestFacts::from_body(&req.body),
                ctx,
                host,
                "schemas_declare_compat",
                400,
                &e,
            )
        }
    };
    match body.intent {
        SchemaSyncIntent::CatalogSync => {
            execute_catalog_schema_sync(req, ctx, host, "schemas_declare_compat", declare).await
        }
        SchemaSyncIntent::Check => execute_catalog_schema_check(ctx, host, declare).await,
    }
}
