//! Method + path routing. Stateless routes (health, root) are answered
//! without the cold-start state; every other route is matched in a themed
//! sub-dispatcher that calls one handler under `routes/`.
//!
//! The themed path prefixes are disjoint, so the order of the sub-dispatchers
//! does not matter; within one sub-dispatcher the arms are ordered most
//! specific first.

use crate::http::json_response;
use crate::routes;
use crate::snapshot::handle_snapshot_export;
use lambda_http::{Body, Error, Request, Response};
use schema_service_server_shared::state::SchemaServiceState;
use serde_json::json;

type RouteResult = Option<Result<Response<Body>, Error>>;

/// Dispatch a request without touching the cold-start state.
pub(crate) fn dispatch_stateless(method: &str, path: &str) -> RouteResult {
    match (method, path) {
        // Health check — versioned and unversioned. The unversioned
        // /health path is retained intentionally: API Gateway health
        // probes and the schema-infra CDK cutover (PR 4/5) both still
        // address `/health` during the deploy-transition window.
        ("GET", "/health" | "/v1/health") => Some(json_response(
            200,
            &json!({
                "status": "healthy",
                "service": "schema-service"
            }),
        )),

        // Root endpoint — lists the supported routes. Kept here (rather
        // than under /v1/) so operators probing the Lambda's API
        // Gateway root see the service identity without needing to
        // know the version prefix.
        ("GET" | "POST", "/") => Some(json_response(200, &root_listing())),
        _ => None,
    }
}

fn root_listing() -> serde_json::Value {
    json!({
        "service": "FoldDB Schema & View Registry",
        "version": "2.0.0",
        "endpoints": {
            "GET /health": "Health check (unversioned alias)",
            "GET /v1/health": "Health check",
            "GET /v1/schemas": "List schema names",
            "GET /v1/registry/index": "Signed compact registry index for local dedup caches",
            "GET /v1/schemas/available": "Get all schemas with definitions",
            "GET /v1/schemas/similar/{name}?threshold=0.5": "Find similar schemas",
            "GET /v1/schema/{name}": "Get specific schema",
            "POST /v1/schemas": "Add new schema",
            "POST /v1/schemas/mutation-challenge": "Issue a short-lived node-key proof-of-work challenge for shared schema mutation",
            "POST /v1/apps": "Register an app namespace as sandbox (dev cert + signature)",
            "GET /v1/apps": "Browse promoted, non-revoked apps (public shelf)",
            "GET /v1/apps/{app_id}": "Read a registered app incl. tier (public; lookup-by-known-id)",
            "PUT /v1/apps/{app_id}": "Update app metadata (owner-only; display_name immutable)",
            "POST /v1/apps/{app_id}/promote": "Promote a sandbox app to live (owner-only; authorized_publisher required)",
            "POST /v1/schemas/batch-check-reuse": "Batch lookup of reusable schemas by descriptive name + field list",
            "POST /v1/schemas/resolve": "Stateless read-only schema dedup for cache misses",
            "POST /v1/schemas/reload": "Reload schemas from storage",
            "GET /v1/canonicalization-near-misses": "Shadow-mode audit log of single-signal vs dual-signal canonicalization disagreements",
            "GET /v1/snapshot": "Export the registry as a single JSON envelope (requires X-API-Key)",
            "GET /v1/snapshot/shared-only": "Export shared-only projection for resolver packs (requires X-API-Key)"
        }
    })
}

/// Dispatch a request against an already-initialized state.
pub(crate) async fn dispatch_with_state(
    state: &SchemaServiceState,
    event: Request,
) -> Result<Response<Body>, Error> {
    // Copy the method/path before we borrow the event for body + query, so
    // the sub-dispatchers can still borrow `event` without fighting the
    // borrow checker.
    let method = event.method().as_str().to_owned();
    let path = event.uri().path().to_owned();
    let (m, p) = (method.as_str(), path.as_str());

    if let Some(resp) = dispatch_schemas(state, &event, m, p).await {
        return resp;
    }
    if let Some(resp) = dispatch_apps(state, &event, m, p).await {
        return resp;
    }
    if let Some(resp) = dispatch_fields(state, &event, m, p).await {
        return resp;
    }
    if let Some(resp) = dispatch_admin(state, &event, m, p).await {
        return resp;
    }

    json_response(
        404,
        &json!({"error": format!("Not found: {method} {path}")}),
    )
}

/// `/v1/schemas*`, `/v1/schema/*`, and the registry index.
async fn dispatch_schemas(
    state: &SchemaServiceState,
    event: &Request,
    m: &str,
    p: &str,
) -> RouteResult {
    Some(match (m, p) {
        ("GET", "/v1/schemas") => match state.get_schema_names() {
            Ok(schema_names) => json_response(200, &json!({ "schemas": schema_names })),
            Err(e) => json_response(
                500,
                &json!({"error": format!("Failed to get schema names: {e}")}),
            ),
        },
        ("GET", "/v1/registry/index") => routes::schemas::get_registry_index(state, event),
        ("GET", "/v1/schemas/available") => routes::schemas::get_schemas_available(state, event),
        ("GET", _) if p.starts_with("/v1/schemas/similar/") => {
            routes::schemas::get_schemas_similar(state, event, p)
        }
        ("GET", _) if p.starts_with("/v1/schema/") || p.starts_with("/v1/schemas/") => {
            routes::schemas::get_schema(state, p)
        }
        ("POST", "/v1/schemas/mutation-challenge") => {
            routes::schemas::post_schemas_mutation_challenge(state, event)
        }
        ("POST", "/v1/schemas") => routes::schemas::post_schemas(state, event).await,
        ("POST", "/v1/schemas/batch-check-reuse") => {
            routes::schemas::post_schemas_batch_check_reuse(state, event)
        }
        ("POST", "/v1/schemas/resolve") => routes::schemas::post_schemas_resolve(state, event),
        ("POST", "/v1/schemas/reload") => reload_schemas(state).await,
        _ => return None,
    })
}

async fn reload_schemas(state: &SchemaServiceState) -> Result<Response<Body>, Error> {
    match state.load_schemas().await {
        Ok(_) => {
            let count = state.get_schema_count();
            json_response(
                200,
                &json!({
                    "ok": true,
                    "count": count,
                    "message": format!("Reloaded {count} schemas")
                }),
            )
        }
        Err(e) => json_response(500, &json!({"error": format!("Failed to reload: {e}")})),
    }
}

/// `/v1/apps*` and `/v2/apps*`, `/v2/releases/*`.
async fn dispatch_apps(
    state: &SchemaServiceState,
    event: &Request,
    m: &str,
    p: &str,
) -> RouteResult {
    Some(match (m, p) {
        ("POST", "/v1/apps") => routes::apps::post_app_v1(state, event).await,
        // Public promoted-app shelf. This mirrors the actix route's
        // shared handler behavior: anonymous browsing is allowed, but
        // only Live, non-revoked apps appear.
        ("GET", "/v1/apps") => json_response(200, &json!(state.list_live_apps())),
        ("GET", _) if p.starts_with("/v1/apps/") => routes::apps::get_app_v1(state, p),
        ("PUT", _) if p.starts_with("/v1/apps/") => routes::apps::put_app_v1(state, event, p).await,
        ("POST", _) if p.starts_with("/v1/apps/") && p.ends_with("/promote") => {
            routes::apps::promote_app_v1(state, event, p).await
        }
        ("POST", "/v2/apps") => routes::apps::post_apps_v2(state, event).await,
        ("POST", _) if p.starts_with("/v2/apps/") && p.ends_with("/releases") => {
            routes::apps::post_apps_releases(state, event, p).await
        }
        ("POST", _) if p.starts_with("/v2/apps/") && p.ends_with("/revocations") => {
            routes::apps::post_apps_revocations(state, event, p).await
        }
        ("PUT", _) if p.starts_with("/v2/apps/") && p.contains("/channels/") => {
            routes::apps::put_apps_channels(state, event, p).await
        }
        ("GET", _) if p.starts_with("/v2/apps/") && p.contains("/channels/") => {
            routes::apps::get_apps_channels(state, p)
        }
        ("GET", _) if p.starts_with("/v2/releases/") => routes::apps::get_releases(state, p),
        ("GET", _) if p.starts_with("/v2/apps/") => routes::apps::get_app_v2(state, p),
        _ => return None,
    })
}

/// `/v1/fields/*`.
async fn dispatch_fields(
    state: &SchemaServiceState,
    event: &Request,
    m: &str,
    p: &str,
) -> RouteResult {
    Some(match (m, p) {
        ("POST", "/v1/fields/declare") => routes::fields::post_fields_declare(state, event).await,
        ("GET", _) if p.starts_with("/v1/fields/") => routes::fields::get_fields(state, p),
        _ => return None,
    })
}

/// Admin, debug, snapshot, and near-miss audit routes.
async fn dispatch_admin(
    state: &SchemaServiceState,
    event: &Request,
    m: &str,
    p: &str,
) -> RouteResult {
    Some(match (m, p) {
        ("POST", "/v1/debug/field-match-probe") => {
            routes::admin::post_debug_field_match_probe(state, event)
        }
        ("POST", "/v1/admin/warm-embeddings") => {
            routes::admin::post_admin_warm_embeddings(state).await
        }
        ("POST", "/v1/admin/dedupe-descriptive-names") => {
            routes::admin::post_admin_dedupe_descriptive_names(state).await
        }
        ("POST", "/v1/admin/deprecate-schemas") => {
            routes::admin::post_admin_deprecate_schemas(state, event).await
        }
        ("GET", "/v1/admin/schema-match-telemetry") => {
            routes::admin::get_admin_schema_match_telemetry(state)
        }
        // `GET /v1/snapshot` is auth-gated by X-API-Key (validated
        // against the exemem ApiKeys DynamoDB table). The dev-only
        // `POST /v1/snapshot/import` route is intentionally NOT
        // mounted here — Lambda storage is `External` (S3-backed) and
        // the importer is Sled-only.
        ("GET", "/v1/snapshot") => handle_snapshot_export(event, state).await,
        ("GET", "/v1/snapshot/shared-only") => {
            routes::admin::get_snapshot_shared_only(state, event).await
        }
        ("GET", "/v1/canonicalization-near-misses") => {
            routes::admin::get_canonicalization_near_misses(state, event)
        }
        _ => return None,
    })
}
