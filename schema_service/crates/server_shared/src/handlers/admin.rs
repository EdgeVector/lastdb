//! Reset, snapshot, dedupe, deprecation and match telemetry routes.

use super::*;

#[allow(
    clippy::unused_async,
    reason = "actix-web route handler — signature must be async to satisfy the Handler trait"
)]
pub async fn reset_database(
    state: web::Data<SchemaServiceState>,
    req: web::Json<ResetRequest>,
) -> impl Responder {
    if !req.confirm {
        return HttpResponse::BadRequest().json(ResetResponse {
            success: false,
            message: "Reset confirmation required. Set 'confirm' to true.".to_string(),
        });
    }

    tracing::info!(
            target: "schema_service::schema",
        "Resetting schema service database"
    );

    {
        let mut schemas = match state.schemas.write() {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(
                target: "schema_service::schema",
                        "Failed to acquire schemas write lock: {}",
                        e
                    );
                return HttpResponse::InternalServerError().json(ResetResponse {
                    success: false,
                    message: "Failed to acquire schemas write lock".to_string(),
                });
            }
        };
        schemas.clear();
    }

    // Local Last Store supports clear; S3/Lambda backends return an error.
    if let Err(e) = state.storage.backend().clear_all_schemas().await {
        return HttpResponse::BadRequest().json(ResetResponse {
            success: false,
            message: format!("reset not supported for this storage backend: {e}"),
        });
    }

    tracing::info!(
            target: "schema_service::schema",
        "Schema service database reset successfully"
    );

    HttpResponse::Ok().json(ResetResponse {
        success: true,
        message: "Schema service database reset successfully. All schemas have been cleared."
            .to_string(),
    })
}

// ============== Snapshot Handlers ==============

/// `GET /v1/snapshot` — capture the registry's current state as a single
/// JSON envelope (`SnapshotEnvelope`).
///
/// Used by the dev binary's `--hydrate-from` flow to seed a local
/// instance with production data on demand. See
/// `projects/schema-service-dev-hydration` for the design.
///
/// Auth is enforced by the framework wrapper (the Lambda gates on
/// `X-API-Key`; the dev binary leaves the route open for localhost
/// loopback). The handler itself is deliberately auth-agnostic so the
/// shared handlers crate stays free of cloud-specific identity wiring.
#[allow(
    clippy::unused_async,
    reason = "actix-web route handler — signature must be async to satisfy the Handler trait"
)]
pub async fn snapshot_export(state: web::Data<SchemaServiceState>) -> impl Responder {
    match state.export_snapshot() {
        Ok(mut envelope) => {
            // Strip embeddings before responding — they balloon the
            // response past Lambda's 6 MB sync-invoke cap (verified in
            // prod 2026-05-05). See `handle_snapshot_export` in
            // `server_lambda` for the full rationale; the dev binary
            // mirrors the behavior so consumers see the same shape
            // across both surfaces.
            envelope.embeddings = SnapshotEmbeddings::default();
            HttpResponse::Ok().json(envelope)
        }
        Err(e) => {
            tracing::error!(
                target: "schema_service::snapshot",
                "Failed to export snapshot: {}",
                e
            );
            HttpResponse::InternalServerError().json(ErrorResponse {
                error: format!("Failed to export snapshot: {e}"),
            })
        }
    }
}

/// `GET /v1/snapshot/shared-only` — shared-surface projection of the registry.
///
/// Same auth/embedding-strip behavior as [`snapshot_export`], but schemas
/// are filtered to the shared-only set used by resolver-pack publishing
/// (system-owned + explicit shared; no private legacy bootstrap).
#[allow(
    clippy::unused_async,
    reason = "actix-web route handler — signature must be async to satisfy the Handler trait"
)]
pub async fn snapshot_export_shared_only(state: web::Data<SchemaServiceState>) -> impl Responder {
    match state.export_shared_only_snapshot() {
        Ok(mut envelope) => {
            envelope.embeddings = SnapshotEmbeddings::default();
            HttpResponse::Ok().json(envelope)
        }
        Err(e) => {
            tracing::error!(
                target: "schema_service::snapshot",
                "Failed to export shared-only snapshot: {}",
                e
            );
            HttpResponse::InternalServerError().json(ErrorResponse {
                error: format!("Failed to export shared-only snapshot: {e}"),
            })
        }
    }
}

/// `POST /v1/snapshot/import` — replace every persisted dimension of the
/// local registry with the contents of the supplied
/// [`SnapshotEnvelope`]. Sled-only (the Lambda binary intentionally
/// does not mount this route).
///
/// Status codes:
///   * 200 — import succeeded; body is a `SnapshotImportReport`.
///   * 400 — envelope rejected (bad `format_version`, embedder
///     mismatch, or non-Sled storage).
///   * 500 — Sled write or in-memory cache update failed mid-way.
#[allow(
    clippy::unused_async,
    reason = "actix-web route handler — signature must be async to satisfy the Handler trait"
)]
pub async fn snapshot_import(
    payload: web::Json<SnapshotEnvelope>,
    state: web::Data<SchemaServiceState>,
) -> impl Responder {
    let envelope = payload.into_inner();
    match state.import_snapshot(envelope) {
        Ok(report) => HttpResponse::Ok().json(report),
        Err(e) => {
            let msg = e.to_string();
            tracing::warn!(
                target: "schema_service::snapshot",
                "Snapshot import rejected: {}",
                msg
            );
            // The errors that come back from `import_snapshot` are all
            // caller-correctable: format_version mismatch, embedder
            // mismatch, or Sled-only-route. Surface them as 400.
            HttpResponse::BadRequest().json(ErrorResponse {
                error: format!("Failed to import snapshot: {msg}"),
            })
        }
    }
}

/// `POST /v1/admin/dedupe-descriptive-names` — one-shot cleanup that
/// resolves duplicate-descriptive_name groups by keeping the largest
/// field-set as the survivor and marking the rest superseded. Idempotent;
/// a clean registry returns `{ "groups": [] }`.
///
/// Mounted by the actix dev binary AND by the Lambda router so the same
/// shape works in dev and (when we get there) prod cleanup.
pub async fn dedupe_descriptive_names(state: web::Data<SchemaServiceState>) -> impl Responder {
    #[derive(serde::Serialize)]
    struct Response {
        groups: Vec<schema_service_core::types::DescriptiveNameDedupeGroup>,
    }
    match state.dedupe_descriptive_names().await {
        Ok(groups) => HttpResponse::Ok().json(Response { groups }),
        Err(e) => {
            tracing::error!(
                target: "schema_service::dedupe",
                "dedupe-descriptive-names failed: {}", e
            );
            HttpResponse::InternalServerError().json(ErrorResponse {
                error: format!("dedupe-descriptive-names failed: {e}"),
            })
        }
    }
}

/// `POST /v1/admin/deprecate-schemas` — mark selected registry entries
/// inactive without deleting immutable schema records. Accepts explicit schema
/// identity hashes and/or descriptive names. Idempotent.
pub async fn deprecate_schemas(
    state: web::Data<SchemaServiceState>,
    request: web::Json<DeprecateSchemasRequest>,
) -> impl Responder {
    match state.deprecate_schemas(request.into_inner()).await {
        Ok(report) => HttpResponse::Ok().json(report),
        Err(e) => {
            tracing::error!(
                target: "schema_service::deprecate",
                "deprecate-schemas failed: {}", e
            );
            HttpResponse::InternalServerError().json(ErrorResponse {
                error: format!("deprecate-schemas failed: {e}"),
            })
        }
    }
}

#[allow(
    clippy::unused_async,
    reason = "actix-web route handler — signature must be async to satisfy the Handler trait"
)]
pub async fn schema_match_telemetry(state: web::Data<SchemaServiceState>) -> impl Responder {
    HttpResponse::Ok().json(state.schema_match_telemetry_snapshot())
}
