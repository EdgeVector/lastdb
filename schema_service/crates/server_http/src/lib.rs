//! schema_service_server_http
//!
//! Actix wrapper around the shared handlers. Owns the route table
//! (mounted under `/v1/*`) and the dev binary entrypoint.

mod cors;
pub mod middleware;

use std::sync::Arc;

use actix_cors::Cors;
use actix_web::{web, App, HttpServer as ActixHttpServer};
use tracing_actix_web::TracingLogger;

use schema_types::{FoldDbError, FoldDbResult};

use schema_service_client::SchemaServiceClient;
use schema_service_core::snapshot::SnapshotImportReport;
use schema_service_core::state::SchemaServiceState;
use schema_service_server_shared::{handlers, Embedder};

use crate::middleware::otel::W3CParentContext;

/// Default port for the dev binary. Matches the existing
/// `fold_db_node` schema service default so local tooling that
/// auto-slots ports does not need updates during Phase 0.
pub const DEFAULT_DEV_SCHEMA_PORT: u16 = 9102;

/// Configure the actix `/v1/*` route table on an existing `App`.
///
/// Pulled out so integration tests can spin up an in-process server
/// without re-declaring routes and risking drift.
pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    let v1 = web::scope("/v1").route("/health", web::get().to(handlers::health_check));
    let v1 = v1_schema_routes(v1);
    let v1 = v1_app_routes(v1);
    let v1 = v1_field_and_registry_routes(v1);
    let v1 = v1_snapshot_and_admin_routes(v1);
    cfg.service(v1);
    // `/v2` — the Exemem app release registry. The eight-route registry
    // surface is `POST /v1/dev-cert` (exemem auth service) plus the seven
    // routes in this scope.
    cfg.service(v2_scope());
}

/// `/v1` schema CRUD, reuse, resolve and matching routes.
fn v1_schema_routes(scope: actix_web::Scope) -> actix_web::Scope {
    scope
        .service(
            web::resource("/schemas")
                .route(web::get().to(handlers::list_schemas))
                .route(web::post().to(handlers::add_schema)),
        )
        .route(
            "/schemas/mutation-challenge",
            web::post().to(handlers::issue_schema_mutation_challenge),
        )
        .route(
            "/schemas/batch-check-reuse",
            web::post().to(handlers::batch_check_reuse),
        )
        .route(
            "/schemas/resolve",
            web::post().to(handlers::resolve_schemas),
        )
        .route(
            "/debug/field-match-probe",
            web::post().to(handlers::field_match_probe),
        )
        .route("/schemas/reload", web::post().to(handlers::reload_schemas))
        .route(
            "/schemas/available",
            web::get().to(handlers::get_available_schemas),
        )
        .route(
            "/schemas/similar/{name}",
            web::get().to(handlers::find_similar),
        )
        .route("/schema/{name}", web::get().to(handlers::get_schema))
        // Phase C: shadow-mode canonicalization audit log.
        .route(
            "/canonicalization-near-misses",
            web::get().to(handlers::list_near_misses),
        )
}

/// `/v1` app registry routes (app_identity v3.1, Lane B2b). Register an app
/// namespace (cert-gated), and browse the promoted public shelf (no developer
/// credential required).
fn v1_app_routes(scope: actix_web::Scope) -> actix_web::Scope {
    scope
        .service(
            web::resource("/apps")
                .route(web::get().to(handlers::list_apps))
                .route(web::post().to(handlers::register_app)),
        )
        // Owner-authenticated sandbox→live promotion. Registered BEFORE
        // the `/apps/{app_id}` dynamic resource so the literal
        // `…/promote` path matches first — otherwise actix would capture
        // `app_id="…"` including the `/promote` suffix.
        .route(
            "/apps/{app_id}/promote",
            web::post().to(handlers::promote_app),
        )
        // Public, lookup-by-known-id app registry read (GET) +
        // owner-authenticated metadata update (PUT). The GET is what
        // nodes use to populate their local app registry without
        // holding a developer `em_` bearer; PUT remains cert-gated.
        .service(
            web::resource("/apps/{app_id}")
                .route(web::get().to(handlers::get_app))
                .route(web::put().to(handlers::update_app)),
        )
}

/// `/v1` declared-field and registry index routes.
fn v1_field_and_registry_routes(scope: actix_web::Scope) -> actix_web::Scope {
    scope
        // Declared fields (brain `design-lastdb-declared-fields`).
        // Declaring is ALWAYS DevCert-gated — it claims a slot other
        // schemas' writes fold into. Reading a declaration by id is not:
        // the id is not a secret, and a node needs to resolve one it
        // already holds. Registered BEFORE the dynamic `/fields/{id}`
        // resource so the literal `/fields/declare` path matches first.
        .route("/fields/declare", web::post().to(handlers::declare_field))
        .route(
            "/fields/{declaration_id}",
            web::get().to(handlers::get_declared_field),
        )
        .route("/registry/index", web::get().to(handlers::registry_index))
}

/// `/v1` snapshot, reset and admin routes.
fn v1_snapshot_and_admin_routes(scope: actix_web::Scope) -> actix_web::Scope {
    scope
        // Snapshot — local-only routes for the `--hydrate-from`
        // dev workflow. The Lambda binary intentionally does NOT
        // mount these (Lambda will get GET `/v1/snapshot` behind
        // an X-API-Key check in a follow-up; the import and reset
        // routes need the in-memory dev backend).
        .route("/snapshot", web::get().to(handlers::snapshot_export))
        .route(
            "/snapshot/shared-only",
            web::get().to(handlers::snapshot_export_shared_only),
        )
        .route(
            "/snapshot/import",
            web::post().to(handlers::snapshot_import),
        )
        .route("/system/reset", web::post().to(handlers::reset_database))
        // Admin — one-shot dev cleanup for the duplicate-descriptive_name
        // pile produced by the pre-409 cross-schema_type fall-through and
        // by concurrent Lambda races. Idempotent.
        .route(
            "/admin/dedupe-descriptive-names",
            web::post().to(handlers::dedupe_descriptive_names),
        )
        .route(
            "/admin/deprecate-schemas",
            web::post().to(handlers::deprecate_schemas),
        )
        .route(
            "/admin/schema-match-telemetry",
            web::get().to(handlers::schema_match_telemetry),
        )
}

/// The `/v2` app release registry scope.
///
/// Literal-path routes are registered before the dynamic
/// `/apps/{app_id}` resource so `…/releases`, `…/revocations`, and
/// `…/channels/{channel}` match first — otherwise actix would capture
/// `app_id` including the suffix (the same ordering rule the `/v1`
/// `…/promote` route documents).
fn v2_scope() -> actix_web::Scope {
    web::scope("/v2")
        // Writes — one DevCert + one signed envelope each.
        .route(
            "/apps/{app_id}/releases",
            web::post().to(handlers::publish_release),
        )
        .route(
            "/apps/{app_id}/revocations",
            web::post().to(handlers::revoke_release),
        )
        .service(
            web::resource("/apps/{app_id}/channels/{channel}")
                .route(web::put().to(handlers::set_channel))
                // Anonymous read of the desired release id + generation.
                .route(web::get().to(handlers::get_channel)),
        )
        .route("/apps", web::post().to(handlers::register_app))
        // Anonymous reads.
        .route(
            "/releases/{release_id}",
            web::get().to(handlers::get_release),
        )
        .route("/apps/{app_id}", web::get().to(handlers::get_app))
}

/// Schema service HTTP server (actix wrapper).
pub struct SchemaServiceServer {
    state: web::Data<SchemaServiceState>,
    bind_address: String,
}

impl SchemaServiceServer {
    /// Construct a server backed by the in-memory registry (nothing
    /// survives a restart). Does NOT seed built-in schemas — see
    /// `new_with_builtins` for the production constructor.
    fn new(bind_address: &str) -> FoldDbResult<Self> {
        let embedder = default_embedder();
        let state = SchemaServiceState::new_in_memory(embedder)?;
        // App-identity verification config (app_identity v3.1, Lane B2b).
        // Empty unless APP_IDENTITY_ROOT_PUBKEYS is set — the dev binary
        // is then a passthrough, matching pre-app-identity behavior.
        state.configure_app_identity(
            schema_service_core::app_identity::AppIdentityConfig::from_env(),
        );
        state.configure_schema_mutation_gate_from_env();
        Ok(Self {
            state: web::Data::new(state),
            bind_address: bind_address.to_string(),
        })
    }

    /// Construct a server, seed Phase 1 SystemSeed fingerprint schemas,
    /// and seed the Schema.org StarterSeed canonical fields + schemas.
    /// Idempotent.
    pub async fn new_with_builtins(bind_address: &str) -> FoldDbResult<Self> {
        let server = Self::new(bind_address)?;
        server.seed_builtins().await?;
        Ok(server)
    }

    /// Seed every pool the service ships with:
    ///   * curated canonical fields (`builtin_canonical_fields`)
    ///   * SystemSeed + app-template schemas (`builtin_schemas`)
    ///
    /// Schema.org types and Schema.org property rows are **not** seeded
    /// (`preference-schema-org-not-live-language`).
    ///
    /// Ordering matters — canonical fields go in before any schema load so
    /// the schema-add path's per-field canonical lookup hits cached entries
    /// instead of the Anthropic classifier.
    async fn seed_builtins(&self) -> FoldDbResult<()> {
        let state = self.state.as_ref();
        schema_service_core::builtin_canonical_fields::seed(state).await?;
        schema_service_core::builtin_schemas::seed(state).await?;
        Ok(())
    }

    /// Pull a snapshot from `base_url` and import it into the in-memory
    /// registry. The dev binary's `--hydrate-from` flow, run once at
    /// startup (the registry then holds only the built-in seeds).
    ///
    /// `api_key` is sent as `X-API-Key` on the upstream snapshot
    /// request to match the schema-infra Lambda's auth convention.
    pub async fn hydrate_from(
        &self,
        base_url: &str,
        api_key: &str,
    ) -> FoldDbResult<SnapshotImportReport> {
        let envelope = SchemaServiceClient::new(base_url)
            .fetch_snapshot(Some(api_key))
            .await
            .map_err(|err| FoldDbError::Other(err.to_string()))?;
        self.state.import_snapshot(envelope)
    }

    pub async fn run(&self) -> FoldDbResult<()> {
        tracing::info!(
            target: "schema_service::http_server",
            "Schema service starting on {}",
            self.bind_address
        );

        let state = self.state.clone();

        let server = ActixHttpServer::new(move || {
            let cors = Cors::default()
                .allowed_origin_fn(|origin, _req_head| {
                    origin.to_str().is_ok_and(cors::is_allowed_origin)
                })
                .allow_any_method()
                .allow_any_header()
                .max_age(3600);

            // Middleware order: the *last* `.wrap` is the OUTERMOST on
            // the request path. To get the on-the-wire order
            // CORS → TracingLogger → W3CParentContext → handler,
            // register them in reverse — the handler-adjacent layer
            // first, CORS last. TracingLogger creates the root span;
            // W3CParentContext, running as its inner, attaches any
            // incoming `traceparent` as that span's parent.
            App::new()
                .wrap(W3CParentContext)
                .wrap(TracingLogger::default())
                .wrap(cors)
                .app_data(state.clone())
                .configure(configure_routes)
        })
        .bind(&self.bind_address)
        .map_err(|e| FoldDbError::Config(format!("Failed to bind schema service: {e}")))?
        .run();

        server
            .await
            .map_err(|e| FoldDbError::Config(format!("Schema service error: {e}")))?;

        Ok(())
    }
}

#[cfg(feature = "fastembed")]
fn default_embedder() -> Arc<dyn Embedder> {
    Arc::new(schema_service_server_shared::FoldDbFastEmbedder::new())
}

#[cfg(not(feature = "fastembed"))]
fn default_embedder() -> Arc<dyn Embedder> {
    Arc::new(schema_service_core::DisabledEmbeddingModel)
}
