use super::*;

// ---------------------------------------------------------------------------
// Setup surface (full socket only)
// ---------------------------------------------------------------------------

/// `POST /api/schemas/load` — fetch the published schema catalog from the
/// schema service and load it (optionally scoped by name) into the node.
///
/// This is a SETUP verb, served only on the full-surface socket
/// (`folddb-full.sock`), never on the narrow data socket — matching the full
/// node, whose clients (fkanban init, fbrain bootstrap) probe for the full
/// socket for first-time schema loads. Same request (`{ schemas: [...] }`,
/// optional) and response (`{ available_schemas_loaded, schemas_loaded_to_db,
/// failed_schemas }` in the envelope) as the full node's route.
///
/// When every requested name looks like a 64-hex identity hash, each schema is
/// fetched via `GET /v1/schema/{hash}` (fast) instead of downloading the full
/// ~1.6MB available catalog. Descriptive-name scopes still use the catalog.
// lint:fn-size-ok moved verbatim from exec.rs; splitting this function is separate work.
pub async fn execute_load_schemas_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Default, Deserialize)]
    struct SchemaLoadRequest {
        #[serde(default)]
        schemas: Vec<String>,
    }

    let request = if req.body.iter().all(u8::is_ascii_whitespace) {
        SchemaLoadRequest::default()
    } else {
        match serde_json::from_slice::<SchemaLoadRequest>(&req.body) {
            Ok(parsed) => parsed,
            Err(e) => {
                return error_response(400, &format!("invalid /api/schemas/load body: {e}"), ctx)
            }
        }
    };

    let url = folddb_profile::endpoints::schema_service_url();
    let client = schema_service_client::SchemaServiceClient::new(&url);

    let looks_like_identity_hash = |s: &str| {
        let t = s.trim();
        t.len() == 64 && t.chars().all(|c| c.is_ascii_hexdigit())
    };

    let (locally_loaded_count, remote_schema_requests) = if request.schemas.is_empty() {
        (0usize, request.schemas.clone())
    } else {
        match partition_locally_loaded_schema_requests(
            host.db.schema_manager().get_schemas().ok().as_ref(),
            &request.schemas,
        ) {
            Some((count, pending)) => (count, pending),
            None => (0usize, request.schemas.clone()),
        }
    };

    let (schemas, mut failed_schemas): (Vec<_>, Vec<String>) = if request.schemas.is_empty() {
        // Full catalog load — heavy; clients should prefer a scoped list.
        match client.get_available_schemas().await {
            Ok(envelopes) => (
                envelopes.into_iter().map(|e| e.schema).collect(),
                Vec::new(),
            ),
            Err(e) => return error_response(503, &e.to_string(), ctx),
        }
    } else if remote_schema_requests.is_empty() {
        (Vec::new(), Vec::new())
    } else if remote_schema_requests
        .iter()
        .all(|s| looks_like_identity_hash(s))
    {
        // Fast path: identity hashes → per-schema GET (avoids the 20s catalog).
        let mut kept = Vec::new();
        let mut missing = Vec::new();
        for name in &remote_schema_requests {
            let key = name.trim();
            match client.get_schema(key).await {
                Ok(envelope) => kept.push(envelope.schema),
                Err(e) => {
                    tracing::warn!("schema service get_schema({key}) failed: {e}");
                    missing.push(format!("{key} (not found)"));
                }
            }
        }
        (kept, missing)
    } else {
        // Descriptive names / mixed scope → catalog + filter.
        let envelopes = match client.get_available_schemas().await {
            Ok(envelopes) => envelopes,
            Err(e) => return error_response(503, &e.to_string(), ctx),
        };
        let kept: Vec<_> = envelopes
            .into_iter()
            .map(|e| e.schema)
            .filter(|s| {
                remote_schema_requests.iter().any(|r| {
                    schema_matches_load_request(
                        &s.name,
                        s.descriptive_name.as_deref(),
                        s.owner_app_id.as_deref(),
                        r,
                    )
                })
            })
            .collect();
        let not_found = remote_schema_requests
            .iter()
            .filter(|r| {
                !kept.iter().any(|s| {
                    schema_matches_load_request(
                        &s.name,
                        s.descriptive_name.as_deref(),
                        s.owner_app_id.as_deref(),
                        r,
                    )
                })
            })
            .map(|r| format!("{r} (not found)"))
            .collect();
        (kept, not_found)
    };

    let available_schemas_loaded = locally_loaded_count + schemas.len();
    let mut schemas_loaded_to_db = locally_loaded_count;
    for schema in schemas {
        let schema_name = schema.name.clone();
        match host
            .db
            .schema_manager()
            .load_schema_internal(schema.into())
            .await
        {
            Ok(_) => schemas_loaded_to_db += 1,
            Err(e) => {
                tracing::error!("Failed to load schema {schema_name}: {e}");
                failed_schemas.push(schema_name);
            }
        }
    }

    let payload = serde_json::json!({
        "available_schemas_loaded": available_schemas_loaded,
        "schemas_loaded_to_db": schemas_loaded_to_db,
        "failed_schemas": failed_schemas,
    });
    json_ok(&envelope(&payload, ctx.user_id.as_str()))
}

/// Optional `/api/schemas/load` name scoping, matching the full node's
/// descriptive-name behavior plus app-owned refs such as
/// `lastsecrets/LastSecret`.
///
/// Accepts either fold_db or schema_types Schema via shared field access
/// (diet cut #3 keeps both types; wire identity is shared).
pub(super) fn schema_matches_load_request(
    name: &str,
    descriptive_name: Option<&str>,
    owner_app_id: Option<&str>,
    req_name: &str,
) -> bool {
    let trimmed = req_name.trim();
    if name == req_name || name == trimmed {
        return true;
    }

    if descriptive_name.is_some_and(|d| d == req_name || d.trim().eq_ignore_ascii_case(trimmed)) {
        return true;
    }

    let Some((owner, local_name)) = trimmed.split_once('/') else {
        return false;
    };
    if owner.is_empty() || local_name.is_empty() {
        return false;
    }
    owner_app_id == Some(owner)
        && (name == local_name
            || descriptive_name
                .is_some_and(|d| d == local_name || d.trim().eq_ignore_ascii_case(local_name)))
}

pub(super) fn partition_locally_loaded_schema_requests(
    schemas: Option<&HashMap<String, Schema>>,
    requests: &[String],
) -> Option<(usize, Vec<String>)> {
    let schemas = schemas?;
    let mut seen_loaded = HashSet::new();
    let mut pending = Vec::new();

    for req in requests {
        // Identity-scoped load is also the catalog refresh operation. Never
        // short-circuit it merely because a same-hash definition is present:
        // an older/local definition may lack catalog field_mappers. Fetching
        // and loading the exact identity is idempotent and repairs that drift.
        let trimmed = req.trim();
        if trimmed.len() == 64 && trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
            pending.push(req.clone());
            continue;
        }
        if let Some(schema_name) = schemas
            .values()
            .find(|schema| {
                schema_matches_load_request(
                    &schema.name,
                    schema.descriptive_name.as_deref(),
                    schema.owner_app_id.as_deref(),
                    req,
                )
            })
            .map(|schema| schema.name.clone())
        {
            seen_loaded.insert(schema_name);
        } else {
            pending.push(req.clone());
        }
    }

    Some((seen_loaded.len(), pending))
}

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
    let body: AppsDeclareRequest = match serde_json::from_slice(&req.body) {
        Ok(b) => b,
        Err(e) => {
            return reject_schema_sync_request(
                req,
                ctx,
                host,
                "apps_declare_schema",
                400,
                &format!("invalid /api/apps/declare-schema body: {e}"),
            )
        }
    };
    let declare = match normalize_direct_declare_proposal("app_id", &body.app_id, body.schema) {
        Ok(declare) => declare,
        Err(e) => {
            return reject_schema_sync_request(req, ctx, host, "apps_declare_schema", 400, &e)
        }
    };
    match body.intent {
        SchemaSyncIntent::CatalogSync => {
            execute_catalog_schema_sync(req, ctx, host, "apps_declare_schema", declare).await
        }
        SchemaSyncIntent::Check => execute_catalog_schema_check(ctx, host, declare).await,
    }
}

/// `POST /api/apps/shared-surface/publish-attach` — explicit shared publish
/// or attach via the local-first facade.
///
/// Mode/pack wiring is install-owned (`{home}/schema_resolver.json` + env).
/// Defaults to LiveOnly when pack is not configured; Shadow/Enforce currently
/// clamp to LiveOnly because Mini no longer ships an in-process embedder.
/// Private declare routes never call this path.
pub async fn execute_apps_shared_surface_publish_attach_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    use schema_service_client::{validate_shared_surface_request_pub, SharedSurfacePublishRequest};

    let body: SharedSurfacePublishRequest = match serde_json::from_slice(&req.body) {
        Ok(b) => b,
        Err(e) => {
            return error_response(
                400,
                &format!("invalid /api/apps/shared-surface/publish-attach body: {e}"),
                ctx,
            )
        }
    };

    if let Err(e) = crate::shared_surface::validate_publish_body(
        &body.request.local_schema_id,
        &body.descriptive_name,
        &body.fields,
    ) {
        return error_response(400, &e, ctx);
    }
    if let Err(e) = validate_shared_surface_request_pub(&body.request) {
        return error_response(400, &e, ctx);
    }

    let url = folddb_profile::endpoints::schema_service_url();
    let result =
        match crate::schema_resolver_host::publish_attach_with_host_config(&host.home, &url, body)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                return error_response(
                    502,
                    &format!("shared-surface publish/attach failed: {e}"),
                    ctx,
                )
            }
        };

    // Persist attachment locally for audit (atomic write under node home).
    let path = crate::shared_surface::SharedSurfaceAttachmentStore::path_for_home(&host.home);
    let mut store = match crate::shared_surface::SharedSurfaceAttachmentStore::load(&path) {
        Ok(s) => s,
        Err(e) => {
            return error_response(
                500,
                &format!("failed to load shared-surface attachments: {e}"),
                ctx,
            )
        }
    };
    store.upsert(result.attachment.clone());
    if let Err(e) = store.save_atomic(&path) {
        return error_response(
            500,
            &format!("failed to persist shared-surface attachment: {e}"),
            ctx,
        );
    }

    json_ok(&envelope(
        &serde_json::to_value(&result).unwrap_or_default(),
        ctx.user_id.as_str(),
    ))
}

/// `GET /api/apps/shared-surface/attachments` — list local attachment records.
#[allow(
    clippy::unused_async,
    reason = "route handlers share the async UdsResponse signature with sibling routes"
)]
pub async fn execute_apps_shared_surface_attachments_route(
    _req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let path = crate::shared_surface::SharedSurfaceAttachmentStore::path_for_home(&host.home);
    let store = match crate::shared_surface::SharedSurfaceAttachmentStore::load(&path) {
        Ok(s) => s,
        Err(e) => {
            return error_response(
                500,
                &format!("failed to load shared-surface attachments: {e}"),
                ctx,
            )
        }
    };
    let payload = serde_json::json!({ "attachments": store.list() });
    json_ok(&envelope(&payload, ctx.user_id.as_str()))
}

/// `POST /api/apps/verify-distribution-ready` — confirm every required schema
/// identity exists on Schema Service before packaging/sharing an app.
pub async fn execute_apps_verify_distribution_ready_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    _host: &Host,
) -> UdsResponse {
    use crate::distribution::{
        distribution_ready, DistributionReadyItem, DistributionReadyStatus,
        VerifyDistributionReadyRequest, VerifyDistributionReadyResponse,
    };

    let body: VerifyDistributionReadyRequest = match serde_json::from_slice(&req.body) {
        Ok(b) => b,
        Err(e) => {
            return error_response(
                400,
                &format!("invalid /api/apps/verify-distribution-ready body: {e}"),
                ctx,
            )
        }
    };

    let app_id = body.app_id.trim().to_string();
    if app_id.is_empty() {
        return error_response(400, "app_id must be non-empty", ctx);
    }
    if body.schema_identities.is_empty() {
        return error_response(400, "schema_identities must be a non-empty array", ctx);
    }

    let url = folddb_profile::endpoints::schema_service_url();
    let client = schema_service_client::SchemaServiceClient::new(&url);
    let mut items = Vec::with_capacity(body.schema_identities.len());

    for identity in body.schema_identities {
        let id = identity.trim().to_string();
        if id.is_empty() {
            items.push(DistributionReadyItem {
                identity,
                status: DistributionReadyStatus::Error,
                error: Some("empty identity".into()),
            });
            continue;
        }
        match client.get_schema(&id).await {
            Ok(_) => items.push(DistributionReadyItem {
                identity: id,
                status: DistributionReadyStatus::Present,
                error: None,
            }),
            Err(e) => {
                let msg = e.to_string();
                let missing = msg.contains("404") || msg.to_ascii_lowercase().contains("not found");
                items.push(DistributionReadyItem {
                    identity: id,
                    status: if missing {
                        DistributionReadyStatus::Missing
                    } else {
                        DistributionReadyStatus::Error
                    },
                    error: Some(msg),
                });
            }
        }
    }

    let ready = distribution_ready(&items);
    let payload = VerifyDistributionReadyResponse {
        app_id,
        items,
        ready,
    };
    json_ok(&envelope(
        &serde_json::to_value(payload).unwrap_or_default(),
        ctx.user_id.as_str(),
    ))
}
// lint:file-size-ok moved verbatim from exec.rs; cohesive unit, split further in a later pass
