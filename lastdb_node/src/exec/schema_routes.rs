use super::*;

// lint:fn-size-ok moved verbatim from exec.rs; splitting this function is separate work.
pub(super) async fn execute_list_schemas_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let include_system = query_flag(&req.target, "include_system");
    // Default is identity/summary-only: per-schema live record counts walk the
    // store (collect_matches with limit MAX), and full schema rows carry large
    // per-field maps that list callers do not use. Opt in to each cost with
    // ?include_counts=true and ?include_full=true.
    let include_counts = query_flag(&req.target, "include_counts");
    let include_full = query_flag(&req.target, "include_full");
    // App reads are unrestricted once the app is approved
    // (decision-2026-07-10-apps-read-all-write-own-namespace): the full catalog
    // is visible to every consented caller — no per-principal filter.

    // Explicit not-ready: never pretend an empty catalog is a finished boot when
    // the in-memory schema map has not finished its initial load. Empty homes
    // that truly have zero schemas still report ready=true after boot.
    if !host.db.schema_manager().catalog_ready() {
        return error_response(
            503,
            "schema catalog not ready: still hydrating from store",
            ctx,
        );
    }

    // Catalog rows stay FLAT: clients read `name`/`descriptive_name`/
    // `owner_app_id`/`fields` at the top level. The default projection omits
    // heavy per-field metadata; `GET /api/schema/{name}` and `include_full`
    // retain the full schema shape for diagnostics and migration tooling.
    let annotated = if include_full {
        let Ok(mut schemas) = host.db.schema_manager().get_active_schemas_with_states() else {
            return content_free(500, "Internal Server Error");
        };
        if !include_system {
            schemas.retain(|s| s.schema.source != SchemaSource::SystemSeed);
        }
        let mut annotated = Vec::with_capacity(schemas.len());
        for schema in schemas {
            let schema_name = schema.schema.name.clone();
            let Ok(Value::Object(mut row)) = serde_json::to_value(&schema) else {
                return content_free(500, "Internal Server Error");
            };
            if include_counts {
                let record_count = count_schema_records(host, &schema_name).await;
                row.insert("record_count".into(), record_count.into());
                row.insert("has_data".into(), (record_count > 0).into());
            } else {
                row.insert("record_count".into(), Value::Null);
                row.insert("has_data".into(), Value::Null);
            }
            annotated.push(Value::Object(row));
        }
        annotated
    } else {
        let Ok(mut schemas) = host
            .db
            .schema_manager()
            .get_active_schema_list_entries_with_states()
        else {
            return content_free(500, "Internal Server Error");
        };
        if !include_system {
            schemas.retain(|s| s.source != SchemaSource::SystemSeed);
        }
        let mut annotated = Vec::with_capacity(schemas.len());
        for schema in schemas {
            let schema_name = schema.name.clone();
            let Ok(Value::Object(mut row)) = serde_json::to_value(&schema) else {
                return content_free(500, "Internal Server Error");
            };
            if include_counts {
                let record_count = count_schema_records(host, &schema_name).await;
                row.insert("record_count".into(), record_count.into());
                row.insert("has_data".into(), (record_count > 0).into());
            } else {
                // Omit expensive counts; clients that need them opt in.
                row.insert("record_count".into(), Value::Null);
                row.insert("has_data".into(), Value::Null);
            }
            annotated.push(Value::Object(row));
        }
        annotated
    };

    // Which listed schemas no longer answer `descriptive_name` resolution.
    // Stamped per row AND summarized, so an operator can see at a glance that a
    // readable name now addresses exactly one Available schema.
    let retired_name_claims = host
        .db
        .schema_manager()
        .retired_name_claims()
        .unwrap_or_default();
    let mut annotated = annotated;
    for row in &mut annotated {
        let Some(row) = row.as_object_mut() else {
            continue;
        };
        let retired = row
            .get("name")
            .and_then(Value::as_str)
            .is_some_and(|name| retired_name_claims.contains(name));
        row.insert("name_claim_retired".into(), retired.into());
    }
    let mut retired_name_claims: Vec<String> = retired_name_claims.into_iter().collect();
    retired_name_claims.sort();

    let count = annotated.len();
    // Identity-algorithm provenance. `identity_algo_version` is what THIS
    // binary implements; `newer_identity_algo_schemas` are rows whose identity
    // was minted by something newer, which `load_schema_internal` preserved
    // rather than downgrading. A non-empty list means the node is older than
    // its own data — upgrade the node, do not re-register the schemas.
    let newer_identity_algo_schemas = host
        .db
        .schema_manager()
        .schemas_with_newer_identity_algo()
        .unwrap_or_default();
    let payload = serde_json::json!({
        "schemas": annotated,
        "count": count,
        "ready": true,
        "counts_included": include_counts,
        "full_included": include_full,
        "identity_algo_version": schema_types::IDENTITY_HASH_ALGO_VERSION,
        "newer_identity_algo_count": newer_identity_algo_schemas.len(),
        "newer_identity_algo_schemas": newer_identity_algo_schemas,
        "retired_name_claim_count": retired_name_claims.len(),
        "retired_name_claims": retired_name_claims,
    });
    json_ok(&envelope(&payload, ctx.user_id.as_str()))
}

pub(super) fn execute_get_schema_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let Some(name) = schema_name_from_target(&req.target) else {
        return content_free(400, "Bad Request");
    };

    let canonical = match handlers::resolve_schema_name(host, &name) {
        Ok(Some(canonical)) => canonical,
        Ok(None) => return error_response(404, &format!("Schema not found: {name}"), ctx),
        Err(e) => return render(Err(e), ctx),
    };

    let mgr = host.db.schema_manager();
    let schema_with_state = match mgr.get_schema_metadata(&canonical) {
        Ok(Some(schema)) => {
            let state = mgr
                .get_schema_states()
                .ok()
                .and_then(|states| states.get(&canonical).copied())
                .unwrap_or_default();
            SchemaWithState::new(schema, state)
        }
        Ok(None) => return error_response(404, &format!("Schema not found: {name}"), ctx),
        Err(_) => return content_free(500, "Internal Server Error"),
    };

    let payload = serde_json::json!({ "schema": schema_with_state });
    json_ok(&envelope(&payload, ctx.user_id.as_str()))
}

pub(super) async fn execute_declare_schema_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let body = match serde_json::from_slice::<SchemaDeclareRequest>(&req.body) {
        Ok(body) => body,
        Err(e) => {
            return reject_schema_sync_request(
                req,
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
            return reject_schema_sync_request(req, ctx, host, "schemas_declare_compat", 400, &e)
        }
    };
    match body.intent {
        SchemaSyncIntent::CatalogSync => {
            execute_catalog_schema_sync(req, ctx, host, "schemas_declare_compat", declare).await
        }
        SchemaSyncIntent::Check => execute_catalog_schema_check(ctx, host, declare).await,
    }
}

/// `POST /api/schemas/seed-system` — install a system-owned schema only for
/// an explicitly isolated node.
///
/// Catalog sync is intentionally unable to create a `SystemSeed` schema: it
/// resolves application proposals through the shared catalog and gives the
/// resulting binding an application owner. Attribution and copy harnesses need
/// one real system root on a throwaway daemon, however, so this setup route
/// loads the supplied declarative schema without contacting the catalog.
pub(super) async fn execute_seed_system_schema_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Request {
        schema: DeclarativeSchemaDefinition,
        #[serde(default)]
        isolated_copy: bool,
    }

    let request = match serde_json::from_slice::<Request>(&req.body) {
        Ok(request) => request,
        Err(error) => {
            return error_response(
                400,
                &format!("invalid system seed setup request: {error}"),
                ctx,
            )
        }
    };
    if !request.isolated_copy || !is_isolated_copy_process() {
        return error_response(
            409,
            "system seed setup requires isolated_copy=true and LASTDB_ISOLATED_COPY=1",
            ctx,
        );
    }
    if request.schema.source != SchemaSource::SystemSeed {
        return error_response(
            400,
            "system seed setup requires schema.source=system_seed",
            ctx,
        );
    }
    if request
        .schema
        .owner_app_id
        .as_deref()
        .is_some_and(|owner| !owner.trim().is_empty())
    {
        return error_response(
            400,
            "system seed setup requires a schema without owner_app_id",
            ctx,
        );
    }

    let schema_name = request.schema.name.clone();
    let schema = match fold_db::schema::SchemaInterpreter::interpret(request.schema) {
        Ok(schema) => schema,
        Err(error) => return mapped_error_response("system seed setup failed", error.into(), ctx),
    };
    match host.db.schema_manager().load_schema_internal(schema).await {
        Ok(()) => json_ok(&envelope(
            &serde_json::json!({
                "system_seed_schema": schema_name,
                "source": "system_seed",
            }),
            ctx.user_id.as_str(),
        )),
        Err(error) => mapped_error_response("system seed setup failed", error.into(), ctx),
    }
}

pub(super) fn is_isolated_copy_process() -> bool {
    env_flag::var_truthy("LASTDB_ISOLATED_COPY")
}

/// `POST /api/schemas/retire-name-claim` — retire or restore one installed
/// schema's claim on its `descriptive_name`.
///
/// Body: `{ "schema": "<canonical name or identity hash>", "retired": true }`.
/// `retired` defaults to `true`; pass `false` to put the claim back.
///
/// `schema` is resolved as a canonical name / identity hash FIRST, exactly like
/// every other by-hash reader, because the point of this route is to act on a
/// claimant that a `descriptive_name` lookup can no longer disambiguate. Naming
/// it by the ambiguous descriptive name is refused rather than guessed.
pub(super) async fn execute_retire_schema_name_claim_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct RetireNameClaimRequest {
        schema: String,
        #[serde(default = "default_true")]
        retired: bool,
    }
    fn default_true() -> bool {
        true
    }

    let body = match serde_json::from_slice::<RetireNameClaimRequest>(&req.body) {
        Ok(body) => body,
        Err(e) => {
            return error_response(
                400,
                &format!("invalid /api/schemas/retire-name-claim body: {e}"),
                ctx,
            )
        }
    };
    let schema_name = body.schema.trim();
    if schema_name.is_empty() {
        return error_response(400, "schema must be non-empty", ctx);
    }

    let mgr = host.db.schema_manager();
    match mgr.get_schema_metadata(schema_name) {
        Ok(Some(_)) => {}
        Ok(None) => {
            return error_response(
                404,
                &format!(
                    "no schema is installed under '{schema_name}'; name the claimant by its \
                     canonical name or identity hash, not by the descriptive name it shares"
                ),
                ctx,
            )
        }
        Err(_) => return content_free(500, "Internal Server Error"),
    }

    let changed = if body.retired {
        mgr.retire_name_claim(schema_name).await
    } else {
        mgr.restore_name_claim(schema_name).await
    };
    let changed = match changed {
        Ok(changed) => changed,
        Err(e) => return error_response(500, &format!("retire-name-claim failed: {e}"), ctx),
    };

    let payload = serde_json::json!({
        "schema": schema_name,
        "name_claim_retired": body.retired,
        "changed": changed,
    });
    json_ok(&envelope(&payload, ctx.user_id.as_str()))
}

/// `POST /api/schemas/drop` — remove one catalog identity, or every identity
/// owned by one app. ACK is catalog absence. Product rows stay.
pub(super) async fn execute_drop_schema_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct DropSchemaRequest {
        #[serde(default)]
        schema: String,
        #[serde(default)]
        owner_app: String,
        #[serde(default)]
        must_exist: bool,
    }

    let body = match serde_json::from_slice::<DropSchemaRequest>(&req.body) {
        Ok(body) => body,
        Err(e) => return error_response(400, &format!("invalid /api/schemas/drop body: {e}"), ctx),
    };
    let schema = body.schema.trim();
    let owner_app = body.owner_app.trim();
    if schema.is_empty() == owner_app.is_empty() {
        return error_response(400, "pass exactly one of schema or owner_app", ctx);
    }

    let mgr = host.db.schema_manager();
    if !owner_app.is_empty() {
        let dropped = match mgr.drop_schemas_owned_by(owner_app).await {
            Ok(dropped) => dropped,
            Err(e) => return error_response(500, &format!("schema drop failed: {e}"), ctx),
        };
        if body.must_exist && dropped.is_empty() {
            return error_response(
                404,
                &format!("no installed schema is owned by '{owner_app}'"),
                ctx,
            );
        }
        let payload = serde_json::json!({
            "dropped": dropped,
            "existed": !dropped.is_empty(),
            "owner_app": owner_app,
        });
        return json_ok(&envelope(&payload, ctx.user_id.as_str()));
    }

    let canonical = match handlers::resolve_schema_name(host, schema) {
        Ok(Some(canonical)) => canonical,
        Ok(None) => {
            if body.must_exist {
                return error_response(
                    404,
                    &format!("no schema is installed under '{schema}'"),
                    ctx,
                );
            }
            let payload = serde_json::json!({
                "dropped": Vec::<String>::new(),
                "existed": false,
                "schema": schema,
            });
            return json_ok(&envelope(&payload, ctx.user_id.as_str()));
        }
        Err(e) => return render(Err(e), ctx),
    };

    let existed = match mgr.drop_schema(&canonical).await {
        Ok(existed) => existed,
        Err(e) => return error_response(500, &format!("schema drop failed: {e}"), ctx),
    };
    if body.must_exist && !existed {
        return error_response(
            404,
            &format!("no schema is installed under '{canonical}'"),
            ctx,
        );
    }
    let payload = serde_json::json!({
        "dropped": if existed { vec![canonical.clone()] } else { Vec::new() },
        "existed": existed,
        "schema": canonical,
    });
    json_ok(&envelope(&payload, ctx.user_id.as_str()))
}

// ---------------------------------------------------------------------------
// Identity + native index + history + atom — shared handler bodies
// ---------------------------------------------------------------------------
// lint:file-size-ok moved verbatim from exec.rs; cohesive unit, split further in a later pass
