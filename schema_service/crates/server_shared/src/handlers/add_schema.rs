//! The add-schema route.

use super::*;

pub async fn add_schema(
    req: HttpRequest,
    payload: web::Json<Value>,
    state: web::Data<SchemaServiceState>,
) -> impl Responder {
    // lint:fn-size-ok moved verbatim from the original file; splitting is a separate change
    let body = payload.into_inner();
    // The `schema_claim` envelope signs the `schema` sub-object, so keep
    // it verbatim before deserializing the typed request.
    let Some(schema_value) = body.get("schema").cloned() else {
        return HttpResponse::BadRequest().json(ErrorResponse {
            error: "Missing 'schema' field".to_string(),
        });
    };
    let request: AddSchemaRequest = match serde_json::from_value(body) {
        Ok(r) => r,
        Err(e) => {
            return HttpResponse::BadRequest().json(ErrorResponse {
                error: format!("Invalid add-schema request: {e}"),
            });
        }
    };
    let schema_name = request.schema.name.clone();
    let telemetry_source = request.schema_match_source.clone();
    let telemetry_reason = request.fallback_reason.clone();

    // Shared-surface migration observe: classify every legacy POST /v1/schemas
    // caller. Does not reject yet — enforcement waits for caller migration.
    let has_shared_surface_envelope = request.shared_surface.is_some();
    if let Some(surface) = &request.shared_surface {
        // Transitional clients may attach the envelope on the legacy route;
        // require well-formed governance metadata. Dedicated publish/attach
        // lands later; private Mini declare never sets this field.
        if let Err(e) = validate_surface_metadata(surface) {
            return HttpResponse::BadRequest().json(ErrorResponse {
                error: format!("Invalid shared_surface metadata: {e}"),
            });
        }
    }
    observe_legacy_schema_caller(
        request.schema.owner_app_id.as_deref(),
        request.offer_to_shared_discovery,
        has_shared_surface_envelope,
    );

    let observation = state.observe_schema_mutation_gate(
        &request.schema,
        &request.mutation_mappers,
        request.offer_to_shared_discovery,
    );

    // Idempotent re-POST short-circuit (cert-free). If the incoming
    // schema would resolve to a pure `SchemaAddOutcome::AlreadyExists`
    // — same canonical identity hash, same `owner_app_id`, same
    // `schema_type`, no new fields — return 200 with the existing body
    // before running the cert gate. Otherwise a brand-new dev (no
    // DevCert) hits a hard 401 the moment they `fbrain init` an
    // already-published app, which deadlocks onboarding for every user
    // who is not the publishing developer. The canonical hash and
    // body are already publicly readable via `GET /v1/schemas`, so
    // echoing an existing entry leaks nothing beyond enumeration.
    // Create / expand / conflict outcomes still require a cert below.
    if request
        .schema
        .owner_app_id
        .as_deref()
        .filter(|s| !s.is_empty())
        .is_some()
    {
        if let Some(existing) =
            state.classify_idempotent_repost(&request.schema, &request.mutation_mappers)
        {
            let system = state.is_system_schema(&existing.name);
            return HttpResponse::Ok().json(AddSchemaResponse {
                schema: existing,
                mutation_mappers: request.mutation_mappers,
                replaced_schema: None,
                system,
                composed: false,
                composition: None,
            });
        }
    }

    let registration_cover = match state.try_native_component_cover_registration(&request.schema) {
        Ok(cover) => cover,
        Err(e) => return internal_error("Failed to evaluate schema component cover", e),
    };
    if let Some(composition) = registration_cover.clone() {
        if composition.residue_fields.is_empty() {
            state.record_schema_match_outcome("component_cover", &telemetry_source, None);
            if let Some(hash) = composition.matched_shared_schema_hash.as_deref() {
                match state.get_schema_by_name(hash) {
                    Ok(Some(schema)) => {
                        let system = state.is_system_schema(&schema.name);
                        return HttpResponse::Ok().json(AddSchemaResponse {
                            schema,
                            mutation_mappers: request.mutation_mappers,
                            replaced_schema: None,
                            system,
                            composed: false,
                            composition: Some(composition),
                        });
                    }
                    Ok(None) => {}
                    Err(e) => return internal_error("Failed to fetch covered schema", e),
                }
            }
            let system = state.is_system_schema(&request.schema.name);
            return HttpResponse::Ok().json(AddSchemaResponse {
                schema: request.schema,
                mutation_mappers: request.mutation_mappers,
                replaced_schema: None,
                system,
                composed: true,
                composition: Some(composition),
            });
        }
    }

    // owner_app_id ↔ dev-cert gate (app_identity v3.1, Lane B2b; local-first
    // app-namespacing). Un-namespaced submissions are accepted
    // unconditionally. A namespaced submission is cert-free when it merely
    // CLAIMS a local namespace (`offer_to_shared_discovery = false`, the
    // default node/init path — the deterministic identity_hash is computed
    // locally) and cert-gated only when it OFFERS the schema into the shared
    // registry's discovery (`offer_to_shared_discovery = true`).
    let cert = header_value(&req, "X-Exemem-Dev-Cert");
    let sig = header_value(&req, "X-Signature");
    if let Err(e) = state.authorize_schema_claim(
        request.schema.owner_app_id.as_deref(),
        cert.as_deref(),
        sig.as_deref(),
        &schema_value,
        request.offer_to_shared_discovery,
    ) {
        let (status, body) = e.to_http();
        return json_status(status, body);
    }

    // Declared-field references: a schema may only stamp a declared identity
    // it has permission to use. Sharing a field is a permission, not a second
    // identity scheme — so a stranger stamping someone else's declared
    // identity is refused here, with a reason naming the declaration.
    //
    // Identities belonging to no declaration pass straight through: every one
    // of the live schemas carries locally-minted v1 field hashes, and this
    // feature is forward-only.
    if let Err(e) = state.authorize_declared_field_references(
        request.schema.owner_app_id.as_deref(),
        &request.schema.field_hashes,
    ) {
        let (status, body) = e.to_http();
        return json_status(status, body);
    }

    let gate_result = state.enforce_schema_mutation_gate(
        &observation,
        &request.schema,
        &schema_value,
        &schema_mutation_gate_headers(&req),
        request_ip(&req).as_deref(),
    );
    log_schema_mutation_gate_result(&gate_result);
    if let Err(e) = gate_result {
        let (status, body) = e.to_http();
        return json_status(status, body);
    }

    tracing::info!(
            target: "schema_service::schema",
        "Schema service: adding schema '{}' with {} mutation mappers",
        schema_name,
        request.mutation_mappers.len()
    );

    let (schema_to_add, mutation_mappers_to_add, response_composition) =
        if let Some(composition) = registration_cover {
            if let Some(residual) =
                state.residual_schema_for_registration(&request.schema, &composition)
            {
                let residue: std::collections::HashSet<&str> = composition
                    .residue_fields
                    .iter()
                    .map(String::as_str)
                    .collect();
                let residual_mappers = request
                    .mutation_mappers
                    .into_iter()
                    .filter(|(from, to)| {
                        residue.contains(from.as_str()) || residue.contains(to.as_str())
                    })
                    .collect();
                (residual, residual_mappers, Some(composition))
            } else {
                (request.schema, request.mutation_mappers, None)
            }
        } else {
            (request.schema, request.mutation_mappers, None)
        };

    match state
        .add_schema(schema_to_add, mutation_mappers_to_add)
        .await
    {
        Ok(SchemaAddOutcome::Added(schema, mutation_mappers)) => {
            state.record_schema_match_outcome(
                if response_composition.is_some() {
                    "residual_created"
                } else {
                    "created_new"
                },
                &telemetry_source,
                telemetry_reason.as_deref(),
            );
            let system = state.is_system_schema(&schema.name);
            HttpResponse::Created().json(AddSchemaResponse {
                schema,
                mutation_mappers,
                replaced_schema: None,
                system,
                composed: response_composition.is_some(),
                composition: response_composition,
            })
        }
        Ok(SchemaAddOutcome::AlreadyExists(schema, mutation_mappers)) => {
            state.record_schema_match_outcome(
                "matched_existing",
                &telemetry_source,
                telemetry_reason.as_deref(),
            );
            let system = state.is_system_schema(&schema.name);
            HttpResponse::Ok().json(AddSchemaResponse {
                schema,
                mutation_mappers,
                replaced_schema: None,
                system,
                composed: false,
                composition: response_composition,
            })
        }
        Ok(SchemaAddOutcome::Expanded(old_name, schema, mutation_mappers)) => {
            state.record_schema_match_outcome(
                "expanded_existing",
                &telemetry_source,
                telemetry_reason.as_deref(),
            );
            let system = state.is_system_schema(&schema.name);
            HttpResponse::Created().json(AddSchemaResponse {
                schema,
                mutation_mappers,
                replaced_schema: Some(old_name),
                system,
                composed: false,
                composition: response_composition,
            })
        }
        // A composed canonical is newly registered: the compositional apply
        // step rewrote one or more nested `ref_fields` to reuse an existing
        // canonical via SchemaRef instead of re-inlining its fields. Serialized
        // like `Added` (HTTP 201, no replaced schema) PLUS `composed: true` —
        // the authoritative wire marker so a client can tell genuine
        // composition apart from a plain registration that merely carried
        // ref_fields (the per-component advice rides on the outcome
        // internally). Only produced when the apply step is enabled
        // (`SCHEMA_COMPOSITIONAL_DECOMPOSITION=apply`, off by default).
        Ok(SchemaAddOutcome::Composed(schema, mutation_mappers, _advice)) => {
            state.record_schema_match_outcome(
                "composed_existing",
                &telemetry_source,
                telemetry_reason.as_deref(),
            );
            let system = state.is_system_schema(&schema.name);
            HttpResponse::Created().json(AddSchemaResponse {
                schema,
                mutation_mappers,
                replaced_schema: None,
                system,
                composed: true,
                composition: response_composition,
            })
        }
        Ok(SchemaAddOutcome::DescriptiveNameConflict(conflict)) => {
            state.record_schema_match_outcome(
                "rejected",
                &telemetry_source,
                telemetry_reason.as_deref(),
            );
            tracing::warn!(
                target: "schema_service::schema",
                descriptive_name = %conflict.descriptive_name,
                existing_canonical = %conflict.existing_canonical,
                "Refused duplicate descriptive_name registration (409)",
            );
            HttpResponse::Conflict().json(conflict)
        }
        Err(error) => {
            state.record_schema_match_outcome(
                "rejected",
                &telemetry_source,
                telemetry_reason.as_deref(),
            );
            tracing::error!(
            target: "schema_service::schema",
                "Failed to add schema '{}': {}",
                schema_name,
                error
            );
            HttpResponse::BadRequest().json(ErrorResponse {
                error: format!("Failed to add schema: {error}"),
            })
        }
    }
}
