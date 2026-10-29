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

    let fetched = if request.schemas.is_empty() {
        fetch_full_catalog(&client).await
    } else if remote_schema_requests.is_empty() {
        Ok((Vec::new(), Vec::new()))
    } else if remote_schema_requests
        .iter()
        .all(|s| looks_like_identity_hash(s))
    {
        Ok(fetch_schemas_by_hash(&client, &remote_schema_requests).await)
    } else {
        fetch_schemas_by_name(&client, &remote_schema_requests).await
    };
    let (schemas, mut failed_schemas) = match fetched {
        Ok(fetched) => fetched,
        Err(e) => return error_response(503, &e, ctx),
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

fn looks_like_identity_hash(s: &str) -> bool {
    let t = s.trim();
    t.len() == 64 && t.chars().all(|c| c.is_ascii_hexdigit())
}

/// Schemas fetched from the schema service, plus the requests it lacked.
type FetchedSchemas = (Vec<schema_types::Schema>, Vec<String>);

/// Full catalog load — heavy; clients should prefer a scoped list.
async fn fetch_full_catalog(
    client: &schema_service_client::SchemaServiceClient,
) -> Result<FetchedSchemas, String> {
    let envelopes = client
        .get_available_schemas()
        .await
        .map_err(|e| e.to_string())?;
    Ok((
        envelopes.into_iter().map(|e| e.schema).collect(),
        Vec::new(),
    ))
}

/// Fast path: identity hashes → per-schema GET (avoids the 20s catalog).
async fn fetch_schemas_by_hash(
    client: &schema_service_client::SchemaServiceClient,
    requests: &[String],
) -> FetchedSchemas {
    let mut kept = Vec::new();
    let mut missing = Vec::new();
    for name in requests {
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
}

/// Descriptive names / mixed scope → catalog + filter.
async fn fetch_schemas_by_name(
    client: &schema_service_client::SchemaServiceClient,
    requests: &[String],
) -> Result<FetchedSchemas, String> {
    let envelopes = client
        .get_available_schemas()
        .await
        .map_err(|e| e.to_string())?;
    let matches = |s: &schema_types::Schema, r: &String| {
        schema_matches_load_request(
            &s.name,
            s.descriptive_name.as_deref(),
            s.owner_app_id.as_deref(),
            r,
        )
    };
    let kept: Vec<_> = envelopes
        .into_iter()
        .map(|e| e.schema)
        .filter(|s| requests.iter().any(|r| matches(s, r)))
        .collect();
    let not_found = requests
        .iter()
        .filter(|r| !kept.iter().any(|s| matches(s, r)))
        .map(|r| format!("{r} (not found)"))
        .collect();
    Ok((kept, not_found))
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

mod apps;
pub use apps::*;
