//! Compatibility handlers for retired native-search and app-vector routes.
// lint:file-size-ok moved verbatim from handlers.rs; one handler family per file

use super::*;

// ---------------------------------------------------------------------------
// Native search implementation used behind app-scoped Search routes.
// ---------------------------------------------------------------------------

pub(super) const SEARCH_APP_ID: &str = "search";

/// Parsed native-search params, filled by socket executors from query strings.
pub struct NativeSearchParams {
    /// Query text. The retired native-search handler does not read it.
    pub term: String,
    /// Whether internal schemas were requested. The retired handler does not read it.
    pub include_internal: bool,
    /// Whether an exact match was requested. The retired handler does not read it.
    pub exact: bool,
    /// Optional minimum score. The retired handler does not read it.
    pub min_score: Option<f64>,
    /// Schemas to search. `search_app_query` keeps only schemas owned by the Search app.
    pub schemas: Option<Vec<String>>,
}

/// Parsed app-search params for `POST /api/app/search`.
pub struct AppSearchParams {
    /// Query text copied into the retired native-search call.
    pub term: String,
    /// `Some(0)` returns an empty result. Any other limit still reaches the retired backend.
    pub limit: Option<usize>,
    /// Optional schema name that narrows the owner or app scope.
    pub target: Option<String>,
}

/// Error body when Mini cannot run native semantic search. Clients must use
/// LastSeek (app on top of LastDB), not treat empty 200 as success.
pub fn search_plane_required_message(reason: Option<&str>) -> String {
    format!(
        "search_plane_required: {}",
        reason.unwrap_or("semantic search is not available in this Mini binary; use LastSeek")
    )
}

/// Mini has no native index. LastSeek owns search
/// (`north-star-lastdb-strip-native-index`). An `ok: true` hit list here is
/// a regression.
#[allow(clippy::needless_pass_by_value)]
pub fn native_index_search<H: HostNode>(
    _host: &H,
    _params: NativeSearchParams,
    _ctx: &AccessContext,
) -> Result<Value, HostError> {
    let body = serde_json::json!({
        "ok": false,
        "retired": true,
        "message": "native-index search route removed; use LastSeek"
    });
    Err(HostError::structured(404, "route removed", body))
}

/// Execute Search-app text query as a verified first-party app. It reuses the
/// native ranking path, but always narrows scope to schemas owned by the
/// `search` app before querying.
///
/// # Errors
/// [`HostError`] `401` for an unverified/non-Search caller, `403` for an
/// explicitly requested non-Search schema, `404` for an unknown schema, `500`
/// on a native-search failure.
pub fn search_app_query<H: HostNode>(
    host: &H,
    params: NativeSearchParams,
    ctx: &AccessContext,
) -> Result<Value, HostError> {
    match ctx.verified_app_id() {
        Some(SEARCH_APP_ID) => {}
        Some(app_id) => {
            return Err(HostError::new(
                401,
                format!("app '{app_id}' is not allowed to use the Search query route"),
            ));
        }
        None => {
            return Err(HostError::new(
                401,
                "Search query route requires a verified Search app identity",
            ));
        }
    }

    let schemas = if let Some(requested) = params.schemas.as_ref() {
        let mut scoped = Vec::with_capacity(requested.len());
        for schema in requested {
            let Some(canonical) = resolve_schema_name(host, schema)? else {
                return Err(HostError::new(404, format!("Schema not found: {schema}")));
            };
            let owner_app_id = host
                .fold_db()
                .schema_manager()
                .get_schema_metadata(&canonical)
                .ok()
                .flatten()
                .and_then(|mut s| s.owner_app_id.take());
            if owner_app_id.as_deref() != Some(SEARCH_APP_ID) {
                return Err(HostError::new(
                    403,
                    format!("Search app cannot query schema '{schema}'"),
                ));
            }
            scoped.push(canonical);
        }
        scoped
    } else {
        host.fold_db()
            .schema_manager()
            .get_active_schemas_with_states()
            .map_err(HostError::from)?
            .into_iter()
            .filter(|entry| entry.schema.owner_app_id.as_deref() == Some(SEARCH_APP_ID))
            .map(|entry| entry.schema.name.clone())
            .collect()
    };

    if schemas.is_empty() {
        return Err(HostError::new(
            503,
            search_plane_required_message(Some("in-process native index removed; use LastSeek")),
        ));
    }

    native_index_search(
        host,
        NativeSearchParams {
            schemas: Some(schemas),
            ..params
        },
        ctx,
    )
}

/// Preserve app-search validation and retired-backend responses.
///
/// **Principals**
/// - **Owner** (owner UDS / NodeOwner): allowed. Scope is the whole node
///   (optional `target` narrows to one schema). Same posture as owner-visible
///   change-feed (`change_visible_to_caller` allows `ctx.is_owner`).
/// - **Verified app**: scoped to schemas with `owner_app_id == app_id`;
///   `target` may only narrow within that set.
/// - **Neither**: `403 capability_required` (jailed app socket without
///   code-signature identity).
///
/// Search-as-app (first-party Search app) still ships as a verified app
/// principal; this owner allow keeps Mini CLI/agent (`fbrain`/`brain`)
/// working on the owner socket while that migration is in flight.
pub fn app_search<H: HostNode>(
    host: &H,
    params: AppSearchParams,
    ctx: &AccessContext,
) -> Result<Value, HostError> {
    let schemas = if ctx.is_owner {
        owner_search_scope(host, params.target.as_deref())?
    } else if let Some(app_id) = ctx.verified_app_id() {
        app_owned_search_scope(host, app_id, params.target.as_deref())?
    } else {
        return Err(HostError::new(403, "capability_required"));
    };

    if schemas.is_empty() || params.limit == Some(0) {
        // limit=0 is a real empty result; empty schema scope with semantic off
        // must still fail closed toward Search plane.
        // Mini semantic search is retired; only limit=0 is a legitimate empty hit.
        if params.limit != Some(0) {
            return Err(HostError::new(
                503,
                search_plane_required_message(Some(
                    "in-process native index removed; use LastSeek",
                )),
            ));
        }
        return Ok(empty_search_payload(host));
    }

    native_index_search(
        host,
        NativeSearchParams {
            term: params.term,
            include_internal: false,
            exact: false,
            min_score: None,
            schemas: Some(schemas),
        },
        ctx,
    )
}

pub(super) fn empty_search_payload<H: HostNode>(host: &H) -> Value {
    serde_json::json!({
        "results": [],
        "semantic_search_available": false,
        "degraded": true,
        "semantic_search_unavailable_reason": "in-process native index removed; use the Search app plane",
        "indexing_pending": host.fold_db().pending_tasks().count() > 0,
    })
}

/// Owner-socket scope: any schema on the node (optional single-schema target).
pub(super) fn owner_search_scope<H: HostNode>(
    host: &H,
    target: Option<&str>,
) -> Result<Vec<String>, HostError> {
    if let Some(target) = target {
        let Some(canonical) = resolve_schema_name(host, target)? else {
            return Ok(Vec::new());
        };
        return Ok(vec![canonical]);
    }

    Ok(host
        .fold_db()
        .schema_manager()
        .get_active_schemas_with_states()
        .map_err(HostError::from)?
        .into_iter()
        .map(|entry| entry.schema.name.clone())
        .collect())
}

pub(super) fn app_owned_search_scope<H: HostNode>(
    host: &H,
    app_id: &str,
    target: Option<&str>,
) -> Result<Vec<String>, HostError> {
    if let Some(target) = target {
        let Some(canonical) = resolve_schema_name(host, target)? else {
            return Ok(Vec::new());
        };
        let owner_app_id = host
            .fold_db()
            .schema_manager()
            .get_schema_metadata(&canonical)
            .ok()
            .flatten()
            .and_then(|mut s| s.owner_app_id.take());
        return Ok((owner_app_id.as_deref() == Some(app_id))
            .then_some(canonical)
            .into_iter()
            .collect());
    }

    Ok(host
        .fold_db()
        .schema_manager()
        .get_active_schemas_with_states()
        .map_err(HostError::from)?
        .into_iter()
        .filter(|entry| entry.schema.owner_app_id.as_deref() == Some(app_id))
        .map(|entry| entry.schema.name.clone())
        .collect())
}

// ---------------------------------------------------------------------------
// Retired app-vector routes retain their request bodies for wire compatibility.
// ---------------------------------------------------------------------------

/// Request body accepted before the embeddings route returns `503`.
#[derive(Debug, serde::Deserialize)]
pub struct AppVectorPutRequest {
    /// Schema name on the retired embeddings body. The handler does not store it.
    pub schema: String,
    /// Record key on the retired embeddings body. The handler does not store it.
    pub key: KeyValue,
    /// Field name on the retired embeddings body. The handler does not store it.
    pub field: String,
    /// Embedder name on the retired embeddings body. The handler does not store it.
    pub embedder: String,
    /// Embedding floats on the retired embeddings body. The handler does not store them.
    pub vector: Vec<f32>,
}

/// Seed record reference for "more like record X" k-NN queries.
#[derive(Debug, serde::Deserialize)]
pub struct KnnSeed {
    /// Schema of the seed record on the retired k-NN body.
    pub schema: String,
    /// Key of the seed record on the retired k-NN body.
    pub key: KeyValue,
    /// Field of the seed record on the retired k-NN body.
    pub field: String,
}

/// Request body accepted before the k-NN route returns `503`.
#[derive(Debug, serde::Deserialize)]
pub struct KnnRequest {
    /// Schema names on the retired k-NN body. The handler does not query them.
    pub schemas: Vec<String>,
    /// Neighbor count on the retired k-NN body. The handler does not use it.
    pub k: usize,
    /// Embedder name on the retired k-NN body. The handler does not use it.
    pub embedder: String,
    #[serde(default)]
    /// Optional query vector on the retired k-NN body. The handler does not use it.
    pub vector: Option<Vec<f32>>,
    #[serde(default)]
    /// Optional seed record on the retired k-NN body. The handler does not use it.
    pub seed: Option<KnnSeed>,
}

/// Return the retired native-index response for `POST /api/native-index/embeddings`.
///
/// # Errors
/// [`HostError`] `503`: the in-process native index was removed.
pub fn native_index_put_app_vector<H: HostNode>(
    host: &H,
    request: AppVectorPutRequest,
    ctx: &AccessContext,
) -> Result<Value, HostError> {
    let _ = (host, request, ctx);
    Err(HostError::new(
        503,
        search_plane_required_message(Some(
            "in-process native index removed; use the Search app plane",
        )),
    ))
}

/// Return the retired native-index response for `POST /api/native-index/knn`.
///
/// # Errors
/// [`HostError`] `503`: the in-process native index was removed.
pub fn native_index_knn<H: HostNode>(host: &H, request: KnnRequest) -> Result<Value, HostError> {
    let _ = (host, request);
    Err(HostError::new(
        503,
        search_plane_required_message(Some(
            "in-process native index removed; use the Search app plane",
        )),
    ))
}
