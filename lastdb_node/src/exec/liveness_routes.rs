use super::*;

/// `POST /api/storage/liveness/explain` — one target partition only.
pub(super) async fn execute_liveness_explain_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct Request {
        class: String,
        #[serde(default)]
        id: String,
        /// Atom class only: many atom ids in one request, read in batches.
        #[serde(default)]
        ids: Vec<String>,
    }

    let request = match serde_json::from_slice::<Request>(&req.body) {
        Ok(request)
            if !request.class.trim().is_empty()
                && (!request.id.trim().is_empty() || !request.ids.is_empty()) =>
        {
            request
        }
        Ok(_) => return error_response(400, "liveness explain requires class and id", ctx),
        Err(error) => {
            return error_response(
                400,
                &format!("invalid liveness explain request: {error}"),
                ctx,
            )
        }
    };
    let class = request.class.trim().to_ascii_lowercase();
    let id = request.id.trim();
    let atoms = host.db.db_ops().atoms();
    if class == "atom" {
        return explain_atoms(host, ctx, id, request.ids).await;
    }
    let result: Result<(bool, serde_json::Value), fold_db::error::FoldDbError> =
        match class.as_str() {
            "molecule" => atoms
                .molecule_ref_edges_for_molecule(id, None)
                .await
                .and_then(|lookup| {
                    serde_json::to_value(lookup.edges)
                        .map(|edges| (lookup.complete, edges))
                        .map_err(|error| {
                            fold_db::schema::SchemaError::InvalidData(format!(
                                "serialize molecule liveness edges: {error}"
                            ))
                        })
                })
                .map_err(fold_db::error::FoldDbError::Schema),
            "blob" => atoms
                .blob_ref_edges_for_blob(id, None)
                .await
                .and_then(|lookup| {
                    serde_json::to_value(lookup.edges)
                        .map(|edges| (lookup.complete, edges))
                        .map_err(|error| {
                            fold_db::schema::SchemaError::InvalidData(format!(
                                "serialize blob liveness edges: {error}"
                            ))
                        })
                })
                .map_err(fold_db::error::FoldDbError::Schema),
            _ => return error_response(400, "class must be atom, molecule, or blob", ctx),
        };

    match result {
        Ok((complete, edges)) => {
            let active_edges = edges.as_array().map_or(0, Vec::len);
            let reclaim_state = if !complete {
                "blocked_index_incomplete"
            } else if active_edges > 0 {
                "blocked_active_edges"
            } else {
                "candidate"
            };
            json_ok(&envelope(
                &serde_json::json!({
                    "liveness": {
                        "class": class,
                        "id": id,
                        "complete": complete,
                        "reclaim_state": reclaim_state,
                        "edges": edges,
                    }
                }),
                ctx.user_id.as_str(),
            ))
        }
        Err(error) => mapped_error_response("liveness explain failed", error, ctx),
    }
}

/// `POST /api/storage/liveness/bootstrap` — copy-only derived-edge rebuild.
pub(super) async fn execute_liveness_bootstrap_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct Request {
        #[serde(default)]
        isolated_copy: bool,
        #[serde(default)]
        storage_prefix: Option<String>,
    }

    let request = match serde_json::from_slice::<Request>(&req.body) {
        Ok(request) => request,
        Err(error) => {
            return error_response(
                400,
                &format!("invalid liveness bootstrap request: {error}"),
                ctx,
            )
        }
    };
    let isolated_process = is_isolated_copy_process();
    if !request.isolated_copy || !isolated_process {
        return error_response(
            409,
            "liveness bootstrap requires isolated_copy=true and LASTDB_ISOLATED_COPY=1",
            ctx,
        );
    }
    let storage_prefix = request
        .storage_prefix
        .as_deref()
        .map(str::trim)
        .filter(|prefix| !prefix.is_empty());
    match host
        .db
        .db_ops()
        .bootstrap_liveness_edges_on_isolated_copy(storage_prefix)
        .await
    {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "liveness_bootstrap": report }),
            ctx.user_id.as_str(),
        )),
        Err(error) => mapped_error_response(
            "liveness bootstrap failed",
            fold_db::error::FoldDbError::Schema(error),
            ctx,
        ),
    }
}

/// The counter of each named atom. An atom has no list of referrers: the
/// counter holds the number of tip records and conflict records that name it.
async fn explain_atoms(
    host: &Host,
    ctx: &AccessContext,
    id: &str,
    mut ids: Vec<String>,
) -> UdsResponse {
    if !id.is_empty() {
        ids.insert(0, id.to_string());
    }
    ids.truncate(10_000);
    match host
        .db
        .db_ops()
        .atoms()
        .atom_counter_views(&ids, None)
        .await
    {
        Ok(views) => {
            let rows: Vec<serde_json::Value> = views
                .iter()
                .map(|view| {
                    let state = match (view.refs, view.body_present, view.suspect) {
                        (Some(_), _, true) => "suspect",
                        (Some(_), true, false) => "live",
                        (Some(_), false, false) | (None, true, _) => "inconsistent",
                        (None, false, _) => "absent",
                    };
                    serde_json::json!({
                        "atom": view.atom,
                        "refs": view.refs,
                        "suspect": view.suspect,
                        "blobs": view.blobs,
                        "body_present": view.body_present,
                        "state": state,
                    })
                })
                .collect();
            json_ok(&envelope(
                &serde_json::json!({ "liveness": { "class": "atom", "atoms": rows } }),
                ctx.user_id.as_str(),
            ))
        }
        Err(error) => mapped_error_response(
            "liveness explain failed",
            fold_db::error::FoldDbError::Schema(error),
            ctx,
        ),
    }
}
