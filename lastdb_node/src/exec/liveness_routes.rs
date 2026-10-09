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
        id: String,
    }

    let request = match serde_json::from_slice::<Request>(&req.body) {
        Ok(request) if !request.class.trim().is_empty() && !request.id.trim().is_empty() => request,
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
    let result: Result<(bool, serde_json::Value), fold_db::error::FoldDbError> =
        match class.as_str() {
            "atom" => atoms
                .atom_ref_edges_for_atom(id, None)
                .await
                .and_then(|lookup| {
                    serde_json::to_value(lookup.edges)
                        .map(|edges| (lookup.complete, edges))
                        .map_err(|error| {
                            fold_db::schema::SchemaError::InvalidData(format!(
                                "serialize atom liveness edges: {error}"
                            ))
                        })
                })
                .map_err(fold_db::error::FoldDbError::Schema),
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
