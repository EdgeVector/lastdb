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
        /// Several ids in one call. The classes `atom` and `molecule` read them.
        #[serde(default)]
        ids: Vec<String>,
        /// Atom and molecule storage scope. Without it the request scope
        /// applies. Blob references span the whole node.
        #[serde(default)]
        scope: Option<String>,
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
    if !matches!(class.as_str(), "atom" | "molecule" | "blob") {
        return error_response(400, "class must be atom, molecule, or blob", ctx);
    }
    if class == "blob" && (id.is_empty() || !request.ids.is_empty()) {
        return error_response(400, "blob liveness explain requires one id", ctx);
    }
    // A named handle can map each schema to a different storage instance.
    // This target-only diagnostic has no schema with which to resolve it.
    if ctx.db_locator.is_some() {
        return error_response(
            400,
            "liveness explain requires the default owner scope; named database schema mappings are unsupported",
            ctx,
        );
    }
    let scope = request
        .scope
        .as_deref()
        .map(str::trim)
        .filter(|scope| !scope.is_empty())
        .or(ctx.storage_prefix.as_deref());
    if let Err(error) = fold_db::db_operations::AtomStore::validate_scope(scope) {
        return error_response(400, &error.to_string(), ctx);
    }
    if class == "atom" {
        return explain_atoms(host, ctx, scope, id, request.ids).await;
    }
    if class == "molecule" {
        return explain_molecules(host, ctx, scope, id, request.ids).await;
    }
    match atoms.blob_ref_edges_for_blob(id, None).await {
        Ok(references) => {
            let reclaim_state = if !references.complete {
                "blocked_index_incomplete"
            } else if !references.edges.is_empty() {
                "blocked_active_edges"
            } else {
                "candidate"
            };
            let report = serde_json::json!({
                "class": class,
                "id": id,
                "complete": references.complete,
                "active_references": references.edges.len(),
                "reclaim_state": reclaim_state,
                "edges": references.edges,
            });
            json_ok(&envelope(
                &serde_json::json!({ "liveness": report }),
                ctx.user_id.as_str(),
            ))
        }
        Err(error) => mapped_error_response("liveness explain failed", error.into(), ctx),
    }
}

/// The counter of each named atom. An atom has no list of referrers: the
/// counter holds the number of tip records and conflict records that name it.
async fn explain_atoms(
    host: &Host,
    ctx: &AccessContext,
    storage_prefix: Option<&str>,
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
        .atom_counter_views(&ids, storage_prefix)
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
