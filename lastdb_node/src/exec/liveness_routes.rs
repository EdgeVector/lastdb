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
        /// Several ids in one call. The classes `atom`, `molecule` and `blob` read them.
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
    if !matches!(class.as_str(), "atom" | "molecule" | "blob") {
        return error_response(400, "class must be atom, molecule, or blob", ctx);
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
    if class == "atom" {
        return explain_atoms(host, ctx, scope, id, request.ids).await;
    }
    if class == "molecule" {
        return explain_molecules(host, ctx, scope, id, request.ids).await;
    }
    explain_blobs(host, ctx, id, request.ids).await
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

/// The counter of each named blob. A blob counts the atom counters that list it
/// and the live tips that name it; it has no list of referrers. A blob at zero
/// is in its dwell until `zero_since` plus the dwell has passed.
async fn explain_blobs(
    host: &Host,
    ctx: &AccessContext,
    id: &str,
    mut ids: Vec<String>,
) -> UdsResponse {
    if !id.is_empty() {
        ids.insert(0, id.to_string());
    }
    ids.truncate(10_000);
    match host.db.db_ops().atoms().blob_counter_views(&ids).await {
        Ok(views) => {
            let rows: Vec<serde_json::Value> = views
                .iter()
                .map(|view| {
                    let state = match (view.refs, view.zero_since, view.suspect) {
                        _ if view.unreadable => "unreadable",
                        (Some(_), _, true) => "suspect",
                        // Atoms name the blob and the store has no row for it: a
                        // write named the blob after its dwell deleted it.
                        (Some(refs), _, false) if refs > 0 && !view.row_present => {
                            "referenced_row_missing"
                        }
                        (Some(refs), _, false) if refs > 0 => "referenced",
                        (Some(_), Some(_), false) => "in_dwell",
                        (Some(_), None, false) => "inconsistent",
                        (None, _, _) if view.row_present => "row_without_counter",
                        (None, _, _) => "absent",
                    };
                    serde_json::json!({
                        "blob": view.blob,
                        "refs": view.refs,
                        "zero_since_unix_ms": view.zero_since,
                        "suspect": view.suspect,
                        "queued": view.queued,
                        "row_present": view.row_present,
                        "unreadable": view.unreadable,
                        "state": state,
                    })
                })
                .collect();
            json_ok(&envelope(
                &serde_json::json!({
                    "liveness": {
                        "class": "blob",
                        "dwell_secs": host.db.db_ops().atoms().blob_counter_report().dwell_secs,
                        "blobs": rows,
                    }
                }),
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
