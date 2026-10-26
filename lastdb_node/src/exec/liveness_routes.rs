use super::*;

mod rules;
pub(super) use rules::target_ids;

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
    // A scope in the request is checked as it is: an empty or blank value is an
    // error, not the personal scope (leave the value out for that).
    if let Err(error) = fold_db::db_operations::AtomStore::validate_scope(request.scope.as_deref())
    {
        return error_response(400, &error.to_string(), ctx);
    }
    let scope = request.scope.as_deref().or(ctx.storage_prefix.as_deref());
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
    ids: Vec<String>,
) -> UdsResponse {
    let ids = target_ids(id, ids);
    match host
        .db
        .db_ops()
        .atoms()
        .atom_counter_views(&ids, storage_prefix)
        .await
    {
        Ok(views) => json_ok(&envelope(&rules::atom_reply(&views), ctx.user_id.as_str())),
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
    ids: Vec<String>,
) -> UdsResponse {
    let ids = target_ids(id, ids);
    match host.db.db_ops().atoms().blob_counter_views(&ids).await {
        Ok(views) => {
            let dwell_secs = host.db.db_ops().atoms().blob_counter_report().dwell_secs;
            json_ok(&envelope(
                &rules::blob_reply(&views, dwell_secs),
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
