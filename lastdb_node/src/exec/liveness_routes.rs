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
    // A named handle can map each schema to a different storage instance.
    // This target-only diagnostic has no schema with which to resolve it.
    if ctx.db_locator.is_some() {
        return error_response(
            400,
            "liveness explain requires the default owner scope; named database schema mappings are unsupported",
            ctx,
        );
    }
    if class == "atom" {
        return explain_atoms(host, ctx, id, request.ids).await;
    }
    if class == "molecule" {
        return explain_molecules(host, ctx, id, request.ids).await;
    }
    let result = read_liveness_references(&class, id, ctx.storage_prefix.as_deref(), atoms).await;

    match result {
        Ok(references) => {
            let reclaim_state = if !references.complete {
                "blocked_index_incomplete"
            } else if references.active_references.is_some_and(|count| count > 0) {
                "blocked_active_edges"
            } else {
                "candidate"
            };
            let mut report = serde_json::json!({
                "class": class,
                "id": id,
                "complete": references.complete,
                "active_references": references.active_references,
                "reclaim_state": reclaim_state,
            });
            if let Some(edges) = references.edges {
                report["edges"] = edges;
            }
            json_ok(&envelope(
                &serde_json::json!({ "liveness": report }),
                ctx.user_id.as_str(),
            ))
        }
        Err(error) => mapped_error_response("liveness explain failed", error.into(), ctx),
    }
}

struct LivenessReferences {
    complete: bool,
    active_references: Option<u64>,
    edges: Option<serde_json::Value>,
}

impl LivenessReferences {
    fn with_edges<T: serde::Serialize>(
        complete: bool,
        edges: Vec<T>,
    ) -> Result<Self, fold_db::schema::SchemaError> {
        let active_references = Some(edges.len() as u64);
        let edges = serde_json::to_value(edges).map_err(|error| {
            fold_db::schema::SchemaError::InvalidData(format!("serialize liveness edges: {error}"))
        })?;
        Ok(Self {
            complete,
            active_references,
            edges: Some(edges),
        })
    }
}

async fn read_liveness_references(
    class: &str,
    id: &str,
    storage_prefix: Option<&str>,
    atoms: &fold_db::db_operations::AtomStore,
) -> Result<LivenessReferences, fold_db::schema::SchemaError> {
    match class {
        "blob" => {
            let lookup = atoms.blob_ref_edges_for_blob(id, storage_prefix).await?;
            LivenessReferences::with_edges(lookup.complete, lookup.edges)
        }
        _ => Err(fold_db::schema::SchemaError::InvalidData(
            "class must be atom, molecule, or blob".into(),
        )),
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
    let storage_prefix = ctx.storage_prefix.as_deref();
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
