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
        "atom" => {
            let count = atoms.atom_live_ref_count_record(id, storage_prefix).await?;
            let active_references = count
                .filter(|count| count.exact)
                .map(|count| count.live_refs);
            Ok(LivenessReferences {
                complete: atoms.atom_ref_v2_reads_ready().await? && active_references.is_some(),
                active_references,
                // Compact markers do not retain reconstructable edge details.
                edges: None,
            })
        }
        "molecule" => {
            let lookup = atoms
                .molecule_ref_edges_for_molecule(id, storage_prefix)
                .await?;
            LivenessReferences::with_edges(lookup.complete, lookup.edges)
        }
        "blob" => {
            let lookup = atoms.blob_ref_edges_for_blob(id, storage_prefix).await?;
            LivenessReferences::with_edges(lookup.complete, lookup.edges)
        }
        _ => Err(fold_db::schema::SchemaError::InvalidData(
            "class must be atom, molecule, or blob".into(),
        )),
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
