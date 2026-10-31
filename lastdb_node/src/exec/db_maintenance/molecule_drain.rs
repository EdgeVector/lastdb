//! `POST /api/db/molecule-drain`: name a molecule to the drain, and read its report.

use super::*;

/// `POST /api/db/molecule-drain` — body `{molecule?, scope?}`.
///
/// With a `molecule` the call reads that molecule inside the write gate and
/// queues it for the drain when it is dead or refused. The answer says which:
/// `dead` (nothing names it), `bound`, `refused` (a counter or a record keeps it
/// from a drain: the drain never removes it) or `absent`. Without a `molecule`
/// the call only reads the report. An enqueue wakes the drain task. The
/// call deletes nothing itself: the task removes the molecule batch by batch.
pub(in crate::exec) async fn execute_db_molecule_drain_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize, Default)]
    struct Body {
        #[serde(default)]
        molecule: String,
        #[serde(default)]
        scope: Option<String>,
        #[serde(default)]
        cleanup_delete_markers: bool,
        #[serde(default)]
        execute: bool,
        #[serde(default)]
        after_key: Option<String>,
        #[serde(default)]
        limit: Option<usize>,
    }
    let body: Body = if req.body.is_empty() {
        Body::default()
    } else {
        match serde_json::from_slice(&req.body) {
            Ok(body) => body,
            Err(error) => {
                return error_response(
                    400,
                    &format!("invalid /api/db/molecule-drain body: {error}"),
                    ctx,
                )
            }
        }
    };
    // A scope in the body is checked as it is: an empty or blank value is an error.
    if let Err(error) = fold_db::db_operations::AtomStore::validate_scope(body.scope.as_deref()) {
        return error_response(400, &error.to_string(), ctx);
    }
    let atoms = host.db.db_ops().atoms();
    if body.cleanup_delete_markers {
        if !ctx.is_owner {
            return error_response(403, "delete-marker cleanup is owner-only", ctx);
        }
        return match atoms
            .cleanup_delete_markers_page(
                &body.molecule,
                body.scope.as_deref(),
                body.after_key.as_deref(),
                body.limit.unwrap_or(128),
                body.execute,
            )
            .await
        {
            Ok(value) => json_ok(&envelope(&value, ctx.user_id.as_str())),
            Err(error) => mapped_error_response("delete-marker cleanup failed", error.into(), ctx),
        };
    }
    let verdict = if body.molecule.trim().is_empty() {
        None
    } else {
        match atoms
            .molecule_drain_enqueue(body.scope.as_deref(), body.molecule.trim())
            .await
        {
            Ok(verdict) => Some(verdict),
            Err(error) => {
                return mapped_error_response(
                    "molecule drain failed",
                    fold_db::error::FoldDbError::Schema(error),
                    ctx,
                )
            }
        }
    };
    // A status poll must not wake the task or reset its failure back-off.
    // A failed count must not report an empty queue.
    let (candidates, capped) = match atoms.molecule_drain_candidates(10_000).await {
        Ok(value) => value,
        Err(error) => {
            return mapped_error_response("molecule drain count failed", error.into(), ctx)
        }
    };
    json_ok(&envelope(
        &serde_json::json!({
            "molecule_drain": {
                "report": atoms.molecule_drain_report(),
                "verdict": verdict,
                "candidate_rows": candidates,
                "candidate_rows_capped": capped,
            }
        }),
        ctx.user_id.as_str(),
    ))
}
