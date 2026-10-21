//! `POST /api/db/molecule-drain`: name a molecule to the drain, and read its report.

use super::*;

/// `POST /api/db/molecule-drain` — body `{molecule?, scope?}`.
///
/// With a `molecule` the call reads that molecule inside the write gate and
/// queues it for the drain when it is dead or refused. The answer says which:
/// `dead` (nothing names it), `bound`, `refused` (a counter or a record keeps it
/// from a drain: the drain never removes it) or `absent`. Without a `molecule`
/// the call only reads the report. Either way the drain task wakes at once. The
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
    let atoms = host.db.db_ops().atoms();
    let verdict = if body.molecule.trim().is_empty() {
        None
    } else {
        let scope = body.scope.as_deref().filter(|scope| !scope.is_empty());
        match atoms
            .molecule_drain_enqueue(scope, body.molecule.trim())
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
    atoms.wake_molecule_drain();
    json_ok(&envelope(
        &serde_json::json!({
            "molecule_drain": { "report": atoms.molecule_drain_report(), "verdict": verdict }
        }),
        ctx.user_id.as_str(),
    ))
}
