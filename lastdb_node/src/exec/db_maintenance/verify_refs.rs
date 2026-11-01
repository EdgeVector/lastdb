//! `POST` and `GET /api/db/verify-refs`: the reference verifier.

use super::*;

/// `POST /api/db/verify-refs` — one bounded step of the reference verifier.
///
/// Reads the conflict retention queue, the blob counters and the blob dwell
/// queue, in pages, and reports the faults it finds. It writes nothing and
/// takes no write gate. A step ends after `max_pages` pages or `max_ms`
/// milliseconds; the answer holds `next_cursor` while the run is not complete.
pub(in crate::exec) async fn execute_db_verify_refs_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let request: fold_db::db_operations::VerifyRequest = if req.body.is_empty() {
        fold_db::db_operations::VerifyRequest::default()
    } else {
        match serde_json::from_slice(&req.body) {
            Ok(request) => request,
            Err(error) => {
                return error_response(
                    400,
                    &format!("invalid /api/db/verify-refs body: {error}"),
                    ctx,
                )
            }
        }
    };
    if request.cursor.is_none() {
        if let Err(error) =
            fold_db::db_operations::AtomStore::validate_scope(request.scope.as_deref())
        {
            return error_response(400, &error.to_string(), ctx);
        }
    }
    match host.db.verify_refs_step(request).await {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "verify_refs": report }),
            ctx.user_id.as_str(),
        )),
        Err(error) => mapped_error_response("verify-refs failed", error, ctx),
    }
}

/// `GET /api/db/verify-refs` — the last complete run and the run in progress.
/// A read of memory; it touches no store.
pub(in crate::exec) fn execute_db_verify_refs_last_route(
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    json_ok(&envelope(
        &serde_json::json!({ "verify_refs": host.db.verify_refs_summary() }),
        ctx.user_id.as_str(),
    ))
}

/// `GET /api/db/verify-refs/scopes` — the storage scopes that the database
/// catalog names, besides the personal scope. `lastdb db verify-refs
/// --all-scopes` checks the personal scope and each of these.
pub(in crate::exec) async fn execute_db_verify_refs_scopes_route(
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    match host.db.atom_counter_scopes().await {
        Ok(scopes) => json_ok(&envelope(
            &serde_json::json!({ "scopes": scopes }),
            ctx.user_id.as_str(),
        )),
        Err(error) => mapped_error_response("verify-refs scopes failed", error, ctx),
    }
}
