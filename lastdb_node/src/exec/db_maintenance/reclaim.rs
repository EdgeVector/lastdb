//! Owner-socket delete-ledger route.

use super::*;

/// `GET /api/db/delete-ledger` — read the durable atom hard-delete audit trail.
///
/// The read side of the ledger written by `purge`, `delete` and the delete converge.
/// Answers "was this atom body deleted on purpose, or lost?" from the store's
/// own evidence instead of a ~15h log window. `?limit=N` truncates from the
/// oldest end; absent or 0 returns everything.
pub(in crate::exec) async fn execute_db_delete_ledger_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let limit = query_u64(&req.target, "limit").unwrap_or(0) as usize;
    match host.db.list_atom_delete_ledger(limit).await {
        Ok(entries) => json_ok(&envelope(
            &serde_json::json!({
                "delete_ledger": { "count": entries.len(), "entries": entries }
            }),
            ctx.user_id.as_str(),
        )),
        Err(e) => error_response(500, &format!("delete-ledger failed: {e}"), ctx),
    }
}
