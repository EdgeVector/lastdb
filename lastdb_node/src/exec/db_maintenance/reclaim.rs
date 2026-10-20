//! Owner-socket local meter reclaim and delete-ledger routes.

use super::*;

/// `POST /api/db/reclaim-keep-small-snapshot` — drop the current local
/// meter-snapshot group without loading it. The store-side proof requires the
/// group to contain only `keep_small:meters`; a missing snapshot re-enters the
/// normal local bootstrap path.
pub(in crate::exec) async fn execute_db_reclaim_keep_small_snapshot_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct Body {
        #[serde(default)]
        dry_run: Option<bool>,
    }
    let body: Body = if req.body.is_empty() {
        Body { dry_run: None }
    } else {
        match serde_json::from_slice(&req.body) {
            Ok(b) => b,
            Err(e) => {
                return error_response(
                    400,
                    &format!("invalid /api/db/reclaim-keep-small-snapshot body: {e}"),
                    ctx,
                )
            }
        }
    };
    let dry_run = body.dry_run.unwrap_or(true);
    let db = std::sync::Arc::clone(&host.db);
    let outcome =
        tokio::task::spawn_blocking(move || db.reclaim_keep_small_snapshot(dry_run)).await;
    match outcome {
        Ok(Ok(report)) => json_ok(&envelope(
            &serde_json::json!({ "reclaim_keep_small_snapshot": report }),
            ctx.user_id.as_str(),
        )),
        Ok(Err(e)) => error_response(
            409,
            &format!("reclaim-keep-small-snapshot refused: {e}"),
            ctx,
        ),
        Err(e) => error_response(
            500,
            &format!("reclaim-keep-small-snapshot task failed: {e}"),
            ctx,
        ),
    }
}

/// `GET /api/db/delete-ledger` — read the durable atom hard-delete audit trail.
///
/// The read side of the ledger written by `purge` and `gc-atoms --execute`.
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
