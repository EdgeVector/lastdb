use super::*;

/// `GET /api/storage/home` — one persisted snapshot point-read.
///
/// The explicit reconcile route owns filesystem and ownership work. This read
/// validates only in-memory arithmetic on the decoded snapshot, so it cannot
/// turn an operator status request into a filesystem or product-store scan.
pub(super) async fn execute_home_storage_route(ctx: &AccessContext, host: &Host) -> UdsResponse {
    use crate::home_storage::{HomeStorageReport, HOME_STORAGE_SNAPSHOT_KEY};

    let report = match host
        .db
        .db_ops()
        .metadata()
        .get_typed::<HomeStorageReport>(HOME_STORAGE_SNAPSHOT_KEY)
        .await
    {
        Ok(Some(report)) => report.validated_for_read(),
        Ok(None) => HomeStorageReport::missing(),
        Err(error) => {
            return error_response(
                500,
                &format!("home storage snapshot read failed: {error}"),
                ctx,
            )
        }
    };
    json_ok(&envelope(
        &serde_json::json!({ "home_storage": report }),
        ctx.user_id.as_str(),
    ))
}

/// `POST /api/storage/home/reconcile` — one bounded filesystem page.
///
/// The durable checkpoint contains the remaining path queue. Each request
/// measures at most the requested number of entries, writes an honest partial
/// snapshot, and returns the opaque next cursor. Symlinks are never followed.
pub(super) async fn execute_home_storage_reconcile_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    use crate::home_storage::{
        reconcile_page, HomeStorageReconcileCheckpoint, HOME_STORAGE_RECONCILE_KEY,
        HOME_STORAGE_RECONCILE_WORK_DEFAULT, HOME_STORAGE_RECONCILE_WORK_MAX,
        HOME_STORAGE_SNAPSHOT_KEY,
    };

    #[derive(Deserialize, Default)]
    struct ReconcileBody {
        work_budget: Option<u64>,
        page_size: Option<u64>,
        #[serde(default)]
        reset: bool,
    }

    let body = match serde_json::from_slice::<ReconcileBody>(&req.body) {
        Ok(body) => body,
        Err(_) if req.body.is_empty() => ReconcileBody::default(),
        Err(error) => return error_response(400, &format!("invalid reconcile body: {error}"), ctx),
    };
    if body.work_budget.is_some() && body.page_size.is_some() {
        return error_response(400, "set only one of work_budget or page_size", ctx);
    }
    let work_budget = body
        .work_budget
        .or(body.page_size)
        .unwrap_or(HOME_STORAGE_RECONCILE_WORK_DEFAULT as u64)
        .clamp(1, HOME_STORAGE_RECONCILE_WORK_MAX as u64) as usize;
    let metadata = host.db.db_ops().metadata();
    let stored = if body.reset {
        None
    } else {
        match metadata
            .get_typed::<HomeStorageReconcileCheckpoint>(HOME_STORAGE_RECONCILE_KEY)
            .await
        {
            Ok(value) => value,
            Err(error) => {
                return error_response(
                    500,
                    &format!("home reconcile checkpoint read failed: {error}"),
                    ctx,
                )
            }
        }
    };
    let mut checkpoint = match stored {
        Some(checkpoint) if !checkpoint.finished => checkpoint,
        _ => HomeStorageReconcileCheckpoint::start(chrono::Utc::now()),
    };
    let visited = reconcile_page(&host.home, &mut checkpoint, work_budget);
    let report = checkpoint.report(chrono::Utc::now());

    if let Err(error) = metadata
        .put_typed_durable(HOME_STORAGE_RECONCILE_KEY, &checkpoint)
        .await
    {
        return error_response(
            500,
            &format!("persist home reconcile checkpoint: {error}"),
            ctx,
        );
    }
    if let Err(error) = metadata
        .put_typed_durable(HOME_STORAGE_SNAPSHOT_KEY, &report)
        .await
    {
        return error_response(500, &format!("persist home storage snapshot: {error}"), ctx);
    }

    json_ok(&envelope(
        &serde_json::json!({
            "reconciliation": {
                "run_id": checkpoint.run_id,
                "visited": visited as u64,
                "visited_total": checkpoint.visited_entries,
                "work_budget": work_budget as u64,
                "pending_entries": checkpoint.pending_work.len() as u64,
                "resume_cursor": checkpoint.resume_cursor(),
                "finished": checkpoint.finished,
                "complete": report.complete,
                "unresolved_scopes": report.unresolved_scopes.clone(),
                "report": report,
            }
        }),
        ctx.user_id.as_str(),
    ))
}
