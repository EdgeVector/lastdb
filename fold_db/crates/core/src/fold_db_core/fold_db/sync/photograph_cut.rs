//! Snapshot cuts drain accepted mutations before the storage flush.

use std::sync::Arc;

pub(super) async fn prepare_photograph_cut_components(
    pending_tasks: &crate::fold_db_core::pending_task_tracker::PendingTaskTracker,
    db_ops: &crate::db_operations::DbOperations,
    capture_router: Option<Arc<crate::sync::capture::MutationLogCaptureRouter>>,
) -> Result<(), String> {
    if !pending_tasks
        .wait_for_completion(std::time::Duration::from_secs(30))
        .await
    {
        return Err(format!(
            "photograph cut barrier timed out with {} local task(s) still pending",
            pending_tasks.count()
        ));
    }
    // Each accepted mutation owns a pending task until its schema lane makes
    // the complete envelope durable. The wait above is the photograph cut.
    if let Some(router) = capture_router {
        if !router
            .wait_for_completion(std::time::Duration::from_secs(30))
            .await
        {
            return Err("photograph cut barrier timed out with local capture work pending".into());
        }
    }
    db_ops
        .flush()
        .await
        .map_err(|error| format!("photograph storage flush failed: {error}"))
}
