//! Local access to existing staged records. No new stage or sender exists.
use super::*;
static DECISIONS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
async fn decision_guard() -> tokio::sync::MutexGuard<'static, ()> {
    DECISIONS.lock().await
}

/// `GET /api/sharing/deliveries` — list pending staged deliveries.
pub async fn execute_list_deliveries(ctx: &AccessContext, host: &Host) -> UdsResponse {
    match list_pending_deliveries_in_ops(host.db.db_ops()).await {
        Ok(deliveries) => ok_json(&serde_json::json!({ "deliveries": deliveries }), ctx),
        Err(e) => error_json(500, &format!("list deliveries: {e}"), ctx),
    }
}

/// `POST /api/sharing/deliveries/{id}/reject` — discard staged delivery.
pub async fn execute_reject_delivery(
    delivery_id: &str,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let _decision = decision_guard().await;
    match get_pending_delivery_in_ops(host.db.db_ops(), delivery_id).await {
        Ok(None) => return error_json(404, &format!("Delivery not found: {delivery_id}"), ctx),
        Ok(Some(_)) => {}
        Err(e) => return error_json(500, &format!("get delivery: {e}"), ctx),
    }
    if let Err(e) = remove_pending_delivery_in_ops(host.db.db_ops(), delivery_id).await {
        return error_json(500, &format!("remove delivery: {e}"), ctx);
    }
    ok_json(
        &serde_json::json!({ "delivery_id": delivery_id, "status": "rejected" }),
        ctx,
    )
}
