//! Received signed-log route handlers.
//!
//! Read a received database, append a proposal, and read that proposal's status.

use super::*;
use crate::registered_share::{self, RegisteredShare};

pub(super) async fn submit_signed(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
    share: &RegisteredShare,
) -> UdsResponse {
    if !share.whole_database() {
        return error_response(403, "this share is read-only", ctx);
    }
    let Some(id) = req.header("Idempotency-Key").and_then(mutation_id) else {
        return error_response(400, "a shared write requires a UUID Idempotency-Key", ctx);
    };
    match crate::log_sync::submit_share_write(host, share, &id, &req.body).await {
        Ok(sequence) => accepted("submitted", &id, sequence),
        Err(error) => contribution_failure(&error, ctx),
    }
}

/// The 202 answer of a proposal that the log accepted.
fn accepted(status: &str, id: &uuid::Uuid, sequence: u64) -> UdsResponse {
    let mut response = json_ok(&json!({
        "ok": true, "status": status, "mutation_id": id.to_string(),
        "submission_sequence": sequence,
    }));
    if response.status == 200 {
        response.status = 202;
        response.reason = "Accepted";
    }
    response
}

fn contribution_failure(
    error: &crate::log_sync::ContributionError,
    ctx: &AccessContext,
) -> UdsResponse {
    use crate::log_sync::ContributionError;
    match error {
        ContributionError::Unavailable => render(
            Err(HostError::new(503, error.to_string())
                .with_retry_after(super::database_share::RETRY_AFTER_SECS)),
            ctx,
        ),
        ContributionError::Forbidden => error_response(403, &error.to_string(), ctx),
        ContributionError::Invalid => error_response(400, &error.to_string(), ctx),
        ContributionError::Conflict => error_response(409, &error.to_string(), ctx),
    }
}

/// `POST /api/databases/{id}/change`: one change to one record, from the
/// proposal document. The node decides. When it may sign a direct entry now
/// (the same question `/v1/write` asks), the change is applied and the answer
/// is `applied`. Otherwise it is queued as a proposal and the answer is
/// `pending`. `share` is the received database, or `None` for an owned or the
/// personal database, which always apply.
pub(super) async fn change_record(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
    selector: &str,
    share: Option<&RegisteredShare>,
) -> UdsResponse {
    let Some(id) = req.header("Idempotency-Key").and_then(mutation_id) else {
        return error_response(400, "a change requires a UUID Idempotency-Key", ctx);
    };
    if let Err(error) = super::app_api::require_change_grant(&req.body) {
        return render(Err(error), ctx);
    }
    let propose = match share {
        None => false,
        Some(share) if !share.whole_database() => {
            return error_response(403, "this share is read-only", ctx);
        }
        // A change this node already queued stays a proposal, whatever the lease says now.
        Some(share) => match crate::log_sync::share_write_submitted(host, share, &id) {
            Ok(queued) => queued || !super::app_api::direct_write_allowed(host, share),
            Err(error) => return contribution_failure(&error, ctx),
        },
    };
    if let (true, Some(share)) = (propose, share) {
        return queue(host, share, &id, &req.body, ctx).await;
    }
    match apply(host, selector, &req.body).await {
        Ok(()) => json_ok(&json!({
            "ok": true, "status": "applied", "mutation_id": id.to_string(),
        })),
        // The lease margin can pass between the decision and the write. The
        // call answers applied or pending, so the node queues the change once.
        Err(error)
            if error.status == 403
                && share
                    .is_some_and(|share| !super::app_api::direct_write_allowed(host, share)) =>
        {
            match share {
                Some(share) => queue(host, share, &id, &req.body, ctx).await,
                None => render(Err(error), ctx),
            }
        }
        Err(error) => render(Err(error), ctx),
    }
}

async fn queue(
    host: &Host,
    share: &RegisteredShare,
    id: &uuid::Uuid,
    body: &[u8],
    ctx: &AccessContext,
) -> UdsResponse {
    match crate::log_sync::submit_share_write(host, share, id, body).await {
        Ok(sequence) => accepted("pending", id, sequence),
        Err(error) => contribution_failure(&error, ctx),
    }
}

async fn apply(host: &Host, selector: &str, body: &[u8]) -> Result<(), HostError> {
    let change = super::app_api::parse_change(body, selector)?;
    super::app_api::apply_change(host, change).await
}

/// A non-nil lowercase hyphenated UUID, the proposal id.
fn mutation_id(text: &str) -> Option<uuid::Uuid> {
    let id = uuid::Uuid::parse_str(text).ok()?;
    (!id.is_nil() && id.to_string() == text).then_some(id)
}

pub(super) async fn read_signed(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
    share: &RegisteredShare,
) -> UdsResponse {
    let Ok(store) = registered_share::receiver_store(host, &share.id).await else {
        return error_response(503, "database store is unavailable", ctx);
    };
    crate::log_sync::note_receiver(&host.home, share.id.as_str());
    host.with_records(
        store,
        super::execute_data_route_with_timeout(route, req, ctx, host),
    )
    .await
}

pub(super) fn submission_status(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
    share: &RegisteredShare,
) -> UdsResponse {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct StatusRequest {
        mutation_id: String,
    }
    if req.body.len() > 256 {
        return error_response(400, "invalid submission status request", ctx);
    }
    let Ok(body) = serde_json::from_slice::<StatusRequest>(&req.body) else {
        return error_response(400, "invalid submission status request", ctx);
    };
    let Some(id) = mutation_id(&body.mutation_id) else {
        return error_response(400, "invalid submission status request", ctx);
    };
    match crate::log_sync::share_write_status(host, share, &id) {
        Ok(status) => {
            json_ok(&json!({"ok": true, "mutation_id": body.mutation_id, "status": status}))
        }
        Err(_) => error_response(503, "submission status is unavailable", ctx),
    }
}
