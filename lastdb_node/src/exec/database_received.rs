//! Received signed-log route handlers.
//!
//! Read a received database, append a proposal, and read that proposal's status.

use super::*;
use crate::registered_share::{self, RegisteredShare};

pub(super) fn submit_signed(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
    share: &RegisteredShare,
) -> UdsResponse {
    use crate::log_sync::ContributionError;
    if !share.whole_database() {
        return error_response(403, "this share is read-only", ctx);
    }
    let Some(id) = req.header("Idempotency-Key").and_then(mutation_id) else {
        return error_response(400, "a shared write requires a UUID Idempotency-Key", ctx);
    };
    match crate::log_sync::submit_share_write(host, share, &id, &req.body) {
        Ok(sequence) => {
            let mut response = json_ok(&json!({
                "ok": true, "status": "submitted", "mutation_id": id.to_string(),
                "submission_sequence": sequence,
            }));
            if response.status == 200 {
                response.status = 202;
                response.reason = "Accepted";
            }
            response
        }
        Err(error @ ContributionError::Unavailable) => render(
            Err(HostError::new(503, error.to_string())
                .with_retry_after(super::database_share::RETRY_AFTER_SECS)),
            ctx,
        ),
        Err(error @ ContributionError::Forbidden) => error_response(403, &error.to_string(), ctx),
        Err(error @ ContributionError::Invalid) => error_response(400, &error.to_string(), ctx),
        Err(error @ ContributionError::Conflict) => error_response(409, &error.to_string(), ctx),
    }
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
    crate::log_sync::note_receiver(share.id.as_str(), &store);
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
