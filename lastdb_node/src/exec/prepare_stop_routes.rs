//! `POST /api/system/prepare-stop` and its cancel: the write hold and the upload before a stop.

use fold_db::access::AccessContext;
use serde::Deserialize;
use serde_json::json;

use super::admin_routes::{require_empty_json_body, require_owner};
use super::reply::{json_ok, render, HostError};
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

/// The body of `POST /api/system/prepare-stop`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PrepareStopRequest {
    #[serde(default = "default_upload")]
    upload: bool,
    #[serde(default = "default_deadline_ms")]
    deadline_ms: u64,
    #[serde(default = "default_hold_ms")]
    hold_ms: u64,
}

const fn default_upload() -> bool {
    true
}
const fn default_deadline_ms() -> u64 {
    120_000
}
const fn default_hold_ms() -> u64 {
    300_000
}

/// The state a prepare-stop answer reports: the spool head and the acknowledged position of the
/// personal database, and what the cloud holds.
fn prepare_stop_state(host: &Host, detail: &str) -> UdsResponse {
    let status = host.db.db_ops().native_runtime().accepted_status();
    let cloud = if crate::cloud::relay_base_url(&host.home).is_none() {
        "off"
    } else if status.acked >= status.head {
        "current"
    } else {
        "behind"
    };
    json_ok(&json!({
        "head": status.head, "acked": status.acked, "cloud": cloud, "detail": detail,
    }))
}

/// `POST /api/system/prepare-stop`: refuse new writes, then send the spool to the cloud.
pub(super) async fn prepare_stop(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    use std::time::{Duration, Instant};

    use crate::prepare_stop as hold;

    let parsed: Result<PrepareStopRequest, HostError> = (|| {
        require_owner(ctx, "prepare-stop is owner-only")?;
        let request: PrepareStopRequest = if req.body.is_empty() {
            PrepareStopRequest {
                upload: default_upload(),
                deadline_ms: default_deadline_ms(),
                hold_ms: default_hold_ms(),
            }
        } else {
            serde_json::from_slice(&req.body).map_err(|e| {
                HostError::new(
                    400,
                    format!(
                        "invalid /api/system/prepare-stop body: expected {{upload, deadline_ms, hold_ms}}: {e}"
                    ),
                )
            })?
        };
        if request.hold_ms == 0 || request.hold_ms > hold::MAX_HOLD_MS {
            return Err(HostError::new(
                400,
                format!("hold_ms must be from 1 to {}", hold::MAX_HOLD_MS),
            ));
        }
        if request.deadline_ms > hold::MAX_DEADLINE_MS {
            return Err(HostError::new(
                400,
                format!("deadline_ms must be at most {}", hold::MAX_DEADLINE_MS),
            ));
        }
        Ok(request)
    })();
    let request = match parsed {
        Ok(request) => request,
        Err(error) => return render(Err(error), ctx),
    };
    let began = Instant::now();
    let deadline = Duration::from_millis(request.deadline_ms);
    if hold::begin(Duration::from_millis(request.hold_ms)) {
        // A hold is set: answer with the state now. The first call keeps its own upload.
        return prepare_stop_state(host, &hold::detail());
    }
    tracing::info!(
        target: "lastdbd::prepare_stop",
        upload = request.upload,
        deadline_ms = request.deadline_ms,
        hold_ms = request.hold_ms,
        "prepare-stop: new writes are refused"
    );
    let in_flight = hold::wait_idle(deadline).await;
    let detail = if in_flight > 0 {
        format!("{in_flight} writes were still running when the deadline passed")
    } else if request.upload {
        let left = deadline.saturating_sub(began.elapsed());
        match tokio::time::timeout(left, crate::log_sync::send_spool(host)).await {
            Ok(Ok(())) => "uploaded the spool".to_owned(),
            Ok(Err(error)) => error,
            Err(_) => "the deadline passed before the upload finished".to_owned(),
        }
    } else {
        "upload was not asked for".to_owned()
    };
    hold::set_detail(&detail);
    prepare_stop_state(host, &detail)
}

/// `POST /api/system/prepare-stop/cancel`: accept writes again.
pub(super) fn prepare_stop_cancel(req: &UdsRequest, ctx: &AccessContext) -> UdsResponse {
    let res: Result<(), HostError> = (|| {
        require_owner(ctx, "prepare-stop is owner-only")?;
        require_empty_json_body(
            &req.body,
            "invalid /api/system/prepare-stop/cancel body: expected an empty JSON object",
        )?;
        Ok(())
    })();
    if let Err(error) = res {
        return render(Err(error), ctx);
    }
    let was = crate::prepare_stop::cancel();
    tracing::info!(target: "lastdbd::prepare_stop", was_holding = was, "prepare-stop cancelled");
    json_ok(&json!({ "holding": false, "was_holding": was }))
}
