//! Owner-socket file-count and upload-order audit.

use super::*;

/// `POST /api/db/pin-log-audit` — bounded, resumable, read-only pin-log plane
/// audit. Classifies entry rows via durable published-F; never deletes.
pub(in crate::exec) async fn execute_db_pin_log_audit_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize, Default)]
    struct Body {
        /// Cap on pin-log keys this call walks before returning a resume cursor.
        #[serde(default)]
        max_keys: Option<usize>,
        /// Resume cursor from a previous call's `next_after_key`.
        #[serde(default)]
        after_key: Option<String>,
    }
    let body: Body = if req.body.is_empty() {
        Body::default()
    } else {
        match serde_json::from_slice(&req.body) {
            Ok(b) => b,
            Err(e) => {
                return error_response(
                    400,
                    &format!("invalid /api/db/pin-log-audit body: {e}"),
                    ctx,
                )
            }
        }
    };

    {
        match host
            .db
            .audit_pin_log(body.max_keys, body.after_key.as_deref())
            .await
        {
            Ok(report) => json_ok(&envelope(
                &serde_json::json!({ "pin_log_audit": report }),
                ctx.user_id.as_str(),
            )),
            // A malformed resume cursor arrives as
            // `SchemaError::InvalidCursor` and maps to 400 on its own; this
            // used to sniff the message text for "after_key must be", which
            // classified one message and missed every other caller fault the
            // route can raise.
            Err(e) => mapped_error_response("pin-log-audit failed", e, ctx),
        }
    }
}
