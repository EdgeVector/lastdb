//! Turn an owner-route reply into the app API reply.
//!
//! A 2xx body is the owner JSON, including submit's 202. Any other status
//! becomes the app API error object. Status 402 is permission_denied so the
//! payment message is not replaced by "internal error".

use serde_json::Value;

use crate::exec::reply::HostError;
use crate::lastdb_uds::uds_http::UdsResponse;

pub(super) fn pass_owner(response: UdsResponse) -> Result<UdsResponse, HostError> {
    if (200..300).contains(&response.status) {
        if serde_json::from_slice::<Value>(&response.body).is_err() {
            return Err(HostError::internal("owner reply was not JSON"));
        }
        return Ok(response);
    }
    let status = if response.status == 402 {
        403
    } else {
        response.status
    };
    let mut error = HostError::new(status, message(&response.body));
    if let Some(code) = owner_code(&response.body) {
        error = error.with_code(code);
    }
    if let Some(seconds) = retry_after(&response) {
        error = error.with_retry_after(seconds);
    }
    Err(error)
}

/// The owner code that the status hides. A final 409 is not a revision
/// conflict, and a 503 that waits for enrollment is not an unconfirmed answer.
fn owner_code(body: &[u8]) -> Option<&'static str> {
    let value = serde_json::from_slice::<Value>(body).ok()?;
    match text_at(&value, "error")?.as_str() {
        "no_cloud_account" => Some("no_cloud_account"),
        "database_enrolling" => Some("enrolling"),
        _ => None,
    }
}

fn retry_after(response: &UdsResponse) -> Option<u64> {
    response.headers.iter().find_map(|(name, value)| {
        name.eq_ignore_ascii_case("retry-after")
            .then(|| value.parse().ok())
            .flatten()
    })
}

fn message(body: &[u8]) -> String {
    if let Ok(value) = serde_json::from_slice::<Value>(body) {
        let detail = text_at(&value, "message");
        let code = text_at(&value, "error");
        if let (Some(detail), Some(code)) = (&detail, &code) {
            if share_sentence(code) == "request refused" {
                return format!("{code}: {detail}");
            }
        }
        if let Some(detail) = detail {
            return detail;
        }
        if let Some(text) = text_at(&value, "reason") {
            return text;
        }
        if let Some(code) = code {
            return share_sentence(&code).to_owned();
        }
    }
    let text = String::from_utf8_lossy(body);
    let trimmed = text.trim();
    if trimmed.is_empty() {
        "request refused".to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn text_at(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .map(ToOwned::to_owned)
}

fn share_sentence(code: &str) -> &'static str {
    match code {
        "share_revision_conflict" => "the share configuration is at another revision",
        "share_configuration_unconfirmed" => "share configuration is not confirmed",
        "database_enrolling" => "the database is still enrolling with the cloud; retry",
        "no_cloud_account" => "this node has no cloud account",
        "payment_required" => "payment is required",
        "invalid_share_configuration" => "invalid share configuration",
        _ => "request refused",
    }
}
