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
    Err(HostError::new(status, message(&response.body)))
}

fn message(body: &[u8]) -> String {
    if let Ok(value) = serde_json::from_slice::<Value>(body) {
        if let Some(text) = text_at(&value, "message") {
            return text;
        }
        if let Some(text) = text_at(&value, "reason") {
            return text;
        }
        if let Some(code) = text_at(&value, "error") {
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
        "payment_required" => "payment is required",
        "invalid_share_configuration" => "invalid share configuration",
        _ => "request refused",
    }
}
