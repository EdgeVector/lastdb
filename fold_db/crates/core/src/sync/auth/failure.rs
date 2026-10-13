//! Decode structured cloud refusals before legacy success response types.

use crate::sync::error::{SyncError, SyncResult};
use serde::de::DeserializeOwned;
use serde_json::Value;

pub(super) fn decode_service_response<T: DeserializeOwned>(
    action: &str,
    value: Value,
) -> SyncResult<T> {
    reject_structured_failure(action, &value)?;
    Ok(serde_json::from_value(value)?)
}

/// String-only legacy failures stay with the caller's existing refusal handling.
/// This is deliberately not a transport-wide `ok: false` check: some callers
/// accept missing pointers, lock conflicts, or CAS preconditions as outcomes.
pub(super) fn reject_structured_failure(action: &str, value: &Value) -> SyncResult<()> {
    if value.get("ok").and_then(Value::as_bool) == Some(true) {
        return Ok(());
    }
    let error = match value.get("error") {
        Some(error) if error.is_object() => error,
        Some(error)
            if error.is_string()
                && value.get("code").is_some()
                && value.get("retryable").is_some() =>
        {
            value
        }
        _ => return Ok(()),
    };
    let action = if valid_token(action) {
        action
    } else {
        "cloud_request"
    };
    let code = error
        .get("code")
        .and_then(Value::as_str)
        .filter(|code| valid_token(code))
        .ok_or_else(|| {
            SyncError::Serialization(format!("{action}: invalid structured cloud failure code"))
        })?;
    let retryable = error
        .get("retryable")
        .and_then(Value::as_bool)
        .ok_or_else(|| {
            SyncError::Serialization(format!("{action}: invalid structured cloud retry hint"))
        })?;
    Err(SyncError::CloudServiceResponse {
        action: action.to_string(),
        code: code.to_string(),
        retryable,
    })
}

fn valid_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 100
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}
