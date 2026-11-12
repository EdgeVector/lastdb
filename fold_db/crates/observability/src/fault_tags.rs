//! Sentry tags for a sanitized fault event.
//!
//! The service name is the one passed to [`crate::init_lambda`]. Function
//! name and version come from the Lambda environment. A value that does not
//! match its fixed character set is omitted.

use std::collections::BTreeMap;
use std::env;

use once_cell::sync::OnceCell;

static LAMBDA_SERVICE_NAME: OnceCell<&'static str> = OnceCell::new();

/// Record the service name supplied to [`crate::init_lambda`].
pub(crate) fn remember_lambda_service(service_name: &'static str) {
    let _ = LAMBDA_SERVICE_NAME.set(service_name);
}

fn lambda_service_name() -> Option<&'static str> {
    LAMBDA_SERVICE_NAME.get().copied()
}

/// Tags attached to one sanitized Sentry fault event.
pub fn fault_tags(category: &str) -> BTreeMap<String, String> {
    let mut tags = BTreeMap::from([
        ("fault_category".to_string(), category.to_string()),
        ("os".to_string(), env::consts::OS.to_string()),
    ]);
    if let Some(service) = lambda_service_name().and_then(service_tag) {
        tags.insert("service".to_string(), service);
    }
    if let Some(version) = function_version_tag() {
        tags.insert("function_version".to_string(), version);
    }
    if let Some(name) = function_name_tag() {
        tags.insert("function_name".to_string(), name);
    }
    tags
}

/// Keep a service name that is ASCII alphanumeric or `_`.
pub fn service_tag(value: &str) -> Option<String> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return None;
    }
    Some(value.to_string())
}

/// Read `AWS_LAMBDA_FUNCTION_VERSION` when it is `$LATEST` or digits.
pub fn function_version_tag() -> Option<String> {
    let value = env::var("AWS_LAMBDA_FUNCTION_VERSION").ok()?;
    let value = value.trim();
    let allowed = value == "$LATEST"
        || (!value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()));
    allowed.then(|| value.to_string())
}

/// Read `AWS_LAMBDA_FUNCTION_NAME` when it is ASCII alphanumeric, `_`, or `-`.
pub fn function_name_tag() -> Option<String> {
    let value = env::var("AWS_LAMBDA_FUNCTION_NAME").ok()?;
    let value = value.trim();
    let allowed = !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-');
    allowed.then(|| value.to_string())
}
