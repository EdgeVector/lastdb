//! Pure helpers for erase messages.

use crate::schema::types::key_value::KeyValue;

/// Human-readable rendering of a KeyValue for log/error messages.
pub(crate) fn describe_key(kv: &KeyValue) -> String {
    match (&kv.hash, &kv.range) {
        (Some(h), Some(r)) => format!("hash='{h}' range='{r}'"),
        (Some(h), None) => format!("hash='{h}'"),
        (None, Some(r)) => format!("range='{r}'"),
        (None, None) => "<single>".to_string(),
    }
}
