//! Tombstone content shape.
//!
//! A tombstone is an Atom whose `content` is the reserved JSON object
//! `{"_fold_tombstone": {"key": "...", "reason": "...", "actor": "..."}}`.
//! At molecule resolution, tombstones are filtered from query results
//! unless the caller explicitly asks for `include_tombstones = true`.
//!
//! This module owns the shape and the predicate so every read surface
//! recognizes the same wire form. Writing tombstones (e.g. repurposing
//! `MutationType::Delete`) is intentionally NOT done here — that's a
//! follow-up task (T2 in the tombstoning plan).

use serde_json::{json, Value};

/// The single top-level key that identifies a tombstone content object.
pub const TOMBSTONE_KEY: &str = "_fold_tombstone";

/// Build a tombstone content payload — used by future delete-writers (T2)
/// and by tests that need to synthesize a tombstone atom directly.
#[must_use]
pub fn tombstone_content(key: &str, reason: &str, actor: &str) -> Value {
    json!({
        TOMBSTONE_KEY: {
            "key": key,
            "reason": reason,
            "actor": actor,
        }
    })
}

/// Returns `true` if `content` is the reserved tombstone shape:
/// a JSON object with exactly one top-level key (`_fold_tombstone`)
/// whose value is itself a JSON object. Anything else — including
/// objects that happen to *also* carry a `_fold_tombstone` key
/// alongside other fields — returns `false`, so user data that
/// happens to use the name as a regular field is never mistaken for
/// a tombstone.
#[must_use]
pub fn is_tombstone_value(content: &Value) -> bool {
    let Some(obj) = content.as_object() else {
        return false;
    };
    if obj.len() != 1 {
        return false;
    }
    obj.get(TOMBSTONE_KEY).is_some_and(Value::is_object)
}
