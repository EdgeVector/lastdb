//! Safe response bodies for typed schema errors.

use super::SchemaError;

impl SchemaError {
    /// The discriminated `409 cas_conflict` response body for a
    /// [`Self::CasConflict`]. The HTTP layer renders this verbatim so a client
    /// (e.g. lastgit's ref-update path) can key on `error == "cas_conflict"`
    /// and retry its check-then-set. Returns `None` for every other variant.
    pub fn cas_conflict_body(&self) -> Option<serde_json::Value> {
        match self {
            Self::CasConflict {
                schema,
                key,
                expected,
                actual,
            } => Some(serde_json::json!({
                "ok": false,
                "error": "cas_conflict",
                "schema": schema,
                "key": key,
                "expected": expected,
                "actual": actual,
                "message": self.to_string(),
            })),
            _ => None,
        }
    }

    /// The discriminated `413 field_value_too_large` response body for a
    /// [`Self::RecordTooLarge`]. Clients key on `error == "field_value_too_large"`.
    /// Returns `None` for every other variant.
    pub fn field_value_too_large_body(&self) -> Option<serde_json::Value> {
        match self {
            Self::RecordTooLarge { size, limit } => Some(serde_json::json!({
                "ok": false,
                "error": "field_value_too_large",
                "scope": "record",
                "size": size,
                "limit": limit,
                "hint": crate::content_limit::RECORD_TOO_LARGE_HINT,
                "message": self.to_string(),
            })),
            _ => None,
        }
    }

    /// The discriminated `507 storage_full` response body for a
    /// [`Self::StorageFull`]. Clients key on `error == "storage_full"` to tell a
    /// host condition from a rejected payload — retrying the same write is
    /// correct here, but only once an operator has freed space. Returns `None`
    /// for every other variant.
    pub fn storage_full_body(&self) -> Option<serde_json::Value> {
        match self {
            Self::StorageFull { detail } => Some(serde_json::json!({
                "ok": false,
                "error": "storage_full",
                "detail": detail,
                "retryable": true,
                "hint": "the host filesystem is out of space; free disk space, then retry the same write",
                "message": self.to_string(),
            })),
            _ => None,
        }
    }

    /// Discriminated `503 persist_queue_full` body. Clients retry after drain.
    pub fn persist_queue_full_body(&self) -> Option<serde_json::Value> {
        match self {
            Self::PersistQueueFull { schema, kind } => Some(serde_json::json!({
                "ok": false,
                "error": "persist_queue_full",
                "schema": schema,
                "kind": kind,
                "retryable": true,
                "message": self.to_string(),
            })),
            _ => None,
        }
    }
}
