//! Safe response bodies for typed schema errors.

use super::SchemaError;

impl SchemaError {
    /// The discriminated `403 transport_not_attested` response body for a
    /// [`Self::TransportNotAttested`] denial, byte-compatible with the body the
    /// `OwnerVerbGate` middleware and the slice-3 owner verbs emit (clients key on `error == "transport_not_attested"`).
    ///
    /// Returns `None` for every other variant.
    pub fn transport_not_attested_body(&self) -> Option<serde_json::Value> {
        match self {
            Self::TransportNotAttested { verb } => Some(serde_json::json!({
                "ok": false,
                "error": "transport_not_attested",
                "verb": verb,
                "message": "this data operation hit a governed namespace from an \
                            unattested surface (bare loopback TCP); use the owner Unix socket / attested control plane and retry",
            })),
            _ => None,
        }
    }

    /// The discriminated `409 cas_conflict` response body for a
    /// [`Self::CasConflict`]. The HTTP layer renders this verbatim so a client
    /// (e.g. lastgit's ref-update path) can key on `error == "cas_conflict"`
    /// and retry its check-then-set. Returns `None` for every other variant.
    pub fn cas_conflict_body(&self) -> Option<serde_json::Value> {
        match self {
            Self::CasConflict {
                schema,
                field,
                key,
                expected,
                actual,
            } => Some(serde_json::json!({
                "ok": false,
                "error": "cas_conflict",
                "schema": schema,
                "field": field,
                "key": key,
                "expected": expected,
                "actual": actual,
                "message": self.to_string(),
            })),
            _ => None,
        }
    }

    /// The discriminated `413 atom_content_too_large` response body for a
    /// [`Self::AtomContentTooLarge`]. Clients key on
    /// `error == "atom_content_too_large"`; `hint` says how to reshape the data
    /// (one record per entry, or a file blob). Returns `None` for every other variant.
    pub fn atom_content_too_large_body(&self) -> Option<serde_json::Value> {
        match self {
            Self::AtomContentTooLarge { size, limit } => Some(serde_json::json!({
                "ok": false,
                "error": "atom_content_too_large",
                "size": size,
                "limit": limit,
                "default_limit": crate::atom::DEFAULT_MAX_ATOM_CONTENT_BYTES,
                "absolute_max_limit": crate::atom::ABSOLUTE_MAX_ATOM_CONTENT_BYTES,
                "env": crate::atom::MAX_ATOM_CONTENT_BYTES_ENV,
                "hint": crate::atom::ATOM_CONTENT_TOO_LARGE_HINT,
                "docs": "fold_db/docs/ATOM_CONTENT_SIZE_LIMIT.md",
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

    /// True when this error is the host running out of disk space.
    ///
    /// The reporting paths use this to keep an operator condition out of the
    /// channels reserved for code faults.
    #[must_use]
    pub const fn is_storage_full(&self) -> bool {
        matches!(self, Self::StorageFull { .. })
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
