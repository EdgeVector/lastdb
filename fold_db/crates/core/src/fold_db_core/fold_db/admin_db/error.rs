//! Mapping of inner admin-op errors onto the owner-socket error type.

use crate::error::FoldDbError;

/// Wrap an inner admin-op [`SchemaError`] for the owner socket **without
/// throwing away a caller fault**.
///
/// Every wrapper in this file used to end with
/// `.map_err(|e| FoldDbError::Database(format!("op: {e}")))`. The label reads
/// well in a log line and is destructive on the wire: `Database` is the one
/// [`FoldDbError`] variant the socket's `HostError` mapping has no typed arm
/// for, so it collapses to `500 Internal Server Error`. A malformed
/// `after_key` and an unknown schema name are both pure caller input, and both
/// arrive here already typed one layer down — the label erased that and
/// reported the caller's own typo as a server bug. The socket logs every 5xx at
/// ERROR and observability promotes each into its own Sentry issue, so a single
/// bad cursor became a storm with zero users affected (the same shape as Sentry
/// `7620011366` and `7641868650`).
///
/// So: classify, then label. A variant that can only mean "the request was
/// wrong" keeps its type and gets its own 4xx. Everything else — including
/// [`SchemaError::InvalidData`], which this codebase uses for store and IO
/// failures as well as bad input — keeps the operation label and stays a `500`.
/// Under-classifying costs a noisy 500; over-classifying tells a caller its
/// request was permanently malformed when the store merely hiccuped, and the
/// caller then drops the work. Prefer the noisy 500.
pub(super) fn admin_op_error(operation: &str, error: crate::schema::SchemaError) -> FoldDbError {
    use crate::schema::SchemaError as SE;
    match error {
        // Unambiguously the caller's request: a name that does not resolve, a
        // cursor outside the keyspace, a denial, a lost CAS race, an oversized
        // payload, a full disk. Each has exactly one typed status.
        caller_fault @ (SE::NotFound(_)
        | SE::InvalidCursor(_)
        | SE::InvalidField(_)
        | SE::Blocked(_)
        | SE::PermissionDenied(_)
        | SE::InvalidPermission(_)
        | SE::CatalogMembershipDenied { .. }
        | SE::TransportNotAttested { .. }
        | SE::CasConflict { .. }
        | SE::AtomContentTooLarge { .. }
        | SE::StorageFull { .. }) => FoldDbError::Schema(caller_fault),
        // Overloaded or internal: keep the operation label and stay a 500.
        other => FoldDbError::Database(format!("{operation}: {other}")),
    }
}
