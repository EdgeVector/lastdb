//! Handler error type and its conversions.

use super::*;

/// An error from a shared handler, carrying the HTTP status class and an
/// owner-visible diagnostic message. The caller renders it through
/// [`crate::envelope::owner_or_content_free`]: the owner sees `message` (for
/// `4xx`/`503`), every non-owner stays content-free, and a `500` is content-free
/// even for the owner (**I4**).
#[derive(Debug, Clone)]
pub struct HostError {
    pub status: u16,
    pub message: String,
    structured_body: Option<Value>,
}

impl HostError {
    #[must_use]
    pub fn new(status: u16, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
            structured_body: None,
        }
    }

    /// An owner-visible structured rejection. Remote apps still receive the
    /// content-free status phrase through [`render`]. `pub` so other socket
    /// executors (e.g. `lastdb_node::deliver`) can build the same
    /// `kind`/`remediation` shape as this crate's own rejections instead of
    /// falling back to a flat message.
    #[must_use]
    pub fn structured(status: u16, message: impl Into<String>, body: Value) -> Self {
        Self {
            status,
            message: message.into(),
            structured_body: Some(body),
        }
    }

    /// A `500` — always rendered content-free, so its message never leaks
    /// (**I4**). The message is still carried for server-side logging clarity.
    pub(super) fn internal(message: impl Into<String>) -> Self {
        Self::new(500, message)
    }
}

/// Map a core [`FoldDbError`] to a socket-facing status, matching the full
/// node's [`HandlerError`] mapping so isolation denials stay `400` (content-
/// free for a non-owner) rather than collapsing to an opaque `500`.
impl From<FoldDbError> for HostError {
    fn from(err: FoldDbError) -> Self {
        match &err {
            FoldDbError::Schema(schema_err) => Self::from(schema_err.clone()),
            FoldDbError::Permission(msg) => Self::new(401, msg.clone()),
            FoldDbError::Config(msg) | FoldDbError::Serialization(msg) => {
                Self::new(400, msg.clone())
            }
            // Permanent cloud storage cap — surface as 429 (not 500) so the
            // render() choke point does not ERROR-log every attempt to Sentry.
            FoldDbError::QuotaExceeded(msg) => Self::new(
                429,
                format!(
                    "storage quota exceeded: {msg}; free space or run `lastdb cloud upgrade`, then `lastdb cloud status`"
                ),
            ),
            FoldDbError::InProgress(msg) => Self::new(503, msg.clone()),
            other => Self::internal(other.to_string()),
        }
    }
}

impl From<SchemaError> for HostError {
    fn from(err: SchemaError) -> Self {
        match &err {
            SchemaError::NotFound(msg) => Self::new(404, msg.clone()),
            // Blocked and cross-app/isolation ACL denials (I3c) both map to a
            // content-free 400 for a non-owner, matching the full node's
            // HandlerError::BadRequest arm.
            SchemaError::Blocked(msg) | SchemaError::PermissionDenied(msg) => {
                Self::new(400, msg.clone())
            }
            schema_err @ SchemaError::CatalogMembershipDenied {
                db_locator,
                schema_name,
            } => Self::structured(
                403,
                schema_err.to_string(),
                serde_json::json!({
                    "error": "catalog_membership_denied",
                    "db_locator": db_locator,
                    "schema_name": schema_name,
                    "message": schema_err.to_string(),
                }),
            ),
            SchemaError::InvalidPermission(msg) => Self::new(401, msg.clone()),
            // A caller-supplied resume cursor that names no key in the walked
            // keyspace. Typed separately from `InvalidData` (which this
            // codebase also uses for store failures) precisely so paging admin
            // routes can answer 400 instead of a Sentry-promoting 500.
            SchemaError::InvalidCursor(msg) => Self::new(400, msg.clone()),
            schema_err @ SchemaError::TransportNotAttested { .. } => Self::new(
                403,
                schema_err
                    .transport_not_attested_body()
                    .unwrap_or_else(|| serde_json::json!({}))
                    .to_string(),
            ),
            schema_err @ SchemaError::CasConflict { .. } => Self::new(
                409,
                schema_err
                    .cas_conflict_body()
                    .unwrap_or_else(|| serde_json::json!({}))
                    .to_string(),
            ),
            schema_err @ SchemaError::AtomContentTooLarge { .. } => Self::new(
                413,
                schema_err
                    .atom_content_too_large_body()
                    .unwrap_or_else(|| serde_json::json!({}))
                    .to_string(),
            ),
            // A full disk is the host's fault, not the request's. `400` sent
            // the writer to audit a payload that was never wrong; `507` names
            // the condition and `storage_full_body` says the same write will
            // succeed once space is freed.
            schema_err @ SchemaError::StorageFull { .. } => Self::new(
                507,
                schema_err
                    .storage_full_body()
                    .unwrap_or_else(|| serde_json::json!({}))
                    .to_string(),
            ),
            schema_err @ SchemaError::PersistQueueFull { .. } => Self::new(
                503,
                schema_err
                    .persist_queue_full_body()
                    .unwrap_or_else(|| serde_json::json!({}))
                    .to_string(),
            ),
            // Transient capture backpressure. `400` told the writer its payload
            // was wrong when it never was; `503` plus `capture_queue_full_body`
            // says the same write succeeds on retry, with no operator action.
            schema_err @ SchemaError::CaptureQueueFull { .. } => Self::new(
                503,
                schema_err
                    .capture_queue_full_body()
                    .unwrap_or_else(|| serde_json::json!({}))
                    .to_string(),
            ),
            other => Self::new(400, other.to_string()),
        }
    }
}

/// Render a shared handler's `Result` into the owner-socket [`UdsResponse`]: the
/// success payload is wrapped in the `{ ok, ...data, user_hash }` envelope and
/// serialized; an error is mapped through [`owner_or_content_free`] so a
/// non-owner stays content-free and a `500` never leaks its message (**I4**).
/// Both socket executors funnel every route's result through this one choke
/// point so the wire shape (and the I4 property) cannot drift.
#[must_use]
pub fn render(result: Result<Value, HostError>, ctx: &AccessContext) -> UdsResponse {
    match result {
        Ok(payload) => json_ok(&envelope(&payload, ctx.user_id.as_str())),
        Err(e) => {
            // A 5xx goes out content-free (I4), so the carried message is
            // ONLY visible here — an unlogged 500 leaves the owner staring at
            // "Internal Server Error" with an empty node log.
            // Permanent quota (429) is operator-actionable and must not ERROR
            // on every retry (Sentry issue 7654526497 storm). Sample as warn.
            if e.status == 429
                && (e.message.contains("quota exceeded")
                    || e.message.contains("QUOTA_EXCEEDED")
                    || e.message.contains("storage_quota_exceeded"))
            {
                tracing::warn!(
                    target: "lastdb_host::handlers",
                    status = e.status,
                    "handler permanent quota: {}",
                    e.message
                );
            } else if e.status == 507 {
                // A full disk (507) is the same shape as the quota case above:
                // operator-actionable, permanent until someone frees space, and
                // repeated by every write that follows. At ERROR one episode
                // becomes hundreds of identical Sentry issues (issue
                // 7620061902: 207 events in 8 hours, 0 users affected).
                tracing::warn!(
                    target: "lastdb_host::handlers",
                    status = e.status,
                    "handler storage full: {}",
                    e.message
                );
            } else if e.status >= 500 && e.status != 503 {
                tracing::error!(
                    target: "lastdb_host::handlers",
                    status = e.status,
                    "handler error: {}",
                    e.message
                );
            }
            match e.structured_body.as_ref() {
                Some(body) => owner_json_or_content_free(e.status, body, ctx),
                None => owner_or_content_free(e.status, &e.message, ctx),
            }
        }
    }
}

impl From<ReadBusy> for HostError {
    fn from(busy: ReadBusy) -> Self {
        // Match the full node's read-gate shed: a 503 with a human-readable
        // retry hint (owner-visible; content-free for a non-owner).
        Self::new(
            503,
            format!(
                "node is busy: too many concurrent reads; retry after {}s",
                busy.retry_after_secs
            ),
        )
    }
}
