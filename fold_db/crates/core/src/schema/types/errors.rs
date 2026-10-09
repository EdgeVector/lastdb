use serde::Serialize;
use std::fmt;

#[derive(Debug, Clone, Serialize)]
pub enum SchemaError {
    NotFound(String),
    Blocked(String),
    InvalidField(String),
    InvalidPermission(String),
    InvalidTransform(String),
    InvalidData(String),
    /// A normal Delete won while an imported Put waited to publish in memory.
    /// The replay caller removes stale mutations and retries the remaining batch.
    ReplayDeleteBarrierChanged,
    PermissionDenied(String),
    /// A named database has no catalog membership for the requested schema.
    /// This is a distinct failure so callers never infer authorization from an
    /// empty result or fall back to a legacy storage prefix.
    CatalogMembershipDenied {
        db_locator: String,
        schema_name: String,
    },
    /// A legacy owner-transport denial that re-pairing the caller's surface
    /// would fix.
    ///
    /// Carried separately from [`Self::PermissionDenied`] so the HTTP layer can
    /// render the discriminated `403 { error: "transport_not_attested", … }`
    /// body the `OwnerVerbGate` middleware and slice-3 owner verbs return — the
    /// SPA's `PairBrowserScreen` keys on that discriminator to fire the
    /// re-pairing recovery flow (NB2). Other permission denials stay ordinary
    /// [`Self::PermissionDenied`].
    ///
    /// `verb` is the request path/operation the denial was raised for (echoed in
    /// the 403 body, mirroring the gate's `verb` field).
    TransportNotAttested {
        verb: String,
    },
    /// Transform exceeded its fuel budget (MDT-E). Carried separately from
    /// `InvalidTransform` so the view resolver can recognize a fuel trap
    /// and surface a `gas exceeded` cause string without parsing the
    /// inner message — `max_gas` must fail identically on every device,
    /// so mis-classifying a fuel trap as a generic execution error would
    /// hide the canonical failure shape from the audit log.
    TransformGasExceeded {
        input_size: u64,
    },
    /// A `__registry_call` chain exceeded `MAX_REGISTRY_CALL_DEPTH`
    /// (see `crate::view::registry_call`). Carried separately from
    /// `InvalidTransform` for the same reason as `TransformGasExceeded`: a
    /// self-referential or mutually-recursive call chain must fail with a
    /// recognizable, stable shape rather than a stringly-typed message, so
    /// the resolver/audit-log path can classify it without sniffing text.
    TransformCallDepthExceeded {
        max_depth: u32,
    },
    /// A compare-and-set (CAS) mutation's precondition did not match the
    /// current state at its key, so the node applied nothing. The loser of a
    /// concurrent check-then-set race gets this — NOT a silent overwrite.
    ///
    /// Carried as a distinct variant (not a stringly-typed `InvalidData`) so
    /// clients — and the HTTP layer, which renders a discriminated
    /// `409 { error: "cas_conflict", ... }` body via
    /// [`Self::cas_conflict_body`] — can recognize a lost CAS race and retry
    /// (re-read, re-compute, re-submit) without sniffing an error message.
    /// This is the failure shape lastgit ref updates key on to reject a losing
    /// push. `schema`/`field`/`key` locate the record; `expected`/`actual`
    /// describe the mismatch (`actual` is `None` when the key had no live
    /// value — e.g. a `Value`/`ContentHash` expectation against an
    /// absent/tombstoned head).
    CasConflict {
        schema: String,
        field: String,
        key: String,
        expected: String,
        actual: Option<String>,
    },
    /// Atom field content exceeded the hard size limit
    /// ([`crate::atom::max_atom_content_bytes`], default 64 KiB).
    ///
    /// Carried as a distinct variant (not stringly-typed `InvalidData`) so the
    /// HTTP layer can render a discriminated `413 { error: "atom_content_too_large",
    /// size, limit }` body and clients can fail closed without sniffing text.
    /// Measure is pre-encryption serialized JSON of the field payload.
    /// Raise (within 1 MiB absolute max) via env `LASTDB_MAX_ATOM_CONTENT_BYTES`;
    /// larger payloads must use file-blob / CAS. See
    /// `fold_db/docs/ATOM_CONTENT_SIZE_LIMIT.md`.
    AtomContentTooLarge {
        size: usize,
        limit: usize,
    },
    /// A caller-supplied resume cursor (`after_key`, `cursor`) does not name a
    /// key inside the keyspace the operation pages over, so the walk cannot
    /// start.
    ///
    /// Carried as a distinct variant (not stringly-typed [`Self::InvalidData`])
    /// for the same reason as [`Self::CasConflict`]: the HTTP layer has to tell
    /// a *caller* fault from a *store* fault, and `InvalidData` carries both.
    /// A paging admin route that flattened this into `InvalidData` — and then
    /// into `FoldDbError::Database` — reached the socket as `500`, which
    /// `render` logs at ERROR and observability promotes into its own Sentry
    /// issue. One operator typo became a storm with zero users affected.
    /// A malformed cursor is `400`: the request is wrong and retrying it
    /// unchanged cannot help.
    ///
    /// `detail` names the keyspace the cursor had to be inside.
    InvalidCursor(String),

    /// The filesystem holding the database is full, so the write did not land.
    ///
    /// Carried as a distinct variant (not stringly-typed `InvalidData`) because
    /// a full disk is the opposite of a caller fault: the request was fine, the
    /// host is out of room, and only an operator can clear it. As `InvalidData`
    /// it rendered `400 Bad Request` / "Invalid data", which sends the writer to
    /// audit a payload that was never wrong, and it raised one ERROR — one
    /// Sentry issue — per failed write (issue `7620061902`: 207 events from a
    /// single 8-hour episode, 0 users affected).
    ///
    /// The HTTP layer renders this as a discriminated
    /// `507 { error: "storage_full", … }` body, and `render` keeps it out of the
    /// ERROR log for the same reason it spares permanent `429` quota denials.
    ///
    /// `detail` is the underlying storage error, for the log.
    StorageFull {
        detail: String,
    },
    /// Persist-queue reservation failed before resident apply. The request
    /// did not change resident state. Retry after the lane drains.
    PersistQueueFull {
        schema: String,
        kind: String,
    },
    /// The bounded mutation-capture queue stayed full for the whole admission
    /// window, so the write was refused before the local commit.
    ///
    /// Carried as a distinct variant for the same reason as `StorageFull`, and
    /// answered like `PersistQueueFull`: the request was well formed, nothing
    /// changed, and the same write succeeds once the capture worker drains. As
    /// `InvalidData` it rendered `400 Bad Request` / "Invalid data" — sending
    /// the writer to audit a payload that was never wrong — and raised one
    /// ERROR, so one Sentry event, per refused write (issue `7699865707`: 123
    /// events in 13 hours on a single node, 0 users affected).
    ///
    /// Unlike a full disk, no operator action clears this. It is self-clearing
    /// backpressure, so the answer is `503` and an immediate retry is correct.
    CaptureQueueFull {
        waited_ms: u64,
    },
}

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::NotFound(msg) => write!(f, "Schema not found: {msg}"),
            Self::Blocked(msg) => write!(f, "Schema blocked: {msg}"),
            Self::InvalidField(msg) => write!(f, "Invalid field: {msg}"),
            Self::InvalidPermission(msg) => write!(f, "Invalid permission: {msg}"),
            Self::InvalidTransform(msg) => write!(f, "Invalid transform: {msg}"),
            Self::InvalidData(msg) => write!(f, "Invalid data: {msg}"),
            Self::ReplayDeleteBarrierChanged => {
                write!(f, "Delete barrier changed during mutation replay")
            }
            Self::InvalidCursor(msg) => write!(f, "Invalid resume cursor: {msg}"),
            Self::PermissionDenied(msg) => write!(f, "Permission denied: {msg}"),
            Self::CatalogMembershipDenied {
                db_locator,
                schema_name,
            } => write!(
                f,
                "Database catalog membership denied: database '{db_locator}' does not contain schema '{schema_name}'"
            ),
            Self::TransportNotAttested { verb } => write!(
                f,
                "transport not attested: a governed namespace was denied to an owner \
                 context on an unattested transport (use the owner Unix socket / attested control plane, not bare loopback TCP) \
                 for '{verb}'"
            ),
            Self::TransformGasExceeded { input_size } => {
                write!(f, "Transform gas exceeded (input_size={input_size})")
            }
            Self::TransformCallDepthExceeded { max_depth } => {
                write!(f, "Transform call depth exceeded (max_depth={max_depth})")
            }
            Self::CasConflict {
                schema,
                field,
                key,
                expected,
                actual,
            } => write!(
                f,
                "CAS conflict on schema '{schema}' field '{field}' key '{key}':                  expected {expected}, found {}",
                actual.as_deref().unwrap_or("<absent>")
            ),
            Self::AtomContentTooLarge { size, limit } => write!(
                f,
                "atom content too large: {size} bytes exceeds hard limit of {limit} bytes \
                 (default 64 KiB; env LASTDB_MAX_ATOM_CONTENT_BYTES, absolute max 1 MiB). \
                 Atoms are not a blob store — use file-blob/CAS for large payloads \
                 (see fold_db/docs/ATOM_CONTENT_SIZE_LIMIT.md)"
            ),
            Self::StorageFull { detail } => write!(
                f,
                "no space left on device — the filesystem holding the database is full, \
                 so the write did not land. Free disk space and retry; nothing already \
                 stored was lost. Underlying error: {detail}"
            ),
            Self::PersistQueueFull { schema, kind } => write!(
                f,
                "persist queue full for schema '{schema}' ({kind}); retry after drain"
            ),
            Self::CaptureQueueFull { waited_ms } => write!(
                f,
                "mutation capture queue remained full for {waited_ms}ms, so the write did \
                 not land and nothing already stored was lost. This is transient \
                 backpressure from the capture worker — retry the same write"
            ),
        }
    }
}

impl std::error::Error for SchemaError {}

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
    /// `error == "atom_content_too_large"` and should move oversized payloads
    /// to file-blob / CAS. Returns `None` for every other variant.
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
                "hint": "atoms are structured field values, not a blob store; use file-blob/CAS for large payloads",
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

    /// The discriminated `503 capture_queue_full` response body for a
    /// [`Self::CaptureQueueFull`]. Clients key on `error ==
    /// "capture_queue_full"` to tell self-clearing backpressure from a
    /// rejected payload: retrying the same write immediately is correct, and
    /// no operator action is needed. Returns `None` for every other variant.
    pub fn capture_queue_full_body(&self) -> Option<serde_json::Value> {
        match self {
            Self::CaptureQueueFull { waited_ms } => Some(serde_json::json!({
                "ok": false,
                "error": "capture_queue_full",
                "waited_ms": waited_ms,
                "retryable": true,
                "hint": "the mutation capture worker is behind; retry the same write",
                "message": self.to_string(),
            })),
            _ => None,
        }
    }

    /// True when this error is transient mutation-capture backpressure.
    ///
    /// The reporting paths use this to keep a self-clearing condition out of
    /// the channels reserved for code faults.
    #[must_use]
    pub const fn is_capture_queue_full(&self) -> bool {
        matches!(self, Self::CaptureQueueFull { .. })
    }
}

impl From<crate::storage::StorageError> for SchemaError {
    fn from(error: crate::storage::StorageError) -> Self {
        match error {
            crate::storage::StorageError::CatalogMembershipDenied {
                db_locator,
                schema_name,
            } => Self::CatalogMembershipDenied {
                db_locator,
                schema_name,
            },
            // A full disk keeps its identity across the boundary. Folding it
            // into `InvalidData` here is what made every storage path report a
            // host condition as a malformed request.
            crate::storage::StorageError::StorageFull { detail } => Self::StorageFull { detail },
            // `ENOSPC` also reaches this boundary as a plain IO error, from the
            // paths that build a `StorageError` straight off `std::io::Error`
            // instead of through a backend's `map_error`. It is the same host
            // condition either way, so the caller's status must not depend on
            // which path happened to report it.
            crate::storage::StorageError::IoError(io)
                if crate::storage::StorageError::is_storage_full_io(&io) =>
            {
                Self::StorageFull {
                    detail: io.to_string(),
                }
            }
            // Capture backpressure keeps its identity for the same reason a
            // full disk does: the request was fine and the caller's status
            // must say "retry", not "your payload is invalid".
            crate::storage::StorageError::CaptureQueueFull { waited_ms } => {
                Self::CaptureQueueFull { waited_ms }
            }
            other => Self::InvalidData(other.to_string()),
        }
    }
}
