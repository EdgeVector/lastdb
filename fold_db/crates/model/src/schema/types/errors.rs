use serde::Serialize;
use std::fmt;

#[derive(Debug, Clone, Serialize)]
pub enum SchemaError {
    NotFound(String),
    Blocked(String),
    /// A finite storage read limit, never record absence.
    ReadResourceLimit {
        detail: String,
    },
    /// Complete stored-write admission failed before append.
    WriteResourceLimit {
        detail: String,
    },
    /// A selected tip names an atom that this read cannot resolve.
    /// This is a store failure, not an absent key or an optional field.
    // Preserve the serialized error identity for existing diagnostic consumers.
    #[serde(rename = "MissingLocalAtom")]
    MissingSelectedAtom,
    /// Storage or decode failed after a read selected the stored record.
    StorageRead {
        detail: String,
    },
    InvalidField(String),
    InvalidPermission(String),
    InvalidTransform(String),
    InvalidData(String),
    /// A retired source needs a current snapshot before it can replay again.
    ReplayBeforeDeleteCutoff {
        cutoff_nanos: u64,
    },
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
    /// value — e.g. a `Value` expectation against an absent or deleted
    /// record).
    CasConflict {
        schema: String,
        field: String,
        key: String,
        expected: String,
        actual: Option<String>,
    },
    /// Atom field content exceeded the hard size limit
    /// (core's `fold_db::atom::max_atom_content_bytes`, default 256 KiB).
    ///
    /// Carried as a distinct variant (not stringly-typed `InvalidData`) so the
    /// HTTP layer can render a discriminated `413 { error: "atom_content_too_large",
    /// size, limit }` body and clients can fail closed without sniffing text.
    /// Measure is pre-encryption serialized JSON of the field payload.
    /// Raise (within 1 MiB absolute max) via env `LASTDB_MAX_ATOM_CONTENT_BYTES`.
    /// The right shape depends on the data: see
    /// [`crate::atom::ATOM_CONTENT_TOO_LARGE_HINT`] and
    /// `fold_db/docs/ATOM_CONTENT_SIZE_LIMIT.md`.
    AtomContentTooLarge {
        size: usize,
        limit: usize,
    },
    /// The encoded record of a format 7 home exceeded the record cap
    /// (`MAX_RECORD_BYTES`, 64 KiB, in `record_value.rs`). The HTTP layer
    /// renders the same discriminated `413 atom_content_too_large` body with
    /// `scope: "record"`, so a client that keys on the error keeps working.
    RecordTooLarge {
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
    /// ERROR log.
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
}

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::NotFound(msg) => write!(f, "Schema not found: {msg}"),
            Self::Blocked(msg) => write!(f, "Schema blocked: {msg}"),
            Self::ReadResourceLimit {detail} => write!(f,"read resource limit exceeded: {detail}"),
            Self::WriteResourceLimit {detail} => write!(f,"write resource limit exceeded: {detail}"),
            Self::MissingSelectedAtom => write!(f, "selected tip has no readable atom body"),
            Self::StorageRead { detail } => write!(f, "storage read failed: {detail}"),
            Self::InvalidField(msg) => write!(f, "Invalid field: {msg}"),
            Self::InvalidPermission(msg) => write!(f, "Invalid permission: {msg}"),
            Self::InvalidTransform(msg) => write!(f, "Invalid transform: {msg}"),
            Self::InvalidData(msg) => write!(f, "Invalid data: {msg}"),
            Self::ReplayBeforeDeleteCutoff { cutoff_nanos } => write!(f,
                "replay_cutoff_requires_resync: original write time is missing or at/before {cutoff_nanos}; restore a current snapshot before replay"),
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
                 (default {} KiB; env LASTDB_MAX_ATOM_CONTENT_BYTES, absolute max 1 MiB). \
                 {} See fold_db/docs/ATOM_CONTENT_SIZE_LIMIT.md.",
                crate::atom::DEFAULT_MAX_ATOM_CONTENT_BYTES / 1024,
                crate::atom::ATOM_CONTENT_TOO_LARGE_HINT
            ),
            Self::RecordTooLarge { size, limit } => write!(
                f,
                "record too large: {size} bytes exceeds the record cap of {limit} bytes. {}",
                crate::atom::RECORD_TOO_LARGE_HINT
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
        }
    }
}

impl std::error::Error for SchemaError {}

mod responses;
