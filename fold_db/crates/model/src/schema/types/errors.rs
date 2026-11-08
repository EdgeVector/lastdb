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
    /// Storage or decode failed after a read selected the stored record.
    StorageRead {
        detail: String,
    },
    InvalidField(String),
    InvalidData(String),
    /// A retired source needs a current snapshot before it can replay again.
    ReplayBeforeDeleteCutoff {
        cutoff_nanos: u64,
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
        key: String,
        expected: String,
        actual: Option<String>,
    },
    /// The encoded record of a home exceeded the record cap
    /// (`MAX_RECORD_BYTES`, 64 KiB, in `record_value.rs`). The HTTP layer
    /// renders the same discriminated `413 field_value_too_large` body with
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
            Self::StorageRead { detail } => write!(f, "storage read failed: {detail}"),
            Self::InvalidField(msg) => write!(f, "Invalid field: {msg}"),
            Self::InvalidData(msg) => write!(f, "Invalid data: {msg}"),
            Self::ReplayBeforeDeleteCutoff { cutoff_nanos } => write!(f,
                "replay_cutoff_requires_resync: original write time is missing or at/before {cutoff_nanos}; restore a current snapshot before replay"),
            Self::InvalidCursor(msg) => write!(f, "Invalid resume cursor: {msg}"),
            Self::CasConflict {
                schema,
                key,
                expected,
                actual,
            } => write!(
                f,
                "CAS conflict on schema '{schema}' key '{key}':                  expected {expected}, found {}",
                actual.as_deref().unwrap_or("<absent>")
            ),
            Self::RecordTooLarge { size, limit } => write!(
                f,
                "record too large: {size} bytes exceeds the record cap of {limit} bytes. {}",
                crate::content_limit::RECORD_TOO_LARGE_HINT
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
