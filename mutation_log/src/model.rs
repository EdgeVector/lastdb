use std::collections::{BTreeMap, BTreeSet};

use crate::{
    AppendOutcome, DatabaseId, DeviceId, LogError, Namespace, NamespacePolicy, PrincipalId,
    ScopedHead,
};

/// Opaque pointer to snapshot bytes held by the shell.
///
/// The core stores the pointer and returns it on reset. It does not store
/// the bytes. Any non-empty text without a control character is accepted.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SnapshotRef(String);

impl SnapshotRef {
    /// Reject an empty pointer or one that contains a control character.
    pub fn new(value: &str) -> Result<Self, LogError> {
        if !value.is_empty() && value.chars().all(|ch| !ch.is_control()) {
            Ok(Self(value.to_owned()))
        } else {
            Err(LogError::InvalidId)
        }
    }

    /// Borrow the pointer.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One writer inside a database log: the node key plus the device.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Writer {
    /// Node key that authored the entry.
    pub key: PrincipalId,
    /// Device of that node key.
    pub device: DeviceId,
}

/// Merge key. Global delivery order is not this order.
///
/// The tuple is `(ordering time, writer, sequence)`. The ordering time is
/// the older of the author time and the acceptance time.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Stamp {
    /// Older of the author time and the acceptance time.
    pub authored_at_ms: u64,
    /// Writer (node key, device) that composed the entry.
    pub writer: Writer,
    /// Writer sequence for that writer. Gap-free per writer, not global.
    pub sequence: u64,
}

impl Stamp {
    /// Ordering time. A sent time later than acceptance uses the acceptance time.
    #[must_use]
    pub fn ordering_millis(authored_at_ms: u64, accepted_at_ms: u64) -> u64 {
        authored_at_ms.min(accepted_at_ms)
    }
}

/// Where a publisher re-append came from.
///
/// The key is the submission's writer and its writer sequence, once in total.
/// A second device of the publisher key does not copy it again.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Incorporation {
    /// Writer (node key, device) that stored the submission.
    pub writer: Writer,
    /// Writer sequence of that submission.
    pub writer_seq: u64,
}

/// One mutation the log can store. The payload is opaque to this crate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// Database this entry belongs to.
    pub database_id: DatabaseId,
    /// Writer (node key, device) that authored it.
    pub writer: Writer,
    /// Gap-free sequence for `writer`. Zero is rejected.
    pub writer_seq: u64,
    /// Authoring time, supplied by the shell. Opening the payload uses this
    /// number. Merge order uses the older of this time and the acceptance time.
    pub authored_at_ms: u64,
    /// Namespace of the schema, without the schema name.
    pub namespace: Namespace,
    /// Set when the publisher key copied a submission into the log.
    pub incorporated_from: Option<Incorporation>,
    /// Opaque record bytes. This crate does not decode them.
    pub payload: Vec<u8>,
}

impl Entry {
    pub(crate) fn merge_stamp(&self, accepted_at_ms: u64) -> Stamp {
        Stamp {
            authored_at_ms: Stamp::ordering_millis(self.authored_at_ms, accepted_at_ms),
            writer: self.writer.clone(),
            sequence: self.writer_seq,
        }
    }
}

/// Tiny head row. `delivery_seq` is delivery order, not merge order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Head {
    /// Last delivery sequence in the log. The first accepted entry is 1.
    pub delivery_seq: u64,
    /// Ordering stamp of the entry stored at `delivery_seq`.
    pub stamp: Stamp,
}

/// An entry plus the delivery sequence assigned when it was accepted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredEntry {
    /// Position in delivery order.
    pub delivery_seq: u64,
    /// Time the shell accepted the batch. Retention uses this, not `authored_at_ms`.
    pub accepted_at_ms: u64,
    /// The accepted entry. Its author time stays as sealed.
    pub entry: Entry,
}

impl StoredEntry {
    pub(crate) fn merge_stamp(&self) -> Stamp {
        self.entry.merge_stamp(self.accepted_at_ms)
    }
}

/// A database is personal or shareable. It is never both.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DatabaseKind {
    /// The user's own database. It is not shareable.
    ///
    /// App-private namespaces are stored here. A receiver grant is rejected.
    Personal,
    /// A database receivers may be granted.
    ///
    /// Only official namespaces can be appended. A receiver reads entries
    /// whose writer key is `publisher`, including re-appends.
    Shareable {
        /// Node key that authors the readable entries and incorporates submissions.
        publisher: PrincipalId,
    },
}

/// What a grant allows. A grant does not name storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Capability {
    /// Read and append a personal database from any device of this node key.
    Owner,
    /// Read entries whose writer key is the publisher key.
    Receive,
    /// Append pending submissions from any device of this node key, and read those entries.
    Submit,
}

/// One read or append grant on the log's database.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Grant {
    /// Node key that holds the capability.
    pub principal: PrincipalId,
    /// What that key may do.
    pub capability: Capability,
}

/// One append transaction for a single writer (node key, device).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendBatch {
    /// Writer (node key, device) the entries extend.
    pub writer: Writer,
    /// Entries in writer-sequence order. At most [`crate::APPEND_BATCH_LIMIT`].
    pub entries: Vec<Entry>,
    /// When the shell accepted the batch. Every stored entry in the batch shares it.
    pub accepted_at_ms: u64,
}

/// Why a dropped pending submission cannot be copied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubmissionStatus {
    /// The publisher rejected the submission.
    Rejected,
    /// The submission aged out before it was copied.
    Expired,
}

/// A pending submission whose entry has been dropped.
///
/// The value is the writer, that writer's sequence, why it ended, and the
/// time the entry was dropped. It carries no payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubmissionRecord {
    /// Writer (node key, device) that stored the submission.
    pub writer: Writer,
    /// Writer sequence of that submission.
    pub writer_seq: u64,
    /// Rejected or expired.
    pub status: SubmissionStatus,
    /// When trim dropped the entry. The shell supplies this clock reading.
    pub time_ms: u64,
}

/// A snapshot cut. The bytes live in the shell at `snapshot_ref`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    /// Last delivery sequence the snapshot includes.
    pub through_delivery_seq: u64,
    /// When the shell cut the snapshot.
    pub at_ms: u64,
    /// Shell pointer to the snapshot bytes.
    pub snapshot_ref: SnapshotRef,
}

/// The whole log as a value. The shell stores it. This crate does not.
///
/// This models the personal log or the share's published log, including any
/// legacy mixed history. ExeMem stores new pending whole-database mutations
/// separately. Writer tips and read positions are keyed by node key and device.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogState {
    /// Database this log belongs to.
    pub database_id: DatabaseId,
    /// Personal or shareable.
    pub kind: DatabaseKind,
    /// Delivery head. Absent while the log has no entries.
    pub head: Option<Head>,
    /// Last accepted writer sequence for each writer. Missing means zero.
    pub tips: BTreeMap<Writer, u64>,
    /// Highest writer sequence already dropped for each writer.
    pub trimmed_tips: BTreeMap<Writer, u64>,
    /// Who may read or append. This list is not a storage path.
    pub grants: Vec<Grant>,
    /// Retained entries in delivery order.
    pub entries: Vec<StoredEntry>,
    /// Open appends, one per writer (node key, device).
    pub inflight: BTreeSet<Writer>,
    /// Latest snapshot cut, if the shell has taken one.
    pub snapshot: Option<Snapshot>,
    /// Highest delivery sequence already dropped.
    pub trimmed_through: u64,
    /// Publisher-confirmed delivery prefix of consumed whole-database submissions.
    pub submissions_consumed_through: u64,
    /// Submissions already copied by the publisher key.
    pub incorporated: BTreeSet<(Writer, u64)>,
    /// Dropped pending submissions, kept for the life of the log.
    pub submission_records: BTreeMap<(Writer, u64), SubmissionRecord>,
}

impl LogState {
    /// Open an empty log for one database.
    #[must_use]
    pub fn open(database_id: DatabaseId, kind: DatabaseKind) -> Self {
        Self {
            database_id,
            kind,
            head: None,
            tips: BTreeMap::new(),
            trimmed_tips: BTreeMap::new(),
            grants: Vec::new(),
            entries: Vec::new(),
            inflight: BTreeSet::new(),
            snapshot: None,
            trimmed_through: 0,
            submissions_consumed_through: 0,
            incorporated: BTreeSet::new(),
            submission_records: BTreeMap::new(),
        }
    }
}

/// One log operation. The shell supplies every value. Nothing here reads the world.
pub enum Command {
    /// Mark one writer's append open.
    BeginAppend {
        /// Writer (node key, device) that will append.
        writer: Writer,
    },
    /// Apply one batch on an open writer.
    Append(AppendBatch),
    /// Mark the writer's append closed.
    FinishAppend {
        /// Writer (node key, device) whose append is closing.
        writer: Writer,
    },
    /// Read the last entry visible to a principal.
    Head {
        /// Who is reading.
        reader: PrincipalId,
    },
    /// Allow a principal to read or append.
    Grant {
        /// Publisher key, or an owner key making the grant.
        caller: PrincipalId,
        /// Node key receiving the grant.
        principal: PrincipalId,
        /// Capability they receive.
        capability: Capability,
    },
}

/// What a successful step did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The writer's append is open.
    AppendBegun,
    /// The batch was classified and, when it had new entries, stored.
    Appended(AppendOutcome),
    /// The writer's append is closed.
    AppendFinished,
    /// The last visible entry, or nothing visible yet.
    Head(Option<ScopedHead>),
    /// The grant was stored, or it was already present.
    Granted,
}

/// The state to keep, and what the step did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StepResult {
    /// Next log value. Read steps clone the input.
    pub state: LogState,
    /// What the command produced.
    pub outcome: Outcome,
}

/// Apply one command. On `Err`, the caller keeps its previous state.
pub fn step(
    state: &LogState,
    command: Command,
    policy: &impl NamespacePolicy,
) -> Result<StepResult, LogError> {
    let (state, outcome) = match command {
        Command::BeginAppend { writer } => (state.begin_append(&writer)?, Outcome::AppendBegun),
        Command::Append(batch) => {
            let (state, append) = state.append(&batch, policy)?;
            (state, Outcome::Appended(append))
        }
        Command::FinishAppend { writer } => {
            (state.finish_append(&writer)?, Outcome::AppendFinished)
        }
        Command::Head { reader } => (
            state.clone(),
            Outcome::Head(state.head_for(&reader, policy)?),
        ),
        Command::Grant {
            caller,
            principal,
            capability,
        } => (
            state.grant(&caller, principal, capability)?,
            Outcome::Granted,
        ),
    };
    Ok(StepResult { state, outcome })
}
