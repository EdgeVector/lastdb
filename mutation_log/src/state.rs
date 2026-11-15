use std::collections::{BTreeMap, BTreeSet};

use crate::{
    AppendBatch, AppendOutcome, Capability, DatabaseId, DatabaseKind, Grant, Head, LogError,
    NamespacePolicy, PrincipalId, ScopedHead, Snapshot, SubmissionRecord, Writer,
};

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
    pub entries: Vec<crate::StoredEntry>,
    /// Open appends, one per writer (node key, device).
    pub inflight: BTreeSet<Writer>,
    /// Latest snapshot cut, if the shell has taken one.
    pub snapshot: Option<Snapshot>,
    /// Highest delivery sequence already dropped.
    pub trimmed_through: u64,
    /// Publisher-confirmed delivery prefix of consumed whole-database submissions.
    pub submissions_consumed_through: u64,
    /// Submissions already copied by the publisher key.
    ///
    /// The key is `(submission writer, writer_seq)`, once in total. A persistent
    /// adapter stores this set with the log. Trim drops a key when the
    /// submission it names is dropped.
    pub incorporated: BTreeSet<(Writer, u64)>,
    /// Dropped pending submissions, kept for the life of the log.
    ///
    /// The value is the writer, sequence, rejected or expired, and the time
    /// the entry was dropped. It has no payload. A persistent adapter stores
    /// this map with the log.
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

    /// Open one append for `writer`. A second open for the same writer is refused.
    pub fn begin_append(&self, writer: &Writer) -> Result<Self, LogError> {
        self.authorize_append(writer)?;
        if self.inflight.contains(writer) {
            return Err(LogError::AppendInFlight);
        }
        let mut next = self.clone();
        next.inflight.insert(writer.clone());
        Ok(next)
    }

    /// Close an open append for `writer`.
    pub fn finish_append(&self, writer: &Writer) -> Result<Self, LogError> {
        if !self.inflight.contains(writer) {
            return Err(LogError::AppendNotOpen);
        }
        self.authorize_append(writer)?;
        let mut next = self.clone();
        next.inflight.remove(writer);
        Ok(next)
    }

    pub(crate) fn authorize_append(&self, writer: &Writer) -> Result<(), LogError> {
        let allowed = match &self.kind {
            DatabaseKind::Personal => crate::read::holds_owner(self, &writer.key),
            DatabaseKind::Shareable { publisher } => {
                &writer.key == publisher || crate::read::holds_submit(self, &writer.key)
            }
        };
        if allowed {
            Ok(())
        } else {
            Err(LogError::Unauthorized)
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
