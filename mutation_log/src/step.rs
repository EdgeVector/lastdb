use crate::{
    AppendBatch, AppendOutcome, Capability, ListPage, LogError, LogState, NamespacePolicy,
    PrincipalId, ScopedHead, Snapshot, SnapshotRef, Writer,
};

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
    /// Read the next visible page.
    List {
        /// Who is reading.
        reader: PrincipalId,
        /// Delivery sequence this reader already has.
        after: u64,
        /// Maximum visible entries to return.
        limit: usize,
        /// Shell clock reading, used for the retention floor.
        now_ms: u64,
    },
    /// Record one device's delivery cursor.
    Ack {
        /// Reading device (node key, device).
        reader: Writer,
        /// Highest delivery sequence that device has scanned.
        through: u64,
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
    /// Remove a grant. This is how a participant is removed.
    Revoke {
        /// Publisher key, or an owner key removing the grant.
        caller: PrincipalId,
        /// Node key losing the grant.
        principal: PrincipalId,
        /// Capability they lose.
        capability: Capability,
    },
    /// Cut a snapshot at the current head.
    Snapshot {
        /// Publisher key, or an owner key.
        caller: PrincipalId,
        /// Shell clock reading.
        at_ms: u64,
        /// Shell pointer to the snapshot bytes.
        snapshot_ref: SnapshotRef,
    },
    /// Drop the trimmable prefix.
    Trim {
        /// Publisher key, or an owner key.
        caller: PrincipalId,
        /// Shell clock reading.
        now_ms: u64,
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
    /// A page of visible entries.
    Page(ListPage),
    /// The ack was stored.
    Acked {
        /// Stored delivery sequence.
        through: u64,
    },
    /// The grant was stored, or it was already present.
    Granted,
    /// The grant was removed, or it was already absent.
    Revoked,
    /// The snapshot cut that was stored.
    Snapshot(Snapshot),
    /// The prefix through `through` has been dropped.
    Trimmed {
        /// New trim floor.
        through: u64,
        /// A newer snapshot is required before the floor can move further.
        snapshot_due: bool,
    },
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
        Command::List {
            reader,
            after,
            limit,
            now_ms,
        } => (
            state.clone(),
            Outcome::Page(state.list(&reader, after, limit, now_ms, policy)?),
        ),
        Command::Ack { reader, through } => ack(state, &reader, through)?,
        Command::Grant {
            caller,
            principal,
            capability,
        } => (
            state.grant(&caller, principal, capability)?,
            Outcome::Granted,
        ),
        Command::Revoke {
            caller,
            principal,
            capability,
        } => (
            state.revoke(&caller, &principal, &capability)?,
            Outcome::Revoked,
        ),
        Command::Snapshot {
            caller,
            at_ms,
            snapshot_ref,
        } => {
            let (state, snapshot) = state.cut_snapshot(&caller, at_ms, snapshot_ref)?;
            (state, Outcome::Snapshot(snapshot))
        }
        Command::Trim { caller, now_ms } => trim(state, &caller, now_ms)?,
    };
    Ok(StepResult { state, outcome })
}

fn ack(state: &LogState, reader: &Writer, through: u64) -> Result<(LogState, Outcome), LogError> {
    let state = state.ack(reader, through)?;
    let stored = state.acks.get(reader).copied().unwrap_or(through);
    Ok((state, Outcome::Acked { through: stored }))
}

fn trim(
    state: &LogState,
    caller: &PrincipalId,
    now_ms: u64,
) -> Result<(LogState, Outcome), LogError> {
    let state = state.trim(caller, now_ms)?;
    let through = state.trimmed_through;
    let snapshot_due = state.snapshot_due(now_ms);
    Ok((
        state,
        Outcome::Trimmed {
            through,
            snapshot_due,
        },
    ))
}
