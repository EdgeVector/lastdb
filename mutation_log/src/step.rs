use crate::{
    AppendBatch, AppendOutcome, Capability, ListPage, LogError, LogState, NamespacePolicy,
    PrincipalId, ScopedHead, Snapshot, StreamId,
};

/// One log operation. The shell supplies every value. Nothing here reads the world.
pub enum Command {
    /// Mark one stream's append open.
    BeginAppend {
        /// Principal who will write.
        author: PrincipalId,
        /// Stream they will extend.
        stream: StreamId,
    },
    /// Apply one batch on an open stream.
    Append(AppendBatch),
    /// Mark the stream's append closed.
    FinishAppend {
        /// Principal who opened the append.
        caller: PrincipalId,
        /// Stream whose append is closing.
        stream: StreamId,
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
        /// Cursor this reader already has, in that reader's sequence space.
        after: u64,
        /// Maximum visible entries to return.
        limit: usize,
        /// Shell clock reading, used for the retention floor.
        now_ms: u64,
    },
    /// Record a consumer's cursor.
    Ack {
        /// Consumer who has the entries.
        consumer: PrincipalId,
        /// Highest cursor the consumer has in the sequence space it can see.
        through: u64,
    },
    /// Allow a principal to read or append.
    Grant {
        /// Publisher, or a device grantee making the grant.
        caller: PrincipalId,
        /// Principal receiving the grant.
        principal: PrincipalId,
        /// Capability they receive.
        capability: Capability,
    },
    /// Remove a grant. This is how a participant is removed.
    Revoke {
        /// Publisher, or a device grantee removing the grant.
        caller: PrincipalId,
        /// Principal losing the grant.
        principal: PrincipalId,
        /// Capability they lose.
        capability: Capability,
    },
    /// Cut a snapshot at the current head.
    Snapshot {
        /// Publisher, or a device grantee.
        caller: PrincipalId,
        /// Shell clock reading.
        at_ms: u64,
    },
    /// Drop the trimmable prefix.
    Trim {
        /// Publisher, or a device grantee.
        caller: PrincipalId,
        /// Shell clock reading.
        now_ms: u64,
    },
}

/// What a successful step did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The stream's append is open.
    AppendBegun,
    /// The batch was classified and, when it had new entries, stored.
    Appended(AppendOutcome),
    /// The stream's append is closed.
    AppendFinished,
    /// The last visible entry, or nothing visible yet.
    Head(Option<ScopedHead>),
    /// A page of visible entries.
    Page(ListPage),
    /// The ack was stored.
    Acked {
        /// Stored cursor.
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
        Command::BeginAppend { author, stream } => {
            (state.begin_append(&author, &stream)?, Outcome::AppendBegun)
        }
        Command::Append(batch) => {
            let (state, append) = state.append(&batch, policy)?;
            (state, Outcome::Appended(append))
        }
        Command::FinishAppend { caller, stream } => (
            state.finish_append(&caller, &stream)?,
            Outcome::AppendFinished,
        ),
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
        Command::Ack { consumer, through } => (
            state.ack(&consumer, through, policy)?,
            Outcome::Acked { through },
        ),
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
        Command::Snapshot { caller, at_ms } => {
            let (state, snapshot) = state.cut_snapshot(&caller, at_ms)?;
            (state, Outcome::Snapshot(snapshot))
        }
        Command::Trim { caller, now_ms } => {
            let state = state.trim(&caller, now_ms, policy)?;
            let through = state.trimmed_through;
            let snapshot_due = state.snapshot_due(now_ms, policy);
            (
                state,
                Outcome::Trimmed {
                    through,
                    snapshot_due,
                },
            )
        }
    };
    Ok(StepResult { state, outcome })
}
