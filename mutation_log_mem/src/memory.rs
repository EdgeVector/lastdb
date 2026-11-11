use mutation_log::{
    step, AppendBatch, AppendOutcome, Capability, Command, ListPage, LogError, LogState,
    NamespacePolicy, Outcome, PrincipalId, ScopedHead, Snapshot,
};

use crate::MutationLog;

/// One database log held in memory. The pure step decides every change.
pub struct InMemoryMutationLog<P> {
    state: LogState,
    policy: P,
}

impl<P> InMemoryMutationLog<P> {
    pub(crate) fn new(state: LogState, policy: P) -> Self {
        Self { state, policy }
    }
}

impl<P: NamespacePolicy> InMemoryMutationLog<P> {
    fn apply(&mut self, command: Command) -> Result<Outcome, LogError> {
        let stepped = step(&self.state, command, &self.policy)?;
        self.state = stepped.state;
        Ok(stepped.outcome)
    }

    fn peek(&self, command: Command) -> Result<Outcome, LogError> {
        Ok(step(&self.state, command, &self.policy)?.outcome)
    }
}

impl<P: NamespacePolicy> MutationLog for InMemoryMutationLog<P> {
    fn append(&mut self, batch: AppendBatch) -> Result<AppendOutcome, LogError> {
        let stream = batch.stream.clone();
        let author = batch.author.clone();
        self.apply(Command::BeginAppend {
            author: author.clone(),
            stream: stream.clone(),
        })?;
        let written = self.apply(Command::Append(batch));
        let closed = self.apply(Command::FinishAppend {
            caller: author,
            stream,
        });
        let outcome = require_outcome(written?, |outcome| match outcome {
            Outcome::Appended(value) => Some(value),
            _ => None,
        })?;
        require_outcome(closed?, |outcome| match outcome {
            Outcome::AppendFinished => Some(()),
            _ => None,
        })?;
        Ok(outcome)
    }

    fn head(&self, reader: &PrincipalId) -> Result<Option<ScopedHead>, LogError> {
        require_outcome(
            self.peek(Command::Head {
                reader: reader.clone(),
            })?,
            |outcome| match outcome {
                Outcome::Head(head) => Some(head),
                _ => None,
            },
        )
    }

    fn list(
        &self,
        reader: &PrincipalId,
        after: u64,
        limit: usize,
        now_ms: u64,
    ) -> Result<ListPage, LogError> {
        require_outcome(
            self.peek(Command::List {
                reader: reader.clone(),
                after,
                limit,
                now_ms,
            })?,
            |outcome| match outcome {
                Outcome::Page(page) => Some(page),
                _ => None,
            },
        )
    }

    fn ack(&mut self, consumer: &PrincipalId, through: u64) -> Result<u64, LogError> {
        require_outcome(
            self.apply(Command::Ack {
                consumer: consumer.clone(),
                through,
            })?,
            |outcome| match outcome {
                Outcome::Acked { through } => Some(through),
                _ => None,
            },
        )
    }

    fn grant(
        &mut self,
        caller: &PrincipalId,
        principal: PrincipalId,
        capability: Capability,
    ) -> Result<(), LogError> {
        require_outcome(
            self.apply(Command::Grant {
                caller: caller.clone(),
                principal,
                capability,
            })?,
            |outcome| match outcome {
                Outcome::Granted => Some(()),
                _ => None,
            },
        )
    }

    fn revoke(
        &mut self,
        caller: &PrincipalId,
        principal: &PrincipalId,
        capability: &Capability,
    ) -> Result<(), LogError> {
        require_outcome(
            self.apply(Command::Revoke {
                caller: caller.clone(),
                principal: principal.clone(),
                capability: capability.clone(),
            })?,
            |outcome| match outcome {
                Outcome::Revoked => Some(()),
                _ => None,
            },
        )
    }

    fn snapshot(&mut self, caller: &PrincipalId, at_ms: u64) -> Result<Snapshot, LogError> {
        require_outcome(
            self.apply(Command::Snapshot {
                caller: caller.clone(),
                at_ms,
            })?,
            |outcome| match outcome {
                Outcome::Snapshot(snapshot) => Some(snapshot),
                _ => None,
            },
        )
    }
}

pub(crate) fn require_outcome<T>(
    outcome: Outcome,
    take: impl FnOnce(Outcome) -> Option<T>,
) -> Result<T, LogError> {
    take(outcome).ok_or(LogError::UnexpectedOutcome)
}
