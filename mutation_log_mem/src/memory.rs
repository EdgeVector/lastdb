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
        self.apply(Command::BeginAppend {
            author: batch.author.clone(),
            stream: stream.clone(),
        })?;
        let written = self.apply(Command::Append(batch));
        let closed = self.apply(Command::FinishAppend { stream });
        let Outcome::Appended(outcome) = written? else {
            return Err(LogError::InvalidEntry);
        };
        match closed? {
            Outcome::AppendFinished => Ok(outcome),
            _ => Err(LogError::InvalidEntry),
        }
    }

    fn head(&self, reader: &PrincipalId) -> Result<Option<ScopedHead>, LogError> {
        match self.peek(Command::Head {
            reader: reader.clone(),
        })? {
            Outcome::Head(head) => Ok(head),
            _ => Err(LogError::InvalidEntry),
        }
    }

    fn list(
        &self,
        reader: &PrincipalId,
        after: u64,
        limit: usize,
        now_ms: u64,
    ) -> Result<ListPage, LogError> {
        match self.peek(Command::List {
            reader: reader.clone(),
            after,
            limit,
            now_ms,
        })? {
            Outcome::Page(page) => Ok(page),
            _ => Err(LogError::InvalidEntry),
        }
    }

    fn ack(&mut self, consumer: &PrincipalId, through: u64) -> Result<u64, LogError> {
        match self.apply(Command::Ack {
            consumer: consumer.clone(),
            through,
        })? {
            Outcome::Acked { through } => Ok(through),
            _ => Err(LogError::InvalidEntry),
        }
    }

    fn grant(&mut self, principal: PrincipalId, capability: Capability) -> Result<(), LogError> {
        match self.apply(Command::Grant {
            principal,
            capability,
        })? {
            Outcome::Granted => Ok(()),
            _ => Err(LogError::InvalidEntry),
        }
    }

    fn revoke(&mut self, principal: &PrincipalId, capability: &Capability) -> Result<(), LogError> {
        match self.apply(Command::Revoke {
            principal: principal.clone(),
            capability: capability.clone(),
        })? {
            Outcome::Revoked => Ok(()),
            _ => Err(LogError::InvalidEntry),
        }
    }

    fn snapshot(&mut self, at_ms: u64) -> Snapshot {
        match self
            .apply(Command::Snapshot { at_ms })
            .expect("snapshot cut stores the current head")
        {
            Outcome::Snapshot(snapshot) => snapshot,
            _ => unreachable!("step returns the snapshot it stored"),
        }
    }
}
