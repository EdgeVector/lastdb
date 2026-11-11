use mutation_log::{
    AppendBatch, AppendOutcome, Capability, ListPage, LogError, PrincipalId, ScopedHead, Snapshot,
};

/// Persistence port for one database log.
///
/// `append` accepts at most [`mutation_log::APPEND_BATCH_LIMIT`] entries and
/// holds one in-flight batch per stream. The other methods are the read,
/// grant, and snapshot surface. Trim stays on the pure step until a shell
/// needs it.
pub trait MutationLog {
    /// Begin, apply, and finish one batch. A failed apply still finishes.
    fn append(&mut self, batch: AppendBatch) -> Result<AppendOutcome, LogError>;

    /// Last entry this reader may see.
    fn head(&self, reader: &PrincipalId) -> Result<Option<ScopedHead>, LogError>;

    /// One page of entries this reader may see.
    fn list(
        &self,
        reader: &PrincipalId,
        after: u64,
        limit: usize,
        now_ms: u64,
    ) -> Result<ListPage, LogError>;

    /// Record the reader's cursor. The value is the stored cursor.
    fn ack(&mut self, consumer: &PrincipalId, through: u64) -> Result<u64, LogError>;

    /// Grant read or append on this database.
    fn grant(&mut self, principal: PrincipalId, capability: Capability) -> Result<(), LogError>;

    /// Remove a grant. A missing grant succeeds.
    fn revoke(&mut self, principal: &PrincipalId, capability: &Capability) -> Result<(), LogError>;

    /// Cut a snapshot at the supplied time.
    fn snapshot(&mut self, at_ms: u64) -> Snapshot;
}
