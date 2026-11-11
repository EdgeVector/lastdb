use crate::{
    AppendBatch, AppendOutcome, Capability, ListPage, LogError, PrincipalId, Recovery, ScopedHead,
    Snapshot, Writer,
};

/// Persistence port for one database log.
///
/// `append` accepts at most [`crate::APPEND_BATCH_LIMIT`] entries and holds one
/// in-flight batch per writer (node key, device). The other methods are the
/// read, grant, and snapshot surface. Trim stays on the pure step until a
/// shell needs it. Adapters implement this trait. The sync step depends on it
/// and not on an adapter.
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

    /// Record one device's delivery cursor. The value is the stored cursor.
    fn ack(&mut self, reader: &Writer, through: u64) -> Result<u64, LogError>;

    /// Still-stored entries for a device whose cursor is below the trim floor.
    ///
    /// The result carries the snapshot and every visible entry that device has
    /// not acked. The caller installs the snapshot, applies the entries, then
    /// acks [`Recovery::next`].
    fn recover(&self, reader: &Writer, now_ms: u64) -> Result<Recovery, LogError>;

    /// Grant read or append on this database. `caller` is the publisher or an owner key.
    fn grant(
        &mut self,
        caller: &PrincipalId,
        principal: PrincipalId,
        capability: Capability,
    ) -> Result<(), LogError>;

    /// Remove a grant. A missing grant succeeds. `caller` must be allowed to revoke.
    fn revoke(
        &mut self,
        caller: &PrincipalId,
        principal: &PrincipalId,
        capability: &Capability,
    ) -> Result<(), LogError>;

    /// Cut a snapshot at the supplied time. `caller` must be allowed to maintain the log.
    fn snapshot(&mut self, caller: &PrincipalId, at_ms: u64) -> Result<Snapshot, LogError>;

    /// Last accepted writer sequence for `writer`. `reader` must be that node key.
    fn writer_tip(&self, reader: &PrincipalId, writer: &Writer) -> Result<u64, LogError>;
}
