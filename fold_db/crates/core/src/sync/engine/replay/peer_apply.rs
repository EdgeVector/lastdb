//! The peer-apply row, written before peer data is applied.

use super::super::SyncEngine;
use crate::sync::error::{SyncError, SyncResult};
use crate::sync::log::{LogEntry, LogOp};
use std::sync::atomic::Ordering;

/// Whether replaying `entry` can change local state. A digest row and an
/// unknown tag change nothing, so they must not set the row.
fn entry_can_change_state(entry: &LogEntry) -> bool {
    !matches!(
        entry.op,
        LogOp::PhysicalDigest { .. } | LogOp::Unknown { .. }
    )
}

impl SyncEngine {
    /// Make `peer-apply:v1 = seen` durable before the first peer data lands.
    ///
    /// Every replayed entry reaches `replay_entry_with_schema_refresh`, which
    /// calls this just before it applies the entry: cloud download replay, the
    /// bootstrap tail, mutation-log segment apply, `MutationIntent`, a logical
    /// commit and the legacy `mk:` Put. Only an entry that cannot change state
    /// is exempt. A failure to write the row fails the replay, so no peer data
    /// is applied with the row unset. Free after the first call.
    pub(crate) async fn mark_peer_apply_before_replay(&self, entry: &LogEntry) -> SyncResult<()> {
        if self.peer_apply_marked.load(Ordering::Acquire) || !entry_can_change_state(entry) {
            return Ok(());
        }
        self.mark_peer_apply_seen_now().await
    }

    /// Write the row again after a snapshot restore cleared it. Ignores the
    /// in-process cache, which still says `seen`.
    pub(crate) async fn rewrite_peer_apply_seen_after_restore(&self) -> SyncResult<()> {
        self.peer_apply_marked.store(false, Ordering::Release);
        let atoms = self.automatic_gc_atom_store.lock().await.clone();
        let Some(atoms) = atoms else {
            return Ok(());
        };
        atoms
            .rewrite_peer_apply_seen()
            .await
            .map_err(|error| SyncError::Storage(format!("record the peer-apply state: {error}")))?;
        self.peer_apply_marked.store(true, Ordering::Release);
        Ok(())
    }

    /// Make the row durable now, for a path that installs peer data without
    /// replaying log entries (a snapshot restore, a share seed import).
    pub(crate) async fn mark_peer_apply_seen_now(&self) -> SyncResult<()> {
        if self.peer_apply_marked.load(Ordering::Acquire) {
            return Ok(());
        }
        let atoms = self.automatic_gc_atom_store.lock().await.clone();
        let Some(atoms) = atoms else {
            // No serving atom store is attached, so nothing here runs atom GC.
            return Ok(());
        };
        atoms
            .mark_peer_apply_seen()
            .await
            .map_err(|error| SyncError::Storage(format!("record the peer-apply state: {error}")))?;
        self.peer_apply_marked.store(true, Ordering::Release);
        Ok(())
    }
}
