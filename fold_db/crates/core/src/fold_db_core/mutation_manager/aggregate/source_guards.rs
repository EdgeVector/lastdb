//! Shared absence protection; enrollment and existing aggregates stay exclusive.

use super::*;

pub(super) enum OrdinarySourceGuards {
    Unenrolled {
        _guards: Vec<tokio::sync::OwnedRwLockReadGuard<()>>,
    },
    Enrolled {
        _guards: Vec<tokio::sync::OwnedRwLockWriteGuard<()>>,
    },
}

impl OrdinarySourceGuards {
    pub(super) fn unenrolled(&self) -> bool {
        matches!(self, Self::Unenrolled { .. })
    }
}

impl MutationManager {
    /// Transfer critical-section ownership to each accepted native batch so a
    /// handler timeout cannot release a guard before its accepted writes finish.
    pub(super) async fn with_aggregate_guards<T>(
        &self,
        guards: impl Send + Sync + 'static,
        future: impl std::future::Future<Output = Result<T, SchemaError>> + Send,
    ) -> Result<T, SchemaError> {
        crate::storage::disk_pack::transaction::with_resource(
            self.db_ops.namespaced_store().disk_pack_runtime(),
            guards,
            // Bound the async layout in nested replay and aggregate callers.
            futures::FutureExt::boxed(future),
        )
        .await
    }

    pub(super) async fn acquire_ordinary_source_guards(
        &self,
        mut keys: Vec<String>,
    ) -> Result<OrdinarySourceGuards, SchemaError> {
        keys.sort_unstable();
        keys.dedup();
        let locks: Vec<_> = keys
            .iter()
            .map(|key| self.aggregate_named_lock(format!("aggregate_guard_lock\u{1f}{key}")))
            .collect();
        let mut reads = Vec::with_capacity(locks.len());
        for lock in &locks {
            reads.push(Arc::clone(lock).read_owned().await);
        }
        // This probe never reconciles a provisional guard or writes metadata.
        // Keep all shared guards through the complete ordinary write so an
        // enrollment cannot mistake an in-flight source value for absence.
        if !self.db_ops.metadata().contains_any(&keys).await? {
            return Ok(OrdinarySourceGuards::Unenrolled { _guards: reads });
        }
        // Release every read before waiting for any exclusive guard. Keep the
        // Arc identities, and preserve the existing sorted acquisition order.
        // The caller rereads/reconciles every guard after exclusive acquisition.
        drop(reads);
        let mut writes = Vec::with_capacity(locks.len());
        for lock in locks {
            writes.push(lock.write_owned().await);
        }
        Ok(OrdinarySourceGuards::Enrolled { _guards: writes })
    }
}
