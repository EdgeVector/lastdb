//! Molecule, tip, liveness and delete-barrier locks.

use super::*;
use futures::{future::BoxFuture, FutureExt};

impl AtomStore {
    /// Keep protein selection, membership changes, and orphan checks together.
    /// Lock order is native preparation, membership, then molecule liveness.
    /// An inherited native scope keeps its stages private until the outer commit;
    /// the clone-shared gate also serializes physical stores without that scope.
    /// Box the complete scope so callers do not inherit its full future layout.
    pub(crate) fn with_protein_membership<'a, T: Send + 'a>(
        &'a self,
        work: impl std::future::Future<Output = Result<T, crate::schema::SchemaError>> + Send + 'a,
    ) -> BoxFuture<'a, Result<T, crate::schema::SchemaError>> {
        let runtime = self
            .namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime());
        crate::storage::disk_pack::transaction::scope(runtime, async move {
            let _membership = self.protein_membership_gate.lock().await;
            work.await
        })
        .boxed()
    }

    /// Run `work` inside one native write request scope. The scope holds the
    /// write gate from its first read to the accept of its one batch, so a
    /// read-modify-write inside it cannot interleave with another writer.
    pub(crate) fn in_write_scope<'a, T: Send + 'a>(
        &'a self,
        work: impl std::future::Future<Output = Result<T, crate::schema::SchemaError>> + Send + 'a,
    ) -> BoxFuture<'a, Result<T, crate::schema::SchemaError>> {
        let runtime = self
            .namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime());
        crate::storage::disk_pack::transaction::scope(runtime, work).boxed()
    }

    /// Run `work` inside one native write request scope of a background job.
    /// It holds the write gate like [`Self::in_write_scope`], and it returns
    /// after the batch is durable. Unreserved background work shares the
    /// current collection window instead of sealing one for each batch.
    pub(crate) fn in_background_scope<'a, T: Send + 'a>(
        &'a self,
        work: impl std::future::Future<Output = Result<T, crate::schema::SchemaError>> + Send + 'a,
    ) -> BoxFuture<'a, Result<T, crate::schema::SchemaError>> {
        let runtime = self
            .namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime());
        crate::storage::disk_pack::transaction::background_scope(runtime, work).boxed()
    }

    /// Guard the short (read `moc:` → build items → commit) section for one
    /// molecule. See [`AtomStore::molecule_commit_locks`].
    pub(crate) async fn lock_molecule_commit(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> tokio::sync::OwnedRwLockWriteGuard<()> {
        let lock_key = format!(
            "{}\u{1f}{molecule_uuid}",
            storage_prefix.unwrap_or_default()
        );
        let mutex = {
            let mut map = self
                .molecule_commit_locks
                .lock()
                .expect("molecule_commit_locks poisoned");
            Arc::clone(map.entry(lock_key).or_default())
        };
        mutex.write_owned().await
    }

    /// Share the molecule barrier across ordinary sparse-log appends.
    ///
    /// Sparse append keys need no exclusive sequence allocator, so concurrent
    /// writers take read guards and do not queue behind each other. The guard is
    /// retained only to exclude the rare delete-and-rewrite path, which takes
    /// [`Self::lock_molecule_commit`] exclusively across both durable batches.
    pub(crate) async fn lock_molecule_append(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> tokio::sync::OwnedRwLockReadGuard<()> {
        let lock_key = format!(
            "{}\u{1f}{molecule_uuid}",
            storage_prefix.unwrap_or_default()
        );
        let mutex = {
            let mut map = self
                .molecule_commit_locks
                .lock()
                .expect("molecule_commit_locks poisoned");
            Arc::clone(map.entry(lock_key).or_default())
        };
        mutex.read_owned().await
    }

    /// Share append barriers for a multi-molecule sparse-log batch.
    pub(crate) async fn lock_molecule_appends(
        &self,
        molecule_uuids: &[String],
        storage_prefix: Option<&str>,
    ) -> Vec<tokio::sync::OwnedRwLockReadGuard<()>> {
        let mut uuids: Vec<&str> = molecule_uuids.iter().map(String::as_str).collect();
        uuids.sort_unstable();
        uuids.dedup();
        let mut guards = Vec::with_capacity(uuids.len());
        for uuid in uuids {
            guards.push(self.lock_molecule_append(uuid, storage_prefix).await);
        }
        guards
    }

    /// Serialize durable winner selection for exact tip rows.
    pub(crate) async fn lock_tip_commits(
        &self,
        storage_keys: &[String],
    ) -> Vec<tokio::sync::OwnedMutexGuard<()>> {
        Self::lock_exact_tip_keys(&self.tip_commit_locks, storage_keys).await
    }

    /// Serialize foreground Put and Delete publication for exact tip rows.
    ///
    /// A caller that needs both lock families takes the durable commit locks
    /// first. A normal Put takes only these short publication locks and sends
    /// its memory ack without waiting for the background disk flush.
    pub(crate) async fn lock_tip_publications(
        &self,
        storage_keys: &[String],
    ) -> Vec<tokio::sync::OwnedMutexGuard<()>> {
        Self::lock_exact_tip_keys(&self.tip_publication_locks, storage_keys).await
    }

    async fn lock_exact_tip_keys(
        locks_by_key: &ExactTipLocks,
        storage_keys: &[String],
    ) -> Vec<tokio::sync::OwnedMutexGuard<()>> {
        const REAP_AT: usize = 4096;
        let mut keys = storage_keys.to_vec();
        keys.sort_unstable();
        keys.dedup();
        let locks = {
            let mut map = locks_by_key.lock().expect("exact tip locks poisoned");
            if map.len() >= REAP_AT {
                map.retain(|_, lock| Arc::strong_count(lock) > 1);
            }
            keys.into_iter()
                .map(|key| Arc::clone(map.entry(key).or_default()))
                .collect::<Vec<_>>()
        };
        let mut guards = Vec::with_capacity(locks.len());
        for lock in locks {
            guards.push(lock.lock_owned().await);
        }
        guards
    }

    /// Publish the memory winner before a normal Delete sends its ack.
    /// Callers hold the mutation apply gate while they register the barriers.
    pub(crate) fn register_pending_delete_barriers(
        &self,
        barriers: impl IntoIterator<Item = crate::atom::delete_barrier::DeleteBarrier>,
    ) {
        let mut pending = self
            .pending_delete_barriers
            .lock()
            .expect("pending_delete_barriers poisoned");
        for barrier in barriers {
            match pending.entry(barrier.mk_key.clone()) {
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(barrier);
                }
                std::collections::hash_map::Entry::Occupied(mut slot) => {
                    if barrier.is_newer_than(slot.get()) {
                        slot.insert(barrier);
                    }
                }
            }
        }
    }

    /// Remove only the exact pending Delete whose durable barrier reached disk.
    /// A later Delete for the same key remains visible to peer replay.
    pub(crate) fn clear_pending_delete_barriers(
        &self,
        barriers: &[crate::atom::delete_barrier::DeleteBarrier],
    ) {
        let mut pending = self
            .pending_delete_barriers
            .lock()
            .expect("pending_delete_barriers poisoned");
        for barrier in barriers {
            if pending
                .get(&barrier.mk_key)
                .is_some_and(|current| current.order_key() == barrier.order_key())
            {
                pending.remove(&barrier.mk_key);
            }
        }
    }

    /// Remove every pending Delete barrier that the named mutations registered,
    /// for a Delete that failed before its own release. A later Delete for the
    /// same key has another mutation uuid, so it stays.
    pub(crate) fn clear_pending_delete_barriers_of(&self, mutation_uuids: &[&str]) {
        self.pending_delete_barriers
            .lock()
            .expect("pending_delete_barriers poisoned")
            .retain(|_, barrier| !mutation_uuids.contains(&barrier.mutation_uuid.as_str()));
    }

    /// Serialize source changes and reclaim checks for blob targets.
    pub(crate) async fn lock_liveness_blobs(
        &self,
        blob_refs: &[String],
    ) -> Vec<tokio::sync::OwnedMutexGuard<()>> {
        lock_target_stripes(&self.blob_liveness_locks, blob_refs).await
    }
}
