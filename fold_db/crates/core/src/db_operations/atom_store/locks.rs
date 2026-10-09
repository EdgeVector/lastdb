//! Molecule, tip, atom-ref, and delete-barrier locks plus GC protection.

use super::*;

impl AtomStore {
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

    pub(crate) fn pending_delete_barrier(
        &self,
        mk_key: &str,
    ) -> Option<crate::atom::delete_barrier::DeleteBarrier> {
        self.pending_delete_barriers
            .lock()
            .expect("pending_delete_barriers poisoned")
            .get(mk_key)
            .cloned()
    }

    pub(crate) async fn durable_delete_barrier(
        &self,
        mk_key: &str,
    ) -> Result<Option<crate::atom::delete_barrier::DeleteBarrier>, crate::schema::SchemaError>
    {
        let barrier_key = crate::atom::delete_barrier::delete_barrier_key(mk_key.as_bytes());
        let barrier: Option<crate::atom::delete_barrier::DeleteBarrier> = self
            .main_store
            .get_item(&barrier_key)
            .await
            .map_err(|error| {
                crate::schema::SchemaError::InvalidData(format!(
                    "read durable Delete barrier: {error}"
                ))
            })?;
        if barrier
            .as_ref()
            .is_some_and(|barrier| !barrier.matches_key(mk_key.as_bytes()))
        {
            return Err(crate::schema::SchemaError::InvalidData(
                "durable Delete barrier key identity differs from the molecule key".into(),
            ));
        }
        Ok(barrier)
    }

    pub(crate) async fn winning_delete_barrier(
        &self,
        mk_key: &str,
    ) -> Result<Option<crate::atom::delete_barrier::DeleteBarrier>, crate::schema::SchemaError>
    {
        let pending = self.pending_delete_barrier(mk_key);
        let durable = self.durable_delete_barrier(mk_key).await?;
        Ok(match (pending, durable) {
            (Some(pending), Some(durable)) if pending.is_newer_than(&durable) => Some(pending),
            (Some(_), Some(durable)) => Some(durable),
            (Some(pending), None) => Some(pending),
            (None, durable) => durable,
        })
    }

    /// Active automatic-GC generation, or zero outside a delete sweep.
    pub(crate) fn automatic_gc_atoms_generation(&self) -> u64 {
        self.automatic_gc_atoms_generation.load(Ordering::Acquire)
    }

    /// Publish or clear the automatic-GC generation for write-side guards.
    pub(crate) fn set_automatic_gc_atoms_generation(&self, generation: u64) {
        self.automatic_gc_atoms_generation
            .store(generation, Ordering::Release);
    }

    /// Serialize body writes and automatic deletes for the named atom UUIDs.
    ///
    /// Sorted acquisition gives every multi-atom batch the same lock order.
    pub(crate) async fn lock_automatic_gc_atoms(
        &self,
        atom_uuids: &[String],
    ) -> Vec<tokio::sync::OwnedMutexGuard<()>> {
        if atom_uuids.is_empty() {
            return Vec::new();
        }
        let mut stripes = atom_uuids
            .iter()
            .map(|uuid| {
                let mut hasher = DefaultHasher::new();
                uuid.hash(&mut hasher);
                hasher.finish() as usize % self.automatic_gc_atom_locks.len()
            })
            .collect::<Vec<_>>();
        stripes.sort_unstable();
        stripes.dedup();
        let mut guards = Vec::with_capacity(stripes.len());
        for stripe in stripes {
            guards.push(
                Arc::clone(&self.automatic_gc_atom_locks[stripe])
                    .lock_owned()
                    .await,
            );
        }
        guards
    }

    /// Serialize durable reference-count changes for the named atoms.
    pub(crate) async fn lock_atom_ref_counts(
        &self,
        atom_uuids: &[String],
    ) -> Vec<tokio::sync::OwnedMutexGuard<()>> {
        lock_target_stripes(&self.atom_ref_count_locks, atom_uuids).await
    }

    /// Serialize source changes and reclaim checks for molecule targets.
    pub(crate) async fn lock_liveness_molecules(
        &self,
        molecule_uuids: &[String],
    ) -> Vec<tokio::sync::OwnedMutexGuard<()>> {
        lock_target_stripes(&self.molecule_liveness_locks, molecule_uuids).await
    }

    /// Serialize source changes and reclaim checks for blob targets.
    pub(crate) async fn lock_liveness_blobs(
        &self,
        blob_refs: &[String],
    ) -> Vec<tokio::sync::OwnedMutexGuard<()>> {
        lock_target_stripes(&self.blob_liveness_locks, blob_refs).await
    }

    /// Protect atom references added after an automatic probe lap starts.
    ///
    /// The marker write uses the same per-atom guards as the delete pass. The
    /// append path writes the marker before it commits a legacy reference-only
    /// pin-log row. A marker without a row only retains bytes; a row without a
    /// marker could lose acknowledged data, so this order fails safe.
    #[cfg(any(feature = "cloud-sync", test))]
    pub(crate) async fn protect_automatic_gc_atom_references(
        &self,
        atom_uuids: &[String],
        storage_prefix: Option<&str>,
    ) -> Result<(), crate::schema::SchemaError> {
        if atom_uuids.is_empty() {
            return Ok(());
        }
        let _guards = self.lock_automatic_gc_atoms(atom_uuids).await;
        for atom_uuid in atom_uuids {
            if self
                .get_atom_by_uuid(atom_uuid, storage_prefix)
                .await?
                .is_none()
            {
                return Err(crate::schema::SchemaError::InvalidData(format!(
                    "cannot protect missing pin-log atom {atom_uuid}"
                )));
            }
        }
        if self.automatic_gc_atoms_generation() == 0 {
            return Ok(());
        }
        let items = self.automatic_gc_reference_marker_items(atom_uuids, storage_prefix)?;
        if items.is_empty() {
            return Ok(());
        }
        self.raw().batch_put_items(items).await.map_err(|error| {
            crate::schema::SchemaError::InvalidData(format!(
                "protect automatic gc-atoms pin-log references: {error}"
            ))
        })
    }
}
