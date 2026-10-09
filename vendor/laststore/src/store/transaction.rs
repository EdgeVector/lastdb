// lint:file-size-ok verbatim move from store.rs; splitting this file further is separate work
use super::*;

impl LastStore {
    /// Apply all ops, then sync the groups **this transaction wrote**.
    ///
    /// The barrier covers the caller's own ops and nothing else. That is the
    /// promise `transaction` has always documented; until 2026-08-04 it was
    /// implemented as a whole-store [`Self::flush`], which also synced every
    /// group any *other* writer had dirtied since the last barrier.
    ///
    /// That difference is not academic. A point [`Self::put`] is deferred by
    /// design — it rides the group-commit thresholds and the embedder's
    /// background flusher — so on a busy store the dirty set at any instant is
    /// whatever every concurrent writer has appended, and the next small
    /// transaction paid an `fsync` for all of it. Measured with
    /// `examples/flush_barrier_cost`: a 2-key transaction costs ~10 ms alone
    /// and ~1.03 s with 256 foreign dirty groups (103x, ~4-5 ms per foreign
    /// group). On the primary that convoy was 43% of all mutation wall time —
    /// the node's 2-key change-feed append measured 409 ms per mutation.
    ///
    /// No documented durability promise changes. Every op in `ops` is synced
    /// before this returns, exactly as before. Foreign groups keep the deferred
    /// contract they were written under; they were never this caller's to
    /// promise, and losing the incidental early sync cannot corrupt synced
    /// state. [`Self::flush`] remains the whole-store barrier for the callers
    /// that want one (shutdown, periodic flush, snapshot).
    pub fn transaction(&self, ops: Vec<TxnOp>) -> Result<()> {
        self.apply_txn_ops(ops, true)
    }

    /// Sync exactly `scope`, with the same descriptor-exhaustion survival as
    /// [`Self::flush`] — see that method for why the retry pass exists.
    ///
    /// Split out rather than parameterizing `flush` so the whole-store walk
    /// keeps building its key list from the live handle map: a scoped barrier
    /// names its groups up front, a whole-store one cannot.
    ///
    /// Public because the database crate is a separate crate from this one.
    /// An empty scope does not take a barrier. [`Self::groups_synced_last_flush`]
    /// becomes `scope.len()` for this call, not the lifetime sync counter.
    pub fn flush_scope(&self, scope: &[ShardKey]) -> Result<()> {
        self.groups_synced_last_flush
            .store(scope.len() as u64, Ordering::Relaxed);
        if scope.is_empty() {
            return Ok(());
        }
        self.flush_barriers.fetch_add(1, Ordering::Relaxed);
        let result = if scope.len() >= PARALLEL_SCOPE_MIN_GROUPS {
            if let Ok(_permit) = PARALLEL_SCOPE_FLUSH.try_lock() {
                self.sync_shard_keys_parallel(scope)
            } else {
                self.sync_shard_keys(scope, scope)
            }
        } else {
            self.sync_shard_keys(scope, scope)
        };
        if result.is_ok() {
            // A clean pin in this scope can leave. Whole-store flush handles
            // clean foreign pins without charging this scoped barrier.
            self.reap_clean_unheld_pins_in(scope);
        }
        result
    }

    /// Apply all ops **without** the trailing durability barrier.
    ///
    /// The caller owns durability: the ops ride the per-shard dirty buffers —
    /// group-committed at `max_dirty_ops` / `max_dirty_bytes` — and the next
    /// explicit [`Self::flush`], the same deferred contract as a point
    /// [`Self::put`]. Read-your-writes is unchanged: ops are visible to reads
    /// immediately. A crash before the next flush can lose the not-yet-synced
    /// tail; it cannot corrupt earlier synced state.
    pub fn transaction_deferred(&self, ops: Vec<TxnOp>) -> Result<()> {
        self.apply_txn_ops(ops, false)
    }

    /// Apply all operations or restore the prior values after an error.
    ///
    /// A transaction captures each prior body from the group that the write
    /// already opened. Thus, a successful write adds no cold load.
    pub(super) fn apply_txn_ops(&self, ops: Vec<TxnOp>, durable: bool) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }

        let mut scope: Vec<ShardKey> = ops
            .iter()
            .map(|op| self.point_key(op.collection(), op.id()))
            .collect();
        scope.sort_unstable();
        scope.dedup();

        let mut gate_indexes: Vec<usize> = ops
            .iter()
            .map(|op| Self::transaction_gate_index(op.collection(), op.id()))
            .collect();
        gate_indexes.sort_unstable();
        gate_indexes.dedup();
        let transaction_guards: Vec<_> = gate_indexes
            .iter()
            .map(|index| self.transaction_gates[*index].lock().expect("poison"))
            .collect();

        // Pin every target group once. Rollback and residency refresh use the
        // same handle, so the success path adds no second cold load.
        // The touch comes from this transaction's ids. An unspecified touch
        // would leave an atom-plane group unprotected and would skip the
        // interactive stamp on a batch write.
        let handles: Vec<ShardHandle> = scope
            .iter()
            .map(|key| {
                let touch = self.transaction_warm_touch(&ops, key);
                self.shard_handle_at(&key.0, key.1, key.2, WarmAdmission::Point, touch)
            })
            .collect::<Result<_>>()?;
        let mut undo: Vec<TxnUndo> = Vec::with_capacity(ops.len());
        for op in ops {
            let key = self.point_key(op.collection(), op.id());
            let shard_index = scope.binary_search(&key).expect("transaction scope");
            if let Err(error) = self.apply_one_txn_op_undoable(
                op,
                key,
                shard_index,
                &handles[shard_index],
                &mut undo,
            ) {
                let result = self.fail_transaction(error, &undo, &scope, &handles);
                drop(transaction_guards);
                return result;
            }
            let maintenance = {
                let mut shard = handles[shard_index].lock().expect("poison");
                if Self::sorted_snapshot_needed(&shard) {
                    Self::sync_open(&mut shard)
                } else {
                    Ok(())
                }
            };
            if let Err(error) = maintenance {
                let result = self.fail_transaction(error, &undo, &scope, &handles);
                drop(transaction_guards);
                return result;
            }
        }

        let sync_result = if durable {
            // flush_scope spills a leased point handle. That spill is the
            // barrier. A later residency refresh must not be what keeps the
            // bytes alive: its error is ignored on the success path.
            self.flush_scope(&scope)
        } else {
            self.sync_transaction_thresholds(&handles)
                .and_then(|_| self.spill_leased_handles(&scope, &handles))
        };
        if let Err(error) = sync_result {
            let result = self.fail_transaction(error, &undo, &scope, &handles);
            drop(transaction_guards);
            return result;
        }

        drop(transaction_guards);
        // The sync above establishes the commit result. Residency refresh only
        // maintains the cache budget, so its failure must not reject a commit
        // that callers can already observe and retry over a later write.
        if self.refresh_transaction_handles(&scope, &handles).is_err() {
            self.transaction_residency_refresh_failures
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Apply one operation and append its inverse to `undo`.
    ///
    /// Transaction appends do not run a group sync after the buffer mutation.
    /// Thus, an append error precedes the mutation, and this method adds the
    /// inverse only after `append_transaction` returns a location.
    pub(super) fn apply_one_txn_op_undoable(
        &self,
        op: TxnOp,
        written: ShardKey,
        shard_index: usize,
        handle: &ShardHandle,
        undo: &mut Vec<TxnUndo>,
    ) -> Result<()> {
        match op {
            TxnOp::Put {
                collection: _,
                id,
                body,
            } => {
                let line = encode_put(&id, &body)?;
                let (previous_loc, previous) = {
                    let mut shard = handle.lock().expect("poison");
                    let previous_loc = shard.lookup(&id)?;
                    let previous = Self::current_body_locked(&mut shard, &id)?;
                    let loc = self.append_transaction(
                        &mut shard,
                        &line,
                        &id,
                        CaptureOp::Put,
                        previous_loc.map(|location| location.record_len()),
                    )?;
                    shard.insert_index_known(
                        id.clone(),
                        loc,
                        previous_loc.map(|location| location.record_len()),
                    );
                    if shard.cache_legacy_bodies() {
                        shard.values.insert(id.clone(), body.clone());
                    }
                    (previous_loc, previous)
                };
                // The append landed. Record the group point_key already chose.
                self.note_write(&id, written);
                undo.push(TxnUndo {
                    shard_index,
                    id,
                    previous_loc,
                    previous,
                    applied_len: Some(line.len() as u64),
                });
                Ok(())
            }
            TxnOp::Delete { collection: _, id } => {
                let prior = {
                    let mut shard = handle.lock().expect("poison");
                    let previous_loc = shard.lookup(&id)?;
                    let previous = Self::current_body_locked(&mut shard, &id)?;
                    if previous.is_some() {
                        let line = encode_del(&id)?;
                        let loc = self.append_transaction(
                            &mut shard,
                            &line,
                            &id,
                            CaptureOp::Delete,
                            previous_loc.map(|location| location.record_len()),
                        )?;
                        shard.remove_index_known_at(
                            &id,
                            previous_loc.map(|location| location.record_len()),
                            Some(loc),
                        );
                        shard.values.remove(&id);
                    }
                    previous.map(|body| (previous_loc, body))
                };
                if let Some((previous_loc, previous)) = prior {
                    // Absent ids append nothing, so only a real tombstone counts.
                    self.note_write(&id, written);
                    undo.push(TxnUndo {
                        shard_index,
                        id,
                        previous_loc,
                        previous: Some(previous),
                        applied_len: None,
                    });
                }
                Ok(())
            }
        }
    }

    /// Read one body from an open group without a second group lookup.
    pub(super) fn current_body_locked(shard: &mut Shard, id: &str) -> Result<Option<Vec<u8>>> {
        if shard.cache_legacy_bodies() {
            if let Some(body) = shard.values.get(id) {
                return Ok(Some(body.clone()));
            }
        }
        let loc = match shard.index.get(id) {
            Some(Some(location)) => *location,
            Some(None) => return Ok(None),
            None => {
                for segment in shard.sorted_segments.iter().rev() {
                    if let Some(record) = segment.get(shard.data_key.as_ref(), id)? {
                        return Ok(record.body);
                    }
                }
                return Ok(None);
            }
        };
        let body = Self::read_at(shard, loc)?;
        if shard.cache_legacy_bodies() {
            shard.values.insert(id.to_string(), body.clone());
        }
        Ok(Some(body))
    }

    /// Restore applied operations in reverse order through pinned handles.
    pub(super) fn rollback_txn_undo(
        &self,
        undo: &[TxnUndo],
        handles: &[ShardHandle],
    ) -> Result<()> {
        let mut first_error = None;
        for entry in undo.iter().rev() {
            let handle = &handles[entry.shard_index];
            let mut shard = handle.lock().expect("poison");
            let (line, op) = match &entry.previous {
                Some(body) => (encode_put(&entry.id, body), CaptureOp::Put),
                None => (encode_del(&entry.id), CaptureOp::Delete),
            };
            let restored = line.and_then(|line| {
                match self.append_transaction(&mut shard, &line, &entry.id, op, entry.applied_len) {
                    Err(_) if shard.uses_sorted_index() => {
                        // A full merge can remove every previous location.
                        // Preserve the captured inverse in the append buffer
                        // even when file I/O currently prevents a normal append.
                        // fail_transaction still requires a successful flush.
                        Self::append_plain_rollback_buffer(&mut shard, &line, op)
                    }
                    result => result,
                }
            });
            match (restored, &entry.previous) {
                (Ok(loc), Some(body)) => {
                    shard.insert_index_known(entry.id.clone(), loc, entry.applied_len);
                    if shard.cache_legacy_bodies() {
                        shard.values.insert(entry.id.clone(), body.clone());
                    }
                }
                (Ok(loc), None) => {
                    shard.remove_index_known_at(&entry.id, entry.applied_len, Some(loc));
                    shard.values.remove(&entry.id);
                }
                (Err(error), _) if shard.uses_sorted_index() => {
                    // Sequence exhaustion can also prevent the emergency
                    // buffer append. Report rollback failure and preserve the
                    // current valid index; never reinstall a retired location.
                    first_error.get_or_insert(error);
                }
                (Err(error), Some(body)) => {
                    if let Some(loc) = entry.previous_loc {
                        shard.insert_index_known(entry.id.clone(), loc, entry.applied_len);
                    } else {
                        shard.remove_index_known(&entry.id, entry.applied_len);
                    }
                    if shard.cache_legacy_bodies() {
                        shard.values.insert(entry.id.clone(), body.clone());
                    }
                    first_error.get_or_insert(error);
                }
                (Err(error), None) => {
                    shard.remove_index_known(&entry.id, entry.applied_len);
                    shard.values.remove(&entry.id);
                    first_error.get_or_insert(error);
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Convert a failed apply or sync into a durable restore before gate release.
    pub(super) fn fail_transaction(
        &self,
        error: Error,
        undo: &[TxnUndo],
        scope: &[ShardKey],
        handles: &[ShardHandle],
    ) -> Result<()> {
        if undo.is_empty() {
            return Err(error);
        }
        self.torn_transaction_rollbacks
            .fetch_add(1, Ordering::Relaxed);
        let restore_result = self.rollback_txn_undo(undo, handles);
        let flush_result = self.flush_scope(scope);
        let rollback_error = match (restore_result, flush_result) {
            (Ok(()), Ok(())) => None,
            (Err(restore), Ok(())) => Some(restore),
            (Ok(()), Err(flush)) => Some(flush),
            (Err(restore), Err(flush)) => Some(Error::Io(std::io::Error::other(format!(
                "restore failed: {restore}; rollback flush failed: {flush}"
            )))),
        };
        let refresh_result = self.refresh_transaction_handles(scope, handles);
        if let Some(rollback_error) = rollback_error {
            self.torn_transaction_rollback_failures
                .fetch_add(1, Ordering::Relaxed);
            return Err(Error::Io(std::io::Error::other(format!(
                "transaction failed: {error}; rollback failed: {rollback_error}"
            ))));
        }
        if let Err(refresh_error) = refresh_result {
            return Err(Error::Io(std::io::Error::other(format!(
                "transaction failed: {error}; residency refresh failed after rollback: {refresh_error}"
            ))));
        }
        Err(error)
    }

    /// Apply group-commit thresholds after every transaction op is visible.
    pub(super) fn sync_transaction_thresholds(&self, handles: &[ShardHandle]) -> Result<()> {
        let mut first_error = None;
        for handle in handles {
            let mut shard = handle.lock().expect("poison");
            if shard.dirty_ops >= self.opts.max_dirty_ops
                || shard.dirty_bytes >= self.opts.max_dirty_bytes
            {
                if let Err(error) = Self::sync_open(&mut shard) {
                    first_error.get_or_insert(error);
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Spill point handles that never entered the warm set.
    ///
    /// A deferred transaction does not call [`Self::flush_scope`]. The leased
    /// buffer dies with the last `Arc`. This spill is that call's barrier, and
    /// its error fails the transaction. A resident handle stays deferred.
    pub(super) fn spill_leased_handles(
        &self,
        scope: &[ShardKey],
        handles: &[ShardHandle],
    ) -> Result<()> {
        let mut first_error = None;
        for (key, handle) in scope.iter().zip(handles) {
            let resident = {
                let warm = self.shards.lock().expect("poison");
                warm.handles
                    .get(key)
                    .is_some_and(|current| Arc::ptr_eq(current, handle))
            };
            if resident {
                continue;
            }
            let mut shard = handle.lock().expect("poison");
            if let Err(error) = Self::sync_open(&mut shard) {
                first_error.get_or_insert(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub(super) fn refresh_transaction_handles(
        &self,
        scope: &[ShardKey],
        handles: &[ShardHandle],
    ) -> Result<()> {
        let mut first_error = None;
        for (key, handle) in scope.iter().zip(handles) {
            if let Err(error) = self.refresh_warm_resident_bytes(key, handle) {
                first_error.get_or_insert(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
