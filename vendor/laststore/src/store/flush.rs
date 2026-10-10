use super::*;

impl LastStore {
    /// Spill pending buffers and `sync_data` all open shards.
    ///
    /// **This pass must survive descriptor exhaustion**, because the condition
    /// that makes it necessary is the condition that makes it fail. A dirty
    /// group whose append handle is not currently open — a cold-loaded group, or
    /// one past a segment roll — needs a *new* descriptor to spill, and the
    /// spill path opens it lazily ([`Self::spill_open`]). So a process holding a
    /// descriptor per resident group has nothing left to open with at exactly
    /// the moment it is trying to make acknowledged writes durable.
    ///
    /// That is not hypothetical. On 2026-07-30 the primary node exhausted its
    /// 8,192 descriptors on resident group handles; both the periodic flush
    /// (`background mutation flush failed`) and then the shutdown flush
    /// (`FoldDB shutdown flush failed; some recent mutations may not be
    /// durable`) failed with `EMFILE`, and the node runs memory-first writes —
    /// so mutations were acknowledged to clients and then not written.
    ///
    /// Two properties fix that, and neither costs anything in the healthy case:
    ///
    /// * **Release as it goes, but only once short.** The walk behaves exactly
    ///   as before until an open actually fails for want of a descriptor. From
    ///   that point every group hands its descriptor back after it is synced, so
    ///   the pass needs a couple of descriptors at a time instead of one per
    ///   dirty group. Releasing unconditionally would make every periodic flush
    ///   re-open the whole warm set on the next write, which is the cost the
    ///   warm set exists to avoid.
    /// * **Finish the walk.** One failing group no longer abandons the groups
    ///   behind it. Failures are collected, retried once after the reclaim pass
    ///   below, and reported as a *count* — "may not be durable" is not
    ///   something an operator can act on; "3 of 4,915 groups did not reach
    ///   disk" is.
    pub fn flush(&self) -> Result<()> {
        self.require_writable()?;
        self.flush_barriers.fetch_add(1, Ordering::Relaxed);
        let seal = self.assigned_through();
        let keys = self.flush_sync_keys();
        let result = self.sync_shard_keys(&keys, &keys);
        if result.is_ok() {
            self.publish_durable_through(seal);
            self.reap_clean_unheld_pins();
        }
        result
    }

    pub(super) fn flush_sync_keys(&self) -> Vec<ShardKey> {
        let mut keys: Vec<ShardKey> = self
            .shards
            .lock()
            .expect("poison")
            .handles
            .keys()
            .cloned()
            .collect();
        let pin_keys: Vec<ShardKey> = self
            .pins
            .lock()
            .expect("poison")
            .handles
            .keys()
            .cloned()
            .collect();
        keys.extend(pin_keys);
        keys.sort();
        keys.dedup();
        keys
    }

    /// Restore-only whole-store barrier with bounded parallel shard sync.
    ///
    /// A photograph load dirties thousands of independent hash groups at once.
    /// The normal flush keeps its serial, descriptor-conservative behavior for
    /// steady writes. Restore owns an isolated store before socket exposure, so
    /// it can sync independent group files in parallel and release each append
    /// descriptor as soon as that group reaches disk.
    pub fn flush_restore_parallel(&self, max_workers: usize) -> Result<()> {
        self.require_writable()?;
        self.flush_barriers.fetch_add(1, Ordering::Relaxed);
        let seal = self.assigned_through();
        let keys = self.flush_sync_keys();
        if keys.is_empty() {
            self.publish_durable_through(seal);
            self.reap_clean_unheld_pins();
            return Ok(());
        }

        let workers = max_workers.max(1).min(keys.len());
        let chunk_size = keys.len().div_ceil(workers);
        let retry = std::sync::Mutex::new(Vec::<ShardKey>::new());
        std::thread::scope(|scope| {
            for chunk in keys.chunks(chunk_size) {
                let retry = &retry;
                scope.spawn(move || {
                    for key in chunk {
                        if self.sync_resident_shard(key, true).is_err() {
                            retry.lock().expect("poison").push(key.clone());
                        }
                    }
                });
            }
        });

        let retry = retry.into_inner().expect("poison");
        let mut first_err: Option<Error> = None;
        let mut failed = 0usize;
        for key in &retry {
            if let Err(error) = self.sync_resident_shard(key, true) {
                failed += 1;
                first_err.get_or_insert(error);
            }
        }
        let _ = self.reclaim_append_descriptors();

        if let Some(error) = first_err {
            let kind = match &error {
                Error::Io(io) => io.kind(),
                _ => std::io::ErrorKind::Other,
            };
            return Err(Error::Io(std::io::Error::new(
                kind,
                format!(
                    "parallel restore flush: {failed} of {} resident group(s) did not reach disk after retry; first error: {error}",
                    keys.len()
                ),
            )));
        }
        self.publish_durable_through(seal);
        self.reap_clean_unheld_pins();
        Ok(())
    }

    /// Sync a wide transaction scope with bounded workers. Each group still
    /// reaches its own durability barrier before the transaction returns.
    /// The scope is independent of foreign dirty groups, as in the serial path.
    pub(super) fn sync_shard_keys_parallel(&self, keys: &[ShardKey]) -> Result<()> {
        let workers = PARALLEL_SCOPE_WORKERS.min(keys.len());
        let chunk_size = keys.len().div_ceil(workers);
        let release = AtomicBool::new(self.over_append_descriptor_cap());
        let outcomes = std::thread::scope(|scope| {
            let jobs: Vec<_> = keys
                .chunks(chunk_size)
                .map(|chunk| {
                    let release = &release;
                    scope.spawn(move || {
                        let mut deferred = Vec::new();
                        let mut errors = Vec::new();
                        for key in chunk {
                            if !release.load(Ordering::Relaxed) && self.over_append_descriptor_cap()
                            {
                                release.store(true, Ordering::Relaxed);
                            }
                            match self.sync_resident_shard(key, release.load(Ordering::Relaxed)) {
                                Ok(()) => {}
                                Err(error) if is_descriptor_exhaustion(&error) => {
                                    release.store(true, Ordering::Relaxed);
                                    deferred.push(key.clone());
                                }
                                Err(error) => errors.push(error),
                            }
                        }
                        (deferred, errors)
                    })
                })
                .collect();
            jobs.into_iter()
                .map(|job| job.join().expect("scope sync worker panicked"))
                .collect::<Vec<_>>()
        });

        let mut deferred = Vec::new();
        let mut first_err = None;
        let mut failed = 0usize;
        for (retry, errors) in outcomes {
            deferred.extend(retry);
            failed += errors.len();
            if first_err.is_none() {
                first_err = errors.into_iter().next();
            }
        }
        if !deferred.is_empty() {
            self.release_resident_descriptors(keys);
            for key in &deferred {
                if let Err(error) = self.sync_resident_shard(key, true) {
                    failed += 1;
                    first_err.get_or_insert(error);
                }
            }
        }
        let _ = self.reclaim_append_descriptors();

        if let Some(error) = first_err {
            let kind = match &error {
                Error::Io(io) => io.kind(),
                _ => std::io::ErrorKind::Other,
            };
            return Err(Error::Io(std::io::Error::new(
                kind,
                format!(
                    "parallel scope flush: {failed} of {} group(s) did not reach disk; first error: {error}",
                    keys.len()
                ),
            )));
        }
        Ok(())
    }

    /// Sync `keys`, reclaiming descriptors across `reclaim` if the walk runs
    /// short. The two differ only for [`Self::flush_scope`], whose walk is a
    /// handful of groups but whose shortage — if it hits one — was caused by
    /// the whole resident set.
    pub(super) fn sync_shard_keys(&self, keys: &[ShardKey], reclaim: &[ShardKey]) -> Result<()> {
        let mut release = false;
        let mut deferred: Vec<ShardKey> = Vec::new();
        let mut first_err: Option<Error> = None;
        let mut failed = 0usize;

        for key in keys {
            // A flush is the one pass that opens a descriptor for every dirty
            // group at once — it is how 07:48Z's `EMFILE` was reached. Once the
            // walk crosses the configured cap, hand each remaining group's
            // descriptor back as soon as it is durable, instead of waiting to be
            // told by an `EMFILE` that there are none left. The groups opened
            // before the crossing are reclaimed at the tail.
            if !release && self.over_append_descriptor_cap() {
                release = true;
            }
            match self.sync_resident_shard(key, release) {
                Ok(()) => {}
                Err(e) if is_descriptor_exhaustion(&e) => {
                    // Out of descriptors: from here on give each one back as
                    // soon as its group is durable, and come back to this group
                    // once the reclaim pass has freed room for its open.
                    release = true;
                    deferred.push(key.clone());
                }
                Err(e) => {
                    failed += 1;
                    first_err.get_or_insert(e);
                }
            }
        }

        if !deferred.is_empty() {
            // Groups walked *before* the shortage was detected are synced but
            // still holding their descriptors. Reclaim those too, so the retry
            // has the whole warm set's worth of headroom rather than only what
            // the tail of the walk happened to free.
            self.release_resident_descriptors(reclaim);
            for key in &deferred {
                if let Err(e) = self.sync_resident_shard(key, true) {
                    failed += 1;
                    first_err.get_or_insert(e);
                }
            }
        }

        // Give back whatever the walk opened before it started releasing. Its
        // error is deliberately dropped: every group above is already synced, so
        // this pass finds them clean and `sync_open` is a no-op — and a failure
        // to *close* a descriptor is not a durability fact worth failing a flush
        // that met its barrier.
        let _ = self.reclaim_append_descriptors();

        if let Some(e) = first_err {
            // Preserve the kind so callers can still classify it, and put the
            // count where the operator reads it: the node logs this Display.
            let kind = match &e {
                Error::Io(io) => io.kind(),
                _ => std::io::ErrorKind::Other,
            };
            return Err(Error::Io(std::io::Error::new(
                kind,
                format!(
                    "flush: {failed} of {} resident group(s) did not reach disk; first error: {e}",
                    keys.len()
                ),
            )));
        }
        Ok(())
    }

    /// Sync one resident or leased group, optionally handing its descriptor
    /// back afterwards.
    ///
    /// Resolves the handle without [`Self::shard_handle_at`] on purpose. That
    /// path cold-loads a group that is no longer resident — opening files, which
    /// is the one thing a descriptor-starved flush must not do. Eviction syncs
    /// a published group on the way out. A leased point handle never entered
    /// the warm set, so this method spills that lease. A dead lease stays
    /// `Ok`: the group was already synced, or the bytes are already gone.
    pub(super) fn sync_resident_shard(&self, key: &ShardKey, release: bool) -> Result<()> {
        let warm_or_lease = {
            let mut warm = self.shards.lock().expect("poison");
            if let Some(handle) = warm.handles.get(key).cloned() {
                Some(handle)
            } else {
                // Eviction syncs a published group on the way out, so a key
                // that only left `handles` is already durable. A leased point
                // handle never entered `handles`. Its buffer dies with the
                // last Arc. This flush is the barrier for that key.
                warm.leased_handle(key)
            }
        };
        let pin = self.pin_table_handle(key);
        // When the pin Arc is not the warm handle, flush syncs the pin. Tokens
        // claim those bytes. A missing warm handle is not success for a pin.
        let handle = match (warm_or_lease, pin) {
            (Some(warm), Some(pin)) if !Arc::ptr_eq(&warm, &pin) => pin,
            (Some(handle), _) | (None, Some(handle)) => handle,
            (None, None) => {
                // Absent-handle Ok stays only for a former warm key. A live pin
                // was found above. This does not remove a pin and does not drop
                // a tombstone.
                return Ok(());
            }
        };
        let mut sh = handle.lock().expect("poison");
        // Count before the sync, not after: a group that entered the walk dirty
        // is barrier width whether or not its `sync_data` succeeded, and width
        // is what this counter is for.
        if sh.dirty_ops != 0 || sh.dirty_bytes != 0 {
            self.groups_synced.fetch_add(1, Ordering::Relaxed);
        }
        let synced = Self::sync_open(&mut sh);
        if release {
            // Safe at any point, and safe even when the sync above failed:
            // buffered bytes live in `open_buf`, not in the `File`, and the
            // spill path reopens the segment on demand. Dropping the handle of
            // a group that did *not* sync is what lets the next one open.
            sh.open_file = None;
        }
        // sync_open updates the pin in place. Do not remove it here.
        synced
    }

    /// Close the append handle of every listed resident group.
    ///
    /// Purely a descriptor reclaim: every one of these groups was synced by the
    /// walk that precedes it, and any that was not is retried right after.
    pub(super) fn release_resident_descriptors(&self, keys: &[ShardKey]) {
        for key in keys {
            let handle = self
                .shards
                .lock()
                .expect("poison")
                .handles
                .get(key)
                .cloned();
            if let Some(handle) = handle {
                handle.lock().expect("poison").open_file = None;
            }
        }
    }
}
