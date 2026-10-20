use super::*;

impl AtomStore {
    /// Mark the meters dirty and point-put them if the debounce interval has
    /// elapsed, so a later boot sees roughly the same live totals.
    ///
    /// Called from every atom and molecule write path. A persist writes the
    /// small header plus shards for schemas that changed since the last
    /// durable write. A skipped write leaves the dirty flag set;
    /// [`Self::flush_keep_small`] still lands it. See
    /// [`KEEP_SMALL_PERSIST_INTERVAL`].
    pub async fn persist_keep_small(&self) -> Result<(), crate::schema::SchemaError> {
        if self.keep_small_persist.is_none() {
            return Ok(());
        }
        self.keep_small_dirty.store(true, Ordering::Relaxed);
        // A metered write after the clean-stop flush would leave a durable
        // `clean_stop = true` that no longer covers it. Persist it now, with
        // `false`, so the next boot reads "unclean" (a safe under-claim)
        // rather than "clean" (a false exact report).
        if self
            .keep_small_clean_stop_written
            .swap(false, Ordering::Relaxed)
        {
            return self.write_keep_small().await;
        }
        if !self.claim_keep_small_persist_slot() {
            return Ok(());
        }
        self.write_keep_small().await
    }

    /// The shutdown flush: lands the last debounce interval and stamps the
    /// snapshot `clean_stop = true`, so the next boot hydrates it as exact
    /// without a bootstrap. Call it after every metered writer has stopped.
    ///
    /// Always writes, dirty or not: the stamp is the verdict, and a clean
    /// stop whose meters happened to be unchanged since the last debounce
    /// tick must still leave a `true` behind. A metered write that lands
    /// after this flush (a lane that did not drain) persists immediately with
    /// `false` via [`Self::persist_keep_small`], so the durable row can only
    /// under-claim, never over-claim.
    pub async fn flush_keep_small_for_clean_stop(&self) -> Result<(), crate::schema::SchemaError> {
        self.keep_small_clean_stop_written
            .store(true, Ordering::Relaxed);
        self.flush_keep_small_inner(true).await
    }

    pub(super) async fn flush_keep_small_inner(
        &self,
        clean_stop: bool,
    ) -> Result<(), crate::schema::SchemaError> {
        let Some(store) = self.keep_small_persist.as_ref() else {
            return Ok(());
        };
        let _persist_guard = self.keep_small_persist_lock.lock().await;
        let wrote_snapshot = clean_stop || self.keep_small_dirty.load(Ordering::Relaxed);
        if wrote_snapshot {
            if let Ok(mut last) = self.keep_small_last_persist.lock() {
                *last = Some(Instant::now());
            }
            self.write_keep_small_with_clean_stop_locked(clean_stop)
                .await?;
        }
        store.inner().flush().await.map_err(|e| {
            crate::schema::SchemaError::InvalidData(format!("flush keep-small: {e}"))
        })?;
        if wrote_snapshot {
            let checkpoint = self
                .keep_small_hard_erase_applied_seq
                .load(Ordering::Relaxed);
            if self
                .prune_hard_erase_journal_locked(checkpoint, None)
                .await?
                > 0
            {
                store.inner().flush().await.map_err(|e| {
                    crate::schema::SchemaError::InvalidData(format!(
                        "flush checkpointed meter journal prune: {e}"
                    ))
                })?;
            }
        }
        Ok(())
    }

    /// Take the right to write the projection, stamping the clock under the
    /// same lock so concurrent writers cannot all decide they are due.
    pub(super) fn claim_keep_small_persist_slot(&self) -> bool {
        let interval = keep_small_persist_interval();
        let Ok(mut last) = self.keep_small_last_persist.lock() else {
            // Poisoned clock: persist rather than silently stop metering.
            return true;
        };
        let due = last.is_none_or(|t| t.elapsed() >= interval);
        if due {
            *last = Some(Instant::now());
        }
        due
    }

    /// Unconditional durable write of the projection, stamped as a runtime
    /// write (`clean_stop = false`).
    pub(super) async fn write_keep_small(&self) -> Result<(), crate::schema::SchemaError> {
        self.write_keep_small_with_clean_stop(false).await
    }

    /// Unconditional durable write of the projection with an explicit
    /// clean-stop stamp. Only the shutdown flush passes `true`.
    pub(super) async fn write_keep_small_with_clean_stop(
        &self,
        clean_stop: bool,
    ) -> Result<(), crate::schema::SchemaError> {
        if self.keep_small_persist.is_none() {
            return Ok(());
        }
        let _persist_guard = self.keep_small_persist_lock.lock().await;
        self.write_keep_small_with_clean_stop_locked(clean_stop)
            .await
    }

    pub(super) async fn write_keep_small_with_clean_stop_locked(
        &self,
        clean_stop: bool,
    ) -> Result<(), crate::schema::SchemaError> {
        // Clear before the put: a concurrent meter update during the write
        // should re-arm the flag rather than be swallowed by clearing after.
        self.keep_small_dirty.store(false, Ordering::Relaxed);
        if self
            .keep_small_hard_erase_replay_skipped
            .load(Ordering::Relaxed)
        {
            self.keep_small
                .mark_incomplete("hard_erase_journal_requires_repair");
        }
        let mut snapshot = self.keep_small.export();
        snapshot.hard_erase_totals = self
            .keep_small_hard_erase_totals
            .lock()
            .map_err(|_| {
                crate::schema::SchemaError::InvalidData(
                    "keep-small hard-erase totals lock poisoned".to_string(),
                )
            })?
            .clone();
        snapshot.hard_erase_journal_checkpoint_seq = self
            .keep_small_hard_erase_applied_seq
            .load(Ordering::Relaxed);
        snapshot.clean_stop = Some(clean_stop);
        if !clean_stop && snapshot.trust.global.state.is_complete() {
            snapshot.trust.global = MeterDomainTrust::incomplete("dirty_snapshot");
            snapshot.trust.molecules = MeterDomainTrust::incomplete("dirty_snapshot");
            for domain in snapshot.trust.schemas.values_mut() {
                *domain = MeterDomainTrust::incomplete("dirty_snapshot");
            }
        }
        let dirty = self.keep_small.take_dirty_schemas();
        self.write_keep_small_header_and_shards(&snapshot, Some(&dirty))
            .await?;
        // The debounced caller does not flush here. A caller that needs an
        // acknowledgment-grade counter calls `flush_keep_small` next.
        Ok(())
    }

    pub(super) async fn merge_keep_small_shards(
        &self,
        store: &TypedKvStore<dyn KvStore>,
        snapshot: &mut crate::db_operations::KeepSmallSnapshot,
    ) -> Result<(), crate::schema::SchemaError> {
        let shards = store
            .scan_items_with_prefix::<KeepSmallSchemaShard>(
                crate::db_operations::KEEP_SMALL_SCHEMA_SHARD_PREFIX,
            )
            .await
            .map_err(|e| {
                crate::schema::SchemaError::InvalidData(format!("hydrate keep-small shards: {e}"))
            })?;
        for (_key, shard) in shards {
            snapshot.merge_shard(shard);
        }
        if let Some(shard) = store
            .get_item::<KeepSmallSchemaShard>(
                crate::db_operations::KEEP_SMALL_UNATTRIBUTED_SHARD_KEY,
            )
            .await
            .map_err(|e| {
                crate::schema::SchemaError::InvalidData(format!(
                    "hydrate keep-small unattributed shard: {e}"
                ))
            })?
        {
            snapshot.merge_shard(shard);
        }
        Ok(())
    }

    pub(super) async fn write_keep_small_header_and_shards(
        &self,
        snapshot: &crate::db_operations::KeepSmallSnapshot,
        only_schemas: Option<&HashSet<String>>,
    ) -> Result<(), crate::schema::SchemaError> {
        let Some(store) = self.keep_small_persist.as_ref() else {
            return Ok(());
        };
        let (header, shards) = snapshot.clone().into_header_and_shards(only_schemas);
        let mut items = Vec::with_capacity(shards.len() + 1);
        items.push((
            crate::db_operations::KEEP_SMALL_SNAPSHOT_KEY
                .as_bytes()
                .to_vec(),
            serde_json::to_vec(&header).map_err(|e| {
                crate::schema::SchemaError::InvalidData(format!("serialize keep-small header: {e}"))
            })?,
        ));
        for (key, shard) in shards {
            items.push((
                key.into_bytes(),
                serde_json::to_vec(&shard).map_err(|e| {
                    crate::schema::SchemaError::InvalidData(format!(
                        "serialize keep-small shard: {e}"
                    ))
                })?,
            ));
        }
        store.inner().batch_put(items).await.map_err(|e| {
            crate::schema::SchemaError::InvalidData(format!("persist keep-small: {e}"))
        })?;
        if only_schemas.is_none() {
            self.keep_small.clear_dirty_schemas();
        }
        Ok(())
    }

    pub(crate) async fn write_keep_small_snapshot(
        &self,
        snapshot: &crate::db_operations::KeepSmallSnapshot,
    ) -> Result<(), crate::schema::SchemaError> {
        self.write_keep_small_header_and_shards(snapshot, None)
            .await
    }
}
