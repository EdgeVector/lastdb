use super::*;

pub(super) struct PreparedCommit {
    operations: Vec<HomeMutation>,
    batch_bytes: u64,
    space: SpaceReservation,
}

impl PreparedCommit {
    /// A v2 batch: its bytes with the accepted prefix, and the space that
    /// `prepare_run` reserved for them.
    pub(super) fn for_run(
        operations: Vec<HomeMutation>,
        batch_bytes: u64,
        space: SpaceReservation,
    ) -> Self {
        Self {
            operations,
            batch_bytes,
            space,
        }
    }

    pub(super) fn space(&self) -> &SpaceReservation {
        &self.space
    }

    pub(super) fn cas_delete_key_bytes(&self) -> StorageResult<u64> {
        cas_delete::key_bytes(&self.operations)
    }
}

/// New operations and added space; the accepted prefix stays in the queue.
pub(super) struct PreparedAppend {
    incoming: PreparedCommit,
}

impl PreparedAppend {
    pub(super) fn reserve_capacity(
        &self,
        previous: Option<&mut PreparedCommit>,
    ) -> StorageResult<()> {
        if let Some(previous) = previous {
            previous
                .operations
                .try_reserve(self.incoming.operations.len())
                .map_err(|_| StorageError::WriteResourceLimit {
                    detail: "native prefix operation allocation refused".into(),
                })?;
        }
        Ok(())
    }

    /// Reserved capacity and added space precede visible publication.
    pub(super) fn finish(mut self, previous: Option<PreparedCommit>) -> PreparedCommit {
        if let Some(mut previous) = previous {
            previous.operations.append(&mut self.incoming.operations);
            self.incoming.operations = std::mem::take(&mut previous.operations);
            // The write gate retains this exact prefix during preparation.
            // Transfer the space quota without changing its ledger total.
            self.incoming.space.absorb(previous.space);
        }
        self.incoming
    }
}

/// Durable native boundary after all earlier accepted batches finish.
#[derive(serde::Serialize)]
pub struct NativeFlushReceipt {
    pub generation: u64,
    pub frontier: u64,
    pub target_sequence: u64,
    pub completed_sequence: u64,
}

impl PackRuntime {
    /// Most operations that one complete batch holds on this home.
    pub(crate) fn batch_operation_limit(&self) -> usize {
        physical::V2_BATCH_OPERATIONS
    }

    /// Wake an accepted owner and capture the durable boundary after its flush.
    pub async fn flush_native(self: &Arc<Self>) -> StorageResult<NativeFlushReceipt> {
        let target_sequence = self.flush_prefix().await?;
        let view = self.view.read().expect("native durable flush view");
        Ok(NativeFlushReceipt {
            generation: view.current.generation(),
            frontier: view.current.frontier(),
            target_sequence,
            completed_sequence: *self.completed.borrow(),
        })
    }

    pub(super) async fn ensure_namespace(self: &Arc<Self>, name: &str) -> StorageResult<()> {
        let key = format!("{NAMESPACES}{name}");
        let context = read::ReadContext::current(self).await?;
        let pending = transaction::PendingBatch::current(self);
        if self
            .raw_get(&context, REGISTRY, &key, pending.as_ref())?
            .is_none()
        {
            self.mutate(vec![HomeMutation {
                collection: REGISTRY.into(),
                key,
                body: Some(vec![1]),
            }])
            .await?;
        }
        Ok(())
    }

    pub(super) async fn mutate(
        self: &Arc<Self>,
        operations: Vec<HomeMutation>,
    ) -> StorageResult<()> {
        if let Some(pending) = transaction::PendingBatch::current(self) {
            return pending.stage(operations);
        }
        let gate = self.enter_write().await;
        self.require_restore_validated()?;
        let runtime = Arc::clone(self);
        super::run_blocking(move || {
            let result = runtime.commit(operations);
            drop(gate);
            result
        })
        .await
    }

    pub(super) fn commit(&self, operations: Vec<HomeMutation>) -> StorageResult<()> {
        if operations.is_empty() {
            return Ok(());
        }
        self.commit_prepared(self.prepare_commit(operations)?)
    }

    pub(super) fn prepare_commit(
        &self,
        operations: Vec<HomeMutation>,
    ) -> StorageResult<PreparedCommit> {
        self.prepare_commit_extension(operations, None)
    }

    pub(super) fn prepare_append(
        &self,
        operations: Vec<HomeMutation>,
        previous: Option<&PreparedCommit>,
    ) -> StorageResult<PreparedAppend> {
        Ok(PreparedAppend {
            incoming: self.prepare_commit_extension(operations, previous)?,
        })
    }

    fn prepare_commit_extension(
        &self,
        operations: Vec<HomeMutation>,
        previous: Option<&PreparedCommit>,
    ) -> StorageResult<PreparedCommit> {
        self.validate_initialization_write(&operations)?;
        if self.failed.load(Ordering::Acquire) {
            return Err(StorageError::BackendError(
                "native writer requires a snapshot reopen after a failed publication".into(),
            ));
        }
        self.prepare_run(&self.physical, operations, previous)
    }

    pub(super) fn commit_prepared(&self, prepared: PreparedCommit) -> StorageResult<()> {
        let changes = prepared
            .operations
            .iter()
            .map(|operation| (operation.collection.clone(), operation.key.clone()))
            .collect::<Vec<_>>();
        self.commit_prepared_group(prepared, &changes, &[])
    }

    pub(super) fn commit_prepared_group(
        &self,
        prepared: PreparedCommit,
        hidden_changes: &[(String, String)],
        requests: &[Arc<transaction::PendingBatch>],
    ) -> StorageResult<()> {
        let PreparedCommit {
            operations,
            batch_bytes: _,
            space,
        } = prepared;
        // A direct durable publication consumes its bodies below. Keep only
        // membership coordinates; queued flushes already published them.
        let key_changes = if hidden_changes.is_empty() {
            Vec::new()
        } else {
            operations
                .iter()
                .map(|operation| {
                    (
                        operation.collection.clone(),
                        operation.key.clone(),
                        operation.body.is_none(),
                    )
                })
                .collect()
        };
        let changes = (hidden_changes, key_changes.as_slice());
        self.commit_run(&self.physical, operations, space, changes, requests)
    }

    pub(super) async fn flush(self: &Arc<Self>) -> StorageResult<()> {
        if transaction::PendingBatch::current(self).is_some() {
            return Ok(());
        }
        self.flush_prefix().await.map(|_| ())
    }

    async fn flush_prefix(self: &Arc<Self>) -> StorageResult<u64> {
        let target = self.seal_queue();
        self.wait_for_queue(target).await?;
        Ok(target)
    }

    pub(super) async fn enter_write(self: &Arc<Self>) -> WriteGuard {
        loop {
            let gate = self.enter_prepare().await;
            let target = self.seal_queue();
            if target <= *self.completed.borrow() || self.failed.load(Ordering::Acquire) {
                let publication = Arc::clone(&self.write_gate).lock_owned().await;
                return WriteGuard {
                    _preparation: gate,
                    _publication: publication,
                };
            }
            drop(gate);
            let _ = self.wait_for_queue(target).await;
        }
    }
}

pub(super) struct WriteGuard {
    _preparation: tokio::sync::OwnedMutexGuard<()>,
    _publication: tokio::sync::OwnedMutexGuard<()>,
}

pub(super) fn prefix_batch_bytes(
    previous: Option<&PreparedCommit>,
    incoming: &[HomeMutation],
    limit: usize,
) -> StorageResult<u64> {
    let bytes = transaction::batch_bytes(incoming, limit)?;
    let Some(previous) = previous else {
        return Ok(bytes);
    };
    let operations = previous.operations.len().checked_add(incoming.len());
    let bytes = previous
        .batch_bytes
        .checked_add(bytes)
        .and_then(|bytes| bytes.checked_sub(256));
    if operations.is_none_or(|count| count > limit)
        || bytes
            .is_none_or(|bytes| bytes > lastdb_storage_policy::native_write::PENDING_MEMORY_BYTES)
    {
        return Err(StorageError::WriteResourceLimit {
            detail: "native accepted prefix reached its operation or byte limit; retry after flush"
                .into(),
        });
    }
    Ok(bytes.expect("checked native prefix bytes"))
}
