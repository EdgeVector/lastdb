use super::*;

pub(super) struct PreparedCommit {
    operations: Vec<HomeMutation>,
    batch_bytes: u64,
    reservation: laststore::disk_pack::home::MutationReservation,
    space: SpaceReservation,
    accepted_ms: u64,
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
            // Transfer both quotas without changing their ledger totals.
            self.incoming.space.absorb(previous.space);
            self.incoming.reservation.absorb(previous.reservation);
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
        self.view
            .read()
            .expect("native complete write view")
            .current
            .batch_operation_limit()
    }

    /// The exclusive request gate fixes this complete root during preparation.
    pub(crate) fn write_has_feature(&self, feature: u64) -> bool {
        transaction::PendingBatch::current(self).is_some()
            && self
                .view
                .read()
                .expect("native complete write view")
                .current
                .required_features
                & feature
                == feature
    }

    /// Wake an accepted owner and capture the durable boundary after its flush.
    pub async fn flush_native(self: &Arc<Self>) -> StorageResult<NativeFlushReceipt> {
        let target_sequence = self.flush_prefix().await?;
        let view = self.view.read().expect("native durable flush view");
        Ok(NativeFlushReceipt {
            generation: view.current.generation,
            frontier: view.current.frontier,
            target_sequence,
            completed_sequence: *self.completed.borrow(),
        })
    }

    /// Consume only a constructor-proven empty boundary under the native gate.
    pub(crate) fn claim_fresh_empty_boundary(&self) -> bool {
        transaction::PendingBatch::current(self).is_some()
            && self
                .fresh_empty
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
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
        let runtime = Arc::clone(self);
        super::super::laststore::LastStoreKvStore::run_blocking(move || {
            let _gate = gate;
            runtime.commit(operations)
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
        if self.failed.load(Ordering::Acquire) {
            return Err(StorageError::BackendError(
                "native writer requires complete journal recovery after a failed publication"
                    .into(),
            ));
        }
        let writer = self.journal.lock().expect("native journal owner");
        let accepted_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| StorageError::BackendError(error.to_string()))?
            .as_millis()
            .try_into()
            .map_err(|_| StorageError::BackendError("native accepted time overflow".into()))?;
        let limit = writer.current().batch_operation_limit();
        let batch_bytes = prefix_batch_bytes(previous, &operations, limit)?;
        let reservation = match previous {
            Some(previous) => writer.reserve_mutation_extension(&previous.reservation, &operations),
            None => writer.reserve_mutation(&operations),
        }
        .map_err(map)?;
        let physical_bytes = reservation.quote().physical_bytes();
        let space = match previous {
            Some(previous) => previous
                .space
                .reserve_extension(&self.space_policy, physical_bytes),
            None => writer.reserve_space(physical_bytes, &self.space_policy),
        }
        .map_err(map)?;
        Ok(PreparedCommit {
            operations,
            batch_bytes,
            reservation,
            space,
            accepted_ms,
        })
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
            reservation,
            space,
            accepted_ms,
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
        let mut writer = self.journal.lock().expect("native journal owner");
        let result = writer.commit_mutation(operations, accepted_ms, reservation);
        let publication = match result {
            Ok(publication) => publication,
            Err(error) => {
                self.failed.store(true, Ordering::Release);
                *self.failed_write.lock().expect("native failed write owner") = Some(space);
                return Err(map(error));
            }
        };
        self.install_committed(publication.current, hidden_changes, &key_changes, requests)?;
        drop(space);
        self.failed.store(false, Ordering::Release);
        Ok(())
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

    pub(super) async fn enter_write(self: &Arc<Self>) -> tokio::sync::OwnedMutexGuard<()> {
        loop {
            let gate = self.enter_prepare().await;
            let target = self.seal_queue();
            if target <= *self.completed.borrow() || self.failed.load(Ordering::Acquire) {
                return gate;
            }
            drop(gate);
            let _ = self.wait_for_queue(target).await;
        }
    }
}

fn prefix_batch_bytes(
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
        || bytes.is_none_or(|bytes| bytes > 128 * 1024 * 1024)
    {
        return Err(StorageError::WriteResourceLimit {
            detail: "native accepted prefix reached its operation or byte limit; retry after flush"
                .into(),
        });
    }
    Ok(bytes.expect("checked native prefix bytes"))
}
