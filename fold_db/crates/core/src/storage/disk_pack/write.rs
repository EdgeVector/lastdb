use super::*;

pub(super) struct PreparedCommit {
    operations: Vec<HomeMutation>,
    reservation: laststore::disk_pack::home::MutationReservation,
    pub(super) protection: Arc<admission::WriteProtection>,
    accepted_ms: u64,
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
        transaction::validate(&operations, writer.current().batch_operation_limit())?;
        let reservation = writer.reserve_mutation(&operations).map_err(map)?;
        let space = writer
            .reserve_space(reservation.quote().physical_bytes(), &self.space_policy)
            .map_err(map)?;
        let protection = Arc::new(admission::WriteProtection::reserve(space));
        Ok(PreparedCommit {
            operations,
            reservation,
            protection,
            accepted_ms,
        })
    }

    pub(super) fn commit_prepared(&self, prepared: PreparedCommit) -> StorageResult<()> {
        let changes = prepared
            .operations
            .iter()
            .map(|operation| {
                validity::changed(
                    operation.collection.clone(),
                    operation.key.clone(),
                    operation.body.as_deref(),
                )
            })
            .collect();
        self.commit_prepared_group(prepared, changes)
    }

    pub(super) fn commit_prepared_group(
        &self,
        prepared: PreparedCommit,
        hidden_changes: Vec<validity::ChangedCoordinate>,
    ) -> StorageResult<()> {
        let PreparedCommit {
            operations,
            reservation,
            protection,
            accepted_ms,
        } = prepared;
        let mut writer = self.journal.lock().expect("native journal owner");
        let result = writer.commit_mutation(operations, accepted_ms, reservation);
        let publication = match result {
            Ok(publication) => publication,
            Err(error) => {
                self.failed.store(true, Ordering::Release);
                *self.failed_write.lock().expect("native failed write owner") = Some(protection);
                return Err(map(error));
            }
        };
        self.install_committed(publication.current, hidden_changes)?;
        protection.release_space();
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
