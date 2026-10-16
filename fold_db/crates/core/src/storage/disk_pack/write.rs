use super::*;

impl PackRuntime {
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
        let gate = Arc::clone(&self.write_gate).lock_owned().await;
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
        if self.failed.load(Ordering::Acquire) {
            return Err(StorageError::BackendError(
                "native writer requires complete journal recovery after a failed publication"
                    .into(),
            ));
        }
        let mut writer = self.journal.lock().expect("native journal owner");
        let accepted_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| StorageError::BackendError(error.to_string()))?
            .as_millis()
            .try_into()
            .map_err(|_| StorageError::BackendError("native accepted time overflow".into()))?;
        transaction::validate(&operations)?;
        let reservation = writer.reserve_mutation(&operations).map_err(map)?;
        let space = writer
            .reserve_space(
                reservation.quote().physical_bytes(),
                &laststore::disk_pack::maintenance::SpacePolicy::default(),
            )
            .map_err(map)?;
        let protection = admission::WriteProtection::reserve(self, &operations)?.with_space(space);
        let result = writer
            .append_mutation(operations, accepted_ms, reservation)
            .and_then(|_| writer.publish());
        let publication = match result {
            Ok(publication) => publication,
            Err(error) => {
                self.failed.store(true, Ordering::Release);
                *self.failed_write.lock().expect("native failed write owner") = Some(protection);
                return Err(map(error));
            }
        };
        protection.invalidate();
        self.install_committed(publication.current.clone());
        self.failed.store(false, Ordering::Release);
        Ok(())
    }

    pub(super) async fn flush(self: &Arc<Self>) -> StorageResult<()> {
        if transaction::PendingBatch::current(self).is_some() {
            return Ok(());
        }
        let _gate = Arc::clone(&self.write_gate).lock_owned().await;
        if self.failed.load(Ordering::Acquire) {
            return Err(StorageError::BackendError(
                "native flush cannot cover a failed directory publication".into(),
            ));
        }
        // Each ordinary batch returns only after the complete root barrier.
        Ok(())
    }
}
