use super::*;

pub(super) struct PreparedCommit {
    operations: Vec<HomeMutation>,
    batch_bytes: u64,
    /// The v1 part and space quota; a v2 commit has none.
    quota: Option<V1Quota>,
}

struct V1Quota {
    reservation: laststore::disk_pack::home::MutationReservation,
    space: SpaceReservation,
}

impl PreparedCommit {
    pub(super) fn dependency_parts(&self) -> u64 {
        self.quota
            .as_ref()
            .map_or(0, |quota| quota.reservation.quote().dependency_parts())
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
            // Transfer both quotas without changing their ledger totals.
            if let (Some(incoming), Some(previous)) = (&mut self.incoming.quota, previous.quota) {
                incoming.space.absorb(previous.space);
                incoming.reservation.absorb(previous.reservation);
            }
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
                .required_features()
                & feature
                == feature
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
        self.check_part_room()?;
        let gate = self.enter_write().await;
        self.require_restore_validated()?;
        let runtime = Arc::clone(self);
        super::super::laststore::LastStoreKvStore::run_blocking(move || {
            let result = runtime.commit(operations);
            drop(gate);
            runtime.physical.release_idle();
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
        let v1 = match &self.physical {
            physical::Physical::V1(v1) => v1,
            physical::Physical::V2(_) => {
                let limit = self.batch_operation_limit();
                let batch_bytes = prefix_batch_bytes(previous, &operations, limit)?;
                return Ok(PreparedCommit {
                    operations,
                    batch_bytes,
                    quota: None,
                });
            }
        };
        let writer = v1.writer.lock().expect("native snapshot writer");
        let limit = writer.current().batch_operation_limit();
        let batch_bytes = prefix_batch_bytes(previous, &operations, limit)?;
        let previous_quota = previous.and_then(|previous| previous.quota.as_ref());
        let reservation = match previous_quota {
            Some(previous) => writer.reserve_mutation_extension(&previous.reservation, &operations),
            None => writer.reserve_mutation(&operations),
        }
        .map_err(|error| {
            self.note_capacity_refusal(&error);
            map(error)
        })?;
        let physical_bytes = reservation.quote().physical_bytes();
        let space = match previous_quota {
            Some(previous) => previous
                .space
                .reserve_extension(&self.space_policy, physical_bytes),
            None => writer.reserve_space(physical_bytes, &self.space_policy),
        }
        .map_err(map)?;
        Ok(PreparedCommit {
            operations,
            batch_bytes,
            quota: Some(V1Quota { reservation, space }),
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
        self.commit_prepared_group_merged(prepared, hidden_changes, requests, None)
    }

    pub(super) fn commit_prepared_group_merged(
        &self,
        prepared: PreparedCommit,
        hidden_changes: &[(String, String)],
        requests: &[Arc<transaction::PendingBatch>],
        compaction: Option<&maintenance::PackHandoff>,
    ) -> StorageResult<()> {
        let PreparedCommit {
            operations,
            batch_bytes: _,
            quota,
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
        let (v1, V1Quota { reservation, space }) = match (&self.physical, quota) {
            (physical::Physical::V1(v1), Some(quota)) => (v1, quota),
            (physical::Physical::V2(v2), None) if compaction.is_none() => {
                return self.commit_run(v2, operations, hidden_changes, &key_changes, requests);
            }
            _ => {
                return Err(StorageError::BackendError(
                    "native commit quota does not match the physical home".into(),
                ))
            }
        };
        let mut writer = v1.writer.lock().expect("native snapshot writer");
        let started = std::time::Instant::now();
        let (result, receipt) = match compaction {
            Some(handoff) => handoff.commit(&mut writer, operations, reservation),
            None => (writer.commit_mutation(operations, reservation), None),
        };
        let publication = match result {
            Ok(publication) => {
                self.parts
                    .observe(publication.dependency_parts, &publication.current);
                publication
            }
            Err(error) => {
                self.failed.store(true, Ordering::Release);
                *self.failed_write.lock().expect("native failed write owner") = Some(space);
                return Err(map(error));
            }
        };
        let _timing = laststore::commit_timing::stage("memory_publication_us");
        self.install_committed(
            physical::PhysicalRoot::V1(publication.current),
            hidden_changes,
            &key_changes,
            requests,
        )?;
        if let (Some(handoff), Some(receipt)) = (compaction, receipt) {
            handoff.record(self.finish_merged_pack(&writer, receipt, started));
        }
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
