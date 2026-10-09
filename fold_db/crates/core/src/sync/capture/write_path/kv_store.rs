//! `KvStore` wrapper that records physical writes for the mutation log.

use super::*;

pub(super) struct MutationLogCaptureKvStore {
    pub(super) inner: Arc<dyn KvStore>,
    pub(super) router: Arc<MutationLogCaptureRouter>,
    pub(super) namespace: String,
}

impl MutationLogCaptureKvStore {
    pub(super) fn capture_put_items(
        &self,
        items: &[(Vec<u8>, Vec<u8>)],
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        items
            .iter()
            .filter(|(key, _)| !policy::capture_should_skip_key(&self.namespace, key))
            .cloned()
            .collect()
    }

    pub(super) fn capture_delete_keys(&self, keys: &[Vec<u8>]) -> Vec<Vec<u8>> {
        keys.iter()
            .filter(|key| !policy::capture_should_skip_key(&self.namespace, key))
            .cloned()
            .collect()
    }
}

#[async_trait]
impl KvStore for MutationLogCaptureKvStore {
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.inner.get(key).await
    }

    async fn get_many(&self, keys: Vec<Vec<u8>>) -> StorageResult<Vec<Option<Vec<u8>>>> {
        self.inner.get_many(keys).await
    }

    async fn put(&self, key: &[u8], value: Vec<u8>) -> StorageResult<()> {
        if in_suppressed_capture(Some(&self.router)) {
            return self.inner.put(key, value).await;
        }
        let _activation_guard = self.router.enter_mutation().await;
        if policy::capture_should_skip_key(&self.namespace, key) {
            return self.inner.put(key, value).await;
        }
        // db_catalog membership is a small JSON row. Clone the body so a
        // member Mini can apply it from the org log. Other leftover catalog
        // planes hash the body (PhysicalDigest) and skip apply on replay.
        if policy::leftover_capture_is_applyable(&self.namespace) {
            let reservation = self.router.reserve_capture().await?;
            let item = (key.to_vec(), value.clone());
            self.inner.put(key, value).await?;
            if let Some(reservation) = reservation {
                reservation.submit(CaptureJobKind::ApplyablePut {
                    namespace: self.namespace.clone(),
                    items: vec![item],
                });
            }
            return Ok(());
        }
        // Leftover path (schemas / drain / tests): hash the body, do not
        // clone it into LogOp::Put. Product writes go through
        // capture_logical_commit (MutationIntent).
        let digest = leftover_body_digest(&value);
        let reservation = self.router.reserve_capture().await?;
        self.inner.put(key, value).await?;
        if let Some(reservation) = reservation {
            reservation.submit(CaptureJobKind::PhysicalDigest {
                namespace: self.namespace.clone(),
                keys: vec![key.to_vec()],
                digests: vec![(key.to_vec(), digest)],
            });
        }
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> StorageResult<bool> {
        if in_suppressed_capture(Some(&self.router)) {
            return self.inner.delete(key).await;
        }
        let _activation_guard = self.router.enter_mutation().await;
        let should_capture = !policy::capture_should_skip_key(&self.namespace, key);
        let reservation = if should_capture {
            self.router.reserve_capture().await?
        } else {
            None
        };
        let deleted = self.inner.delete(key).await?;
        if deleted {
            if let Some(reservation) = reservation {
                reservation.submit(CaptureJobKind::Delete {
                    namespace: self.namespace.clone(),
                    keys: vec![key.to_vec()],
                    batch: false,
                });
            }
        }
        Ok(deleted)
    }

    async fn exists(&self, key: &[u8]) -> StorageResult<bool> {
        self.inner.exists(key).await
    }

    async fn exists_many(&self, keys: Vec<Vec<u8>>) -> StorageResult<Vec<bool>> {
        self.inner.exists_many(keys).await
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan_prefix(prefix).await
    }

    async fn scan_prefix_keys(&self, prefix: &[u8]) -> StorageResult<Vec<Vec<u8>>> {
        self.inner.scan_prefix_keys(prefix).await
    }

    async fn scan_prefix_paged(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan_prefix_paged(prefix, limit).await
    }

    async fn scan_range(&self, start: &[u8], end: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan_range(start, end).await
    }

    async fn scan_range_paged(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan_range_paged(start, end, limit).await
    }

    async fn max_key_u64_after_marker(
        &self,
        prefix: &[u8],
        marker: &[u8],
    ) -> StorageResult<Option<u64>> {
        self.inner.max_key_u64_after_marker(prefix, marker).await
    }

    async fn scan_range_physical_paged(
        &self,
        start: &[u8],
        end: &[u8],
        cursor: Option<&PhysicalScanCursor>,
        limit: usize,
        max_handles: usize,
    ) -> StorageResult<PhysicalScanPage> {
        self.inner
            .scan_range_physical_paged(start, end, cursor, limit, max_handles)
            .await
    }

    async fn scan_prefix_lossy(&self, prefix: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan_prefix_lossy(prefix).await
    }

    async fn scan_prefix_partition_undecryptable(
        &self,
        prefix: &[u8],
    ) -> StorageResult<PartitionedScan> {
        self.inner.scan_prefix_partition_undecryptable(prefix).await
    }

    async fn batch_put(&self, items: Vec<(Vec<u8>, Vec<u8>)>) -> StorageResult<()> {
        if in_suppressed_capture(Some(&self.router)) {
            return self.inner.batch_put(items).await;
        }
        let _activation_guard = self.router.enter_mutation().await;
        let capture_items = self.capture_put_items(&items);
        if capture_items.is_empty() {
            return self.inner.batch_put(items).await;
        }
        if policy::leftover_capture_is_applyable(&self.namespace) {
            let reservation = self.router.reserve_capture().await?;
            self.inner.batch_put(items).await?;
            if let Some(reservation) = reservation {
                reservation.submit(CaptureJobKind::ApplyablePut {
                    namespace: self.namespace.clone(),
                    items: capture_items,
                });
            }
            return Ok(());
        }
        let capture_keys: Vec<Vec<u8>> = capture_items.iter().map(|(key, _)| key.clone()).collect();
        let digests: Vec<(Vec<u8>, [u8; 32])> = capture_items
            .into_iter()
            .map(|(key, value)| (key, leftover_body_digest(&value)))
            .collect();
        let reservation = self.router.reserve_capture().await?;
        self.inner.batch_put(items).await?;
        if let Some(reservation) = reservation {
            reservation.submit(CaptureJobKind::PhysicalDigest {
                namespace: self.namespace.clone(),
                keys: capture_keys,
                digests,
            });
        }
        Ok(())
    }

    fn batch_put_has_durable_barrier(&self) -> bool {
        self.inner.batch_put_has_durable_barrier()
    }

    async fn batch_mutate(&self, mutations: Vec<KvMutation>) -> StorageResult<()> {
        if in_suppressed_capture(Some(&self.router)) {
            return self.inner.batch_mutate(mutations).await;
        }
        let _activation_guard = self.router.enter_mutation().await;
        let mut digests = Vec::new();
        let mut deletes = Vec::new();
        for mutation in &mutations {
            match mutation {
                KvMutation::Put { key, value }
                    if !policy::capture_should_skip_key(&self.namespace, key) =>
                {
                    digests.push((key.clone(), leftover_body_digest(value)));
                }
                KvMutation::Delete { key }
                    if !policy::capture_should_skip_key(&self.namespace, key) =>
                {
                    deletes.push(key.clone());
                }
                _ => {}
            }
        }
        let keys = digests
            .iter()
            .map(|(key, _)| key.clone())
            .chain(deletes.iter().cloned())
            .collect::<Vec<_>>();
        let reservation = if keys.is_empty() {
            None
        } else {
            self.router.reserve_capture().await?
        };
        self.inner.batch_mutate(mutations).await?;
        if let Some(reservation) = reservation {
            reservation.submit(CaptureJobKind::Mixed {
                namespace: self.namespace.clone(),
                keys,
                digests,
                deletes,
            });
        }
        Ok(())
    }

    async fn batch_delete(&self, keys: Vec<Vec<u8>>) -> StorageResult<()> {
        if in_suppressed_capture(Some(&self.router)) {
            return self.inner.batch_delete(keys).await;
        }
        let _activation_guard = self.router.enter_mutation().await;
        let capture_keys = self.capture_delete_keys(&keys);
        let reservation = if capture_keys.is_empty() {
            None
        } else {
            self.router.reserve_capture().await?
        };
        self.inner.batch_delete(keys).await?;
        if let Some(reservation) = reservation {
            reservation.submit(CaptureJobKind::Delete {
                namespace: self.namespace.clone(),
                keys: capture_keys,
                batch: true,
            });
        }
        Ok(())
    }

    async fn flush(&self) -> StorageResult<()> {
        self.inner.flush().await
    }

    fn backend_name(&self) -> &'static str {
        self.inner.backend_name()
    }

    fn execution_model(&self) -> ExecutionModel {
        self.inner.execution_model()
    }

    fn flush_behavior(&self) -> FlushBehavior {
        self.inner.flush_behavior()
    }
}
