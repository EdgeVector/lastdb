use super::super::laststore::{native_routing, LastStoreKvStore};
use super::super::traits::{ExecutionModel, FlushBehavior, KvMutation, YieldedScan};
use super::*;

impl PackKvStore {
    async fn values(&self, keys: Vec<Vec<u8>>) -> StorageResult<Vec<Option<Vec<u8>>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        if keys.len() > 10000 {
            return Err(StorageError::ReadResourceLimit {
                detail: "native request exceeds 10000 keys".into(),
            });
        }
        for key in &keys {
            super::super::current_keys::validate(&self.namespace, key)?;
        }
        let runtime = Arc::clone(&self.runtime);
        let context = read::ReadContext::current(&runtime).await?;
        let pending = transaction::PendingBatch::current(&runtime);
        let namespace = self.namespace.clone();
        let observed = Arc::clone(&runtime);
        let (values, keys, addresses) = LastStoreKvStore::run_blocking(move || {
            let addresses: Vec<_> = keys
                .iter()
                .map(|key| write_target(&namespace, key))
                .collect();
            let bodies = runtime.raw_many(&context, &addresses, pending.as_ref())?;
            Ok((bodies, keys, addresses))
        })
        .await?;
        for ((requested, (collection, key)), body) in keys.into_iter().zip(addresses).zip(&values) {
            logical_paths::source::note_read_source(
                Arc::clone(&observed),
                &requested,
                body.as_ref().map(|_| (collection.as_str(), key.as_str())),
            )?;
        }
        Ok(values)
    }

    async fn change(&self, mutations: Vec<KvMutation>) -> StorageResult<()> {
        let runtime = Arc::clone(&self.runtime);
        transaction::scope(Some(Arc::clone(&runtime)), async {
            runtime.ensure_namespace(&self.namespace).await?;
            let context = read::ReadContext::current(&runtime).await?;
            let pending = transaction::PendingBatch::current(&runtime);
            let namespace = self.namespace.clone();
            let planning = Arc::clone(&runtime);
            let operations = LastStoreKvStore::run_blocking(move || {
                prepare(&planning, &context, pending.as_ref(), &namespace, mutations)
            })
            .await?;
            runtime.mutate(operations).await
        })
        .await
    }
}

pub(super) fn coordinates(namespace: &str, key: &[u8]) -> [(String, String); 1] {
    [write_target(namespace, key)]
}

fn prepare(
    runtime: &PackRuntime,
    context: &Arc<read::ReadContext>,
    pending: Option<&Arc<transaction::PendingBatch>>,
    namespace: &str,
    mutations: Vec<KvMutation>,
) -> StorageResult<Vec<HomeMutation>> {
    let operation_limit = pending.map_or(4096, |pending| pending.operation_limit());
    if mutations.len() > operation_limit {
        return Err(StorageError::InvalidOperation(format!(
            "native mutation exceeds {operation_limit} operation admission limit"
        )));
    }
    for mutation in &mutations {
        super::super::current_keys::validate(namespace, mutation.key())?;
    }
    let targets: Vec<_> = mutations
        .iter()
        .map(|mutation| write_target(namespace, mutation.key()))
        .collect();
    let mut addresses = Vec::new();
    for (mutation, target) in mutations.iter().zip(&targets) {
        if matches!(mutation, KvMutation::Delete { .. }) {
            addresses.push(target.clone());
        }
    }
    addresses.sort();
    addresses.dedup();
    context.record_stage("write-probes-start", addresses.len());
    let bodies = runtime.raw_many(context, &addresses, pending)?;
    context.record_stage("write-probes-done", addresses.len());
    let mut present: std::collections::BTreeMap<_, _> = addresses
        .into_iter()
        .zip(bodies.into_iter().map(|body| body.is_some()))
        .collect();
    let mut out = Vec::new();
    for (mutation, (target, key)) in mutations.into_iter().zip(targets) {
        match mutation {
            KvMutation::Put { value, .. } => {
                super::super::laststore::reject_enc_on_plaintext_catalog(namespace, &value)?;
                out.push(HomeMutation {
                    collection: target.clone(),
                    key: key.clone(),
                    body: Some(value),
                });
                present.insert((target.clone(), key.clone()), true);
            }
            KvMutation::Delete { .. } => {
                if present.get(&(target.clone(), key.clone())) == Some(&true) {
                    present.insert((target.clone(), key.clone()), false);
                    out.push(HomeMutation {
                        collection: target,
                        key,
                        body: None,
                    });
                }
            }
        }
    }
    Ok(out)
}

fn write_target(namespace: &str, key: &[u8]) -> (String, String) {
    let collection = if namespace == "main" {
        native_routing::write_collection(key)
    } else {
        namespace
    };
    (collection.to_string(), native_routing::encode(key))
}

#[async_trait::async_trait]
impl KvStore for PackKvStore {
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        Ok(self
            .values(vec![key.to_vec()])
            .await?
            .pop()
            .expect("one native key"))
    }

    async fn get_many(&self, keys: Vec<Vec<u8>>) -> StorageResult<Vec<Option<Vec<u8>>>> {
        self.values(keys).await
    }

    async fn put(&self, key: &[u8], value: Vec<u8>) -> StorageResult<()> {
        self.change(vec![KvMutation::put(key, value)]).await
    }

    async fn compare_and_swap(
        &self,
        key: &[u8],
        expected: &[u8],
        new: Option<Vec<u8>>,
    ) -> StorageResult<bool> {
        transaction::scope(Some(Arc::clone(&self.runtime)), async {
            if self.get(key).await?.as_deref() != Some(expected) {
                return Ok(false);
            }
            let mutation = match new {
                Some(value) => KvMutation::put(key, value),
                None => KvMutation::delete(key),
            };
            self.change(vec![mutation]).await?;
            Ok(true)
        })
        .await
    }

    /// One transaction: read every key, then delete the keys that match in one
    /// commit. The compare and the delete sit in the same scope as the single
    /// key compare-and-swap above.
    async fn compare_and_delete_many(
        &self,
        items: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> StorageResult<Vec<bool>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        transaction::scope(Some(Arc::clone(&self.runtime)), async {
            let keys = items.iter().map(|(key, _)| key.clone()).collect();
            let current = self.values(keys).await?;
            let matched: Vec<bool> = items
                .iter()
                .zip(&current)
                .map(|((_, expected), found)| found.as_deref() == Some(expected.as_slice()))
                .collect();
            let deletes: Vec<_> = items
                .iter()
                .zip(&matched)
                .filter(|(_, matched)| **matched)
                .map(|((key, _), _)| KvMutation::delete(key.clone()))
                .collect();
            if !deletes.is_empty() {
                self.change(deletes).await?;
            }
            Ok(matched)
        })
        .await
    }

    async fn delete(&self, key: &[u8]) -> StorageResult<bool> {
        transaction::scope(Some(Arc::clone(&self.runtime)), async {
            if self.get(key).await?.is_none() {
                return Ok(false);
            }
            self.change(vec![KvMutation::delete(key)]).await?;
            Ok(true)
        })
        .await
    }

    async fn exists(&self, key: &[u8]) -> StorageResult<bool> {
        Ok(self.get(key).await?.is_some())
    }

    async fn exists_many(&self, keys: Vec<Vec<u8>>) -> StorageResult<Vec<bool>> {
        Ok(self
            .values(keys)
            .await?
            .into_iter()
            .map(|body| body.is_some())
            .collect())
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.prefix_rows(prefix, 10000, true).await
    }

    async fn scan_prefix_paged(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.prefix_rows(prefix, limit, false).await
    }

    async fn scan_range(&self, start: &[u8], end: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.range_rows(start, end, 10000, true).await
    }

    async fn scan_range_paged(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.range_rows(start, end, limit, false).await
    }

    async fn scan_range_yielding(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
        keys_per_row: usize,
    ) -> StorageResult<YieldedScan> {
        let (rows, resume) = self
            .range_rows_yielding(start, end, limit, keys_per_row)
            .await?;
        Ok(YieldedScan { rows, resume })
    }

    async fn batch_put(&self, items: Vec<(Vec<u8>, Vec<u8>)>) -> StorageResult<()> {
        self.change(
            items
                .into_iter()
                .map(|(key, value)| KvMutation::Put { key, value })
                .collect(),
        )
        .await
    }

    fn batch_put_has_durable_barrier(&self) -> bool {
        transaction::PendingBatch::current(&self.runtime).is_none()
    }

    async fn batch_mutate(&self, mutations: Vec<KvMutation>) -> StorageResult<()> {
        self.change(mutations).await
    }

    async fn batch_delete(&self, keys: Vec<Vec<u8>>) -> StorageResult<()> {
        self.change(keys.into_iter().map(KvMutation::delete).collect())
            .await
    }

    async fn flush(&self) -> StorageResult<()> {
        self.runtime.flush().await
    }
    fn backend_name(&self) -> &'static str {
        "laststore-disk-pack"
    }
    fn execution_model(&self) -> ExecutionModel {
        ExecutionModel::SyncWrapped
    }
    fn flush_behavior(&self) -> FlushBehavior {
        FlushBehavior::Persists
    }
}
