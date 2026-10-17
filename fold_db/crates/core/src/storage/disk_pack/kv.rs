use super::super::laststore::{native_routing, LastStoreKvStore};
use super::super::traits::{ExecutionModel, FlushBehavior, KvMutation};
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
        let runtime = Arc::clone(&self.runtime);
        let context = read::ReadContext::current(&runtime).await?;
        let pending = transaction::PendingBatch::current(&runtime);
        let namespace = self.namespace.clone();
        LastStoreKvStore::run_blocking(move || {
            let plans: Vec<_> = keys
                .iter()
                .map(|key| coordinates(&namespace, key))
                .collect();
            let mut out = vec![None; keys.len()];
            let rounds = plans.iter().map(Vec::len).max().unwrap_or(0);
            for round in 0..rounds {
                let slots: Vec<_> = (0..keys.len())
                    .filter(|slot| out[*slot].is_none() && plans[*slot].len() > round)
                    .collect();
                if slots.is_empty() {
                    break;
                }
                let addresses: Vec<_> = slots
                    .iter()
                    .map(|slot| plans[*slot][round].clone())
                    .collect();
                let bodies = runtime.raw_many(&context, &addresses, pending.as_ref())?;
                for (slot, body) in slots.into_iter().zip(bodies) {
                    out[slot] = body;
                }
            }
            Ok(out)
        })
        .await
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

fn coordinates(namespace: &str, key: &[u8]) -> Vec<(String, String)> {
    if namespace != "main" {
        return vec![(namespace.to_string(), native_routing::encode(key))];
    }
    let Ok(text) = std::str::from_utf8(key) else {
        return native_routing::read_candidates(key)
            .into_iter()
            .map(|collection| (collection.to_string(), native_routing::encode(key)))
            .collect();
    };
    let forms = crate::kind_partition::read_forms(text);
    let mut coordinates = Vec::new();
    for form in forms {
        for collection in native_routing::read_candidates(form.as_bytes()) {
            coordinates.push((
                collection.to_string(),
                native_routing::encode(form.as_bytes()),
            ));
        }
    }
    coordinates
}

fn prepare(
    runtime: &PackRuntime,
    context: &Arc<read::ReadContext>,
    pending: Option<&Arc<transaction::PendingBatch>>,
    namespace: &str,
    mutations: Vec<KvMutation>,
) -> StorageResult<Vec<HomeMutation>> {
    if mutations.len() > 4096 {
        return Err(StorageError::InvalidOperation(
            "native mutation exceeds operation admission limit".into(),
        ));
    }
    let plans: Vec<_> = mutations
        .iter()
        .map(|mutation| coordinates(namespace, mutation.key()))
        .collect();
    let targets: Vec<_> = mutations
        .iter()
        .map(|mutation| write_target(namespace, mutation.key()))
        .collect();
    let mut addresses = Vec::new();
    for ((mutation, plan), target) in mutations.iter().zip(&plans).zip(&targets) {
        for coordinate in plan {
            // A Put always overwrites its canonical target. Only alternate
            // coordinates need presence reads before their cleanup deletes.
            if matches!(mutation, KvMutation::Delete { .. }) || coordinate != target {
                addresses.push(coordinate.clone());
            }
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
    for ((mutation, plan), (target, key)) in mutations.into_iter().zip(plans).zip(targets) {
        match mutation {
            KvMutation::Put { value, .. } => {
                super::super::laststore::reject_enc_on_plaintext_catalog(namespace, &value)?;
                out.push(HomeMutation {
                    collection: target.clone(),
                    key: key.clone(),
                    body: Some(value),
                });
                present.insert((target.clone(), key.clone()), true);
                for (collection, legacy) in plan {
                    if (collection == target && legacy == key)
                        || present.get(&(collection.clone(), legacy.clone())) != Some(&true)
                    {
                        continue;
                    }
                    present.insert((collection.clone(), legacy.clone()), false);
                    out.push(HomeMutation {
                        collection,
                        key: legacy,
                        body: None,
                    });
                }
            }
            KvMutation::Delete { .. } => {
                for (collection, key) in plan {
                    if present.get(&(collection.clone(), key.clone())) == Some(&true) {
                        present.insert((collection.clone(), key.clone()), false);
                        out.push(HomeMutation {
                            collection,
                            key,
                            body: None,
                        });
                    }
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
