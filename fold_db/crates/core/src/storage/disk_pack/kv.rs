use super::super::traits::{KvMutation, YieldedScan};
use super::*;

pub(in crate::storage) mod native_routing {
    use crate::storage::error::{StorageError, StorageResult};
    use crate::storage::format::{MAIN_NAMESPACE, RECORDS_COLLECTION};
    use base64::{engine::general_purpose::STANDARD, Engine as _};

    /// Map a logical namespace to its one native collection.
    pub(in crate::storage) fn collection_for_namespace(namespace: &str) -> &str {
        if namespace == MAIN_NAMESPACE {
            RECORDS_COLLECTION
        } else {
            namespace
        }
    }

    pub(in crate::storage) fn encode(key: &[u8]) -> String {
        if let Ok(key) = std::str::from_utf8(key) {
            if !key.starts_with("b64:") {
                return key.to_string();
            }
        }
        format!("b64:{}", STANDARD.encode(key))
    }

    pub(in crate::storage) fn decode(key: &str) -> StorageResult<Vec<u8>> {
        let Some(encoded) = key.strip_prefix("b64:") else {
            return Ok(key.as_bytes().to_vec());
        };
        STANDARD
            .decode(encoded)
            .map_err(|e| StorageError::BackendError(format!("laststore key decode failed: {e}")))
    }
}

impl PackRuntime {
    pub(super) fn cas_kv(self: &Arc<Self>) -> PackKvStore {
        PackKvStore {
            runtime: Arc::clone(self),
            namespace: CAS_COLLECTION.to_string(),
        }
    }

    /// One cas row. Backup skips a local file whose sealed row is already valid.
    pub(in crate::storage) async fn cas_get(
        self: &Arc<Self>,
        key: &[u8],
    ) -> StorageResult<Option<Vec<u8>>> {
        self.ensure_namespace(CAS_COLLECTION).await?;
        self.cas_kv().get(key).await
    }

    /// Put one sealed cas row. The caller already checked the envelope.
    pub(in crate::storage) async fn cas_put(
        self: &Arc<Self>,
        key: &[u8],
        value: Vec<u8>,
    ) -> StorageResult<()> {
        self.cas_kv().put(key, value).await
    }

    /// Delete cas rows. An empty list publishes nothing.
    pub(in crate::storage) async fn cas_delete(
        self: &Arc<Self>,
        keys: Vec<Vec<u8>>,
    ) -> StorageResult<()> {
        if keys.is_empty() {
            return Ok(());
        }
        self.cas_kv()
            .batch_mutate(keys.into_iter().map(KvMutation::delete).collect())
            .await
    }
}

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
        run_blocking(move || {
            let addresses: Vec<_> = keys.iter().map(|key| coordinate(&namespace, key)).collect();
            runtime.raw_many(&context, &addresses, pending.as_ref())
        })
        .await
    }

    async fn change(&self, mutations: Vec<KvMutation>) -> StorageResult<()> {
        let runtime = Arc::clone(&self.runtime);
        transaction::scope(Arc::clone(&runtime), async {
            runtime.ensure_namespace(&self.namespace).await?;
            let context = read::ReadContext::current(&runtime).await?;
            let pending = transaction::PendingBatch::current(&runtime);
            let namespace = self.namespace.clone();
            let planning = Arc::clone(&runtime);
            let operations = run_blocking(move || {
                prepare(&planning, &context, pending.as_ref(), &namespace, mutations)
            })
            .await?;
            runtime.mutate(operations).await
        })
        .await
    }

    pub(crate) async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        Ok(self
            .values(vec![key.to_vec()])
            .await?
            .pop()
            .expect("one native key"))
    }

    pub(crate) async fn get_many(&self, keys: Vec<Vec<u8>>) -> StorageResult<Vec<Option<Vec<u8>>>> {
        self.values(keys).await
    }

    pub(crate) async fn put(&self, key: &[u8], value: Vec<u8>) -> StorageResult<()> {
        self.change(vec![KvMutation::put(key, value)]).await
    }

    pub(crate) async fn scan_prefix(
        &self,
        prefix: &[u8],
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.prefix_rows(prefix, 10000, true).await
    }

    pub(crate) async fn scan_prefix_paged(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.prefix_rows(prefix, limit, false).await
    }

    pub(crate) async fn scan_range_paged(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.range_rows(start, end, limit, false).await
    }

    pub(crate) async fn scan_range_yielding(
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

    pub(crate) async fn batch_mutate(&self, mutations: Vec<KvMutation>) -> StorageResult<()> {
        self.change(mutations).await
    }

    pub(crate) async fn flush(&self) -> StorageResult<()> {
        self.runtime.flush().await
    }
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
        .map(|mutation| coordinate(namespace, mutation.key()))
        .collect();
    let mut addresses: Vec<_> = mutations
        .iter()
        .zip(&targets)
        .filter(|(m, _)| matches!(m, KvMutation::Delete { .. }))
        .map(|(_, target)| target.clone())
        .collect();
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
                present.insert((target.clone(), key.clone()), true);
                out.push(HomeMutation {
                    collection: target,
                    key,
                    body: Some(value),
                });
            }
            KvMutation::Delete { .. } => {
                if present.insert((target.clone(), key.clone()), false) == Some(true) {
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

pub(in crate::storage::disk_pack) fn coordinate(namespace: &str, key: &[u8]) -> (String, String) {
    (
        native_routing::collection_for_namespace(namespace).to_string(),
        native_routing::encode(key),
    )
}
