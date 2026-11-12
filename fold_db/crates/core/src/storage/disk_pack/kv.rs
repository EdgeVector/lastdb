use super::super::traits::{KvMutation, YieldedScan};
use super::*;
use std::collections::BTreeMap;

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
        match std::str::from_utf8(key) {
            Ok(s) if !s.starts_with("b64:") => s.to_string(),
            _ => format!("b64:{}", STANDARD.encode(key)),
        }
    }

    pub(in crate::storage) fn decode(key: &str) -> StorageResult<Vec<u8>> {
        match key.strip_prefix("b64:") {
            Some(enc) => STANDARD.decode(enc).map_err(|e| {
                StorageError::BackendError(format!("laststore key decode failed: {e}"))
            }),
            None => Ok(key.as_bytes().to_vec()),
        }
    }
}

impl PackRuntime {
    pub(super) fn cas_kv(self: &Arc<Self>) -> PackKvStore {
        PackKvStore {
            runtime: Arc::clone(self),
            namespace: CAS_COLLECTION.to_string(),
        }
    }

    pub(in crate::storage) async fn cas_get(
        self: &Arc<Self>,
        key: &[u8],
    ) -> StorageResult<Option<Vec<u8>>> {
        self.ensure_namespace(CAS_COLLECTION).await?;
        self.cas_kv().get(key).await
    }

    pub(in crate::storage) async fn cas_put(
        self: &Arc<Self>,
        key: &[u8],
        value: Vec<u8>,
    ) -> StorageResult<()> {
        self.cas_kv().put(key, value).await
    }

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
    pub(crate) async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        Ok(self
            .get_many(vec![key.to_vec()])
            .await?
            .pop()
            .expect("one native key"))
    }

    pub(crate) async fn get_many(&self, keys: Vec<Vec<u8>>) -> StorageResult<Vec<Option<Vec<u8>>>> {
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
        let rt = Arc::clone(&self.runtime);
        let ctx = read::ReadContext::current(&rt).await?;
        let pending = transaction::PendingBatch::current(&rt);
        let ns = self.namespace.clone();
        run_blocking(move || {
            let coords: Vec<_> = keys.iter().map(|k| coordinate(&ns, k)).collect();
            rt.raw_many(&ctx, &coords, pending.as_ref())
        })
        .await
    }

    pub(crate) async fn put(&self, key: &[u8], value: Vec<u8>) -> StorageResult<()> {
        self.batch_mutate(vec![KvMutation::put(key, value)]).await
    }

    pub(crate) async fn scan_prefix(
        &self,
        prefix: &[u8],
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.prefix_rows(prefix, 10000, true).await
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
        let rt = Arc::clone(&self.runtime);
        transaction::scope(Arc::clone(&rt), async {
            rt.ensure_namespace(&self.namespace).await?;
            let ctx = read::ReadContext::current(&rt).await?;
            let pending = transaction::PendingBatch::current(&rt);
            let (ns, planning) = (self.namespace.clone(), Arc::clone(&rt));
            let ops =
                run_blocking(move || prepare(&planning, &ctx, pending.as_ref(), &ns, mutations))
                    .await?;
            rt.mutate(ops).await
        })
        .await
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
    let limit = pending.map_or(4096, |p| p.operation_limit);
    if mutations.len() > limit {
        return Err(StorageError::InvalidOperation(format!(
            "native mutation exceeds {limit} operation admission limit"
        )));
    }
    for m in &mutations {
        super::super::current_keys::validate(namespace, m.key())?;
    }
    let targets: Vec<_> = mutations
        .iter()
        .map(|m| coordinate(namespace, m.key()))
        .collect();
    let mut del_coords: Vec<_> = mutations
        .iter()
        .zip(&targets)
        .filter(|(m, _)| matches!(m, KvMutation::Delete { .. }))
        .map(|(_, t)| t.clone())
        .collect();
    del_coords.sort();
    del_coords.dedup();
    let bodies = runtime.raw_many(context, &del_coords, pending)?;
    let mut present: BTreeMap<_, _> = del_coords
        .into_iter()
        .zip(bodies.iter().map(Option::is_some))
        .collect();
    let mut out = Vec::new();
    for (m, (collection, key)) in mutations.into_iter().zip(targets) {
        match m {
            KvMutation::Put { value, .. } => {
                present.insert((collection.clone(), key.clone()), true);
                out.push(HomeMutation {
                    collection,
                    key,
                    body: Some(value),
                });
            }
            KvMutation::Delete { .. } => {
                if present.insert((collection.clone(), key.clone()), false) == Some(true) {
                    out.push(HomeMutation {
                        collection,
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
