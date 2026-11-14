//! Namespace routing, exact key-value queries, and batch mutations.

mod scan;

use super::read::ReadContext;
use super::transaction::traits::KvMutation;
use super::*;
use crate::storage::format::{NAMESPACES, RECORD_FILE_PREFIX, REGISTRY};
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;

pub(in crate::storage) mod native_routing {
    use crate::storage::error::{StorageError, StorageResult};
    use crate::storage::format::{MAIN_NAMESPACE, RECORDS_COLLECTION};
    use base64::{engine::general_purpose::STANDARD, Engine as _};

    pub(in crate::storage) fn collection_for_namespace(ns: &str) -> &str {
        if ns == MAIN_NAMESPACE {
            RECORDS_COLLECTION
        } else {
            ns
        }
    }

    pub(in crate::storage) fn encode(key: &[u8]) -> String {
        match std::str::from_utf8(key)
            .ok()
            .filter(|s| !s.starts_with("b64:"))
        {
            Some(s) => s.to_owned(),
            None => format!("b64:{}", STANDARD.encode(key)),
        }
    }

    pub(in crate::storage) fn decode(key: &str) -> StorageResult<Vec<u8>> {
        let Some(enc) = key.strip_prefix("b64:") else {
            return Ok(key.as_bytes().to_vec());
        };
        STANDARD
            .decode(enc)
            .map_err(|e| StorageError::backend(format!("laststore key decode failed: {e}")))
    }
}

type BatchRef<'a> = Option<&'a Arc<transaction::PendingBatch>>;

fn parse_json<T: DeserializeOwned>(key: &str, bytes: &[u8]) -> StorageResult<T> {
    serde_json::from_slice(bytes)
        .map_err(|e| StorageError::SerializationError(format!("Failed to deserialize {key}: {e}")))
}

impl PackNamespacedStore {
    pub(crate) async fn open_namespace(&self, name: &str) -> StorageResult<PackKvStore> {
        if name.len() > 65531 || name == REGISTRY {
            let msg = "namespace exceeds format limit or uses the reserved registry identity";
            return Err(StorageError::invalid(msg));
        }
        self.runtime.open_store(name).await
    }
}

impl PackRuntime {
    async fn ensure_namespace(self: &Arc<Self>, name: &str) -> StorageResult<()> {
        let key = format!("{NAMESPACES}{name}");
        let ctx = ReadContext::current(self).await?;
        let pending = transaction::PendingBatch::current(self);
        if self.raw_many(&ctx, &[(REGISTRY.into(), key.clone())], pending.as_ref())?[0].is_none() {
            self.mutate(vec![HomeMutation::new(REGISTRY, key, Some(vec![1]))])
                .await?;
        }
        Ok(())
    }

    pub(in crate::storage) async fn open_store(
        self: &Arc<Self>,
        name: &str,
    ) -> StorageResult<PackKvStore> {
        if ReadContext::captured(self).is_none() {
            self.ensure_namespace(name).await?;
        }
        Ok(PackKvStore {
            runtime: Arc::clone(self),
            namespace: name.into(),
        })
    }

    pub(in crate::storage) fn stage_record_file_deletes(
        self: &Arc<Self>,
        keys: &[Vec<u8>],
    ) -> StorageResult<()> {
        let pending = transaction::PendingBatch::current(self).ok_or_else(|| {
            StorageError::invalid("file retirement requires a native write request")
        })?;
        let to_op = |key: &Vec<u8>| {
            let suffix = std::str::from_utf8(key)
                .ok()
                .and_then(|s| s.strip_prefix(RECORD_FILE_PREFIX));
            suffix
                .filter(|s| !s.is_empty())
                .map(|_| HomeMutation::new(CAS_COLLECTION, native_routing::encode(key), None))
                .ok_or_else(|| StorageError::invalid("file retirement requires a record file key"))
        };
        pending.stage(keys.iter().map(to_op).collect::<StorageResult<_>>()?)
    }
}

fn plan_mutations(
    rt: &PackRuntime,
    ctx: &Arc<ReadContext>,
    pending: BatchRef<'_>,
    namespace: &str,
    mutations: Vec<KvMutation>,
) -> StorageResult<Vec<HomeMutation>> {
    let limit = pending.map_or(4096, |p| p.operation_limit);
    if mutations.len() > limit {
        return Err(StorageError::invalid(format!(
            "native mutation exceeds {limit} operation limit"
        )));
    }
    let mut deletes = Vec::new();
    for mutation in &mutations {
        crate::storage::current_keys::validate(namespace, mutation.key())?;
        if matches!(mutation, KvMutation::Delete { .. }) {
            deletes.push(coordinate(namespace, mutation.key()));
        }
    }
    deletes.sort();
    deletes.dedup();
    let bodies = rt.raw_many(ctx, &deletes, pending)?;
    let mut present: BTreeMap<_, _> = deletes
        .into_iter()
        .zip(bodies.iter().map(Option::is_some))
        .collect();
    let mut out = Vec::with_capacity(mutations.len());
    for mutation in mutations {
        let coord = coordinate(namespace, mutation.key());
        match mutation {
            KvMutation::Put { value, .. } => {
                present.insert(coord.clone(), true);
                out.push(HomeMutation::new(coord.0, coord.1, Some(value)));
            }
            KvMutation::Delete { .. } if present.insert(coord.clone(), false) == Some(true) => {
                out.push(HomeMutation::new(coord.0, coord.1, None));
            }
            KvMutation::Delete { .. } => {}
        }
    }
    Ok(out)
}

impl PackKvStore {
    pub(crate) async fn get_item<T: DeserializeOwned>(
        &self,
        key: &str,
    ) -> StorageResult<Option<T>> {
        self.get(key.as_bytes())
            .await?
            .map(|bytes| parse_json(key, &bytes))
            .transpose()
    }

    pub(crate) async fn get_items<T: DeserializeOwned>(
        &self,
        keys: &[String],
    ) -> StorageResult<Vec<Option<T>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let req = keys.iter().map(|k| k.as_bytes().to_vec()).collect();
        let rows = self.get_many(req).await?;
        rows.into_iter()
            .zip(keys)
            .map(|(row, key)| row.map(|bytes| parse_json(key, &bytes)).transpose())
            .collect()
    }

    pub(crate) async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        let mut rows = self.get_many(vec![key.to_vec()]).await?;
        Ok(rows.pop().flatten())
    }

    pub(crate) async fn get_many(&self, keys: Vec<Vec<u8>>) -> StorageResult<Vec<Option<Vec<u8>>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        if keys.len() > 10_000 {
            return Err(StorageError::read_limit(
                "native request exceeds 10000 keys",
            ));
        }
        for key in &keys {
            crate::storage::current_keys::validate(&self.namespace, key)?;
        }
        let (rt, ns) = (Arc::clone(&self.runtime), self.namespace.clone());
        let ctx = ReadContext::current(&rt).await?;
        let pending = transaction::PendingBatch::current(&rt);
        run_blocking(move || {
            let coords: Vec<_> = keys.iter().map(|k| coordinate(&ns, k)).collect();
            rt.raw_many(&ctx, &coords, pending.as_ref())
        })
        .await
    }

    pub(in crate::storage) async fn batch_mutate(
        &self,
        mutations: Vec<KvMutation>,
    ) -> StorageResult<()> {
        let rt = Arc::clone(&self.runtime);
        transaction::scope(Arc::clone(&rt), async {
            rt.ensure_namespace(&self.namespace).await?;
            let ctx = ReadContext::current(&rt).await?;
            let pending = transaction::PendingBatch::current(&rt);
            let (ns, plan_rt) = (self.namespace.clone(), Arc::clone(&rt));
            let work = move || plan_mutations(&plan_rt, &ctx, pending.as_ref(), &ns, mutations);
            rt.mutate(run_blocking(work).await?).await
        })
        .await
    }
}

pub(in crate::storage::disk_pack) fn coordinate(ns: &str, key: &[u8]) -> (String, String) {
    let col = native_routing::collection_for_namespace(ns).into();
    (col, native_routing::encode(key))
}
