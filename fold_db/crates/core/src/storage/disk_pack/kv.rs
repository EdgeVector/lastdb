//! Namespace routing, exact key-value queries, bounded scans, and batch mutations.

use super::read::{NativeRows, ReadContext};
use super::transaction::traits::{KvMutation, YieldedScan};
use super::*;
use crate::storage::format::{NAMESPACES, RECORD_FILE_PREFIX, REGISTRY};
use lastdb_model::schema::types::field::FilterUtils;
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
type KvRows = Vec<(Vec<u8>, Vec<u8>)>;
type ScanRes = StorageResult<KvRows>;
type YieldRes = StorageResult<YieldedScan>;
type JsonRows<T> = StorageResult<Vec<(String, T)>>;

fn parse_json<T: DeserializeOwned>(key: &str, bytes: &[u8]) -> StorageResult<T> {
    serde_json::from_slice(bytes)
        .map_err(|e| StorageError::SerializationError(format!("Failed to deserialize {key}: {e}")))
}

fn decode_json_rows<T: DeserializeOwned>(rows: KvRows) -> JsonRows<T> {
    rows.into_iter()
        .map(|(k, v)| {
            let key = String::from_utf8_lossy(&k).into_owned();
            parse_json(&key, &v).map(|val| (key, val))
        })
        .collect()
}

fn utf8_non_b64<'a>(
    bytes: &'a [u8],
    utf8_err: &'static str,
    b64_err: &'static str,
) -> StorageResult<&'a str> {
    let text = std::str::from_utf8(bytes).map_err(|_| StorageError::invalid(utf8_err))?;
    (!text.starts_with("b64:"))
        .then_some(text)
        .ok_or_else(|| StorageError::invalid(b64_err))
}

fn decode_rows(rows: NativeRows) -> ScanRes {
    rows.into_iter()
        .map(|(k, b)| Ok((native_routing::decode(&k)?, b)))
        .collect()
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

    pub(crate) async fn scan_items_with_prefix<T: DeserializeOwned>(&self, p: &str) -> JsonRows<T> {
        decode_json_rows(self.scan_prefix(p.as_bytes()).await?)
    }

    pub(crate) async fn scan_items_in_range_paged<T: DeserializeOwned>(
        &self,
        start: &str,
        end: &str,
        limit: usize,
    ) -> JsonRows<T> {
        decode_json_rows(
            self.scan_range_paged(start.as_bytes(), end.as_bytes(), limit)
                .await?,
        )
    }

    pub(crate) async fn scan_prefix(&self, prefix: &[u8]) -> ScanRes {
        let utf8_err = "native metadata prefix requires its UTF-8 key codec";
        let start = utf8_non_b64(
            prefix,
            utf8_err,
            "reserved native ids require exact-key reads",
        )?;
        let err = Some("native prefix result requires a bounded continuation");
        let end =
            FilterUtils::create_prefix_end(start).map_or_else(|_| vec![255], String::into_bytes);
        self.bounded_rows(start.as_bytes().to_vec(), end, 10_000, err)
            .await
    }

    pub(crate) async fn scan_range_paged(&self, start: &[u8], end: &[u8], limit: usize) -> ScanRes {
        if limit == 0 || start >= end {
            return Ok(Vec::new());
        }
        const RANGE_IDS: &str = "native range requires UTF-8 ids or a metadata limit marker";
        utf8_non_b64(start, "native range requires a UTF-8 start", RANGE_IDS)?;
        if std::str::from_utf8(end.strip_suffix(&[255]).unwrap_or(end)).is_err() {
            return Err(StorageError::invalid(RANGE_IDS));
        }
        self.bounded_rows(start.to_vec(), end.to_vec(), limit, None)
            .await
    }

    async fn run_scan<T: Send + 'static>(
        &self,
        work: impl FnOnce(&PackRuntime, &Arc<ReadContext>, BatchRef<'_>, &str) -> StorageResult<T>
            + Send
            + 'static,
    ) -> StorageResult<T> {
        let rt = Arc::clone(&self.runtime);
        let ctx = ReadContext::current(&rt).await?;
        let pending = transaction::PendingBatch::current(&rt);
        let col = native_routing::collection_for_namespace(&self.namespace).to_string();
        let obs = Arc::clone(&ctx);
        obs.remembered(run_blocking(move || work(&rt, &ctx, pending.as_ref(), &col)).await)
    }

    async fn bounded_rows(
        &self,
        start: Vec<u8>,
        end: Vec<u8>,
        limit: usize,
        err: Option<&'static str>,
    ) -> ScanRes {
        self.run_scan(move |rt, ctx, pending, col| {
            let (rows, exhausted, _) =
                rt.raw_range(ctx, (col, &start, &end), limit, pending, None)?;
            if let Some(detail) = err.filter(|_| !exhausted) {
                return Err(StorageError::read_limit(detail));
            }
            let mut rows = decode_rows(rows)?;
            rows.sort_by(|a, b| a.0.cmp(&b.0));
            rows.dedup_by(|a, b| a.0 == b.0);
            if err.is_some() && rows.len() > limit {
                let msg = format!("native complete metadata result exceeds {limit} distinct keys");
                return Err(StorageError::read_limit(msg));
            }
            rows.truncate(limit);
            Ok(rows)
        })
        .await
    }

    pub(crate) async fn scan_range_yielding(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
        keys_per_run: usize,
    ) -> YieldRes {
        let fallback = limit == 0
            || start >= end
            || utf8_non_b64(start, "", "").is_err()
            || std::str::from_utf8(end).is_err()
            || transaction::PendingBatch::current(&self.runtime).is_some();
        if fallback {
            let rows = self.scan_range_paged(start, end, limit).await?;
            let resume = rows
                .last()
                .filter(|_| limit > 0 && rows.len() >= limit)
                .map(|(k, _)| k.clone());
            return Ok(YieldedScan { rows, resume });
        }
        let (start, end) = (start.to_vec(), end.to_vec());
        self.run_scan(move |rt, ctx, _, col| {
            let (rows, _, res) =
                rt.raw_range(ctx, (col, &start, &end), limit, None, Some(keys_per_run))?;
            let rows = decode_rows(rows)?;
            let resume = res.map(|k| native_routing::decode(&k)).transpose()?;
            Ok(YieldedScan { rows, resume })
        })
        .await
    }
}

pub(in crate::storage::disk_pack) fn coordinate(ns: &str, key: &[u8]) -> (String, String) {
    let col = native_routing::collection_for_namespace(ns).into();
    (col, native_routing::encode(key))
}
