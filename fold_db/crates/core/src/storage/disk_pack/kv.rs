use super::read::{NativeRows, ReadContext};
use super::transaction::traits::{KvMutation, YieldedScan};
use super::*;
use crate::storage::format::metadata::FILE_REFERENCE_PREFIX;
use crate::storage::format::{METADATA_NAMESPACE, NAMESPACES, RECORD_FILE_PREFIX, REGISTRY};
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;

pub(in crate::storage) mod native_routing {
    use crate::storage::error::{StorageError, StorageResult};
    use crate::storage::format::{MAIN_NAMESPACE, RECORDS_COLLECTION};
    use base64::{engine::general_purpose::STANDARD, Engine as _};

    pub(in crate::storage) fn collection_for_namespace(ns: &str) -> &str {
        [ns, RECORDS_COLLECTION][usize::from(ns == MAIN_NAMESPACE)]
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

type KvRows = Vec<(Vec<u8>, Vec<u8>)>;
type ScanRes = StorageResult<KvRows>;
type YieldRes = StorageResult<YieldedScan>;
type JsonRows<T> = StorageResult<Vec<(String, T)>>;
type BatchRef<'a> = Option<&'a Arc<transaction::PendingBatch>>;

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

fn prefix_end(prefix: &str) -> Vec<u8> {
    let bump = |(idx, ch): (usize, char)| {
        let next = u32::from(ch) + 1;
        char::from_u32(if next == 0xd800 { 0xe000 } else { next })
            .map(|c| format!("{}{c}", &prefix[..idx]).into_bytes())
    };
    prefix
        .char_indices()
        .rev()
        .find_map(bump)
        .unwrap_or_else(|| vec![255])
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
        let pend = transaction::PendingBatch::current(self);
        if self.raw_many(&ctx, &[(REGISTRY.into(), key.clone())], pend.as_ref())?[0].is_none() {
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
        Ok(PackKvStore(Arc::clone(self), name.into()))
    }
    pub(in crate::storage) fn stage_record_file_deletes(
        self: &Arc<Self>,
        keys: &[Vec<u8>],
    ) -> StorageResult<()> {
        let p = transaction::PendingBatch::current(self).ok_or_else(|| {
            StorageError::invalid("file retirement requires a native write request")
        })?;
        let to_op = |k: &Vec<u8>| {
            let s = std::str::from_utf8(k)
                .ok()
                .and_then(|s| s.strip_prefix(RECORD_FILE_PREFIX));
            s.filter(|s| !s.is_empty())
                .map(|_| HomeMutation::new(CAS_COLLECTION, native_routing::encode(k), None))
                .ok_or_else(|| StorageError::invalid("file retirement requires a record file key"))
        };
        p.stage(keys.iter().map(to_op).collect::<StorageResult<_>>()?)
    }
    pub async fn has_external_backup_files(self: &Arc<Self>) -> StorageResult<bool> {
        let (ctx, pend, rt) = (
            ReadContext::current(self).await?,
            transaction::PendingBatch::current(self),
            Arc::clone(self),
        );
        run_blocking(move || {
            let cas = ["fp:", "fu:", "fr:", RECORD_FILE_PREFIX].map(|p| (CAS_COLLECTION, p));
            for (col, pfx) in [(METADATA_NAMESPACE, FILE_REFERENCE_PREFIX)]
                .into_iter()
                .chain(cas)
            {
                let (b, end) = (pfx.as_bytes(), prefix_end(pfx));
                let (rows, _, _) = rt.raw_range(&ctx, (col, b, &end), 1, pend.as_ref(), None)?;
                if let Some((k, _)) = rows.first() {
                    return native_routing::decode(k).map(|_| true);
                }
            }
            Ok(false)
        })
        .await
    }
}

fn utf8_non_b64<'a>(
    b: &'a [u8],
    u8_e: &'static str,
    b64_e: &'static str,
) -> StorageResult<&'a str> {
    let s = std::str::from_utf8(b).map_err(|_| StorageError::invalid(u8_e))?;
    (!s.starts_with("b64:"))
        .then_some(s)
        .ok_or_else(|| StorageError::invalid(b64_e))
}

fn decode_rows(rows: NativeRows) -> ScanRes {
    rows.into_iter()
        .map(|(k, b)| Ok((native_routing::decode(&k)?, b)))
        .collect()
}

fn plan_mutations(
    rt: &PackRuntime,
    ctx: &Arc<ReadContext>,
    pend: BatchRef<'_>,
    ns: &str,
    mutations: Vec<KvMutation>,
) -> StorageResult<Vec<HomeMutation>> {
    let lim = pend.map_or(4096, |p| p.operation_limit);
    if mutations.len() > lim {
        return Err(StorageError::invalid(format!(
            "native mutation exceeds {lim} operation limit"
        )));
    }
    let mut del = Vec::new();
    for m in &mutations {
        super::super::current_keys::validate(ns, m.key())?;
        if matches!(m, KvMutation::Delete { .. }) {
            del.push(coordinate(ns, m.key()));
        }
    }
    del.sort();
    del.dedup();
    let bodies = rt.raw_many(ctx, &del, pend)?;
    let mut present: BTreeMap<_, _> = del
        .into_iter()
        .zip(bodies.iter().map(Option::is_some))
        .collect();
    let mut out = Vec::with_capacity(mutations.len());
    for m in mutations {
        let c = coordinate(ns, m.key());
        match m {
            KvMutation::Put { value, .. } => {
                present.insert(c.clone(), true);
                out.push(HomeMutation::new(c.0, c.1, Some(value)));
            }
            KvMutation::Delete { .. } if present.insert(c.clone(), false) == Some(true) => {
                out.push(HomeMutation::new(c.0, c.1, None));
            }
            KvMutation::Delete { .. } => {}
        }
    }
    Ok(out)
}

impl PackKvStore {
    pub(crate) async fn get_item<T: DeserializeOwned>(&self, k: &str) -> StorageResult<Option<T>> {
        self.get(k.as_bytes())
            .await?
            .map(|b| parse_json(k, &b))
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
            .map(|(r, k)| r.map(|b| parse_json(k, &b)).transpose())
            .collect()
    }
    pub(crate) async fn list_keys_with_prefix(&self, p: &str) -> StorageResult<Vec<String>> {
        let rows = self.scan_prefix(p.as_bytes()).await?;
        Ok(rows
            .into_iter()
            .map(|(k, _)| String::from_utf8_lossy(&k).into_owned())
            .collect())
    }
    pub(crate) async fn scan_items_with_prefix<T: DeserializeOwned>(&self, p: &str) -> JsonRows<T> {
        decode_json_rows(self.scan_prefix(p.as_bytes()).await?)
    }
    pub(crate) async fn scan_items_in_range_paged<T: DeserializeOwned>(
        &self,
        s: &str,
        e: &str,
        n: usize,
    ) -> JsonRows<T> {
        decode_json_rows(self.scan_range_paged(s.as_bytes(), e.as_bytes(), n).await?)
    }
    pub(crate) async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        let mut r = self.get_many(vec![key.to_vec()]).await?;
        Ok(r.pop().flatten())
    }
    pub(crate) async fn get_many(&self, keys: Vec<Vec<u8>>) -> StorageResult<Vec<Option<Vec<u8>>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        if keys.len() > 10000 {
            let msg = "native request exceeds 10000 keys";
            return Err(StorageError::read_limit(msg));
        }
        for key in &keys {
            super::super::current_keys::validate(&self.1, key)?;
        }
        let (rt, ns) = (Arc::clone(&self.0), self.1.clone());
        let ctx = ReadContext::current(&rt).await?;
        let pend = transaction::PendingBatch::current(&rt);
        run_blocking(move || {
            let coords: Vec<_> = keys.iter().map(|k| coordinate(&ns, k)).collect();
            rt.raw_many(&ctx, &coords, pend.as_ref())
        })
        .await
    }
    pub(crate) async fn scan_prefix(&self, pfx: &[u8]) -> ScanRes {
        let u8_e = "native metadata prefix requires its UTF-8 key codec";
        let s = utf8_non_b64(pfx, u8_e, "reserved native ids require exact-key reads")?;
        let err = Some("native prefix result requires a bounded continuation");
        self.bounded_rows(s.as_bytes().to_vec(), prefix_end(s), 10000, err)
            .await
    }
    pub(crate) async fn scan_range_paged(&self, s: &[u8], e: &[u8], lim: usize) -> ScanRes {
        if lim == 0 || s >= e {
            return Ok(Vec::new());
        }
        const RANGE_IDS: &str = "native range requires UTF-8 ids or a metadata limit marker";
        utf8_non_b64(s, "native range requires a UTF-8 start", RANGE_IDS)?;
        if std::str::from_utf8(e.strip_suffix(&[255]).unwrap_or(e)).is_err() {
            return Err(StorageError::invalid(RANGE_IDS));
        }
        self.bounded_rows(s.to_vec(), e.to_vec(), lim, None).await
    }
    async fn run_scan<T: Send + 'static>(
        &self,
        f: impl FnOnce(&PackRuntime, &Arc<ReadContext>, BatchRef<'_>, &str) -> StorageResult<T>
            + Send
            + 'static,
    ) -> StorageResult<T> {
        let rt = Arc::clone(&self.0);
        let ctx = ReadContext::current(&rt).await?;
        let pend = transaction::PendingBatch::current(&rt);
        let col = native_routing::collection_for_namespace(&self.1).to_string();
        let obs = Arc::clone(&ctx);
        obs.remembered(run_blocking(move || f(&rt, &ctx, pend.as_ref(), &col)).await)
    }
    async fn bounded_rows(
        &self,
        start: Vec<u8>,
        end: Vec<u8>,
        limit: usize,
        err: Option<&'static str>,
    ) -> ScanRes {
        self.run_scan(move |rt, ctx, pend, col| {
            let (rows, exhausted, _) = rt.raw_range(ctx, (col, &start, &end), limit, pend, None)?;
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
        s: &[u8],
        e: &[u8],
        lim: usize,
        kpr: usize,
    ) -> YieldRes {
        let fallback = lim == 0
            || s >= e
            || utf8_non_b64(s, "", "").is_err()
            || std::str::from_utf8(e).is_err()
            || transaction::PendingBatch::current(&self.0).is_some();
        if fallback {
            let rows = self.scan_range_paged(s, e, lim).await?;
            let resume = rows
                .last()
                .filter(|_| lim > 0 && rows.len() >= lim)
                .map(|(k, _)| k.clone());
            return Ok(YieldedScan { rows, resume });
        }
        let (start, end) = (s.to_vec(), e.to_vec());
        self.run_scan(move |rt, ctx, _, col| {
            let (rows, _, res) = rt.raw_range(ctx, (col, &start, &end), lim, None, Some(kpr))?;
            let rows = decode_rows(rows)?;
            let resume = res.map(|k| native_routing::decode(&k)).transpose()?;
            Ok(YieldedScan { rows, resume })
        })
        .await
    }
    pub(in crate::storage) async fn batch_mutate(
        &self,
        mutations: Vec<KvMutation>,
    ) -> StorageResult<()> {
        let rt = Arc::clone(&self.0);
        transaction::scope(Arc::clone(&rt), async {
            rt.ensure_namespace(&self.1).await?;
            let ctx = ReadContext::current(&rt).await?;
            let pend = transaction::PendingBatch::current(&rt);
            let (ns, p_rt) = (self.1.clone(), Arc::clone(&rt));
            let work = move || plan_mutations(&p_rt, &ctx, pend.as_ref(), &ns, mutations);
            rt.mutate(run_blocking(work).await?).await
        })
        .await
    }
}

pub(in crate::storage::disk_pack) fn coordinate(ns: &str, key: &[u8]) -> (String, String) {
    let col = native_routing::collection_for_namespace(ns).into();
    (col, native_routing::encode(key))
}
