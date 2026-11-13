use super::super::traits::{KvMutation, YieldedScan};
use super::physical::NativeRows;
use super::*;
use crate::storage::current_keys::families::record_file_space;
use std::collections::BTreeMap;

pub(in crate::storage) mod native_routing {
    use crate::storage::error::{StorageError, StorageResult};
    use crate::storage::format::{MAIN_NAMESPACE, RECORDS_COLLECTION};
    use base64::{engine::general_purpose::STANDARD, Engine as _};

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
    pub(in crate::storage) fn cas_kv(self: &Arc<Self>) -> PackKvStore {
        PackKvStore {
            runtime: Arc::clone(self),
            namespace: CAS_COLLECTION.to_string(),
        }
    }

    pub(in crate::storage) async fn open_cas(self: &Arc<Self>) -> StorageResult<PackKvStore> {
        if read::ReadContext::captured(self).is_none() {
            self.ensure_namespace(CAS_COLLECTION).await?;
        }
        Ok(self.cas_kv())
    }

    pub(in crate::storage) fn stage_record_file_deletes(
        self: &Arc<Self>,
        keys: &[Vec<u8>],
    ) -> StorageResult<()> {
        let inv = |m: &str| StorageError::InvalidOperation(m.into());
        let pending = transaction::PendingBatch::current(self)
            .ok_or_else(|| inv("file retirement requires a native write request"))?;
        let ops = keys
            .iter()
            .map(|key| {
                std::str::from_utf8(key)
                    .ok()
                    .and_then(record_file_space)
                    .ok_or_else(|| inv("file retirement requires a record file key"))?;
                Ok(HomeMutation {
                    collection: CAS_COLLECTION.into(),
                    key: native_routing::encode(key),
                    body: None,
                })
            })
            .collect::<StorageResult<_>>()?;
        pending.stage(ops)
    }
}

fn utf8_non_b64<'a>(
    bytes: &'a [u8],
    bad_utf8: &'static str,
    bad_b64: &'static str,
) -> StorageResult<&'a str> {
    let s =
        std::str::from_utf8(bytes).map_err(|_| StorageError::InvalidOperation(bad_utf8.into()))?;
    (!s.starts_with("b64:"))
        .then_some(s)
        .ok_or_else(|| StorageError::InvalidOperation(bad_b64.into()))
}

async fn blocking_in_ctx<T: Send + 'static>(
    ctx: Arc<read::ReadContext>,
    work: impl FnOnce(&Arc<read::ReadContext>) -> StorageResult<T> + Send + 'static,
) -> StorageResult<T> {
    let observed = Arc::clone(&ctx);
    observed.remembered(run_blocking(move || work(&ctx)).await)
}

fn decode_rows(rows: NativeRows) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
    rows.into_iter()
        .map(|(k, b)| Ok((native_routing::decode(&k)?, b)))
        .collect()
}

fn text_upper(prefix: &str) -> Option<String> {
    prefix.char_indices().rev().find_map(|(idx, ch)| {
        let next = if ch as u32 + 1 == 0xd800 {
            0xe000
        } else {
            ch as u32 + 1
        };
        char::from_u32(next).map(|c| format!("{}{c}", &prefix[..idx]))
    })
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
        let (pending, ns) = (
            transaction::PendingBatch::current(&rt),
            self.namespace.clone(),
        );
        run_blocking(move || {
            let coords: Vec<_> = keys.iter().map(|k| coordinate(&ns, k)).collect();
            rt.raw_many(&ctx, &coords, pending.as_ref())
        })
        .await
    }

    pub(in crate::storage) async fn put(&self, key: &[u8], value: Vec<u8>) -> StorageResult<()> {
        self.batch_mutate(vec![KvMutation::put(key, value)]).await
    }

    pub(crate) async fn scan_prefix(
        &self,
        prefix: &[u8],
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.prefix_rows(prefix, 10000, true).await
    }

    pub(super) async fn prefix_rows(
        &self,
        prefix: &[u8],
        limit: usize,
        require_end: bool,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let s = utf8_non_b64(
            prefix,
            "native metadata prefix requires its UTF-8 key codec",
            "reserved native ids require exact-key reads",
        )?;
        let end = text_upper(s).map_or_else(|| vec![255], String::into_bytes);
        let err = require_end.then_some("native prefix result requires a bounded continuation");
        self.bounded_rows(s.as_bytes().to_vec(), end, limit, err)
            .await
    }

    pub(crate) async fn scan_range_paged(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        if limit == 0 || start >= end {
            return Ok(Vec::new());
        }
        const RANGE_IDS: &str = "native range requires UTF-8 ids or a metadata limit marker";
        utf8_non_b64(start, "native range requires a UTF-8 start", RANGE_IDS)?;
        if std::str::from_utf8(end.strip_suffix(&[255]).unwrap_or(end)).is_err() {
            return Err(StorageError::InvalidOperation(RANGE_IDS.into()));
        }
        self.bounded_rows(start.to_vec(), end.to_vec(), limit, None)
            .await
    }

    async fn bounded_rows(
        &self,
        start: Vec<u8>,
        end: Vec<u8>,
        limit: usize,
        complete_err: Option<&'static str>,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let rt = Arc::clone(&self.runtime);
        let ctx = read::ReadContext::current(&rt).await?;
        let pending = transaction::PendingBatch::current(&rt);
        let col = native_routing::collection_for_namespace(&self.namespace).to_string();
        blocking_in_ctx(ctx, move |ctx| {
            let (rows, exhausted) =
                rt.raw_rows(ctx, &col, &start, &end, limit, pending.as_ref())?;
            if let Some(detail) = complete_err.filter(|_| !exhausted) {
                return Err(StorageError::ReadResourceLimit {
                    detail: detail.into(),
                });
            }
            let mut rows = decode_rows(rows)?;
            rows.sort_by(|a, b| a.0.cmp(&b.0));
            rows.dedup_by(|a, b| a.0 == b.0);
            if complete_err.is_some() && rows.len() > limit {
                return Err(StorageError::ReadResourceLimit {
                    detail: format!(
                        "native complete metadata result exceeds {limit} distinct keys"
                    ),
                });
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
        keys_per_row: usize,
    ) -> StorageResult<YieldedScan> {
        if limit == 0
            || start >= end
            || utf8_non_b64(start, "", "").is_err()
            || std::str::from_utf8(end).is_err()
            || transaction::PendingBatch::current(&self.runtime).is_some()
        {
            let rows = self.scan_range_paged(start, end, limit).await?;
            let resume = (limit > 0 && rows.len() >= limit)
                .then(|| rows.last().map(|(k, _)| k.clone()))
                .flatten();
            return Ok(YieldedScan { rows, resume });
        }
        let col = native_routing::collection_for_namespace(&self.namespace).to_string();
        let (start, end, rt) = (start.to_vec(), end.to_vec(), Arc::clone(&self.runtime));
        let ctx = read::ReadContext::current(&rt).await?;
        blocking_in_ctx(ctx, move |ctx| {
            let (rows, resume) =
                rt.raw_rows_yielding(ctx, &col, &start, &end, limit, keys_per_row)?;
            Ok(YieldedScan {
                rows: decode_rows(rows)?,
                resume: resume.map(|k| native_routing::decode(&k)).transpose()?,
            })
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

    pub(in crate::storage) async fn flush(&self) -> StorageResult<()> {
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
    let (mut targets, mut del) = (Vec::with_capacity(mutations.len()), Vec::new());
    for m in &mutations {
        super::super::current_keys::validate(namespace, m.key())?;
        let t = coordinate(namespace, m.key());
        if matches!(m, KvMutation::Delete { .. }) {
            del.push(t.clone());
        }
        targets.push(t);
    }
    del.sort();
    del.dedup();
    let bodies = runtime.raw_many(context, &del, pending)?;
    let mut present: BTreeMap<_, _> = del
        .into_iter()
        .zip(bodies.iter().map(Option::is_some))
        .collect();
    let mut out = Vec::with_capacity(mutations.len());
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
            KvMutation::Delete { .. }
                if present.insert((collection.clone(), key.clone()), false) == Some(true) =>
            {
                out.push(HomeMutation {
                    collection,
                    key,
                    body: None,
                });
            }
            KvMutation::Delete { .. } => {}
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
