use super::super::traits::{KvMutation, YieldedScan};
use super::read::{NativeRows, ReadContext};
use super::*;
use crate::storage::format::metadata::FILE_REFERENCE_PREFIX;
use crate::storage::format::{METADATA_NAMESPACE, RECORD_FILE_PREFIX};
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
        if let Ok(s) = std::str::from_utf8(key) {
            if !s.starts_with("b64:") {
                return s.to_string();
            }
        }
        format!("b64:{}", STANDARD.encode(key))
    }

    pub(in crate::storage) fn decode(key: &str) -> StorageResult<Vec<u8>> {
        key.strip_prefix("b64:").map_or_else(
            || Ok(key.as_bytes().to_vec()),
            |enc| {
                STANDARD
                    .decode(enc)
                    .map_err(|e| StorageError::backend(format!("laststore key decode failed: {e}")))
            },
        )
    }
}

fn prefix_end(prefix: &str) -> Vec<u8> {
    prefix
        .char_indices()
        .rev()
        .find_map(|(idx, ch)| {
            let next = if ch as u32 + 1 == 0xd800 {
                0xe000
            } else {
                ch as u32 + 1
            };
            char::from_u32(next).map(|c| format!("{}{c}", &prefix[..idx]).into_bytes())
        })
        .unwrap_or_else(|| vec![255])
}

impl PackRuntime {
    pub(in crate::storage) async fn open_cas(self: &Arc<Self>) -> StorageResult<PackKvStore> {
        self.open_store(CAS_COLLECTION).await
    }

    pub(in crate::storage) fn stage_record_file_deletes(
        self: &Arc<Self>,
        keys: &[Vec<u8>],
    ) -> StorageResult<()> {
        let pending = transaction::PendingBatch::current(self).ok_or_else(|| {
            StorageError::invalid("file retirement requires a native write request")
        })?;
        let ops = keys
            .iter()
            .map(|key| {
                std::str::from_utf8(key)
                    .ok()
                    .and_then(|k| k.strip_prefix(RECORD_FILE_PREFIX))
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        StorageError::invalid("file retirement requires a record file key")
                    })?;
                Ok(HomeMutation::new(
                    CAS_COLLECTION,
                    native_routing::encode(key),
                    None,
                ))
            })
            .collect::<StorageResult<_>>()?;
        pending.stage(ops)
    }

    pub(in crate::storage) async fn has_external_file_references(
        self: &Arc<Self>,
    ) -> StorageResult<bool> {
        let context = ReadContext::current(self).await?;
        let runtime = Arc::clone(self);
        run_blocking(move || {
            let end = prefix_end(FILE_REFERENCE_PREFIX);
            let bounds = (
                METADATA_NAMESPACE,
                FILE_REFERENCE_PREFIX.as_bytes(),
                end.as_slice(),
            );
            let (rows, _, _) = runtime.raw_range(&context, bounds, 1, None, None)?;
            Ok(!rows.is_empty())
        })
        .await
    }

    pub async fn has_external_backup_files(self: &Arc<Self>) -> StorageResult<bool> {
        let (ctx, pending, rt) = (
            ReadContext::current(self).await?,
            transaction::PendingBatch::current(self),
            Arc::clone(self),
        );
        run_blocking(move || {
            for (col, pfx) in [
                (METADATA_NAMESPACE, FILE_REFERENCE_PREFIX),
                (CAS_COLLECTION, "fp:"),
                (CAS_COLLECTION, "fu:"),
                (CAS_COLLECTION, "fr:"),
                (CAS_COLLECTION, RECORD_FILE_PREFIX),
            ] {
                let end = prefix_end(pfx);
                let bounds = (col, pfx.as_bytes(), end.as_slice());
                let (rows, _, _) = rt.raw_range(&ctx, bounds, 1, pending.as_ref(), None)?;
                if let Some((k, _)) = rows.first() {
                    native_routing::decode(k)?;
                    return Ok(true);
                }
            }
            Ok(false)
        })
        .await
    }
}

fn utf8_non_b64<'a>(
    bytes: &'a [u8],
    bad_utf8: &'static str,
    bad_b64: &'static str,
) -> StorageResult<&'a str> {
    let s = std::str::from_utf8(bytes).map_err(|_| StorageError::invalid(bad_utf8))?;
    (!s.starts_with("b64:"))
        .then_some(s)
        .ok_or_else(|| StorageError::invalid(bad_b64))
}

fn decode_rows(rows: NativeRows) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
    rows.into_iter()
        .map(|(k, b)| Ok((native_routing::decode(&k)?, b)))
        .collect()
}

impl PackKvStore {
    pub(crate) async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        let mut r = self.get_many(vec![key.to_vec()]).await?;
        Ok(r.pop().expect("one native key"))
    }

    pub(crate) async fn get_many(&self, keys: Vec<Vec<u8>>) -> StorageResult<Vec<Option<Vec<u8>>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        if keys.len() > 10000 {
            return Err(StorageError::read_limit(
                "native request exceeds 10000 keys",
            ));
        }
        for key in &keys {
            super::super::current_keys::validate(&self.namespace, key)?;
        }
        let rt = Arc::clone(&self.runtime);
        let ctx = ReadContext::current(&rt).await?;
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

    pub(crate) async fn scan_prefix(
        &self,
        prefix: &[u8],
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let s = utf8_non_b64(
            prefix,
            "native metadata prefix requires its UTF-8 key codec",
            "reserved native ids require exact-key reads",
        )?;
        let err = Some("native prefix result requires a bounded continuation");
        self.bounded_rows(s.as_bytes().to_vec(), prefix_end(s), 10000, err)
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
            return Err(StorageError::invalid(RANGE_IDS));
        }
        self.bounded_rows(start.to_vec(), end.to_vec(), limit, None)
            .await
    }

    async fn run_scan<T: Send + 'static>(
        &self,
        f: impl FnOnce(
                &PackRuntime,
                &Arc<ReadContext>,
                Option<&Arc<transaction::PendingBatch>>,
                &str,
            ) -> StorageResult<T>
            + Send
            + 'static,
    ) -> StorageResult<T> {
        let rt = Arc::clone(&self.runtime);
        let ctx = ReadContext::current(&rt).await?;
        let pending = transaction::PendingBatch::current(&rt);
        let col = native_routing::collection_for_namespace(&self.namespace).to_string();
        let observed = Arc::clone(&ctx);
        observed.remembered(run_blocking(move || f(&rt, &ctx, pending.as_ref(), &col)).await)
    }

    async fn bounded_rows(
        &self,
        start: Vec<u8>,
        end: Vec<u8>,
        limit: usize,
        complete_err: Option<&'static str>,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.run_scan(move |rt, ctx, pending, col| {
            let (rows, exhausted, _) =
                rt.raw_range(ctx, (col, &start, &end), limit, pending, None)?;
            if let Some(detail) = complete_err.filter(|_| !exhausted) {
                return Err(StorageError::read_limit(detail));
            }
            let mut rows = decode_rows(rows)?;
            rows.sort_by(|a, b| a.0.cmp(&b.0));
            rows.dedup_by(|a, b| a.0 == b.0);
            if complete_err.is_some() && rows.len() > limit {
                return Err(StorageError::read_limit(format!(
                    "native complete metadata result exceeds {limit} distinct keys"
                )));
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
        let (start, end) = (start.to_vec(), end.to_vec());
        self.run_scan(move |rt, ctx, _, col| {
            let (rows, _, resume) =
                rt.raw_range(ctx, (col, &start, &end), limit, None, Some(keys_per_row))?;
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
            let (ctx, pending) = (
                ReadContext::current(&rt).await?,
                transaction::PendingBatch::current(&rt),
            );
            let (ns, planning) = (self.namespace.clone(), Arc::clone(&rt));
            let ops = run_blocking(move || {
                let limit = pending.as_ref().map_or(4096, |p| p.operation_limit);
                if mutations.len() > limit {
                    return Err(StorageError::invalid(format!(
                        "native mutation exceeds {limit} operation admission limit"
                    )));
                }
                let (mut targets, mut del) = (Vec::with_capacity(mutations.len()), Vec::new());
                for m in &mutations {
                    super::super::current_keys::validate(&ns, m.key())?;
                    let t = coordinate(&ns, m.key());
                    if matches!(m, KvMutation::Delete { .. }) {
                        del.push(t.clone());
                    }
                    targets.push(t);
                }
                del.sort();
                del.dedup();
                let bodies = planning.raw_many(&ctx, &del, pending.as_ref())?;
                let mut present: BTreeMap<_, _> = del
                    .into_iter()
                    .zip(bodies.iter().map(Option::is_some))
                    .collect();
                let mut out = Vec::with_capacity(mutations.len());
                for (m, (col, key)) in mutations.into_iter().zip(targets) {
                    match m {
                        KvMutation::Put { value, .. } => {
                            present.insert((col.clone(), key.clone()), true);
                            out.push(HomeMutation::new(col, key, Some(value)));
                        }
                        KvMutation::Delete { .. }
                            if present.insert((col.clone(), key.clone()), false) == Some(true) =>
                        {
                            out.push(HomeMutation::new(col, key, None));
                        }
                        KvMutation::Delete { .. } => {}
                    }
                }
                Ok(out)
            })
            .await?;
            rt.mutate(ops).await
        })
        .await
    }
}

pub(in crate::storage::disk_pack) fn coordinate(namespace: &str, key: &[u8]) -> (String, String) {
    (
        native_routing::collection_for_namespace(namespace).to_string(),
        native_routing::encode(key),
    )
}
