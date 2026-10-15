use super::super::laststore::{native_routing, LastStoreKvStore};
use super::*;
use laststore::disk_pack::home::HomeRead;
use laststore::disk_pack::{HashRangeCursor, RangeBounds};
use std::collections::VecDeque;

type NativeRows = Vec<(String, Vec<u8>)>;

impl PackKvStore {
    pub(super) async fn prefix_rows(
        &self,
        prefix: &[u8],
        limit: usize,
        require_end: bool,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let prefix = std::str::from_utf8(prefix)
            .map_err(|_| {
                StorageError::InvalidOperation(
                    "native metadata prefix requires its UTF-8 key codec".into(),
                )
            })?
            .to_string();
        if prefix.starts_with("b64:") {
            return Err(StorageError::InvalidOperation(
                "reserved native ids require exact-key reads".into(),
            ));
        }
        let runtime = Arc::clone(&self.runtime);
        let context = read::ReadContext::current(&runtime).await?;
        let pending = transaction::PendingBatch::current(&runtime);
        let namespace = self.namespace.clone();
        let observed = Arc::clone(&context);
        let result = LastStoreKvStore::run_blocking(move || {
            let forms = if namespace == "main" {
                crate::kind_partition::read_forms(&prefix)
            } else {
                vec![prefix.clone()]
            };
            let mut combined = Vec::new();
            for form in forms {
                let collections = if namespace == "main" {
                    native_routing::prefix_collections(form.as_bytes())
                        .into_iter()
                        .map(str::to_string)
                        .collect()
                } else {
                    vec![namespace.clone()]
                };
                for collection in collections {
                    let end = text_upper(&form).map_or_else(|| vec![255], String::into_bytes);
                    let (rows, exhausted) = runtime.raw_rows(
                        &context,
                        &collection,
                        form.as_bytes(),
                        &end,
                        limit,
                        pending.as_ref(),
                    )?;
                    if require_end && !exhausted {
                        return Err(StorageError::ReadResourceLimit {
                            detail: "native prefix result requires a bounded continuation".into(),
                        });
                    }
                    for (key, body) in rows {
                        let key = crate::kind_partition::rewrite_key_like(&key, &prefix);
                        combined.push((native_routing::decode(&key)?, body));
                    }
                }
            }
            // Candidate order is authoritative; stable sort retains its first value.
            combined.sort_by(|left, right| left.0.cmp(&right.0));
            combined.dedup_by(|left, right| left.0 == right.0);
            combined.truncate(limit);
            Ok(combined)
        })
        .await;
        observed.remember(&result);
        result
    }

    pub(super) async fn range_rows(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
        require_end: bool,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        if limit == 0 || start >= end {
            return Ok(Vec::new());
        }
        let start_text = std::str::from_utf8(start).map_err(|_| {
            StorageError::InvalidOperation("native range requires a UTF-8 start".into())
        })?;
        let end_is_limit = end == crate::storage::reseal_at_rest::RESEAL_COLLECTION_END
            || end
                .strip_suffix(&[255])
                .is_some_and(|prefix| std::str::from_utf8(prefix).is_ok());
        if start_text.starts_with("b64:") || (std::str::from_utf8(end).is_err() && !end_is_limit) {
            return Err(StorageError::InvalidOperation(
                "native range requires UTF-8 ids or a metadata limit marker".into(),
            ));
        }
        let start = start.to_vec();
        let end = end.to_vec();
        let runtime = Arc::clone(&self.runtime);
        let context = read::ReadContext::current(&runtime).await?;
        let pending = transaction::PendingBatch::current(&runtime);
        let collections = if self.namespace == "main" {
            native_routing::range_collections(&start, &end)
                .into_iter()
                .map(str::to_string)
                .collect()
        } else {
            vec![self.namespace.clone()]
        };
        let observed = Arc::clone(&context);
        let result = LastStoreKvStore::run_blocking(move || {
            let mut out = Vec::new();
            for collection in collections {
                let (rows, exhausted) = runtime.raw_rows(
                    &context,
                    &collection,
                    &start,
                    &end,
                    limit,
                    pending.as_ref(),
                )?;
                if require_end && !exhausted {
                    return Err(StorageError::ReadResourceLimit {
                        detail: "native range requires a bounded continuation".into(),
                    });
                }
                for (key, body) in rows {
                    out.push((native_routing::decode(&key)?, body));
                }
            }
            out.sort_by(|left, right| left.0.cmp(&right.0));
            out.dedup_by(|left, right| left.0 == right.0);
            out.truncate(limit);
            Ok(out)
        })
        .await;
        observed.remember(&result);
        result
    }
}

fn text_upper(prefix: &str) -> Option<String> {
    let mut out = prefix.to_string();
    while let Some(character) = out.pop() {
        let mut next = character as u32 + 1;
        if next == 0xd800 {
            next = 0xe000;
        }
        if let Some(next) = char::from_u32(next) {
            out.push(next);
            return Some(out);
        }
    }
    None
}

fn cursor<'a>(
    call: &'a mut HomeRead,
    collection: &str,
    start: &[u8],
    end: &[u8],
) -> StorageResult<HashRangeCursor<'a>> {
    if let Ok(start_text) = std::str::from_utf8(start) {
        if let Some(at) = start_text.find('\0').filter(|at| *at > 0) {
            let prefix = &start_text[..at + 1];
            let mut hash_end = prefix.as_bytes().to_vec();
            *hash_end.last_mut().expect("hash separator") = 1;
            let end_suffix = if end == hash_end {
                Some(None)
            } else {
                std::str::from_utf8(end)
                    .ok()
                    .and_then(|end| end.strip_prefix(prefix))
                    .map(|suffix| Some(suffix.to_string()))
            };
            if let Some(end) = end_suffix {
                return call
                    .hash_range(
                        collection,
                        prefix,
                        RangeBounds {
                            start: start_text[at + 1..].to_string(),
                            end,
                        },
                    )
                    .map_err(map);
            }
        }
    }
    call.administrative_range(collection, start, end)
        .map_err(map)
}

impl PackRuntime {
    pub(super) fn raw_rows(
        &self,
        context: &Arc<read::ReadContext>,
        collection: &str,
        start: &[u8],
        end: &[u8],
        limit: usize,
        pending: Option<&Arc<transaction::PendingBatch>>,
    ) -> StorageResult<(NativeRows, bool)> {
        let result = self.raw_rows_inner(context, collection, start, end, limit, pending);
        context.remember(&result);
        result
    }

    fn raw_rows_inner(
        &self,
        context: &Arc<read::ReadContext>,
        collection: &str,
        start: &[u8],
        end: &[u8],
        limit: usize,
        pending: Option<&Arc<transaction::PendingBatch>>,
    ) -> StorageResult<(NativeRows, bool)> {
        if limit == 0 {
            return Ok((Vec::new(), start >= end));
        }
        if limit > 10000 {
            return Err(StorageError::ReadResourceLimit {
                detail: "native page exceeds 10000 records".into(),
            });
        }
        let overlay = pending
            .map(|batch| batch.matching(collection, start, end))
            .unwrap_or_default();
        let mut overlay = overlay.into_iter().peekable();
        let mut call = crate::read_receipts::lock(&context.call);
        let mut cursor = cursor(&mut call, collection, start, end)?;
        let mut disk = VecDeque::new();
        let mut exhausted = false;
        let mut out = Vec::new();
        while out.len() < limit {
            if disk.is_empty() && !exhausted {
                let page = cursor
                    .next_page((limit - out.len()).min(128))
                    .map_err(map)?;
                exhausted = page.exhausted;
                disk.extend(page.records);
            }
            let use_overlay = match (overlay.peek(), disk.front()) {
                (Some(memory), Some(record)) => memory.key <= record.key,
                (Some(_), None) => true,
                (None, _) => false,
            };
            if use_overlay {
                let memory = overlay.next().expect("selected pending record");
                cursor
                    .account_memory_record(
                        memory.key.len(),
                        memory.body.as_ref().map_or(0, Vec::len),
                    )
                    .map_err(map)?;
                if disk.front().is_some_and(|record| record.key == memory.key) {
                    disk.pop_front();
                }
                if let Some(body) = &memory.body {
                    out.push((memory.key.clone(), body.clone()));
                }
            } else if let Some(record) = disk.pop_front() {
                out.push((record.key, record.body));
            } else if exhausted {
                break;
            }
        }
        let complete = exhausted && disk.is_empty() && overlay.peek().is_none();
        drop(cursor);
        let mut logical = self.logical.lock().expect("native logical cache");
        if call.current().frontier == self.published.load(Ordering::Acquire) {
            for (key, body) in &out {
                if pending.is_some_and(|batch| batch.lookup(collection, key).is_some()) {
                    continue;
                }
                let epoch = logical.record_epoch(key);
                logical.admit_record(collection, key, Some(body.clone()), epoch);
            }
        }
        Ok((out, complete))
    }
}
