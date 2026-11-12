//! Bounded and yielding native prefix/range row scans.

use super::physical::V2Read;
use super::*;
use laststore::disk_run::home::{RangeBounds, RunHashRange};
use std::collections::VecDeque;
use std::sync::PoisonError;

type NativeRows = Vec<(String, Vec<u8>)>;

fn decode_native_rows(rows: NativeRows) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
    rows.into_iter()
        .map(|(k, body)| Ok((native_routing::decode(&k)?, body)))
        .collect()
}

fn finish_bounded_rows(
    rows: NativeRows,
    exhausted: bool,
    limit: usize,
    require_end: bool,
    detail: &'static str,
) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
    if require_end && !exhausted {
        return Err(StorageError::ReadResourceLimit {
            detail: detail.into(),
        });
    }
    let mut rows = decode_native_rows(rows)?;
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows.dedup_by(|a, b| a.0 == b.0);
    if require_end && rows.len() > limit {
        return Err(StorageError::ReadResourceLimit {
            detail: format!("native complete metadata result exceeds {limit} distinct keys"),
        });
    }
    rows.truncate(limit);
    Ok(rows)
}

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
        let prefix = std::str::from_utf8(prefix).map_err(|_| {
            StorageError::InvalidOperation(
                "native metadata prefix requires its UTF-8 key codec".into(),
            )
        })?;
        if prefix.starts_with("b64:") {
            return Err(StorageError::InvalidOperation(
                "reserved native ids require exact-key reads".into(),
            ));
        }
        let end = text_upper(prefix).map_or_else(|| vec![255], String::into_bytes);
        self.scan_bounded_rows(
            prefix.as_bytes().to_vec(),
            end,
            limit,
            require_end,
            "native prefix result requires a bounded continuation",
        )
        .await
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
        let end_ok = std::str::from_utf8(end).is_ok()
            || end
                .strip_suffix(&[255])
                .is_some_and(|p| std::str::from_utf8(p).is_ok());
        if start_text.starts_with("b64:") || !end_ok {
            return Err(StorageError::InvalidOperation(
                "native range requires UTF-8 ids or a metadata limit marker".into(),
            ));
        }
        self.scan_bounded_rows(
            start.to_vec(),
            end.to_vec(),
            limit,
            require_end,
            "native range requires a bounded continuation",
        )
        .await
    }

    async fn scan_bounded_rows(
        &self,
        start: Vec<u8>,
        end: Vec<u8>,
        limit: usize,
        require_end: bool,
        detail: &'static str,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let rt = Arc::clone(&self.runtime);
        let ctx = read::ReadContext::current(&rt).await?;
        let pending = transaction::PendingBatch::current(&rt);
        let col = native_routing::collection_for_namespace(&self.namespace).to_string();
        let observed = Arc::clone(&ctx);
        let res = run_blocking(move || {
            let (rows, exhausted) =
                rt.raw_rows(&ctx, &col, &start, &end, limit, pending.as_ref())?;
            finish_bounded_rows(rows, exhausted, limit, require_end, detail)
        })
        .await;
        observed.remembered(res)
    }

    /// Read up to `limit` rows of `start..end`, and name where to continue.
    pub(super) async fn range_rows_yielding(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
        keys_per_row: usize,
    ) -> StorageResult<(Vec<(Vec<u8>, Vec<u8>)>, Option<Vec<u8>>)> {
        if limit == 0
            || start >= end
            || !std::str::from_utf8(start).is_ok_and(|t| !t.starts_with("b64:"))
            || std::str::from_utf8(end).is_err()
            || transaction::PendingBatch::current(&self.runtime).is_some()
        {
            let rows = self.range_rows(start, end, limit, false).await?;
            let resume = (limit > 0 && rows.len() >= limit)
                .then(|| rows.last().map(|(k, _)| k.clone()))
                .flatten();
            return Ok((rows, resume));
        }
        let col = native_routing::collection_for_namespace(&self.namespace).to_string();
        let (start, end, rt) = (start.to_vec(), end.to_vec(), Arc::clone(&self.runtime));
        let ctx = read::ReadContext::current(&rt).await?;
        let observed = Arc::clone(&ctx);
        let res = run_blocking(move || {
            let (rows, resume) =
                rt.raw_rows_yielding(&ctx, &col, &start, &end, limit, keys_per_row)?;
            Ok((
                decode_native_rows(rows)?,
                resume.map(|k| native_routing::decode(&k)).transpose()?,
            ))
        })
        .await;
        observed.remembered(res)
    }
}

fn text_upper(prefix: &str) -> Option<String> {
    let mut out = prefix.to_string();
    while let Some(ch) = out.pop() {
        let next = if ch as u32 + 1 == 0xd800 {
            0xe000
        } else {
            ch as u32 + 1
        };
        if let Some(next) = char::from_u32(next) {
            out.push(next);
            return Some(out);
        }
    }
    None
}

fn cursor<'a>(
    call: &'a mut V2Read,
    collection: &str,
    start: &[u8],
    end: &[u8],
) -> StorageResult<RunHashRange<'a>> {
    if let Ok(s) = std::str::from_utf8(start) {
        if let Some(at) = s.find('\0').filter(|&at| at > 0) {
            let prefix = &s[..at + 1];
            let mut hash_end = prefix.as_bytes().to_vec();
            *hash_end.last_mut().expect("hash separator") = 1;
            let end_suffix = if end == hash_end {
                Some(None)
            } else {
                std::str::from_utf8(end)
                    .ok()
                    .and_then(|e| e.strip_prefix(prefix))
                    .map(|suf| Some(suf.to_string()))
            };
            if let Some(end) = end_suffix {
                let bounds = RangeBounds {
                    start: s[at + 1..].to_string(),
                    end,
                };
                return call
                    .home_for(collection)
                    .hash_range(collection, prefix, bounds)
                    .map_err(map);
            }
        }
    }
    call.home_for(collection)
        .administrative_range(collection, start, end)
        .map_err(map)
}

fn require_page_limit(limit: usize) -> StorageResult<()> {
    (limit <= 10000)
        .then_some(())
        .ok_or_else(|| StorageError::ReadResourceLimit {
            detail: "native page exceeds 10000 records".into(),
        })
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
        let result = (|| {
            context.validate_owner(pending)?;
            if limit == 0 {
                return Ok((Vec::new(), start >= end));
            }
            require_page_limit(limit)?;
            let mut overlay = context
                .overlay_rows(pending, collection, start, end)
                .into_iter()
                .peekable();
            let mut call = context.call.lock().unwrap_or_else(PoisonError::into_inner);
            let mut cursor = cursor(&mut call, collection, start, end)?;
            let (mut disk, mut exhausted, mut out) = (VecDeque::new(), false, Vec::new());
            while out.len() < limit {
                if disk.is_empty() && !exhausted {
                    let page = cursor
                        .next_page((limit - out.len()).min(128))
                        .map_err(map)?;
                    exhausted = page.exhausted;
                    disk.extend(page.records);
                }
                if overlay
                    .peek()
                    .is_some_and(|m| disk.front().is_none_or(|(k, _)| m.key <= *k))
                {
                    let m = overlay.next().expect("selected pending record");
                    cursor
                        .account_memory_record(m.key.len(), m.body.as_ref().map_or(0, Vec::len))
                        .map_err(map)?;
                    if disk.front().is_some_and(|(k, _)| *k == m.key) {
                        disk.pop_front();
                    }
                    if let Some(body) = &m.body {
                        out.push((m.key.clone(), body.clone()));
                    }
                } else if let Some(record) = disk.pop_front() {
                    out.push(record);
                } else if exhausted {
                    break;
                }
            }
            Ok((
                out,
                exhausted && disk.is_empty() && overlay.peek().is_none(),
            ))
        })();
        context.remembered(result)
    }

    pub(super) fn raw_rows_yielding(
        &self,
        context: &Arc<read::ReadContext>,
        collection: &str,
        start: &[u8],
        end: &[u8],
        limit: usize,
        keys_per_row: usize,
    ) -> StorageResult<(NativeRows, Option<String>)> {
        let result = (|| {
            context.validate_owner(None)?;
            require_page_limit(limit)?;
            let mut call = context.call.lock().unwrap_or_else(PoisonError::into_inner);
            let mut cursor = cursor(&mut call, collection, start, end)?;
            let mut out: NativeRows = Vec::new();
            loop {
                if out.len() >= limit {
                    let last = out.last().map(|(k, _)| k.clone());
                    return Ok((out, last));
                }
                let page = match cursor.next_page_yielding(
                    (limit - out.len()).min(128),
                    out.len(),
                    keys_per_row,
                ) {
                    Ok(page) => page,
                    Err(laststore::Error::ReadBudgetExceeded) if !out.is_empty() => {
                        let last = out.last().map(|(k, _)| k.clone());
                        return Ok((out, last));
                    }
                    Err(error) => return Err(map(error)),
                };
                out.extend(page.records);
                if page.resume.is_some() || page.exhausted {
                    return Ok((out, page.resume));
                }
            }
        })();
        context.remembered(result)
    }
}
