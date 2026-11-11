//! Bounded and yielding native prefix/range row scans.

use super::physical::PhysicalRead;
use super::*;
use laststore::disk_run::home::{RangeBounds, RunHashRange};
use std::collections::VecDeque;

pub(super) type NativeRows = Vec<(String, Vec<u8>)>;

/// Decode each stored key. The row order stays the same.
fn decode_native_rows(rows: NativeRows) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
    rows.into_iter()
        .map(|(key, body)| Ok((native_routing::decode(&key)?, body)))
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
    rows.sort_by(|left, right| left.0.cmp(&right.0));
    rows.dedup_by(|left, right| left.0 == right.0);
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
        let end_is_limit = end
            .strip_suffix(&[255])
            .is_some_and(|prefix| std::str::from_utf8(prefix).is_ok());
        if start_text.starts_with("b64:") || (std::str::from_utf8(end).is_err() && !end_is_limit) {
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
        let runtime = Arc::clone(&self.runtime);
        let context = read::ReadContext::current(&runtime).await?;
        let pending = transaction::PendingBatch::current(&runtime);
        let collection = native_routing::collection_for_namespace(&self.namespace).to_string();
        let observed = Arc::clone(&context);
        let result = run_blocking(move || {
            let (rows, exhausted) =
                runtime.raw_rows(&context, &collection, &start, &end, limit, pending.as_ref())?;
            finish_bounded_rows(rows, exhausted, limit, require_end, detail)
        })
        .await;
        observed.remembered(result)
    }

    /// Read up to `limit` rows of `start..end`, and name where to continue.
    pub(super) async fn range_rows_yielding(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
        keys_per_row: usize,
    ) -> StorageResult<(Vec<(Vec<u8>, Vec<u8>)>, Option<Vec<u8>>)> {
        let simple = limit > 0
            && start < end
            && std::str::from_utf8(start).is_ok_and(|text| !text.starts_with("b64:"))
            && std::str::from_utf8(end).is_ok()
            && transaction::PendingBatch::current(&self.runtime).is_none();
        if !simple {
            let rows = self.range_rows(start, end, limit, false).await?;
            let resume = (limit > 0 && rows.len() >= limit)
                .then(|| rows.last().map(|(key, _)| key.clone()))
                .flatten();
            return Ok((rows, resume));
        }
        let collection = native_routing::collection_for_namespace(&self.namespace).to_string();
        let (start, end) = (start.to_vec(), end.to_vec());
        let runtime = Arc::clone(&self.runtime);
        let context = read::ReadContext::current(&runtime).await?;
        let observed = Arc::clone(&context);
        let result = run_blocking(move || {
            let (rows, resume) = runtime.raw_rows_yielding(
                &context,
                &collection,
                &start,
                &end,
                limit,
                keys_per_row,
            )?;
            let rows = decode_native_rows(rows)?;
            let resume = resume.map(|key| native_routing::decode(&key)).transpose()?;
            Ok((rows, resume))
        })
        .await;
        observed.remembered(result)
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
    call: &'a mut PhysicalRead,
    collection: &str,
    start: &[u8],
    end: &[u8],
) -> StorageResult<RunHashRange<'a>> {
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

fn require_page_limit(limit: usize) -> StorageResult<()> {
    if limit > 10000 {
        return Err(StorageError::ReadResourceLimit {
            detail: "native page exceeds 10000 records".into(),
        });
    }
    Ok(())
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
            let mut call = context
                .call
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
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
                    (Some(memory), Some((key, _))) => &memory.key <= key,
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
                    if disk.front().is_some_and(|(key, _)| *key == memory.key) {
                        disk.pop_front();
                    }
                    if let Some(body) = &memory.body {
                        out.push((memory.key.clone(), body.clone()));
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
            let mut call = context
                .call
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut cursor = cursor(&mut call, collection, start, end)?;
            let mut out: NativeRows = Vec::new();
            loop {
                if out.len() >= limit {
                    let last = out.last().map(|(key, _)| key.clone());
                    return Ok((out, last));
                }
                let page = match cursor.next_page_yielding(
                    (limit - out.len()).min(128),
                    out.len(),
                    keys_per_row,
                ) {
                    Ok(page) => page,
                    Err(laststore::Error::ReadBudgetExceeded) if !out.is_empty() => {
                        let last = out.last().map(|(key, _)| key.clone());
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
