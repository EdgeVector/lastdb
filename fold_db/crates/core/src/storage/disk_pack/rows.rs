use super::physical::{PhysicalCursor, PhysicalRead};
use super::*;
use laststore::disk_run::home::RangeBounds;
use std::collections::VecDeque;

pub(super) type NativeRows = Vec<(String, Vec<u8>)>;

/// Decode each stored key. The row order stays the same.
pub(super) fn decode_native_rows(rows: NativeRows) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
    rows.into_iter()
        .map(|(key, body)| Ok((native_routing::decode(&key)?, body)))
        .collect()
}

fn extend_bounded_rows(
    out: &mut Vec<(Vec<u8>, Vec<u8>)>,
    rows: NativeRows,
    exhausted: bool,
    require_end: bool,
    detail: &'static str,
) -> StorageResult<()> {
    if require_end && !exhausted {
        return Err(StorageError::ReadResourceLimit {
            detail: detail.into(),
        });
    }
    out.extend(decode_native_rows(rows)?);
    Ok(())
}

/// Exclusive upper bound for a walk of a whole collection.
///
/// Product keys are ASCII prefixes (`rk:`, `ak:`, `aloc:`, …). Four `0xFF`
/// bytes sit past that keyspace without pretending an empty prefix has a
/// string successor (it does not — see `FilterUtils::create_prefix_end`).
/// `PackKvStore::range_rows` accepts it as the end of a range.
pub(crate) const WHOLE_COLLECTION_END: &[u8] = &[0xFF, 0xFF, 0xFF, 0xFF];

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
        let result = run_blocking(move || {
            let mut combined = Vec::new();
            let collections = if namespace == "main" {
                native_routing::prefix_collections(prefix.as_bytes())
                    .into_iter()
                    .map(str::to_string)
                    .collect()
            } else {
                vec![namespace.clone()]
            };
            for collection in collections {
                let end = text_upper(&prefix).map_or_else(|| vec![255], String::into_bytes);
                let (rows, exhausted) = runtime.raw_rows(
                    &context,
                    &collection,
                    prefix.as_bytes(),
                    &end,
                    limit,
                    pending.as_ref(),
                )?;
                extend_bounded_rows(
                    &mut combined,
                    rows,
                    exhausted,
                    require_end,
                    "native prefix result requires a bounded continuation",
                )?;
            }
            finish_rows(combined, limit, require_end)
        })
        .await;
        observed.remembered(result)
    }

    /// A main namespace spans the routed collections. Any other namespace stays one collection.
    pub(super) fn owned_range_collections(&self, start: &[u8], end: &[u8]) -> Vec<String> {
        if self.namespace == "main" {
            native_routing::range_collections(start, end)
                .into_iter()
                .map(str::to_string)
                .collect()
        } else {
            vec![self.namespace.clone()]
        }
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
        let end_is_limit = end == WHOLE_COLLECTION_END
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
        let collections = self.owned_range_collections(&start, &end);
        let observed = Arc::clone(&context);
        let result = run_blocking(move || {
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
                extend_bounded_rows(
                    &mut out,
                    rows,
                    exhausted,
                    require_end,
                    "native range requires a bounded continuation",
                )?;
            }
            finish_rows(out, limit, require_end)
        })
        .await;
        observed.remembered(result)
    }
}

/// Completeness covers the union of current layouts and collections. Private
/// write preparation has a larger read allowance, so it cannot rely on the
/// ordinary call's key cap to detect an oversized combined metadata result.
fn finish_rows(
    mut rows: Vec<(Vec<u8>, Vec<u8>)>,
    limit: usize,
    require_end: bool,
) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
    // Candidate order is authoritative; stable sort retains its first value.
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

pub(super) fn cursor<'a>(
    call: &'a mut PhysicalRead,
    collection: &str,
    start: &[u8],
    end: &[u8],
) -> StorageResult<PhysicalCursor<'a>> {
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

/// Both row readers reject a page above 10000 records.
pub(super) fn require_page_limit(limit: usize) -> StorageResult<()> {
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
        let result = self.raw_rows_inner(context, collection, start, end, limit, pending);
        context.remembered(result)
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
        context.validate_owner(pending)?;
        if limit == 0 {
            return Ok((Vec::new(), start >= end));
        }
        require_page_limit(limit)?;
        let overlay = context.overlay_rows(pending, collection, start, end);
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
        Ok((out, complete))
    }
}
