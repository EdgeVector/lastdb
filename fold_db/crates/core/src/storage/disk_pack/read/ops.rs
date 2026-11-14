//! Multi-key point reads and merged memory-overlay range reads on [`PackRuntime`].

use super::{call_error, unpoison, Coordinate, NativeRows, ReadContext};
use crate::storage::disk_pack::transaction::PendingBatch;
use crate::storage::disk_pack::{map, PackRuntime, StorageError, StorageResult};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

const WRITE_READ_MAX_BYTES: usize = 1024 * 1024;
const WRITE_READ_MAX_VALUE_BYTES: usize = 16 * 1024;

type PageOut = (NativeRows, bool, Option<String>);
type Span<'a> = (&'a str, &'a [u8], &'a [u8]);

fn raw_yielding(
    ctx: &ReadContext,
    (col, start, end): Span<'_>,
    limit: usize,
    keys_per_run: usize,
) -> StorageResult<PageOut> {
    let mut call = unpoison(&ctx.reader);
    let mut cur = call
        .home_for(col)
        .administrative_range(col, start, end)
        .map_err(map)?;
    let mut out: NativeRows = Vec::new();
    while out.len() < limit {
        match cur.next_page_yielding((limit - out.len()).min(128), out.len(), keys_per_run) {
            Ok(page) => {
                out.extend(page.records);
                if page.resume.is_some() || page.exhausted {
                    return Ok((out, page.exhausted, page.resume));
                }
            }
            Err(laststore::Error::ReadBudgetExceeded) if !out.is_empty() => break,
            Err(err) => return Err(map(err)),
        }
    }
    let resume = out.last().map(|(key, _)| key.clone());
    Ok((out, false, resume))
}

impl PackRuntime {
    pub(in crate::storage::disk_pack) fn raw_many(
        &self,
        ctx: &Arc<ReadContext>,
        coords: &[Coordinate],
        pending: Option<&Arc<PendingBatch>>,
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        ctx.remembered((|| {
            ctx.validate_owner(pending)?;
            let max = [10_000, 40_000][usize::from(ctx.owns_write(pending))];
            if coords.len() > max {
                return Err(StorageError::read_limit(format!(
                    "native batch exceeds {max} keys"
                )));
            }
            let mut call = unpoison(&ctx.reader);
            ctx.require_room(&call, coords.len() as u64, 0)?;
            let mut all = Vec::with_capacity(coords.len());
            let overhead = std::mem::size_of::<(Coordinate, Option<Vec<u8>>)>()
                + 8 * std::mem::size_of::<usize>();
            for chunk in coords.chunks(10_000) {
                let (mut out, mut misses) = (vec![None; chunk.len()], Vec::new());
                for (slot, coord) in chunk.iter().enumerate() {
                    let mem = pending
                        .and_then(|b| b.lookup(&coord.0, &coord.1))
                        .or_else(|| {
                            ctx.accepted_batches
                                .iter()
                                .rev()
                                .find_map(|b| b.lookup(&coord.0, &coord.1))
                        });
                    let hit = mem.map(|r| (coord.1.len(), r.body.clone())).or_else(|| {
                        let cached = unpoison(ctx.write_reads.as_ref()?)
                            .entries
                            .get(coord)?
                            .clone();
                        Some((coord.0.len() + coord.1.len(), cached))
                    });
                    if let Some((kb, body)) = hit {
                        ctx.account_memory_record(
                            &mut call,
                            kb,
                            body.as_ref().map_or(0, Vec::len),
                        )?;
                        out[slot] = body;
                    } else {
                        misses.push(slot);
                    }
                }
                let wanted: Vec<_> = misses.iter().map(|&s| chunk[s].clone()).collect();
                let fetched = call.get_many(&wanted).map_err(|e| call_error(e, &call))?;
                for (slot, res) in misses.into_iter().zip(fetched) {
                    let val = res.map_err(|e| call_error(e, &call))?;
                    let (coord, body_len) = (&chunk[slot], val.as_ref().map_or(0, Vec::len));
                    if let Some(reads) = ctx
                        .write_reads
                        .as_ref()
                        .filter(|_| body_len <= WRITE_READ_MAX_VALUE_BYTES)
                    {
                        let mut cache = unpoison(reads);
                        let bytes = coord.0.len() + coord.1.len() + body_len + overhead;
                        if bytes <= WRITE_READ_MAX_BYTES.saturating_sub(cache.used_bytes)
                            && !cache.entries.contains_key(coord)
                        {
                            cache.entries.insert(coord.clone(), val.clone());
                            cache.used_bytes += bytes;
                        }
                    }
                    out[slot] = val;
                }
                all.extend(out);
            }
            Ok(all)
        })())
    }

    pub(in crate::storage::disk_pack) fn raw_range(
        &self,
        ctx: &Arc<ReadContext>,
        (col, start, end): Span<'_>,
        limit: usize,
        pending: Option<&Arc<PendingBatch>>,
        yielding: Option<usize>,
    ) -> StorageResult<PageOut> {
        ctx.remembered((|| {
            ctx.validate_owner(pending)?;
            if limit == 0 && yielding.is_none() {
                return Ok((Vec::new(), start >= end, None));
            }
            if limit > 10_000 {
                return Err(StorageError::read_limit(
                    "native page exceeds 10000 records",
                ));
            }
            if let Some(keys_per_run) = yielding {
                return raw_yielding(ctx, (col, start, end), limit, keys_per_run);
            }
            let mut rows = BTreeMap::new();
            for batch in ctx.accepted_batches.iter().chain(pending) {
                for rec in batch.matching(col, start, end) {
                    rows.insert(rec.key.clone(), rec);
                }
            }
            let (mut overlay, mut call) = (rows.into_values().peekable(), unpoison(&ctx.reader));
            let mut cur = call
                .home_for(col)
                .administrative_range(col, start, end)
                .map_err(map)?;
            let (mut disk, mut done, mut out) = (VecDeque::new(), false, Vec::new());
            while out.len() < limit {
                if disk.is_empty() && !done {
                    let page = cur.next_page((limit - out.len()).min(128)).map_err(map)?;
                    done = page.exhausted;
                    disk.extend(page.records);
                }
                if overlay
                    .peek()
                    .is_some_and(|m| disk.front().is_none_or(|(k, _)| m.key <= *k))
                {
                    let mem = overlay.next().expect("selected pending record");
                    cur.account_memory_record(mem.key.len(), mem.body.as_ref().map_or(0, Vec::len))
                        .map_err(map)?;
                    if disk.front().is_some_and(|(k, _)| *k == mem.key) {
                        disk.pop_front();
                    }
                    if let Some(body) = &mem.body {
                        out.push((mem.key.clone(), body.clone()));
                    }
                } else if let Some(rec) = disk.pop_front() {
                    out.push(rec);
                } else if done {
                    break;
                }
            }
            Ok((
                out,
                done && disk.is_empty() && overlay.peek().is_none(),
                None,
            ))
        })())
    }
}
