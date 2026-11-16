//! Captured read roots, task-local scopes, write-read caching, and overlay reads on [`PackRuntime`].

use super::physical::V2Read;
use super::transaction::PendingBatch;
use super::*;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;

pub(in crate::storage::disk_pack) use crate::platform::unpoison;

pub(in crate::storage::disk_pack) type NativeRows = Vec<(String, Vec<u8>)>;
pub(in crate::storage::disk_pack) type Coordinate = (String, String);
type FailureSlot = Arc<Mutex<Option<RecordedFailure>>>;
type PageOut = (NativeRows, bool, Option<String>);
type Span<'a> = (&'a str, &'a [u8], &'a [u8]);

const WRITE_READ_MAX_BYTES: usize = 1024 * 1024;
const WRITE_READ_MAX_VALUE_BYTES: usize = 16 * 1024;

#[derive(Clone)]
struct RecordedFailure {
    is_read_limit: bool,
    detail: String,
}

#[derive(Default)]
struct WriteReadCache {
    entries: BTreeMap<Coordinate, Option<Vec<u8>>>,
    used_bytes: usize,
}

#[derive(Clone)]
pub(in crate::storage::disk_pack) struct ReadContext {
    pub(in crate::storage::disk_pack) reader: Arc<Mutex<V2Read>>,
    pub(in crate::storage::disk_pack) accepted_batches: Arc<Vec<Arc<PendingBatch>>>,
    pub(in crate::storage::disk_pack) revision: u64,
    home_id: uuid::Uuid,
    failure: FailureSlot,
    write_owner: Option<Arc<PendingBatch>>,
    write_reads: Option<Arc<Mutex<WriteReadCache>>>,
}

tokio::task_local! {
    static READ: Arc<ReadContext>;
    static ITEM: Option<FailureSlot>;
}

impl ReadContext {
    pub(in crate::storage::disk_pack) fn captured(rt: &PackRuntime) -> Option<Arc<Self>> {
        READ.try_with(Arc::clone).ok().filter(|ctx| {
            ctx.home_id == rt.physical.home.identity()
                && ctx
                    .validate_owner(PendingBatch::current(rt).as_ref())
                    .is_ok()
        })
    }

    pub(in crate::storage::disk_pack) fn capture(
        rt: &Arc<PackRuntime>,
        write_owner: Option<Arc<PendingBatch>>,
    ) -> StorageResult<Arc<Self>> {
        if rt.requires_recovery() {
            return Err(StorageError::backend(
                "native publication failed; recovery required",
            ));
        }
        let prep = write_owner.is_some();
        let accepted_batches = if prep {
            let queue = rt.queue.lock().expect("native preparation prefix");
            Arc::new(queue.requests.iter().map(|(p, _)| Arc::clone(p)).collect())
        } else {
            Arc::default()
        };
        rt.adopt_merged_root()?;
        let view = rt.view.read().expect("native complete read view");
        let reader = Arc::new(Mutex::new(rt.physical.read(&view.root, prep)));
        let revision = view.revision;
        drop(view);
        Ok(Arc::new(Self {
            reader,
            accepted_batches,
            revision,
            home_id: rt.physical.home.identity(),
            failure: Arc::default(),
            write_owner,
            write_reads: prep.then(Arc::default),
        }))
    }

    pub(in crate::storage::disk_pack) async fn current(
        rt: &Arc<PackRuntime>,
    ) -> StorageResult<Arc<Self>> {
        if let Ok(ctx) = READ.try_with(Arc::clone) {
            if ctx.home_id != rt.physical.home.identity() {
                return Err(StorageError::invalid(
                    "read scope cannot change home identity",
                ));
            }
            ctx.validate_owner(PendingBatch::current(rt).as_ref())?;
            let mut next = (*ctx).clone();
            next.failure = ITEM
                .try_with(Clone::clone)
                .ok()
                .flatten()
                .unwrap_or_default();
            return Ok(Arc::new(next));
        }
        let rt = Arc::clone(rt);
        run_blocking(move || Self::capture(&rt, None)).await
    }

    pub(in crate::storage::disk_pack) fn remembered<T>(
        &self,
        res: StorageResult<T>,
    ) -> StorageResult<T> {
        if let Err(err) = &res {
            let is_read_limit = matches!(err, StorageError::ReadResourceLimit { .. });
            unpoison(&self.failure).get_or_insert_with(|| RecordedFailure {
                is_read_limit,
                detail: err.to_string(),
            });
        }
        res
    }

    fn owns_write(&self, pending: Option<&Arc<PendingBatch>>) -> bool {
        matches!((&self.write_owner, pending), (Some(a), Some(b)) if Arc::ptr_eq(a, b))
    }

    fn validate_owner(&self, pending: Option<&Arc<PendingBatch>>) -> StorageResult<()> {
        if (self.write_owner.is_none() && pending.is_none()) || self.owns_write(pending) {
            return Ok(());
        }
        self.remembered(Err(StorageError::invalid(
            "read scope cannot change write owner",
        )))
    }

    pub(in crate::storage::disk_pack) fn error(&self) -> Option<StorageError> {
        let RecordedFailure {
            is_read_limit,
            detail,
        } = unpoison(&self.failure).clone()?;
        Some(if is_read_limit {
            StorageError::read_limit(detail)
        } else {
            StorageError::backend(detail)
        })
    }

    pub(in crate::storage::disk_pack) async fn in_scope<T>(
        self: &Arc<Self>,
        item: bool,
        fut: impl Future<Output = T>,
    ) -> T {
        let slot = item.then(|| Arc::clone(&self.failure));
        READ.scope(Arc::clone(self), ITEM.scope(slot, Box::pin(fut)))
            .await
    }

    pub(in crate::storage::disk_pack) fn account_memory_record(
        &self,
        call: &mut V2Read,
        key_bytes: usize,
        body_bytes: usize,
    ) -> StorageResult<()> {
        self.remembered((|| {
            let total = key_bytes
                .checked_add(body_bytes)
                .ok_or_else(|| StorageError::read_limit("record length overflow"))?;
            self.require_room(call, 1, total as u64)?;
            call.main
                .account_memory_record(key_bytes, body_bytes)
                .map_err(|e| call_error(e, call))
        })())
    }

    fn require_room(&self, call: &V2Read, keys: u64, bytes: u64) -> StorageResult<()> {
        if let Some(err) = self.error() {
            return Err(err);
        }
        let (costs, limit) = (call.costs(), call.main.budget());
        let over_k = costs
            .examined_keys
            .checked_add(keys)
            .is_none_or(|n| n > limit.keys);
        let over_b = costs
            .budget_bytes
            .checked_add(bytes)
            .is_none_or(|n| n > limit.bytes);
        if over_k || over_b {
            return Err(StorageError::read_limit(format!(
                "native required work: examined_keys={}+{keys}/{}, charged_bytes={}+{bytes}/{}",
                costs.examined_keys, limit.keys, costs.budget_bytes, limit.bytes,
            )));
        }
        Ok(())
    }
}

fn call_error(error: laststore::Error, call: &V2Read) -> StorageError {
    if !matches!(error, laststore::Error::ReadBudgetExceeded) {
        return map(error);
    }
    let (costs, limit) = (call.costs(), call.main.budget());
    StorageError::read_limit(format!(
        "native call: examined_keys={}/{}, charged_bytes={}/{}",
        costs.examined_keys, limit.keys, costs.budget_bytes, limit.bytes
    ))
}

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
