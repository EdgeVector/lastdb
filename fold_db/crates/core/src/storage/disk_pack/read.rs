//! Captured read roots, task-local scopes, and write-read caching.

use super::physical::V2Read;
use super::transaction::PendingBatch;
use super::*;
use crate::resident::RecordCoordinate;
use lastdb_model::record_value::StoredRecord;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::sync::{MutexGuard, PoisonError};

const WRITE_READ_MAX_BYTES: usize = 1024 * 1024;
const WRITE_READ_MAX_VALUE_BYTES: usize = 16 * 1024;

pub(super) fn unpoison<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

pub(super) type NativeRows = Vec<(String, Vec<u8>)>;
pub(super) type Coordinate = (String, String);
type WriteReads = (BTreeMap<Coordinate, Option<Vec<u8>>>, usize);
type FailureSlot = Arc<Mutex<Option<(bool, String)>>>;
type PageOut = (NativeRows, bool, Option<String>);
type Span<'a> = (&'a str, &'a [u8], &'a [u8]);

#[derive(Clone)]
pub(in crate::storage::disk_pack) struct ReadContext(
    pub(super) Arc<Mutex<V2Read>>,
    pub(super) Arc<Vec<Arc<PendingBatch>>>,
    pub(super) u64,
    uuid::Uuid,
    FailureSlot,
    Option<Arc<PendingBatch>>,
    Option<Arc<Mutex<WriteReads>>>,
);

tokio::task_local! {
    static READ: Arc<ReadContext>;
    static ITEM: Option<FailureSlot>;
}

impl ReadContext {
    pub(super) fn captured(rt: &PackRuntime) -> Option<Arc<Self>> {
        READ.try_with(Arc::clone).ok().filter(|c| {
            c.3 == rt.physical.home.identity()
                && c.validate_owner(PendingBatch::current(rt).as_ref()).is_ok()
        })
    }
    pub(super) fn capture(
        rt: &Arc<PackRuntime>,
        owner: Option<Arc<PendingBatch>>,
    ) -> StorageResult<Arc<Self>> {
        if rt.requires_recovery() {
            return Err(StorageError::backend(
                "native publication failed; recovery required",
            ));
        }
        let prep = owner.is_some();
        let accepted = if prep {
            let q = rt.queue.lock().expect("native preparation prefix");
            Arc::new(q.requests.iter().map(|(p, _)| Arc::clone(p)).collect())
        } else {
            Arc::default()
        };
        rt.adopt_merged_root()?;
        let v = rt.view.read().expect("native complete read view");
        let (call, rev) = (Arc::new(Mutex::new(rt.physical.read(&v.0, prep))), v.1);
        drop(v);
        let (id, fail, wr) = (
            rt.physical.home.identity(),
            Arc::default(),
            prep.then(Arc::default),
        );
        Ok(Arc::new(Self(call, accepted, rev, id, fail, owner, wr)))
    }
    pub(super) async fn current(rt: &Arc<PackRuntime>) -> StorageResult<Arc<Self>> {
        if let Ok(ctx) = READ.try_with(Arc::clone) {
            if ctx.3 != rt.physical.home.identity() {
                return Err(StorageError::invalid(
                    "read scope cannot change home identity",
                ));
            }
            ctx.validate_owner(PendingBatch::current(rt).as_ref())?;
            let mut next = (*ctx).clone();
            let slot = ITEM.try_with(Clone::clone).ok().flatten();
            next.4 = slot.unwrap_or_default();
            return Ok(Arc::new(next));
        }
        let rt = Arc::clone(rt);
        run_blocking(move || Self::capture(&rt, None)).await
    }
    pub(super) fn remembered<T>(&self, res: StorageResult<T>) -> StorageResult<T> {
        if let Err(e) = &res {
            let is_lim = matches!(e, StorageError::ReadResourceLimit { .. });
            unpoison(&self.4).get_or_insert_with(|| (is_lim, e.to_string()));
        }
        res
    }
    fn owns_write(&self, p: Option<&Arc<PendingBatch>>) -> bool {
        matches!((&self.5, p), (Some(a), Some(b)) if Arc::ptr_eq(a, b))
    }
    fn validate_owner(&self, p: Option<&Arc<PendingBatch>>) -> StorageResult<()> {
        if (self.5.is_none() && p.is_none()) || self.owns_write(p) {
            return Ok(());
        }
        self.remembered(Err(StorageError::invalid(
            "read scope cannot change write owner",
        )))
    }
    pub(super) fn error(&self) -> Option<StorageError> {
        let (lim, d) = unpoison(&self.4).clone()?;
        Some(if lim {
            StorageError::read_limit(d)
        } else {
            StorageError::backend(d)
        })
    }
    pub(super) async fn in_scope<T>(
        self: &Arc<Self>,
        item: bool,
        fut: impl Future<Output = T>,
    ) -> T {
        let slot = item.then(|| Arc::clone(&self.4));
        READ.scope(Arc::clone(self), ITEM.scope(slot, Box::pin(fut)))
            .await
    }
    pub(super) fn account_memory_record(
        &self,
        c: &mut V2Read,
        k: usize,
        b: usize,
    ) -> StorageResult<()> {
        self.remembered((|| {
            let n = k
                .checked_add(b)
                .ok_or_else(|| StorageError::read_limit("record length overflow"))?;
            self.require_room(c, 1, n as u64)?;
            c.0.account_memory_record(k, b)
                .map_err(|e| call_error(e, c))
        })())
    }
    fn require_room(&self, call: &V2Read, keys: u64, bytes: u64) -> StorageResult<()> {
        if let Some(err) = self.error() {
            return Err(err);
        }
        let (c, l) = (call.costs(), call.0.budget());
        let over_k = c.examined_keys.checked_add(keys).is_none_or(|n| n > l.keys);
        let over_b = c
            .budget_bytes
            .checked_add(bytes)
            .is_none_or(|n| n > l.bytes);
        if over_k || over_b {
            return Err(StorageError::read_limit(format!(
                "native required work: examined_keys={}+{keys}/{}, charged_bytes={}+{bytes}/{}",
                c.examined_keys, l.keys, c.budget_bytes, l.bytes,
            )));
        }
        Ok(())
    }
}

fn call_error(error: laststore::Error, call: &V2Read) -> StorageError {
    if !matches!(error, laststore::Error::ReadBudgetExceeded) {
        return map(error);
    }
    let (c, l) = (call.costs(), call.0.budget());
    StorageError::read_limit(format!(
        "native call: examined_keys={}/{}, charged_bytes={}/{}",
        c.examined_keys, l.keys, c.budget_bytes, l.bytes
    ))
}

fn raw_yielding(
    ctx: &ReadContext,
    (col, s, e): Span<'_>,
    lim: usize,
    kpr: usize,
) -> StorageResult<PageOut> {
    let mut call = unpoison(&ctx.0);
    let mut cur = call
        .home_for(col)
        .administrative_range(col, s, e)
        .map_err(map)?;
    let mut out: NativeRows = Vec::new();
    while out.len() < lim {
        match cur.next_page_yielding((lim - out.len()).min(128), out.len(), kpr) {
            Ok(p) => {
                out.extend(p.records);
                if p.resume.is_some() || p.exhausted {
                    return Ok((out, p.exhausted, p.resume));
                }
            }
            Err(laststore::Error::ReadBudgetExceeded) if !out.is_empty() => break,
            Err(err) => return Err(map(err)),
        }
    }
    let resume = out.last().map(|(k, _)| k.clone());
    Ok((out, false, resume))
}

impl PackRuntime {
    pub(super) fn raw_many(
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
            let mut call = unpoison(&ctx.0);
            ctx.require_room(&call, coords.len() as u64, 0)?;
            let mut all = Vec::with_capacity(coords.len());
            let oh = std::mem::size_of::<(Coordinate, Option<Vec<u8>>)>()
                + 8 * std::mem::size_of::<usize>();
            for chunk in coords.chunks(10_000) {
                let (mut out, mut misses) = (vec![None; chunk.len()], Vec::new());
                for (slot, c) in chunk.iter().enumerate() {
                    let mem = pending
                        .and_then(|b| b.lookup(&c.0, &c.1))
                        .or_else(|| ctx.1.iter().rev().find_map(|b| b.lookup(&c.0, &c.1)));
                    let hit = mem.map(|r| (c.1.len(), r.body.clone())).or_else(|| {
                        let cached = unpoison(ctx.6.as_ref()?).0.get(c)?.clone();
                        Some((c.0.len() + c.1.len(), cached))
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
                    let (c, body) = (&chunk[slot], val.as_ref().map_or(0, Vec::len));
                    if let Some(reads) = ctx
                        .6
                        .as_ref()
                        .filter(|_| body <= WRITE_READ_MAX_VALUE_BYTES)
                    {
                        let mut s = unpoison(reads);
                        let bytes = c.0.len() + c.1.len() + body + oh;
                        if bytes <= WRITE_READ_MAX_BYTES.saturating_sub(s.1) && !s.0.contains_key(c)
                        {
                            s.0.insert(c.clone(), val.clone());
                            s.1 += bytes;
                        }
                    }
                    out[slot] = val;
                }
                all.extend(out);
            }
            Ok(all)
        })())
    }

    pub(super) fn raw_range(
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
            if let Some(kpr) = yielding {
                return raw_yielding(ctx, (col, start, end), limit, kpr);
            }
            let mut rows = BTreeMap::new();
            for b in ctx.1.iter().chain(pending) {
                for r in b.matching(col, start, end) {
                    rows.insert(r.key.clone(), r);
                }
            }
            let (mut ov, mut call) = (rows.into_values().peekable(), unpoison(&ctx.0));
            let mut cur = call
                .home_for(col)
                .administrative_range(col, start, end)
                .map_err(map)?;
            let (mut disk, mut done, mut out) = (VecDeque::new(), false, Vec::new());
            while out.len() < limit {
                if disk.is_empty() && !done {
                    let p = cur.next_page((limit - out.len()).min(128)).map_err(map)?;
                    done = p.exhausted;
                    disk.extend(p.records);
                }
                if ov
                    .peek()
                    .is_some_and(|m| disk.front().is_none_or(|(k, _)| m.key <= *k))
                {
                    let m = ov.next().expect("selected pending record");
                    cur.account_memory_record(m.key.len(), m.body.as_ref().map_or(0, Vec::len))
                        .map_err(map)?;
                    if disk.front().is_some_and(|(k, _)| *k == m.key) {
                        disk.pop_front();
                    }
                    if let Some(body) = &m.body {
                        out.push((m.key.clone(), body.clone()));
                    }
                } else if let Some(rec) = disk.pop_front() {
                    out.push(rec);
                } else if done {
                    break;
                }
            }
            Ok((out, done && disk.is_empty() && ov.peek().is_none(), None))
        })())
    }

    pub(crate) fn admit_read_records(
        &self,
        read: &WarmRecordRead,
        recs: Vec<(usize, Arc<StoredRecord>, usize)>,
    ) {
        if !recs.is_empty() && self.view.read().expect("native record warm view").1 == read.0 {
            let items = recs
                .into_iter()
                .map(|(at, r, b)| (read.1[at].clone(), r, b))
                .collect();
            unpoison(&self.core).admit_records(items);
        }
    }
}

pub(crate) struct WarmRecordRead(
    pub(super) u64,
    pub(super) Vec<RecordCoordinate>,
    pub(super) Vec<Option<Arc<StoredRecord>>>,
);

impl WarmRecordRead {
    pub(crate) fn take_found(&mut self) -> Vec<Option<Arc<StoredRecord>>> {
        std::mem::take(&mut self.2)
    }
}
