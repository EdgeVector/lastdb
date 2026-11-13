//! The physical home under one node store: the disk format v2 run homes and raw reads.

use super::*;
use crate::storage::format::metadata::FILE_REFERENCE_PREFIX;
use crate::storage::format::{
    METADATA_NAMESPACE, PENDING_UPLOAD_PREFIX, RECORD_FILE_PREFIX, RECORD_FILE_UPLOADED_PREFIX,
    RELEASED_FILE_PREFIX,
};
use laststore::disk_run::home::{RunHome, RunRead, RunReadCosts, RunRoot};
use std::collections::{BTreeMap, VecDeque};
use std::sync::PoisonError;

pub use crate::storage::format::CAS_COLLECTION;

pub(super) type NativeRows = Vec<(String, Vec<u8>)>;

pub(in crate::storage) struct V2Home {
    pub(in crate::storage) home: Arc<RunHome>,
    pub(in crate::storage) cas: Arc<RunHome>,
    pub(in crate::storage) merging: Arc<AtomicBool>,
    pub(in crate::storage) stop: Arc<AtomicBool>,
    pub(in crate::storage) merger: Mutex<Option<std::thread::JoinHandle<()>>>,
    pub(in crate::storage) retry_after: Arc<Mutex<Option<std::time::Instant>>>,
    pub(in crate::storage) cas_delete_recovery: Option<super::cas_delete::CasDeleteRecovery>,
}

impl V2Home {
    pub(in crate::storage) fn new(home: Arc<RunHome>, cas: Arc<RunHome>) -> Self {
        Self {
            home,
            cas,
            merging: Arc::default(),
            stop: Arc::default(),
            merger: Mutex::new(None),
            retry_after: Arc::default(),
            cas_delete_recovery: None,
        }
    }

    pub(in crate::storage) fn current_root(&self) -> V2Root {
        V2Root {
            main: self.home.current_root(),
            cas: self.cas.current_root(),
        }
    }

    pub(in crate::storage) fn read(&self, root: &V2Root, write_prep: bool) -> V2Read {
        let mut limits = budget();
        if write_prep {
            limits.keys = 40_000;
        }
        V2Read {
            main: self.home.read_root(&root.main, limits),
            cas: self.cas.read_root(&root.cas, limits),
        }
    }

    pub(in crate::storage) fn identity(&self) -> uuid::Uuid {
        self.home.identity()
    }
    pub(in crate::storage) fn path(&self) -> &Path {
        self.home.path()
    }
}

#[derive(Clone)]
pub(in crate::storage) struct V2Root {
    pub(in crate::storage) main: RunRoot,
    pub(in crate::storage) cas: RunRoot,
}

impl V2Root {
    pub(in crate::storage) fn generation(&self) -> u64 {
        self.main.generation() + self.cas.generation()
    }
    pub(in crate::storage) fn frontier(&self) -> u64 {
        self.main.frontier() + self.cas.frontier()
    }
}

pub(in crate::storage) struct V2Read {
    pub(in crate::storage) main: RunRead,
    pub(in crate::storage) cas: RunRead,
}

impl V2Read {
    pub(in crate::storage::disk_pack) fn home_for(&mut self, collection: &str) -> &mut RunRead {
        if collection == CAS_COLLECTION {
            &mut self.cas
        } else {
            &mut self.main
        }
    }

    pub(in crate::storage) fn get_many(
        &mut self,
        coordinates: &[(String, String)],
    ) -> laststore::Result<Vec<laststore::Result<Option<Vec<u8>>>>> {
        if !coordinates.iter().any(|(c, _)| c == CAS_COLLECTION) {
            return self.main.get_many(coordinates);
        }
        let mut out: Vec<Option<_>> = (0..coordinates.len()).map(|_| None).collect();
        for (is_cas, read) in [(false, &mut self.main), (true, &mut self.cas)] {
            let (slots, wanted): (Vec<_>, Vec<_>) = coordinates
                .iter()
                .enumerate()
                .filter(|(_, (c, _))| (c == CAS_COLLECTION) == is_cas)
                .map(|(i, coord)| (i, coord.clone()))
                .unzip();
            if !wanted.is_empty() {
                for (slot, found) in slots.into_iter().zip(read.get_many(&wanted)?) {
                    out[slot] = Some(found);
                }
            }
        }
        let (costs, limit) = (self.costs(), self.main.budget());
        if costs.examined_keys > limit.keys || costs.budget_bytes > limit.bytes {
            return Err(laststore::Error::ReadBudgetExceeded);
        }
        Ok(out
            .into_iter()
            .map(|f| f.expect("each slot is read once"))
            .collect())
    }

    pub(in crate::storage) fn costs(&self) -> RunReadCosts {
        let (main, cas) = (self.main.costs(), self.cas.costs());
        RunReadCosts {
            examined_keys: main.examined_keys + cas.examined_keys,
            budget_bytes: main.budget_bytes + cas.budget_bytes,
        }
    }
}

fn require_page_limit(limit: usize) -> StorageResult<()> {
    (limit <= 10000)
        .then_some(())
        .ok_or_else(|| StorageError::ReadResourceLimit {
            detail: "native page exceeds 10000 records".into(),
        })
}

fn with_ctx<T>(ctx: &read::ReadContext, f: impl FnOnce() -> StorageResult<T>) -> StorageResult<T> {
    ctx.remembered(f())
}

impl PackRuntime {
    pub(crate) fn record_read_stage(&self, _stage: &'static str, _items: usize) {}

    pub(in crate::storage::disk_pack) fn raw_many(
        &self,
        context: &Arc<read::ReadContext>,
        coordinates: &[(String, String)],
        pending: Option<&Arc<transaction::PendingBatch>>,
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        with_ctx(context, || {
            context.validate_owner(pending)?;
            let max = if context.owns_write(pending) {
                40_000
            } else {
                10_000
            };
            if coordinates.len() > max {
                return Err(StorageError::ReadResourceLimit {
                    detail: format!("native batch exceeds {max} keys"),
                });
            }
            let mut call = context.call.lock().unwrap_or_else(PoisonError::into_inner);
            context.require_room(&call, coordinates.len() as u64, 0)?;
            let mut out = Vec::with_capacity(coordinates.len());
            for chunk in coordinates.chunks(10_000) {
                out.extend(self.read_chunk(context, &mut call, chunk, pending)?);
            }
            Ok(out)
        })
    }

    fn read_chunk(
        &self,
        context: &read::ReadContext,
        call: &mut V2Read,
        coordinates: &[(String, String)],
        pending: Option<&Arc<transaction::PendingBatch>>,
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        let (mut out, mut misses) = (vec![None; coordinates.len()], Vec::new());
        for (slot, coord) in coordinates.iter().enumerate() {
            if let Some(rec) = context.overlay_record(pending, &coord.0, &coord.1) {
                let len = rec.body.as_ref().map_or(0, Vec::len);
                context.account_memory_record(call, coord.1.len(), len)?;
                out[slot] = rec.body.clone();
            } else if let Some(val) = context.reuse_disk_value(call, coord)? {
                out[slot] = val;
            } else {
                misses.push(slot);
            }
        }
        let wanted: Vec<_> = misses.iter().map(|&s| coordinates[s].clone()).collect();
        let results = call.get_many(&wanted).map_err(|e| call_error(e, call))?;
        for (slot, res) in misses.into_iter().zip(results) {
            let val = res.map_err(|e| call_error(e, call))?;
            context.remember_disk_value(&coordinates[slot], val.as_deref());
            out[slot] = val;
        }
        Ok(out)
    }

    pub(super) fn raw_rows(
        &self,
        context: &Arc<read::ReadContext>,
        collection: &str,
        start: &[u8],
        end: &[u8],
        limit: usize,
        pending: Option<&Arc<transaction::PendingBatch>>,
    ) -> StorageResult<(NativeRows, bool)> {
        with_ctx(context, || {
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
            let mut cur = call
                .home_for(collection)
                .administrative_range(collection, start, end)
                .map_err(map)?;
            let (mut disk, mut exhausted, mut out) = (VecDeque::new(), false, Vec::new());
            while out.len() < limit {
                if disk.is_empty() && !exhausted {
                    let page = cur.next_page((limit - out.len()).min(128)).map_err(map)?;
                    exhausted = page.exhausted;
                    disk.extend(page.records);
                }
                if overlay
                    .peek()
                    .is_some_and(|m| disk.front().is_none_or(|(k, _)| m.key <= *k))
                {
                    let m = overlay.next().expect("selected pending record");
                    cur.account_memory_record(m.key.len(), m.body.as_ref().map_or(0, Vec::len))
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
        })
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
        with_ctx(context, || {
            context.validate_owner(None)?;
            require_page_limit(limit)?;
            let mut call = context.call.lock().unwrap_or_else(PoisonError::into_inner);
            let mut cur = call
                .home_for(collection)
                .administrative_range(collection, start, end)
                .map_err(map)?;
            let mut out: NativeRows = Vec::new();
            while out.len() < limit {
                match cur.next_page_yielding((limit - out.len()).min(128), out.len(), keys_per_row)
                {
                    Ok(page) => {
                        out.extend(page.records);
                        if page.resume.is_some() || page.exhausted {
                            return Ok((out, page.resume));
                        }
                    }
                    Err(laststore::Error::ReadBudgetExceeded) if !out.is_empty() => break,
                    Err(error) => return Err(map(error)),
                }
            }
            let resume = out.last().map(|(k, _)| k.clone());
            Ok((out, resume))
        })
    }

    pub async fn has_external_backup_files(self: &Arc<Self>) -> StorageResult<bool> {
        let meta = PackKvStore {
            runtime: Arc::clone(self),
            namespace: METADATA_NAMESPACE.into(),
        };
        let cas = self.cas_kv();
        for (store, p) in [
            (&meta, FILE_REFERENCE_PREFIX),
            (&cas, PENDING_UPLOAD_PREFIX),
            (&cas, RECORD_FILE_UPLOADED_PREFIX),
            (&cas, RELEASED_FILE_PREFIX),
            (&cas, RECORD_FILE_PREFIX),
        ] {
            if !store.prefix_rows(p.as_bytes(), 1, false).await?.is_empty() {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

fn raw_suffix(op: &HomeMutation) -> Option<String> {
    let decoded = String::from_utf8(native_routing::decode(&op.key).ok()?).ok()?;
    decoded.strip_prefix(RECORD_FILE_PREFIX).map(str::to_owned)
}

fn blob_marker(prefix: &str, suffix: &str, put: bool) -> HomeMutation {
    HomeMutation {
        collection: CAS_COLLECTION.into(),
        key: native_routing::encode(format!("{prefix}{suffix}").as_bytes()),
        body: put.then(|| b"1".to_vec()),
    }
}

pub(in crate::storage::disk_pack) fn queue_new_uploads(cas: &mut BTreeMap<String, HomeMutation>) {
    let queued: Vec<_> = cas
        .values()
        .filter(|op| op.body.is_some())
        .filter_map(raw_suffix)
        .filter(|s| {
            let up = blob_marker(RECORD_FILE_UPLOADED_PREFIX, s, true);
            cas.get(&up.key).is_none_or(|op| op.body.is_none())
        })
        .map(|s| blob_marker(PENDING_UPLOAD_PREFIX, &s, true))
        .collect();
    for op in queued {
        cas.entry(op.key.clone()).or_insert(op);
    }
}

pub(in crate::storage::disk_pack) fn with_releases(
    deletes: Vec<HomeMutation>,
) -> Vec<HomeMutation> {
    let mut out = BTreeMap::new();
    for op in deletes {
        if let Some(s) = raw_suffix(&op) {
            for d in [
                blob_marker(RECORD_FILE_UPLOADED_PREFIX, &s, false),
                blob_marker(PENDING_UPLOAD_PREFIX, &s, false),
                blob_marker(RELEASED_FILE_PREFIX, &s, true),
            ] {
                out.entry(d.key.clone()).or_insert(d);
            }
        }
        out.insert(op.key.clone(), op);
    }
    out.into_values().collect()
}

pub(super) fn call_error(error: laststore::Error, call: &V2Read) -> StorageError {
    if !matches!(error, laststore::Error::ReadBudgetExceeded) {
        return map(error);
    }
    let (costs, limits) = (call.costs(), call.main.budget());
    StorageError::ReadResourceLimit {
        detail: format!(
            "native call: examined_keys={}/{}, charged_bytes={}/{}",
            costs.examined_keys, limits.keys, costs.budget_bytes, limits.bytes,
        ),
    }
}
