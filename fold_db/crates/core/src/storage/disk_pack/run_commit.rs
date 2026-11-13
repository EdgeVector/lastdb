//! Commit admission, publication, and resident warm-record admission.

use super::physical::{V2Home, V2Root, CAS_COLLECTION};
use super::*;
use lastdb_model::storage_policy::native_write::PENDING_MEMORY_BYTES;
use laststore::disk_run::home::{CheckpointKind, RunHome};
use laststore::space::reserve_space;
use std::collections::BTreeMap;

pub(super) struct PreparedCommit(
    pub(super) Vec<HomeMutation>,
    pub(super) u64,
    pub(super) SpaceReservation,
);

impl PackRuntime {
    pub(super) fn commit(&self, operations: Vec<HomeMutation>) -> StorageResult<()> {
        if operations.is_empty() {
            return Ok(());
        }
        let prepared = self.prepare_commit(operations, None)?;
        let changes: Vec<_> = prepared
            .0
            .iter()
            .map(|op| (op.collection.clone(), op.key.clone()))
            .collect();
        self.commit_prepared(prepared, &changes, &[])
    }

    pub(super) fn prepare_commit(
        &self,
        operations: Vec<HomeMutation>,
        previous: Option<&PreparedCommit>,
    ) -> StorageResult<PreparedCommit> {
        let only_ns = operations.iter().all(|op| {
            op.collection == REGISTRY
                && op.key.starts_with(NAMESPACES)
                && op.body.as_deref() == Some(&[1])
        });
        if self.fresh_initialization_pending() && !only_ns {
            return Err(creation_refusal(
                "creation already holds its durable proofs; source writes must wait",
            ));
        }
        if self.requires_recovery() {
            return Err(StorageError::backend(
                "native writer requires a snapshot reopen after a failed publication",
            ));
        }
        RunHome::check_mutations(&operations).map_err(map)?;
        let limit = self.batch_operation_limit();
        let incoming = batch_bytes(&operations, limit)?;
        let bytes = if let Some(prev) = previous {
            let ok_ops = prev
                .0
                .len()
                .checked_add(operations.len())
                .is_some_and(|n| n <= limit);
            prev.1
                .checked_add(incoming)
                .and_then(|b| b.checked_sub(256))
                .filter(|&b| ok_ops && b <= PENDING_MEMORY_BYTES)
                .ok_or_else(|| {
                    StorageError::write_limit(
                        "native accepted prefix reached its operation or byte limit; retry after flush",
                    )
                })?
        } else {
            incoming
        };
        let intent = cas_delete::reserved_space(&operations, previous.map(|p| p.0.as_slice()))?;
        let reserved = bytes.saturating_add(intent);
        let space = if let Some(prev) = previous {
            prev.2.reserve_extension(&self.space_policy, reserved)
        } else {
            reserve_space(self.physical.home.path(), &self.space_policy, reserved)
        }
        .map_err(map)?;
        Ok(PreparedCommit(operations, bytes, space))
    }

    pub(super) fn commit_prepared(
        &self,
        PreparedCommit(operations, _, space): PreparedCommit,
        hidden_changes: &[(String, String)],
        requests: &[Arc<transaction::PendingBatch>],
    ) -> StorageResult<()> {
        let frontier_before = self.physical.current_root().frontier();
        let notes = transaction::take_accepted_notes(requests);
        self.prepare_accepted(frontier_before, &notes)?;
        let split =
            !notes.is_empty() && operations.iter().any(|op| op.collection == CAS_COLLECTION);
        if split {
            self.arm_accepted_split()?;
        }
        let v2 = &self.physical;
        let result = (|| {
            cas_delete::require_clear(v2)?;
            let mut journal = operations
                .iter()
                .any(|op| op.collection != CAS_COLLECTION)
                .then(|| self.mutation_log.begin(v2))
                .flatten();
            commit_split(v2, operations, journal.as_mut())?;
            if let Some(journal) = journal {
                journal.finish(v2);
            }
            if split {
                self.note_accepted_split(frontier_before)?;
            }
            self.publish_accepted(frontier_before, v2.current_root().frontier())
        })();
        if let Err(error) = result {
            self.fail_accepted(frontier_before);
            self.failed.store(true, Ordering::Release);
            *self.failed_write.lock().expect("native failed write owner") = Some(space);
            return Err(error);
        }
        self.install_committed(v2.current_root(), hidden_changes, requests)?;
        drop(space);
        self.failed.store(false, Ordering::Release);
        self.spawn_merges();
        Ok(())
    }

    pub(super) fn install_committed(
        &self,
        current: V2Root,
        hidden_changes: &[(String, String)],
        requests: &[Arc<transaction::PendingBatch>],
    ) -> StorageResult<()> {
        let records = record_warm::committed_records(requests);
        let mut view = self.view.write().expect("native complete read view");
        if !hidden_changes.is_empty() {
            view.1 = view
                .1
                .checked_add(1)
                .ok_or_else(|| StorageError::backend("native accepted revision exhausted"))?;
        }
        if !hidden_changes.is_empty() || !requests.is_empty() || !records.is_empty() {
            let mut core = self.core.lock().expect("committed warm records");
            if requests.is_empty() {
                core.invalidate_records(hidden_changes);
            } else {
                for r in requests {
                    core.invalidate_records(&r.changed_coordinates());
                }
            }
            if !records.is_empty() {
                core.admit_records(records);
            }
        }
        drop((std::mem::replace(&mut view.0, current), view));
        self.adopt_merged_root()
    }
}

pub(in crate::storage::disk_pack) fn account_ops(
    operations: &[HomeMutation],
    mut bytes: u64,
    mut max_body: usize,
    cap: u64,
    limit_detail: &'static str,
) -> StorageResult<(u64, usize)> {
    for op in operations {
        if op.collection.len() > 65531 || op.key.len() > 65535 {
            return Err(StorageError::invalid(
                "native collection or key exceeds format bounds",
            ));
        }
        let body = op.body.as_ref().map_or(0, Vec::len);
        max_body = max_body.max(body);
        bytes = bytes
            .checked_add((160 + op.collection.len() + 2 * op.key.len() + body) as u64)
            .filter(|&b| b <= cap)
            .ok_or_else(|| StorageError::mutation_limit(limit_detail))?;
    }
    Ok((bytes, max_body))
}

pub(in crate::storage::disk_pack) fn batch_bytes(
    operations: &[HomeMutation],
    operation_limit: usize,
) -> StorageResult<u64> {
    if operations.len() > operation_limit {
        return Err(StorageError::mutation_limit(format!(
            "complete native batch exceeds {operation_limit} operations"
        )));
    }
    let lim = "complete native batch exceeds pending memory allowance";
    account_ops(operations, 256, 0, PENDING_MEMORY_BYTES, lim).map(|(b, _)| b)
}

fn commit_split(
    v2: &V2Home,
    operations: Vec<HomeMutation>,
    mut journal: Option<&mut mutation_log::Recording<'_>>,
) -> StorageResult<()> {
    let mut commit_main = |ops: Vec<HomeMutation>| -> StorageResult<()> {
        let committed = v2.home.commit(ops).map_err(map)?;
        if let Some(log) = journal.as_mut() {
            log.retain(mutation_log::CommitRole::Main, &committed);
        }
        Ok(())
    };
    if !operations.iter().any(|op| op.collection == CAS_COLLECTION) {
        return commit_main(operations);
    }
    let (cas, main): (Vec<_>, _) = operations
        .into_iter()
        .partition(|op| op.collection == CAS_COLLECTION);
    let last: BTreeMap<_, _> = cas.into_iter().map(|op| (op.key.clone(), op)).collect();
    let (puts, deletes): (Vec<_>, _) = last.into_values().partition(|op| op.body.is_some());
    cas_delete::save(v2, !puts.is_empty(), !main.is_empty(), &deletes)?;
    if !puts.is_empty() {
        v2.cas.commit(puts).map_err(map)?;
    }
    if !main.is_empty() {
        commit_main(main)?;
    }
    if !deletes.is_empty() {
        v2.cas.commit(deletes).map_err(map)?;
        v2.home
            .clear_checkpoint(CheckpointKind::CasDelete)
            .map_err(map)?;
    }
    Ok(())
}

pub(crate) mod record_warm {
    use super::read::ReadContext;
    use super::transaction::PendingBatch;
    use super::*;
    use crate::resident::{core_warm_bytes, RecordCoordinate};
    use crate::storage::format::MAIN_NAMESPACE;
    use lastdb_model::record_value::StoredRecord;
    use std::collections::HashMap;
    use std::hash::{BuildHasher, BuildHasherDefault, DefaultHasher};
    use std::sync::PoisonError;

    pub(crate) struct WarmRecordRead(u64, Vec<RecordCoordinate>, Vec<Option<Arc<StoredRecord>>>);

    impl WarmRecordRead {
        pub(crate) fn take_found(&mut self) -> Vec<Option<Arc<StoredRecord>>> {
            std::mem::take(&mut self.2)
        }
    }

    pub(crate) struct WrittenRecord(RecordCoordinate, Arc<StoredRecord>, usize, u64);

    impl WrittenRecord {
        pub(crate) fn new(key: &str, record: Arc<StoredRecord>, val: &[u8]) -> Self {
            let coord = kv::coordinate(MAIN_NAMESPACE, key.as_bytes());
            Self(coord, record, val.len(), digest(val))
        }
    }

    impl PackRuntime {
        pub(crate) fn warm_records<K: AsRef<str>>(
            &self,
            keys: &[K],
        ) -> StorageResult<Option<WarmRecordRead>> {
            if PendingBatch::current(self).is_some() {
                return Ok(None);
            }
            let view = self.view.read().expect("native record warm view");
            let ctx = ReadContext::captured(self);
            if ctx.as_ref().is_some_and(|c| c.revision != view.1) {
                return Ok(None);
            }
            let coords: Vec<_> = keys
                .iter()
                .map(|k| kv::coordinate(MAIN_NAMESPACE, k.as_ref().as_bytes()))
                .collect();
            let mut call = ctx
                .as_ref()
                .map(|c| c.call.lock().unwrap_or_else(PoisonError::into_inner));
            let found = self
                .core
                .lock()
                .expect("native record warm set")
                .record_lookup(&coords);
            if let (Some(ctx), Some(call)) = (&ctx, &mut call) {
                for (key, hit) in keys.iter().zip(&found) {
                    if let Some((_, bytes)) = hit {
                        ctx.account_memory_record(call, key.as_ref().len(), *bytes)?;
                    }
                }
            }
            Ok(Some(WarmRecordRead(
                view.1,
                coords,
                found.into_iter().map(|h| h.map(|(r, _)| r)).collect(),
            )))
        }

        pub(crate) fn admit_read_records(
            &self,
            read: &WarmRecordRead,
            records: Vec<(usize, Arc<StoredRecord>, usize)>,
        ) {
            if !records.is_empty() && self.view.read().expect("native record warm view").1 == read.0
            {
                let items = records
                    .into_iter()
                    .map(|(at, r, b)| (read.1[at].clone(), r, b))
                    .collect();
                self.core
                    .lock()
                    .expect("native record warm set")
                    .admit_records(items);
            }
        }

        pub(crate) fn stage_written_records(&self, records: Vec<WrittenRecord>) {
            if let Some(pending) = PendingBatch::current(self) {
                let limit = core_warm_bytes() / 4;
                let mut staged = pending.warm_records.lock().expect("native warm records");
                for r in records {
                    let Some(b) = staged.1.checked_add(r.2).filter(|&b| b <= limit) else {
                        break;
                    };
                    staged.1 = b;
                    staged.0.push(r);
                }
            }
        }
    }

    pub(in crate::storage::disk_pack) fn committed_records(
        requests: &[Arc<PendingBatch>],
    ) -> Vec<(RecordCoordinate, Arc<StoredRecord>, usize)> {
        let mut latest: HashMap<RecordCoordinate, (Arc<StoredRecord>, usize)> = HashMap::new();
        for req in requests {
            let staged =
                std::mem::take(&mut *req.warm_records.lock().expect("native warm records")).0;
            if latest.is_empty() && staged.is_empty() {
                continue;
            }
            for coord in req.changed_coordinates() {
                latest.remove(&coord);
            }
            for WrittenRecord(coord, rec, bytes, dig) in staged {
                if req.lookup(&coord.0, &coord.1).is_some_and(|p| {
                    p.body
                        .as_ref()
                        .is_some_and(|b| b.len() == bytes && digest(b) == dig)
                }) {
                    latest.insert(coord, (rec, bytes));
                }
            }
        }
        latest.into_iter().map(|(c, (r, b))| (c, r, b)).collect()
    }

    fn digest(value: &[u8]) -> u64 {
        BuildHasherDefault::<DefaultHasher>::default().hash_one(value)
    }
}
