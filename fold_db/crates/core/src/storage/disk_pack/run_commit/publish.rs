//! Durable commit preparation, two-home CAS split commits, and read-view installation.

use super::PreparedCommit;
use crate::storage::disk_pack::physical::{V2Home, V2Root, CAS_COLLECTION};
use crate::storage::disk_pack::transaction::{self, batch_bytes, PendingBatch};
use crate::storage::disk_pack::{
    cas_delete, creation_refusal, map, record_warm, PackRuntime, StorageError, StorageResult,
};
use crate::storage::format::{NAMESPACES, REGISTRY};
use lastdb_model::storage_policy::native_write::PENDING_MEMORY_BYTES;
use laststore::disk_run::home::HomeMutation;
use laststore::space::reserve_space;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

type Coord = (String, String);

impl PackRuntime {
    pub(in crate::storage::disk_pack) fn commit(
        &self,
        ops: Vec<HomeMutation>,
    ) -> StorageResult<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let prepared = self.prepare_commit(ops, None)?;
        let changed: Vec<_> = prepared
            .mutations
            .iter()
            .map(|o| (o.collection.clone(), o.key.clone()))
            .collect();
        self.commit_prepared(prepared, &changed, &[])
    }

    pub(super) fn prepare_commit(
        &self,
        mutations: Vec<HomeMutation>,
        prev: Option<&PreparedCommit>,
    ) -> StorageResult<PreparedCommit> {
        let ns_only = mutations.iter().all(|o| {
            o.collection == REGISTRY
                && o.key.starts_with(NAMESPACES)
                && o.body.as_deref() == Some(&[1])
        });
        if self.fresh_initialization_pending() && !ns_only {
            return Err(creation_refusal(
                "creation holds durable proofs; source writes must wait",
            ));
        }
        if self.requires_recovery() {
            return Err(StorageError::backend(
                "writer requires reopen after failed publication",
            ));
        }
        let limit = self.batch_operation_limit();
        let incoming = batch_bytes(&mutations, limit)?;
        let pending_bytes = if let Some(prev) = prev {
            let ok = prev
                .mutations
                .len()
                .checked_add(mutations.len())
                .is_some_and(|n| n <= limit);
            let sum = prev
                .pending_bytes
                .checked_add(incoming)
                .and_then(|b| b.checked_sub(256));
            sum.filter(|&b| ok && b <= PENDING_MEMORY_BYTES)
                .ok_or_else(|| {
                    StorageError::write_limit("accepted prefix limit reached; retry after flush")
                })?
        } else {
            incoming
        };
        let cas_res = cas_delete::reserved_space(&mutations, prev.map(|p| p.mutations.as_slice()))?;
        let reserved = pending_bytes.saturating_add(cas_res);
        let space = match prev {
            Some(prev) => prev.space.reserve_extension(&self.space_policy, reserved),
            None => reserve_space(self.physical.home.path(), &self.space_policy, reserved),
        }
        .map_err(map)?;
        Ok(PreparedCommit {
            mutations,
            pending_bytes,
            space,
        })
    }

    pub(super) fn commit_prepared(
        &self,
        prepared: PreparedCommit,
        hidden: &[Coord],
        reqs: &[Arc<PendingBatch>],
    ) -> StorageResult<()> {
        let PreparedCommit {
            mutations, space, ..
        } = prepared;
        let (v2, before) = (&self.physical, self.physical.current_root().frontier());
        let notes = transaction::take_accepted_notes(reqs);
        self.prepare_accepted(before, &notes)?;
        let split = !notes.is_empty() && mutations.iter().any(|o| o.collection == CAS_COLLECTION);
        if split {
            self.arm_accepted_split()?;
        }
        let result = (|| {
            cas_delete::require_clear(v2)?;
            commit_split(v2, mutations)?;
            if split {
                self.note_accepted_split(before)?;
            }
            self.publish_accepted(before, v2.current_root().frontier())
        })();
        if let Err(error) = result {
            self.fail_accepted(before);
            self.failed.store(true, Ordering::Release);
            *self.failed_write.lock().expect("native failed write owner") = Some(space);
            return Err(error);
        }
        self.install_committed(v2.current_root(), hidden, reqs)?;
        drop(space);
        self.failed.store(false, Ordering::Release);
        self.spawn_merges();
        Ok(())
    }

    pub(super) fn install_committed(
        &self,
        cur: V2Root,
        hidden: &[Coord],
        reqs: &[Arc<PendingBatch>],
    ) -> StorageResult<()> {
        let records = record_warm::committed_records(reqs);
        let mut view = self.view.write().expect("native complete read view");
        if !hidden.is_empty() {
            view.revision = view
                .revision
                .checked_add(1)
                .ok_or_else(|| StorageError::backend("revision exhausted"))?;
        }
        if !hidden.is_empty() || !reqs.is_empty() || !records.is_empty() {
            let mut core = self.core.lock().expect("committed warm records");
            if reqs.is_empty() {
                core.invalidate_records(hidden);
            } else {
                for req in reqs {
                    core.invalidate_records(&req.changed_coordinates());
                }
            }
            if !records.is_empty() {
                core.admit_records(records);
            }
        }
        view.root = cur;
        drop(view);
        self.adopt_merged_root()
    }
}

fn commit_split(v2: &V2Home, ops: Vec<HomeMutation>) -> StorageResult<()> {
    let is_cas = |op: &HomeMutation| op.collection == CAS_COLLECTION;
    if !ops.iter().any(is_cas) {
        return v2.home.commit(ops).map(|_| ()).map_err(map);
    }
    let (cas, main): (Vec<_>, _) = ops.into_iter().partition(is_cas);
    let last: BTreeMap<_, _> = cas.into_iter().map(|op| (op.key.clone(), op)).collect();
    let (puts, deletes): (Vec<_>, _) = last.into_values().partition(|op| op.body.is_some());
    cas_delete::save(v2, !puts.is_empty(), !main.is_empty(), &deletes)?;
    if !puts.is_empty() {
        v2.cas.commit(puts).map_err(map)?;
    }
    if !main.is_empty() {
        v2.home.commit(main).map_err(map)?;
    }
    if !deletes.is_empty() {
        v2.cas.commit(deletes).map_err(map)?;
        v2.home
            .clear_checkpoint(cas_delete::CAS_DELETE_CHECKPOINT)
            .map_err(map)?;
    }
    Ok(())
}
