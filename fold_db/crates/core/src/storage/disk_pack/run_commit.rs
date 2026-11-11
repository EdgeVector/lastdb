//! Commit and equivalent-root adoption on a v2 run home.

use super::physical::{PhysicalRoot, V2Home, V2Root, CAS_COLLECTION};
use super::run_merge::start_merges;
use super::*;
use laststore::disk_run::home::RunHome;

impl PackRuntime {
    pub(in crate::storage::disk_pack) fn prepare_run(
        &self,
        v2: &V2Home,
        operations: Vec<HomeMutation>,
        previous: Option<&write::PreparedCommit>,
    ) -> StorageResult<write::PreparedCommit> {
        RunHome::check_mutations(&operations).map_err(map)?;
        let limit = self.batch_operation_limit();
        let batch_bytes = write::prefix_batch_bytes(previous, &operations, limit)?;
        let intent_keys = cas_delete::key_bytes(&operations)?
            + previous
                .map(write::PreparedCommit::cas_delete_key_bytes)
                .transpose()?
                .unwrap_or(0);
        let intent_bytes = cas_delete::space(intent_keys)?;
        let reserved_bytes = if let Some(shared) = &v2.shared {
            let current = shared.scope.read().expect("shared disk scope");
            shared_layout::Scope::with_writes(
                &current,
                previous
                    .into_iter()
                    .flat_map(write::PreparedCommit::operations)
                    .chain(&operations),
            )?;
            batch_bytes.saturating_mul(3)
        } else {
            batch_bytes.saturating_add(intent_bytes)
        };
        let space = match previous {
            Some(previous) => previous
                .space()
                .reserve_extension(&self.space_policy, reserved_bytes),
            None => {
                laststore::space::reserve_space(v2.home.path(), &self.space_policy, reserved_bytes)
            }
        }
        .map_err(map)?;
        Ok(write::PreparedCommit::for_run(
            operations,
            batch_bytes,
            space,
        ))
    }

    pub(in crate::storage::disk_pack) fn commit_run(
        &self,
        v2: &V2Home,
        operations: Vec<HomeMutation>,
        space: SpaceReservation,
        hidden_changes: &[(String, String)],
        requests: &[Arc<transaction::PendingBatch>],
    ) -> StorageResult<()> {
        let result = (|| {
            cas_delete::require_clear(v2)?;
            if let Some(shared) = &v2.shared {
                shared_layout::journal::write(v2, shared, operations)
            } else {
                let mut journal = operations
                    .iter()
                    .any(|op| op.collection != CAS_COLLECTION)
                    .then(|| self.mutation_log.begin(v2))
                    .flatten();
                commit_split(v2, operations, journal.as_mut())?;
                if let Some(journal) = journal {
                    journal.finish(v2);
                }
                Ok(())
            }
        })();
        if let Err(error) = result {
            self.failed.store(true, Ordering::Release);
            *self.failed_write.lock().expect("native failed write owner") = Some(space);
            return Err(error);
        }
        let root = v2.current_root();
        let (generation, frontier) = (root.generation(), root.frontier());
        self.install_committed(root, hidden_changes, requests)?;
        self.last_root.observe(generation, frontier);
        drop(space);
        self.failed.store(false, Ordering::Release);
        start_merges(
            v2,
            Arc::clone(&self.view),
            Arc::clone(&self.last_root),
            &self.space_policy,
        );
        Ok(())
    }

    pub(in crate::storage::disk_pack) fn resume_merges(&self) {
        let v2 = &self.physical;
        if v2.home.needs_merge()
            || v2.cas.needs_merge()
            || v2
                .shared
                .as_ref()
                .is_some_and(|shared| shared.private.needs_merge())
        {
            start_merges(
                v2,
                Arc::clone(&self.view),
                Arc::clone(&self.last_root),
                &self.space_policy,
            );
        }
    }

    pub(in crate::storage) fn adopt_merged_root(&self) -> StorageResult<()> {
        let mut view = match self.view.try_write() {
            Ok(view) => view,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(()),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                self.failed.store(true, Ordering::Release);
                tracing::error!(
                    "native read view lock is poisoned; restart the process to reopen the home"
                );
                return Err(StorageError::BackendError(
                    "native read view lock is poisoned; restart the process to reopen the home"
                        .into(),
                ));
            }
        };
        let latest = self.physical.current_root();
        let replaced = adopt(&mut view, latest, &self.last_root);
        drop(view);
        drop(replaced);
        Ok(())
    }
}

pub(in crate::storage::disk_pack) fn adopt(
    view: &mut view::NativeReadView,
    latest: V2Root,
    last_root: &view::LastRoot,
) -> Option<PhysicalRoot> {
    let held = &view.current;
    if held.frontier() != latest.frontier() || held.generation() >= latest.generation() {
        return None;
    }
    last_root.observe(latest.generation(), latest.frontier());
    Some(std::mem::replace(&mut view.current, latest))
}

fn commit_split(
    v2: &V2Home,
    operations: Vec<HomeMutation>,
    mut journal: Option<&mut mutation_log::Recording<'_>>,
) -> StorageResult<()> {
    if !operations.iter().any(|op| op.collection == CAS_COLLECTION) {
        let committed = v2.home.commit(operations).map_err(map)?;
        if let Some(log) = journal.as_mut() {
            log.retain(mutation_log::CommitRole::Main, &committed);
        }
        return Ok(());
    }
    let (cas, main): (Vec<_>, Vec<_>) = operations
        .into_iter()
        .partition(|op| op.collection == CAS_COLLECTION);
    let mut last = std::collections::BTreeMap::new();
    for op in cas {
        last.insert(op.key.clone(), op);
    }
    blob_backup::queue_new_uploads(&mut last);
    let (puts, deletes): (Vec<_>, Vec<_>) = last.into_values().partition(|op| op.body.is_some());
    cas_delete::save(v2, !puts.is_empty(), !main.is_empty(), &deletes)?;
    if !puts.is_empty() {
        v2.cas.commit(puts).map_err(map)?;
    }
    if !main.is_empty() {
        let committed = v2.home.commit(main).map_err(map)?;
        if let Some(log) = journal.as_mut() {
            log.retain(mutation_log::CommitRole::Main, &committed);
        }
    }
    if !deletes.is_empty() {
        v2.cas
            .commit(blob_backup::with_releases(deletes))
            .map_err(map)?;
        v2.home.clear_cas_delete_checkpoint().map_err(map)?;
    }
    Ok(())
}
