//! Capture entry points, terminal settlement, local cancel, and legacy-capture retirement.

use super::{
    hold::held_root, invalid, object_name, protocol, run_source::RunSource, store::Store,
    ByteBackupCapture, Inner, PackRuntime, State, StorageResult, MAX_OBJECTS, RUN_INTENT_FILE,
    STATE_FORMAT,
};
use cloud_backup_protocol::v2::{
    jobs::{JobResult, JobStatus},
    Begin,
};
use serde::Deserialize;
use std::fs::File;
use std::sync::Arc;

pub(in crate::storage::disk_backup::byte_backup) type PublicationGate<'a> =
    &'a (dyn Fn() -> StorageResult<File> + Sync);

impl PackRuntime {
    pub async fn prepare_record_backup_capture(
        self: &Arc<Self>,
        begin: Begin,
        key: &[u8; 32],
        recovery: cloud_backup_protocol::RecoveryDescriptor,
    ) -> StorageResult<ByteBackupCapture> {
        prepare_capturing_local_files(self, begin, key, recovery, false).await
    }

    pub async fn prepare_unsubmitted_record_backup_capture(
        self: &Arc<Self>,
        begin: Begin,
        key: &[u8; 32],
        recovery: cloud_backup_protocol::RecoveryDescriptor,
    ) -> StorageResult<ByteBackupCapture> {
        prepare_capturing_local_files(self, begin, key, recovery, true).await
    }

    pub async fn prepare_publication_backup_capture(
        self: &Arc<Self>,
        begin: Begin,
        key: &[u8; 32],
        recovery: cloud_backup_protocol::RecoveryDescriptor,
        renew_unmaterialized: bool,
        acquire_gate: impl Fn() -> StorageResult<File> + Sync,
    ) -> StorageResult<ByteBackupCapture> {
        super::run_capture::prepare(
            self,
            begin,
            key,
            recovery,
            renew_unmaterialized,
            Some(&acquire_gate),
        )
        .await
    }

    pub async fn resume_publication_backup_capture(
        self: &Arc<Self>,
        key: &[u8; 32],
    ) -> StorageResult<ByteBackupCapture> {
        super::run_capture::resume(self, key).await
    }

    pub async fn resume_byte_backup_capture(
        self: &Arc<Self>,
        key: &[u8; 32],
    ) -> StorageResult<ByteBackupCapture> {
        let capture = super::run_capture::resume(self, key).await?;
        if let Err(error) = crate::storage::local_file_ref::release_catalog(self).await {
            tracing::error!(%error, "local file reference release after backup resume");
        }
        Ok(capture)
    }

    pub async fn cancel_unsubmitted_byte_backup_capture(
        self: &Arc<Self>,
        begin: &Begin,
        key: &[u8; 32],
    ) -> StorageResult<()> {
        super::hold::cancel(self, begin, key).await
    }

    pub fn retire_previous_backup_capture(&self, dropped: Option<&str>) -> StorageResult<bool> {
        let _admission = super::super::NativeCloudWorkGuard::acquire()?;
        let store = Store::open(self.physical.path(), &self.space_policy)?;
        retire_previous_format(self, &store, dropped)
    }
}

async fn prepare_capturing_local_files(
    runtime: &Arc<PackRuntime>,
    begin: Begin,
    key: &[u8; 32],
    recovery: cloud_backup_protocol::RecoveryDescriptor,
    renew_unmaterialized: bool,
) -> StorageResult<ByteBackupCapture> {
    let sealed = crate::storage::local_file_ref::materialize(runtime, key).await?;
    let prepared =
        super::run_capture::prepare(runtime, begin, key, recovery, renew_unmaterialized, None)
            .await;
    if let Err(error) = crate::storage::local_file_ref::release(runtime, &sealed).await {
        tracing::error!(%error, "local file reference release after backup capture");
    }
    prepared
}

pub(in crate::storage::disk_backup::byte_backup) fn finish(
    inner: Inner,
    key: &[u8; 32],
    admit_export: bool,
) -> StorageResult<ByteBackupCapture> {
    let mut state = inner
        .state
        .lock()
        .map_err(|_| invalid("byte source state lock"))?;
    if inner.available() {
        let files = inner.files()?;
        let bytes = files
            .iter()
            .try_fold(0u64, |n, f| n.checked_add(f.bytes))
            .ok_or_else(|| invalid("byte source file size overflow"))?;
        if state.native_files != 0
            && (state.native_files != files.len() as u64 || state.native_bytes != bytes)
        {
            return Err(invalid("byte frozen source inventory changed"));
        }
        state.native_files = files.len() as u64;
        state.native_bytes = bytes;
        inner.store.write("job.json", &*state)?;
    }
    let active = state.terminal.is_none() && !state.local_cancelled && !state.released;
    if active && admit_export {
        inner.require_available()?;
    }
    if !state.ready && active && inner.available() {
        super::run_source::certify(&inner, key)?;
        super::planner::build(&inner, &mut state, key)?;
    }
    inner.advance(&mut state)?;
    drop(state);
    let capture = ByteBackupCapture {
        inner: Arc::new(inner),
    };
    capture.finish_terminal()?;
    Ok(capture)
}

pub(in crate::storage::disk_backup::byte_backup) fn clear_previous(
    store: &Store,
    state: &State,
) -> StorageResult<()> {
    for index in 0..=state.objects {
        for name in [object_name(index), format!("payload-{index:08}.enc")] {
            store.remove(&name)?;
            store.remove(&format!("{name}.pending"))?;
        }
    }
    Ok(())
}

impl ByteBackupCapture {
    pub fn terminal(&self, result: &JobResult) -> StorageResult<()> {
        let mut state = self.lock()?;
        self.inner.require_idle()?;
        if state.local_cancelled {
            return Err(invalid(
                "byte locally cancelled source has no cloud terminal",
            ));
        }
        result.validate_for_begin(&state.begin).map_err(protocol)?;
        if matches!(result.status, JobStatus::Active) {
            return Err(invalid("byte source active result is not terminal"));
        }
        if let Some(prior) = &state.terminal {
            if prior != result {
                return Err(invalid("byte source terminal result changed"));
            }
        } else {
            if let JobStatus::Published { backup } = &result.status {
                self.inner.advance(&mut state)?;
                if !state.ready
                    || state.verified != state.objects
                    || backup.root_receipt_id
                        != super::planner::expected_receipt(
                            &self.inner.object(&state, state.objects - 1)?.input,
                        )?
                        .receipt_id
                {
                    return Err(invalid("byte accepted result does not cover source"));
                }
            }
            let mut confirmed = state.clone();
            confirmed.terminal = Some(result.clone());
            self.inner.store.write("job.json", &confirmed)?;
            *state = confirmed;
        }
        drop(state);
        self.finish_terminal()
    }

    pub(in crate::storage::disk_backup::byte_backup) fn finish_terminal(
        &self,
    ) -> StorageResult<()> {
        let mut state = self.lock()?;
        if state.terminal.is_none() && !state.local_cancelled {
            return Ok(());
        }
        self.inner.require_idle()?;
        if !state.released {
            if let Some(anchor) = &state.journal_anchor {
                if state
                    .terminal
                    .as_ref()
                    .is_some_and(|result| matches!(result.status, JobStatus::Published { .. }))
                {
                    self.inner.runtime.mutation_log.snapshot_accepted(anchor)?;
                }
            }
            let root = held_root(&self.inner.runtime, state.reference_id);
            RunSource::release_saved(&root)?;
            self.inner.store.remove(RUN_INTENT_FILE)?;
            let mut released = state.clone();
            released.released = true;
            self.inner.store.write("job.json", &released)?;
            *state = released;
        }
        clear_previous(&self.inner.store, &state)
    }
}

#[derive(Deserialize)]
struct Located {
    format: u16,
    reference_id: uuid::Uuid,
    objects: u64,
}

#[derive(Deserialize)]
struct LocatedIntent {
    reference_id: uuid::Uuid,
    begin: LocatedBegin,
}

#[derive(Deserialize)]
struct LocatedBegin {
    job: LocatedJob,
}

#[derive(Deserialize)]
struct LocatedJob {
    commit_id: String,
}

pub(in crate::storage::disk_backup::byte_backup) fn retire_previous_format(
    runtime: &PackRuntime,
    store: &Store,
    dropped: Option<&str>,
) -> StorageResult<bool> {
    let job = if store.exists("job.json")? {
        let job: Located = store.read("job.json")?;
        if job.format >= STATE_FORMAT {
            return Ok(false);
        }
        Some(job)
    } else {
        None
    };
    let intent = if store.exists(RUN_INTENT_FILE)? {
        Some(store.read::<LocatedIntent>(RUN_INTENT_FILE)?)
    } else {
        None
    };
    let dropped_intent = intent
        .as_ref()
        .is_some_and(|intent| Some(intent.begin.job.commit_id.as_str()) == dropped);
    if job.is_none() && !dropped_intent {
        return Ok(false);
    }
    let references = job
        .as_ref()
        .map(|job| job.reference_id)
        .into_iter()
        .chain(intent.map(|intent| intent.reference_id))
        .filter(|id| !id.is_nil());
    for reference in references {
        let root = held_root(runtime, reference);
        if root.try_exists()? {
            RunSource::release_saved(&root)?;
        }
    }
    if let Some(job) = &job {
        let last = job.objects.min(MAX_OBJECTS);
        for index in 0..=last {
            for name in [object_name(index), format!("payload-{index:08}.enc")] {
                store.remove(&name)?;
                store.remove(&format!("{name}.pending"))?;
            }
        }
    }
    store.remove(RUN_INTENT_FILE)?;
    store.remove("job.json")?;
    Ok(true)
}
