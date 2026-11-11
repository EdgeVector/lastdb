//! Capture entry points, terminal settlement, local cancel, and legacy-capture retirement.

use super::{
    invalid, object_name, protocol,
    run_capture::{self, held_root, Intent},
    run_source::RunSource,
    store::Store,
    ByteBackupCapture, Inner, PackRuntime, State, StorageResult, MAX_OBJECTS, RUN_INTENT_FILE,
    STATE_FORMAT,
};
use crate::storage::disk_backup::NativeCloudWorkGuard;
use crate::storage::disk_pack::run_blocking;
use cloud_backup_protocol::bytes_sha256;
use cloud_backup_protocol::v2::{
    jobs::{JobResult, JobStatus},
    Begin,
};
use serde::Deserialize;
use std::fs::File;
use std::sync::Arc;
use zeroize::Zeroizing;

pub(in crate::storage::disk_backup::byte_backup) type PublicationGate<'a> =
    &'a (dyn Fn() -> StorageResult<File> + Sync);

impl PackRuntime {
    pub async fn prepare_record_backup_capture(
        self: &Arc<Self>,
        begin: Begin,
        key: &[u8; 32],
        recovery: cloud_backup_protocol::RecoveryDescriptor,
        renew_unmaterialized: bool,
    ) -> StorageResult<ByteBackupCapture> {
        let sealed = crate::storage::local_file_ref::materialize(self, key).await?;
        let prepared =
            run_capture::prepare(self, begin, key, recovery, renew_unmaterialized, None).await;
        if let Err(error) = crate::storage::local_file_ref::release(self, &sealed).await {
            tracing::error!(%error, "local file reference release after backup capture");
        }
        prepared
    }

    pub async fn prepare_publication_backup_capture(
        self: &Arc<Self>,
        begin: Begin,
        key: &[u8; 32],
        recovery: cloud_backup_protocol::RecoveryDescriptor,
        renew_unmaterialized: bool,
        acquire_gate: impl Fn() -> StorageResult<File> + Sync,
    ) -> StorageResult<ByteBackupCapture> {
        run_capture::prepare(
            self,
            begin,
            key,
            recovery,
            renew_unmaterialized,
            Some(&acquire_gate),
        )
        .await
    }

    pub async fn resume_byte_backup_capture(
        self: &Arc<Self>,
        key: &[u8; 32],
        release_local_files: bool,
    ) -> StorageResult<ByteBackupCapture> {
        let capture = run_capture::resume(self, key).await?;
        if release_local_files {
            if let Err(error) = crate::storage::local_file_ref::release_catalog(self).await {
                tracing::error!(%error, "local file reference release after backup resume");
            }
        }
        Ok(capture)
    }

    pub async fn cancel_unsubmitted_byte_backup_capture(
        self: &Arc<Self>,
        begin: &Begin,
        key: &[u8; 32],
    ) -> StorageResult<()> {
        begin.validate().map_err(protocol)?;
        let admission = NativeCloudWorkGuard::acquire()?;
        let runtime = Arc::clone(self);
        let begin = begin.clone();
        let key = Zeroizing::new(*key);
        run_blocking(move || {
            let store = Store::open(runtime.physical.path(), &runtime.space_policy)?;
            if !store.exists("job.json")? {
                return cancel_intent(&runtime, &store, &begin, &key, None);
            }
            let mut state: State = store.read("job.json")?;
            state.validate()?;
            if state.begin != begin {
                if !state.released || state.begin.job == begin.job {
                    return Err(invalid("run cancel identity"));
                }
                return cancel_intent(&runtime, &store, &begin, &key, Some(state.reference_id));
            }
            if state.terminal.is_some() || state.key_sha256 != bytes_sha256(key.as_ref()) {
                return Err(invalid("run cancel identity"));
            }
            state.local_cancelled = true;
            store.write("job.json", &state)?;
            ByteBackupCapture {
                inner: Arc::new(Inner::new(runtime, store, state, None, admission)),
            }
            .finish_terminal()
        })
        .await
    }
}

fn cancel_intent(
    runtime: &PackRuntime,
    store: &Store,
    begin: &Begin,
    key: &[u8; 32],
    previous_reference: Option<uuid::Uuid>,
) -> StorageResult<()> {
    if !store.exists(RUN_INTENT_FILE)? {
        return Ok(());
    }
    let intent: Intent = store.read(RUN_INTENT_FILE)?;
    if intent.begin != *begin
        || intent.key_sha256 != bytes_sha256(key)
        || intent.reference_id.is_nil()
        || previous_reference == Some(intent.reference_id)
    {
        return Err(invalid("run cancel intent"));
    }
    RunSource::release_saved(&held_root(runtime, intent.reference_id))?;
    store.remove(RUN_INTENT_FILE)
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

pub(in crate::storage::disk_backup::byte_backup) fn clear_objects(
    store: &Store,
    last: u64,
) -> StorageResult<()> {
    for index in 0..=last {
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
                        != crate::storage::cloud_backup_plan::expected_receipt(
                            &self.inner.object(&state, state.objects - 1)?.input,
                        )
                        .map_err(protocol)?
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
            RunSource::release_saved(&held_root(&self.inner.runtime, state.reference_id))?;
            self.inner.store.remove(RUN_INTENT_FILE)?;
            let mut released = state.clone();
            released.released = true;
            self.inner.store.write("job.json", &released)?;
            *state = released;
        }
        clear_objects(&self.inner.store, state.objects)
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
}

pub(in crate::storage::disk_backup::byte_backup) fn retire_previous_format(
    runtime: &PackRuntime,
    store: &Store,
) -> StorageResult<()> {
    if !store.exists("job.json")? {
        return Ok(());
    }
    let job: Located = store.read("job.json")?;
    if job.format >= STATE_FORMAT {
        return Ok(());
    }
    let intent_ref = store
        .exists(RUN_INTENT_FILE)?
        .then(|| store.read::<LocatedIntent>(RUN_INTENT_FILE))
        .transpose()?
        .map(|intent| intent.reference_id);
    for reference in [Some(job.reference_id), intent_ref]
        .into_iter()
        .flatten()
        .filter(|id| !id.is_nil())
    {
        let root = held_root(runtime, reference);
        if root.try_exists()? {
            RunSource::release_saved(&root)?;
        }
    }
    clear_objects(store, job.objects.min(MAX_OBJECTS))?;
    store.remove(RUN_INTENT_FILE)?;
    store.remove("job.json")
}
