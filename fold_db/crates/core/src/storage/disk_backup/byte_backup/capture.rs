//! Capture entry points, terminal settlement, local cancel, and legacy retirement.

use super::{
    invalid, object_name,
    planner::RunSource,
    protocol,
    run_capture::{self, held_root, Intent},
    store::Store,
    ByteBackupCapture, Inner, PackRuntime, State, StorageResult, MAX_OBJECTS, RUN_INTENT_FILE,
    STATE_FORMAT,
};
use crate::storage::cloud_backup_plan::expected_receipt;
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

pub(super) type PublicationGate<'a> = &'a (dyn Fn() -> StorageResult<File> + Sync);

impl PackRuntime {
    pub async fn prepare_byte_backup_capture(
        self: &Arc<Self>,
        begin: Begin,
        key: &[u8; 32],
        recovery: cloud_backup_protocol::RecoveryDescriptor,
        renew_unmaterialized: bool,
        publication_gate: Option<&(dyn Fn() -> StorageResult<File> + Sync)>,
    ) -> StorageResult<ByteBackupCapture> {
        if publication_gate.is_some() {
            return run_capture::prepare(
                self,
                begin,
                key,
                recovery,
                renew_unmaterialized,
                publication_gate,
            )
            .await;
        }
        let sealed = crate::storage::local_file_ref::materialize(self, key).await?;
        let prepared =
            run_capture::prepare(self, begin, key, recovery, renew_unmaterialized, None).await;
        if let Err(error) = crate::storage::local_file_ref::release(self, &sealed).await {
            tracing::error!(%error, "local file reference release after backup capture");
        }
        prepared
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
        let (runtime, begin, key) = (Arc::clone(self), begin.clone(), Zeroizing::new(*key));
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

fn release_ref(runtime: &PackRuntime, id: uuid::Uuid) -> StorageResult<()> {
    let root = held_root(runtime, id);
    if !id.is_nil() && root.try_exists()? {
        RunSource::release_saved(&root)?;
    }
    Ok(())
}

fn cancel_intent(
    runtime: &PackRuntime,
    store: &Store,
    begin: &Begin,
    key: &[u8; 32],
    prev_ref: Option<uuid::Uuid>,
) -> StorageResult<()> {
    if !store.exists(RUN_INTENT_FILE)? {
        return Ok(());
    }
    let intent: Intent = store.read(RUN_INTENT_FILE)?;
    if intent.begin != *begin
        || intent.key_sha256 != bytes_sha256(key)
        || intent.reference_id.is_nil()
        || prev_ref == Some(intent.reference_id)
    {
        return Err(invalid("run cancel intent"));
    }
    RunSource::release_saved(&held_root(runtime, intent.reference_id))?;
    store.remove(RUN_INTENT_FILE)
}

pub(super) fn finish(
    inner: Inner,
    key: &[u8; 32],
    admit_export: bool,
) -> StorageResult<ByteBackupCapture> {
    let mut state = inner
        .state
        .lock()
        .map_err(|_| invalid("byte source state lock"))?;
    if let Some(run) = &inner.run {
        let (files, bytes) = (run.files.len() as u64, run.total_bytes()?);
        if state.native_files != 0 && (state.native_files, state.native_bytes) != (files, bytes) {
            return Err(invalid("byte frozen source inventory changed"));
        }
        (state.native_files, state.native_bytes) = (files, bytes);
    }
    if state.is_active() && admit_export && inner.run.is_none() {
        return Err(invalid("byte retained source unavailable"));
    }
    if !state.ready && state.is_active() && inner.run.is_some() {
        super::planner::build(&inner, &mut state, key)?;
    }
    inner.advance(&mut state)?;
    drop(state);
    let capture = ByteBackupCapture {
        inner: Arc::new(inner),
    };
    capture.finish_terminal().map(|()| capture)
}

pub(super) fn clear_objects(store: &Store, last: u64) -> StorageResult<()> {
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
                        != expected_receipt(&self.inner.object(&state, state.objects - 1)?.input)
                            .map_err(protocol)?
                            .receipt_id
                {
                    return Err(invalid("byte accepted result does not cover source"));
                }
            }
            state.terminal = Some(result.clone());
            if let Err(e) = self.inner.store.write("job.json", &*state) {
                state.terminal = None;
                return Err(e);
            }
        }
        drop(state);
        self.finish_terminal()
    }

    fn finish_terminal(&self) -> StorageResult<()> {
        let mut state = self.lock()?;
        if state.terminal.is_none() && !state.local_cancelled {
            return Ok(());
        }
        self.inner.require_idle()?;
        if !state.released {
            if let (
                Some(anchor),
                Some(JobResult {
                    status: JobStatus::Published { .. },
                    ..
                }),
            ) = (&state.journal_anchor, &state.terminal)
            {
                self.inner.runtime.mutation_log.snapshot_accepted(anchor)?;
            }
            RunSource::release_saved(&held_root(&self.inner.runtime, state.reference_id))?;
            self.inner.store.remove(RUN_INTENT_FILE)?;
            state.released = true;
            if let Err(e) = self.inner.store.write("job.json", &*state) {
                state.released = false;
                return Err(e);
            }
        }
        clear_objects(&self.inner.store, state.objects)
    }
}

pub(super) fn retire_previous_format(runtime: &PackRuntime, store: &Store) -> StorageResult<()> {
    #[derive(Deserialize)]
    struct Ref {
        #[serde(default)]
        format: u16,
        reference_id: uuid::Uuid,
        #[serde(default)]
        objects: u64,
    }
    let read_opt = |name: &str| {
        store
            .exists(name)?
            .then(|| store.read::<Ref>(name))
            .transpose()
    };
    let Some(job) = read_opt("job.json")? else {
        return Ok(());
    };
    if job.format >= STATE_FORMAT {
        return Ok(());
    }
    release_ref(runtime, job.reference_id)?;
    if let Some(intent) = read_opt(RUN_INTENT_FILE)? {
        release_ref(runtime, intent.reference_id)?;
    }
    clear_objects(store, job.objects.min(MAX_OBJECTS))?;
    store.remove(RUN_INTENT_FILE)?;
    store.remove("job.json")
}
