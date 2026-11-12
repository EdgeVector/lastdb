//! Owner capture of the main home, clean-cut compaction, and hold markers.
use super::{
    invalid,
    planner::{clear_objects, finish, held_root, RunSource},
    protocol,
    store::{private, private_directory, retire_previous_format, sync_dir, Store},
    ByteBackupCapture, Inner, PackRuntime, PublicationGate, State, StorageResult, HELD_NODE_FILE,
    RUN_INTENT_FILE, STATE_FORMAT,
};
use crate::storage::disk_backup::NativeCloudWorkGuard;
use crate::storage::disk_pack::{
    map, run_blocking, run_node, transaction::PendingBatch, WriteGuard, NODE_MARKER,
};
use crate::storage::error::StorageError;
use cloud_backup_protocol::{bytes_sha256, v2::Begin, Digest, RecoveryDescriptor};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::Path;
use std::sync::{atomic::Ordering, Arc};
use std::time::{Duration, Instant};

const PASS_BUDGET: Duration = Duration::from_secs(15);
const MAX_ROUNDS: usize = 64;
const READY: &[u8] = b"ready-v1\n";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Intent {
    pub begin: Begin,
    pub key_sha256: Digest,
    pub reference_id: uuid::Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journal_anchor: Option<super::super::mutation_log::LogAnchor>,
}

impl PackRuntime {
    pub async fn prepare_byte_backup_capture(
        self: &Arc<Self>,
        begin: Begin,
        key: &[u8; 32],
        recovery: RecoveryDescriptor,
        renew: bool,
        publication_gate: Option<PublicationGate<'_>>,
    ) -> StorageResult<ByteBackupCapture> {
        let sealed = match publication_gate {
            None => Some(crate::storage::local_file_ref::materialize(self, key).await?),
            Some(_) => None,
        };
        let res = async {
            begin.validate().map_err(protocol)?;
            recovery
                .validate_for(&begin.job.stream.scope)
                .map_err(protocol)?;
            if !begin.descriptor.as_value().is_null() || PendingBatch::current(self).is_some() {
                return Err(invalid("run capture admission"));
            }
            self.flush_native().await?;
            let admission = NativeCloudWorkGuard::acquire()?;
            let (src, req, ksha) = (Arc::clone(self), begin.clone(), bytes_sha256(key));
            let (store, prev, saved_intent, held) =
                run_blocking(move || saved_cut(&src, &req, &ksha)).await?;
            let edit = publication_gate
                .filter(|_| !held)
                .map(|f| f())
                .transpose()?;
            let gate = if renew && !held && self.serving.load(Ordering::Acquire) {
                enter_clean_cut(self).await?
            } else {
                self.enter_write().await
            };
            if edit.is_some() && self.has_external_backup_files().await? {
                return Err(StorageError::PublicationFile);
            }
            let (rt, key) = (Arc::clone(self), zeroize::Zeroizing::new(*key));
            let (cut, guards) = ((store, prev, saved_intent), (admission, (gate, edit)));
            run_blocking(move || materialize_cut(rt, cut, begin, &key, recovery, renew, guards))
                .await
        }
        .await;
        if let Some(sealed) = &sealed {
            if let Err(error) = crate::storage::local_file_ref::release(self, sealed).await {
                tracing::error!(%error, "local file reference release after backup capture");
            }
        }
        res
    }

    pub async fn resume_byte_backup_capture(
        self: &Arc<Self>,
        key: &[u8; 32],
        release_local_files: bool,
    ) -> StorageResult<ByteBackupCapture> {
        let admission = NativeCloudWorkGuard::acquire()?;
        let (runtime, key) = (Arc::clone(self), zeroize::Zeroizing::new(*key));
        let capture = run_blocking(move || {
            let store = Store::open(runtime.physical.path(), &runtime.space_policy)?;
            let state = store.read("job.json")?;
            reopen(runtime, store, state, &key, admission, false)
        })
        .await?;
        if release_local_files {
            if let Err(error) = crate::storage::local_file_ref::release_catalog(self).await {
                tracing::error!(%error, "local file reference release after backup resume");
            }
        }
        Ok(capture)
    }
}

fn materialize_cut<G>(
    runtime: Arc<PackRuntime>,
    (store, previous, saved_intent): (Store, Option<State>, Option<Intent>),
    begin: Begin,
    key: &[u8; 32],
    recovery: RecoveryDescriptor,
    renew_unmaterialized: bool,
    (admission, gate): (NativeCloudWorkGuard, G),
) -> StorageResult<ByteBackupCapture> {
    require_healthy(&runtime)?;
    crate::storage::disk_pack::cas_delete::require_clear(&runtime.physical)?;
    if let Some(old) = previous {
        if old.begin == begin {
            drop(gate);
            return reopen(runtime, store, old, key, admission, true);
        }
        clear_objects(&store, old.objects)?;
    }
    let mut intent = if let Some(saved) = saved_intent {
        saved
    } else {
        let s = Intent {
            begin,
            key_sha256: bytes_sha256(key),
            reference_id: uuid::Uuid::new_v4(),
            journal_anchor: runtime.mutation_log.snapshot_anchor(&runtime.physical)?,
        };
        store.write(RUN_INTENT_FILE, &s)?;
        s
    };
    let root = held_root(&runtime, intent.reference_id);
    if renew_unmaterialized && root.try_exists()? && !hold_complete(&root)? {
        RunSource::release_saved(&root)?;
    }
    if !root.try_exists()? {
        renew_intent(&runtime, &store, &mut intent, renew_unmaterialized)?;
        capture_main(&runtime, &root)?;
    }
    drop(gate);
    let run = RunSource::open(Arc::clone(&runtime), root, key)?;
    let profile = run.profile()?;
    let state = State {
        format: STATE_FORMAT,
        begin: intent.begin,
        reference_id: intent.reference_id,
        native_root: profile.native_root()?,
        key_sha256: bytes_sha256(key),
        ready: false,
        objects: 0,
        verified: 0,
        native_files: run.files.len() as u64,
        native_bytes: run.total_bytes()?,
        metadata_pack_objects: 0,
        multi_file_pack_objects: 0,
        terminal: None,
        local_cancelled: false,
        released: false,
        recovery,
        run: profile,
        journal_anchor: intent.journal_anchor,
    };
    state.validate()?;
    store.write("job.json", &state)?;
    finish(
        Inner::new(runtime, store, state, Some(run), admission),
        key,
        true,
    )
}

fn require_healthy(runtime: &PackRuntime) -> StorageResult<()> {
    runtime.require_initialized()?;
    (!runtime.failed.load(Ordering::Acquire))
        .then_some(())
        .ok_or_else(|| invalid("run capture writer recovery"))
}

fn saved_cut(
    runtime: &PackRuntime,
    begin: &Begin,
    key_sha256: &Digest,
) -> StorageResult<(Store, Option<State>, Option<Intent>, bool)> {
    require_healthy(runtime)?;
    let store = Store::open(runtime.physical.path(), &runtime.space_policy)?;
    retire_previous_format(runtime, &store)?;
    let previous = store.read_opt::<State>("job.json")?;
    if let Some(old) = &previous {
        old.validate()?;
        if old.begin == *begin {
            return Ok((store, previous, None, true));
        }
        if !old.released || old.begin.job == begin.job {
            return Err(invalid("run saved source needs exact recovery"));
        }
    }
    let Some(saved) = store.read_opt::<Intent>(RUN_INTENT_FILE)? else {
        return Ok((store, previous, None, false));
    };
    if saved.begin != *begin || saved.key_sha256 != *key_sha256 || saved.reference_id.is_nil() {
        return Err(invalid("run capture intent differs"));
    }
    let held = hold_complete(&held_root(runtime, saved.reference_id))?;
    Ok((store, previous, Some(saved), held))
}

fn renew_intent(
    runtime: &PackRuntime,
    store: &Store,
    intent: &mut Intent,
    allowed: bool,
) -> StorageResult<()> {
    let Some(anchor) = &intent.journal_anchor else {
        return Ok(());
    };
    let journal = runtime.mutation_log.status(&runtime.physical)?;
    if runtime.physical.current_root().main.frontier() == anchor.frontiers.main
        && journal.anchor.as_ref() == Some(anchor)
        && journal.gap.is_none()
    {
        return Ok(());
    }
    if !allowed {
        return Err(invalid(
            "snapshot mutation log anchor changed before capture",
        ));
    }
    intent.journal_anchor = runtime.mutation_log.snapshot_anchor(&runtime.physical)?;
    store.write(RUN_INTENT_FILE, intent)
}

fn capture_main(runtime: &PackRuntime, root: &Path) -> StorageResult<()> {
    let holds = root.parent().ok_or_else(|| invalid("run capture parent"))?;
    private_directory(
        holds
            .parent()
            .ok_or_else(|| invalid("run capture parent"))?,
    )?;
    private_directory(holds)?;
    fs::DirBuilder::new().mode(0o700).create(root)?;
    runtime
        .physical
        .home
        .capture_files(&root.join("main"))
        .map_err(map)?;
    let node = runtime
        .physical
        .path()
        .parent()
        .ok_or_else(|| invalid("run node parent"))?
        .join(NODE_MARKER);
    let meta = fs::symlink_metadata(&node)?;
    if !meta.is_file() || meta.len() > 128 || run_node::profile(&fs::read(&node)?).is_none() {
        return Err(invalid("run source node marker"));
    }
    fs::hard_link(&node, root.join(HELD_NODE_FILE))?;
    sync_dir(root)?;
    let pending = root.join("pair-ready.pending");
    let mut marker = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&pending)?;
    marker.write_all(READY)?;
    marker.sync_all()?;
    fs::rename(pending, root.join("pair-ready"))?;
    sync_dir(root).and_then(|()| sync_dir(holds))
}

async fn enter_clean_cut(runtime: &Arc<PackRuntime>) -> StorageResult<WriteGuard> {
    let clean = |rt: &PackRuntime| {
        rt.physical.home.snapshot_has_clean_runs() && rt.physical.cas.snapshot_has_clean_runs()
    };
    let deadline = Instant::now() + PASS_BUDGET;
    for round in 0..=MAX_ROUNDS {
        if clean(runtime) {
            let gate = runtime.enter_write().await;
            if clean(runtime) {
                return Ok(gate);
            }
        }
        if round == MAX_ROUNDS || Instant::now() >= deadline {
            break;
        }
        let rt = Arc::clone(runtime);
        run_blocking(move || {
            let cas_pol = laststore::space::SpacePolicy::default();
            for (h, p) in [
                (&rt.physical.home, &rt.space_policy),
                (&rt.physical.cas, &cas_pol),
            ] {
                if !h.snapshot_has_clean_runs()
                    && h.snapshot_cleanup_step_with_space(p).map_err(map)?
                {
                    rt.adopt_merged_root()?;
                }
            }
            Ok(())
        })
        .await?;
        tokio::task::yield_now().await;
    }
    Err(StorageError::SnapshotCompactionPending)
}

fn hold_complete(root: &Path) -> StorageResult<bool> {
    let mut file = match fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(root.join("pair-ready"))
    {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e.into()),
    };
    let (meta, mut bytes) = (file.metadata()?, [0u8; 9]);
    (private(&meta).is_ok()
        && meta.len() == READY.len() as u64
        && file.read_exact(&mut bytes).is_ok()
        && bytes.as_slice() == READY)
        .then_some(true)
        .ok_or_else(|| invalid("run paired completion marker"))
}

fn reopen(
    runtime: Arc<PackRuntime>,
    store: Store,
    state: State,
    key: &[u8; 32],
    admission: NativeCloudWorkGuard,
    admit: bool,
) -> StorageResult<ByteBackupCapture> {
    state.validate()?;
    if state.key_sha256 != bytes_sha256(key)
        || state.native_root.home_id.as_str() != runtime.native_home_id().to_string()
    {
        return Err(invalid("run capture saved identity"));
    }
    let run = state
        .is_active()
        .then(|| {
            let src = RunSource::open(
                Arc::clone(&runtime),
                held_root(&runtime, state.reference_id),
                key,
            )?;
            (src.profile()? == state.run
                && src.files.len() as u64 == state.native_files
                && src.total_bytes()? == state.native_bytes)
                .then_some(src)
                .ok_or_else(|| invalid("run capture saved cut differs"))
        })
        .transpose()?;
    finish(
        Inner::new(runtime, store, state, run, admission),
        key,
        admit,
    )
}
