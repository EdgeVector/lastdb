//! Owner capture of the main home, clean-cut compaction, and hold markers.

use super::{
    capture::PublicationGate,
    invalid, protocol,
    run_source::RunSource,
    store::{private_directory, Store},
    ByteBackupCapture, Inner, PackRuntime, State, StorageResult, HOLDS_DIR, RUN_INTENT_FILE,
    STATE_FORMAT,
};
use crate::storage::disk_pack::{map, run_blocking, WriteGuard};
use crate::storage::error::StorageError;
use cloud_backup_protocol::{bytes_sha256, v2::Begin, Digest, RecoveryDescriptor};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::{atomic::Ordering, Arc};
use std::time::{Duration, Instant};

const PASS_BUDGET: Duration = Duration::from_secs(15);
const MAX_ROUNDS: usize = 64;
const READY: &[u8] = b"ready-v1\n";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::storage::disk_backup::byte_backup) struct Intent {
    pub(in crate::storage::disk_backup::byte_backup) begin: Begin,
    pub(in crate::storage::disk_backup::byte_backup) key_sha256: Digest,
    pub(in crate::storage::disk_backup::byte_backup) reference_id: uuid::Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::storage::disk_backup::byte_backup) journal_anchor:
        Option<super::super::mutation_log::LogAnchor>,
}

pub(in crate::storage::disk_backup::byte_backup) async fn prepare(
    runtime: &Arc<PackRuntime>,
    begin: Begin,
    key: &[u8; 32],
    recovery: RecoveryDescriptor,
    renew_unmaterialized: bool,
    publication_gate: Option<PublicationGate<'_>>,
) -> StorageResult<ByteBackupCapture> {
    begin.validate().map_err(protocol)?;
    recovery
        .validate_for(&begin.job.stream.scope)
        .map_err(protocol)?;
    if !begin.descriptor.as_value().is_null()
        || crate::storage::disk_pack::transaction::PendingBatch::current(runtime).is_some()
    {
        return Err(invalid("run capture admission"));
    }
    runtime.flush_native().await?;
    let admission = super::super::NativeCloudWorkGuard::acquire()?;
    let source = Arc::clone(runtime);
    let requested = begin.clone();
    let key_sha256 = bytes_sha256(key);
    let (store, previous, saved_intent, held) =
        run_blocking(move || saved_cut(&source, &requested, &key_sha256)).await?;
    let edit = publication_gate
        .filter(|_| !held)
        .map(|acquire| acquire())
        .transpose()?;
    let gate = if renew_unmaterialized && !held && runtime.serving.load(Ordering::Acquire) {
        enter_clean_cut(runtime).await?
    } else {
        runtime.enter_write().await
    };
    if edit.is_some() && runtime.has_external_backup_files().await? {
        return Err(StorageError::PublicationFile);
    }
    let gate = (gate, edit);
    let runtime = Arc::clone(runtime);
    let key = zeroize::Zeroizing::new(*key);
    run_blocking(move || {
        runtime.require_initialized()?;
        crate::storage::disk_pack::cas_delete::require_clear(&runtime.physical)?;
        if runtime.failed.load(Ordering::Acquire) {
            return Err(invalid("run capture writer recovery"));
        }
        if let Some(old) = previous {
            if old.begin == begin {
                drop(gate);
                return reopen(runtime, store, old, &key, admission, true);
            }
            super::capture::clear_objects(&store, old.objects)?;
        }
        let mut intent = if let Some(saved) = saved_intent {
            saved
        } else {
            let intent = Intent {
                begin: begin.clone(),
                key_sha256: bytes_sha256(key.as_ref()),
                reference_id: uuid::Uuid::new_v4(),
                journal_anchor: runtime.mutation_log.snapshot_anchor(&runtime.physical)?,
            };
            store.write(RUN_INTENT_FILE, &intent)?;
            intent
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
        let run = RunSource::open(Arc::clone(&runtime), root, &key)?;
        let state = new_state(&run, intent, recovery, &key)?;
        state.validate()?;
        store.write("job.json", &state)?;
        super::capture::finish(
            Inner::new(runtime, store, state, Some(run), admission),
            &key,
            true,
        )
    })
    .await
}

fn new_state(
    run: &RunSource,
    intent: Intent,
    recovery: RecoveryDescriptor,
    key: &[u8; 32],
) -> StorageResult<State> {
    let profile = run.profile()?;
    Ok(State {
        format: STATE_FORMAT,
        begin: intent.begin,
        reference_id: intent.reference_id,
        native_root: profile.native_root()?,
        key_sha256: bytes_sha256(key),
        ready: false,
        objects: 0,
        verified: 0,
        native_files: run.files.len() as u64,
        native_bytes: total_bytes(run)?,
        metadata_pack_objects: 0,
        multi_file_pack_objects: 0,
        terminal: None,
        local_cancelled: false,
        released: false,
        recovery,
        run: profile,
        journal_anchor: intent.journal_anchor,
    })
}

fn saved_cut(
    runtime: &PackRuntime,
    begin: &Begin,
    key_sha256: &Digest,
) -> StorageResult<(Store, Option<State>, Option<Intent>, bool)> {
    runtime.require_initialized()?;
    if runtime.failed.load(Ordering::Acquire) {
        return Err(invalid("run capture writer recovery"));
    }
    let store = Store::open(runtime.physical.path(), &runtime.space_policy)?;
    super::capture::retire_previous_format(runtime, &store)?;
    let previous = if store.exists("job.json")? {
        let old: State = store.read("job.json")?;
        old.validate()?;
        if old.begin == *begin {
            return Ok((store, Some(old), None, true));
        }
        if !old.released || old.begin.job == begin.job {
            return Err(invalid("run saved source needs exact recovery"));
        }
        Some(old)
    } else {
        None
    };
    if !store.exists(RUN_INTENT_FILE)? {
        return Ok((store, previous, None, false));
    }
    let saved: Intent = store.read(RUN_INTENT_FILE)?;
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
    let actual = runtime.physical.current_root();
    let journal = runtime.mutation_log.status(&runtime.physical)?;
    if actual.main.frontier() == anchor.frontiers.main
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
    let homes = &runtime.physical;
    let holds = root.parent().ok_or_else(|| invalid("run capture parent"))?;
    let base = holds
        .parent()
        .ok_or_else(|| invalid("run capture parent"))?;
    private_directory(base)?;
    private_directory(holds)?;
    fs::DirBuilder::new().mode(0o700).create(root)?;
    homes.home.capture_files(&root.join("main")).map_err(map)?;
    let node = homes.path().join(crate::storage::disk_pack::NODE_MARKER);
    let meta = fs::symlink_metadata(&node)?;
    if !meta.is_file()
        || meta.len() > 128
        || crate::storage::disk_pack::run_node::profile(&fs::read(&node)?).is_none()
    {
        return Err(invalid("run source node marker"));
    }
    fs::hard_link(&node, root.join(super::HELD_NODE_FILE))?;
    File::open(root)?.sync_all()?;
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
    File::open(root)?.sync_all()?;
    File::open(holds)?.sync_all()?;
    Ok(())
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
            let cas_policy = laststore::space::SpacePolicy::default();
            for (home, policy) in [
                (&rt.physical.home, &rt.space_policy),
                (&rt.physical.cas, &cas_policy),
            ] {
                if !home.snapshot_has_clean_runs()
                    && home.snapshot_cleanup_step_with_space(policy).map_err(map)?
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

pub(in crate::storage::disk_backup::byte_backup) fn held_root(
    runtime: &PackRuntime,
    id: uuid::Uuid,
) -> PathBuf {
    runtime
        .physical
        .path()
        .join(laststore::disk_run::home::BACKUP_DIR)
        .join(HOLDS_DIR)
        .join(id.to_string())
}

fn hold_complete(root: &Path) -> StorageResult<bool> {
    let mut file = match fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(root.join("pair-ready"))
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    let meta = file.metadata()?;
    let mut bytes = [0u8; 9];
    if !meta.is_file()
        || meta.uid() != unsafe { libc::geteuid() }
        || meta.mode() & 0o077 != 0
        || meta.nlink() != 1
        || meta.len() != READY.len() as u64
        || file.read_exact(&mut bytes).is_err()
        || bytes.as_slice() != READY
    {
        return Err(invalid("run paired completion marker"));
    }
    Ok(true)
}

fn total_bytes(run: &RunSource) -> StorageResult<u64> {
    run.files
        .iter()
        .try_fold(0u64, |n, f| n.checked_add(f.bytes))
        .ok_or_else(|| invalid("run capture size overflow"))
}

fn reopen(
    runtime: Arc<PackRuntime>,
    store: Store,
    state: State,
    key: &[u8; 32],
    admission: super::super::NativeCloudWorkGuard,
    admit: bool,
) -> StorageResult<ByteBackupCapture> {
    state.validate()?;
    if state.key_sha256 != bytes_sha256(key)
        || state.native_root.home_id.as_str() != runtime.native_home_id().to_string()
    {
        return Err(invalid("run capture saved identity"));
    }
    let run = if state.released || state.terminal.is_some() || state.local_cancelled {
        None
    } else {
        let source = RunSource::open(
            Arc::clone(&runtime),
            held_root(&runtime, state.reference_id),
            key,
        )?;
        if source.profile()? != state.run
            || source.files.len() as u64 != state.native_files
            || total_bytes(&source)? != state.native_bytes
        {
            return Err(invalid("run capture saved cut differs"));
        }
        Some(source)
    };
    super::capture::finish(
        Inner::new(runtime, store, state, run, admission),
        key,
        admit,
    )
}

pub(in crate::storage::disk_backup::byte_backup) async fn resume(
    runtime: &Arc<PackRuntime>,
    key: &[u8; 32],
) -> StorageResult<ByteBackupCapture> {
    let admission = super::super::NativeCloudWorkGuard::acquire()?;
    let runtime = Arc::clone(runtime);
    let key = zeroize::Zeroizing::new(*key);
    run_blocking(move || {
        let store = Store::open(runtime.physical.path(), &runtime.space_policy)?;
        let state = store.read("job.json")?;
        reopen(runtime, store, state, &key, admission, false)
    })
    .await
}
