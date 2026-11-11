//! Owner capture of the main home (and, for a shared database, its CAS home).

use super::{
    capture::PublicationGate,
    invalid, protocol,
    run_source::RunSource,
    snapshot_cleanup::{enter_clean_cut, held_root, hold_complete, publish_hold},
    store::{private_directory, Store},
    ByteBackupCapture, Inner, PackRuntime, State, StorageResult, RUN_INTENT_FILE, STATE_FORMAT,
};
use cloud_backup_protocol::{bytes_sha256, v2::Begin, Digest, RecoveryDescriptor};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::os::unix::fs::DirBuilderExt;
use std::path::Path;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

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
    runtime.require_ordinary_backup_home()?;
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
        crate::storage::disk_pack::run_blocking(move || {
            saved_cut(&source, &requested, &key_sha256)
        })
        .await?;
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
        return Err(crate::storage::StorageError::PublicationFile);
    }
    let gate = (gate, edit);
    let runtime = Arc::clone(runtime);
    let key = zeroize::Zeroizing::new(*key);
    crate::storage::disk_pack::run_blocking(move || {
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
            super::capture::clear_previous(&store, &old)?;
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
            capture_pair(&runtime, &root, false)?;
        }
        drop(gate);
        let run = RunSource::open(Arc::clone(&runtime), root, &key)?;
        let state = new_state(&run, intent, recovery, &key)?;
        state.validate()?;
        store.write("job.json", &state)?;
        super::capture::finish(
            make_inner(runtime, store, state, Some(run), admission),
            &key,
            true,
        )
    })
    .await
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
    super::capture::retire_previous_format(runtime, &store, None)?;
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

pub(in crate::storage::disk_backup::byte_backup) fn require_shared_hold(
    root: &Path,
) -> StorageResult<()> {
    if !hold_complete(root)? {
        return Err(invalid("shared checkpoint hold is incomplete"));
    }
    Ok(())
}

pub(in crate::storage::disk_backup::byte_backup) fn capture_pair(
    runtime: &PackRuntime,
    root: &Path,
    with_cas: bool,
) -> StorageResult<()> {
    let homes = &runtime.physical;
    let holds = root.parent().ok_or_else(|| invalid("run capture parent"))?;
    private_directory(
        holds
            .parent()
            .ok_or_else(|| invalid("run capture parent"))?,
    )?;
    private_directory(holds)?;
    fs::DirBuilder::new().mode(0o700).create(root)?;
    homes
        .home
        .capture_files(&root.join("main"))
        .map_err(crate::storage::disk_pack::map)?;
    if with_cas {
        homes
            .cas
            .capture_files(&root.join("cas"))
            .map_err(crate::storage::disk_pack::map)?;
    }
    let node = homes
        .home
        .path()
        .parent()
        .ok_or_else(|| invalid("run node parent"))?
        .join(crate::storage::disk_pack::NODE_MARKER);
    let meta = fs::symlink_metadata(&node)?;
    if !meta.is_file()
        || meta.len() > 128
        || crate::storage::disk_pack::run_admission::profile(&fs::read(&node)?).is_none()
    {
        return Err(invalid("run source node marker"));
    }
    fs::hard_link(&node, root.join(super::HELD_NODE_FILE))?;
    publish_hold(root)?;
    File::open(root.parent().ok_or_else(|| invalid("run capture parent"))?)?.sync_all()?;
    Ok(())
}

fn total_bytes(run: &RunSource) -> StorageResult<u64> {
    run.files
        .iter()
        .try_fold(0u64, |n, f| n.checked_add(f.bytes))
        .ok_or_else(|| invalid("run capture size overflow"))
}

pub(in crate::storage::disk_backup::byte_backup) fn make_inner(
    runtime: Arc<PackRuntime>,
    store: Store,
    state: State,
    run: Option<RunSource>,
    admission: super::super::NativeCloudWorkGuard,
) -> Inner {
    Inner {
        runtime,
        store,
        state: Mutex::new(state),
        run,
        readers: AtomicUsize::new(0),
        _admission: admission,
    }
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
        make_inner(runtime, store, state, run, admission),
        key,
        admit,
    )
}

pub(in crate::storage::disk_backup::byte_backup) async fn resume(
    runtime: &Arc<PackRuntime>,
    key: &[u8; 32],
) -> StorageResult<ByteBackupCapture> {
    runtime.require_ordinary_backup_home()?;
    let admission = super::super::NativeCloudWorkGuard::acquire()?;
    let runtime = Arc::clone(runtime);
    let key = zeroize::Zeroizing::new(*key);
    crate::storage::disk_pack::run_blocking(move || {
        let store = Store::open(runtime.physical.path(), &runtime.space_policy)?;
        let state = store.read("job.json")?;
        reopen(runtime, store, state, &key, admission, false)
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
        key_sha256: bytes_sha256(key.as_ref()),
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
