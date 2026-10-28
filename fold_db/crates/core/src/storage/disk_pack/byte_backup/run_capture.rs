//! Paired owner capture. No second home owner or storage-format conversion.
use super::{
    invalid, model::State, protocol, run_source::RunSource, store::Store, ByteBackupCapture, Inner,
    PackRuntime, StorageResult,
};
use cloud_backup_protocol::{bytes_sha256, v2::Begin, Digest, RecoveryDescriptor};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::{atomic::AtomicUsize, Arc, Mutex};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Intent {
    begin: Begin,
    key_sha256: Digest,
    reference_id: uuid::Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    journal_anchor: Option<super::super::mutation_log::LogAnchor>,
}
pub(super) fn held_root(runtime: &PackRuntime, id: uuid::Uuid) -> PathBuf {
    runtime
        .physical
        .path()
        .join("cloud-byte-holds-v2")
        .join(id.to_string())
}
fn directory(path: &Path) -> StorageResult<()> {
    match fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => {
            File::open(path.parent().ok_or_else(|| invalid("run hold parent"))?)?.sync_all()?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.into()),
    }
    let meta = fs::symlink_metadata(path)?;
    if !meta.is_dir() || meta.uid() != unsafe { libc::geteuid() } || meta.mode() & 0o077 != 0 {
        return Err(invalid("run capture private directory"));
    }
    Ok(())
}
pub(super) async fn prepare(
    runtime: &Arc<PackRuntime>,
    begin: Begin,
    key: &[u8; 32],
    recovery: RecoveryDescriptor,
    renew_unmaterialized: bool,
) -> StorageResult<ByteBackupCapture> {
    begin.validate().map_err(protocol)?;
    recovery
        .validate_for(&begin.job.stream.scope)
        .map_err(protocol)?;
    if !begin.descriptor.as_value().is_null()
        || super::super::transaction::PendingBatch::current(runtime).is_some()
    {
        return Err(invalid("run capture admission"));
    }
    runtime.flush_native().await?;
    let admission = super::super::NativeCloudWorkGuard::acquire()?;
    let gate = runtime.enter_write().await;
    let runtime = Arc::clone(runtime);
    let key = zeroize::Zeroizing::new(*key);
    crate::storage::disk_pack::run_blocking(move || {
        runtime.require_initialized()?;
        runtime.require_restore_validated()?;
        if runtime.failed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(invalid("run capture writer recovery"));
        }
        let store = Store::open(runtime.physical.path(), &runtime.space_policy)?;
        if store.exists("job.json")? {
            let old: State = store.read("job.json")?;
            old.validate()?;
            if old.begin == begin {
                drop(gate);
                return reopen(runtime, store, old, &key, admission, true);
            }
            if !old.released || old.begin.job == begin.job {
                return Err(invalid("run saved source needs exact recovery"));
            }
            super::capture::clear_previous(&store, &old)?;
        }
        let mut intent = if store.exists("run-intent.json")? {
            let saved: Intent = store.read("run-intent.json")?;
            if saved.begin != begin
                || saved.key_sha256 != bytes_sha256(key.as_ref())
                || saved.reference_id.is_nil()
            {
                return Err(invalid("run capture intent differs"));
            }
            saved
        } else {
            let intent = Intent {
                begin: begin.clone(),
                key_sha256: bytes_sha256(key.as_ref()),
                reference_id: uuid::Uuid::new_v4(),
                journal_anchor: runtime.mutation_log.snapshot_anchor(&runtime.physical)?,
            };
            store.write("run-intent.json", &intent)?;
            intent
        };
        let root = held_root(&runtime, intent.reference_id);
        if !root.try_exists()? {
            renew_intent(&runtime, &store, &mut intent, renew_unmaterialized)?;
            capture_pair(&runtime, &root)?;
        }
        drop(gate);
        let run = RunSource::open(Arc::clone(&runtime), root, &key)?;
        let state = new_state(&run, intent, recovery, &key)?;
        state.validate()?;
        store.write("job.json", &state)?;
        super::capture::finish(
            inner(runtime, store, state, Some(run), admission),
            &key,
            true,
        )
    })
    .await
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
    if actual.main.frontier() == anchor.frontiers.main
        && actual.cas.frontier() == anchor.frontiers.cas
    {
        return Ok(());
    }
    if !allowed {
        return Err(invalid(
            "snapshot mutation log anchor changed before capture",
        ));
    }
    // The caller still owns the same write gate as capture_pair. No source State,
    // held directory, transfer input, or remote begin exists for this request.
    // Old journal epochs remain retained until the replacement snapshot is accepted.
    intent.journal_anchor = runtime.mutation_log.snapshot_anchor(&runtime.physical)?;
    store.write("run-intent.json", intent)
}
fn capture_pair(runtime: &PackRuntime, root: &Path) -> StorageResult<()> {
    let homes = &runtime.physical;
    directory(root.parent().ok_or_else(|| invalid("run capture parent"))?)?;
    fs::DirBuilder::new().mode(0o700).create(root)?;
    homes
        .home
        .capture_files(&root.join("main"))
        .map_err(super::super::map)?;
    homes
        .cas
        .capture_files(&root.join("cas"))
        .map_err(super::super::map)?;
    let node = homes
        .home
        .path()
        .parent()
        .ok_or_else(|| invalid("run node parent"))?
        .join("laststore-pack-node-v1");
    let meta = fs::symlink_metadata(&node)?;
    let expected = match homes.range_order {
        schema_types::key_comparison::KeyOrderVersion::LegacyUtf8V1 => {
            super::super::run_node::RUN_NODE_BYTES
        }
        schema_types::key_comparison::KeyOrderVersion::TypedScalarV1 => {
            super::super::run_admission::TYPED_NODE_BYTES
        }
    };
    if !meta.is_file() || meta.len() != expected.len() as u64 || fs::read(&node)? != expected {
        return Err(invalid("run source node marker"));
    }
    fs::hard_link(&node, root.join("node"))?;
    File::open(root)?.sync_all()?;
    File::open(root.parent().ok_or_else(|| invalid("run capture parent"))?)?.sync_all()?;
    Ok(())
}
fn total_bytes(run: &RunSource) -> StorageResult<u64> {
    run.files
        .iter()
        .try_fold(0u64, |n, f| n.checked_add(f.bytes))
        .ok_or_else(|| invalid("run capture size overflow"))
}
fn inner(
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
        if Some(source.profile()?) != state.run
            || source.files.len() as u64 != state.native_files
            || total_bytes(&source)? != state.native_bytes
        {
            return Err(invalid("run capture saved cut differs"));
        }
        Some(source)
    };
    super::capture::finish(inner(runtime, store, state, run, admission), key, admit)
}
pub(super) async fn resume(
    runtime: &Arc<PackRuntime>,
    key: &[u8; 32],
) -> StorageResult<ByteBackupCapture> {
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
pub(super) async fn cancel(
    runtime: &Arc<PackRuntime>,
    begin: &Begin,
    key: &[u8; 32],
) -> StorageResult<()> {
    begin.validate().map_err(protocol)?;
    let admission = super::super::NativeCloudWorkGuard::acquire()?;
    let runtime = Arc::clone(runtime);
    let begin = begin.clone();
    let key = zeroize::Zeroizing::new(*key);
    crate::storage::disk_pack::run_blocking(move || {
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
            inner: Arc::new(inner(runtime, store, state, None, admission)),
        }
        .finish_terminal()
    })
    .await
}

fn cancel_intent(
    runtime: &PackRuntime,
    store: &Store,
    begin: &Begin,
    key: &[u8; 32],
    previous_reference: Option<uuid::Uuid>,
) -> StorageResult<()> {
    if !store.exists("run-intent.json")? {
        return Ok(());
    }
    let intent: Intent = store.read("run-intent.json")?;
    if intent.begin != *begin
        || intent.key_sha256 != bytes_sha256(key)
        || intent.reference_id.is_nil()
        || previous_reference == Some(intent.reference_id)
    {
        return Err(invalid("run cancel intent"));
    }
    RunSource::release_saved(&held_root(runtime, intent.reference_id))?;
    store.remove("run-intent.json")
}

fn new_state(
    run: &RunSource,
    intent: Intent,
    recovery: RecoveryDescriptor,
    key: &[u8; 32],
) -> StorageResult<State> {
    let profile = run.profile()?;
    Ok(State {
        format: 2,
        begin: intent.begin,
        reference_id: intent.reference_id,
        current_bytes: Vec::new(),
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
        recovery: Some(recovery),
        run: Some(profile),
        journal_anchor: intent.journal_anchor,
    })
}
