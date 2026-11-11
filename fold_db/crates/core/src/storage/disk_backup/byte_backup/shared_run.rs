//! Durable holds on shared roots, clean-cut compaction, hold markers, and cancel.

use super::{
    invalid, protocol,
    run_capture::{self, make_inner, Intent},
    run_source::RunSource,
    store::Store,
    ByteBackupCapture, PackRuntime, RunBackupProfile, State, HOLDS_DIR, RUN_INTENT_FILE,
};
use crate::storage::disk_backup::NativeCloudWorkGuard;
use crate::storage::disk_pack::{map, run_blocking, WriteGuard};
use crate::storage::error::{StorageError, StorageResult};
use cloud_backup_protocol::{bytes_sha256, v2::Begin};
use laststore::disk_run::home::{CapturedRunHome, SHARED_BACKUP_DIR};
use std::fs::{self, File};
use std::future::Future;
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use zeroize::Zeroizing;

const PASS_BUDGET: Duration = Duration::from_secs(15);
const MAX_ROUNDS: usize = 64;
const READY: &[u8] = b"ready-v1\n";

pub struct SharedRunCapture {
    source: RunSource,
    _admission: NativeCloudWorkGuard,
}

impl PackRuntime {
    /// Retain the original main owner lock through a stopped data-directory exchange.
    pub fn retain_rejoin_source_owner(&self) -> StorageResult<Arc<File>> {
        self.require_direct_shared_home()?;
        self.physical.home.retain_owner_lock().map_err(map)
    }

    /// Hold only the device-private generation for an offline rejoin.
    pub async fn capture_rejoin_private(
        self: &Arc<Self>,
        destination: &Path,
    ) -> StorageResult<CapturedRunHome> {
        self.require_direct_shared_home()?;
        let gate = self.enter_write().await;
        self.require_initialized()?;
        if self.failed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(invalid("shared rejoin writer needs recovery"));
        }
        let private = self
            .physical
            .private_home()
            .cloned()
            .ok_or_else(|| invalid("shared rejoin private home"))?;
        let destination = destination.to_path_buf();
        let holds = shared_holds(self);
        run_blocking(move || {
            let _gate = gate;
            require_no_shared_holds(&holds)?;
            private.capture_files(&destination).map_err(map)
        })
        .await
    }

    /// Select logical positions and hold their exact files under one write barrier.
    pub(crate) async fn freeze_shared_checkpoint<T, E>(
        self: &Arc<Self>,
        id: uuid::Uuid,
        key: &[u8; 32],
        cut: impl Future<Output = Result<Option<T>, E>>,
    ) -> Result<Option<(T, SharedRunCapture)>, E>
    where
        E: From<StorageError>,
    {
        self.require_direct_shared_home()?;
        if id.is_nil()
            || crate::storage::disk_pack::transaction::PendingBatch::current(self).is_some()
        {
            return Err(invalid("shared snapshot capture admission").into());
        }
        let admission = NativeCloudWorkGuard::acquire()?;
        let gate = enter_clean_cut(self).await?;
        self.require_initialized()?;
        if self.failed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(invalid("shared snapshot writer needs recovery").into());
        }
        let Some(cut) = cut.await? else {
            return Ok(None);
        };
        let runtime = Arc::clone(self);
        let key = Zeroizing::new(*key);
        let capture = run_blocking(move || {
            let root = shared_hold(&runtime, id);
            if root.try_exists()? {
                RunSource::release_saved(&root)?;
            }
            run_capture::capture_pair(&runtime, &root, true)?;
            drop(gate);
            open_capture(runtime, root, &key, admission)
        })
        .await?;
        Ok(Some((cut, capture)))
    }

    /// Reopen the retained files after an interrupted upload.
    pub async fn capture_shared_checkpoint_files(
        self: &Arc<Self>,
        id: uuid::Uuid,
        key: &[u8; 32],
        resume: bool,
    ) -> StorageResult<SharedRunCapture> {
        self.require_direct_shared_home()?;
        if id.is_nil() || !resume {
            return Err(invalid("shared snapshot requires a saved file hold"));
        }
        let admission = NativeCloudWorkGuard::acquire()?;
        let runtime = Arc::clone(self);
        let key = Zeroizing::new(*key);
        run_blocking(move || {
            let root = shared_hold(&runtime, id);
            run_capture::require_shared_hold(&root)?;
            open_capture(runtime, root, &key, admission)
        })
        .await
    }

    /// Thaw the frozen generation after the publication has a terminal result.
    pub async fn release_shared_checkpoint_files(
        self: &Arc<Self>,
        id: uuid::Uuid,
    ) -> StorageResult<()> {
        self.require_direct_shared_home()?;
        if id.is_nil() {
            return Err(invalid("shared snapshot file hold identifier"));
        }
        let gate = self.enter_write().await;
        let root = shared_hold(self, id);
        run_blocking(move || {
            let _gate = gate;
            RunSource::release_saved(&root)
        })
        .await
    }
}

fn shared_hold(runtime: &PackRuntime, id: uuid::Uuid) -> PathBuf {
    shared_holds(runtime).join(id.to_string())
}

fn shared_holds(runtime: &PackRuntime) -> PathBuf {
    runtime
        .physical
        .path()
        .join(SHARED_BACKUP_DIR)
        .join(HOLDS_DIR)
}

fn require_no_shared_holds(path: &Path) -> StorageResult<()> {
    let mut entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if entries.next().transpose()?.is_some() {
        return Err(invalid("finish the shared snapshot before rejoining"));
    }
    Ok(())
}

fn open_capture(
    runtime: Arc<PackRuntime>,
    root: PathBuf,
    key: &[u8; 32],
    admission: NativeCloudWorkGuard,
) -> StorageResult<SharedRunCapture> {
    Ok(SharedRunCapture {
        source: RunSource::open(runtime, root, key)?,
        _admission: admission,
    })
}

impl SharedRunCapture {
    pub fn profile(&self) -> StorageResult<RunBackupProfile> {
        self.source.profile()
    }

    pub fn files(&self) -> impl Iterator<Item = (&Path, u64)> {
        self.source
            .files
            .iter()
            .map(|file| (file.path.as_path(), file.bytes))
    }

    pub fn destination(
        &self,
        path: &Path,
    ) -> StorageResult<cloud_backup_protocol::v2::Destination> {
        super::run_backup_destination(
            path.to_str()
                .ok_or_else(|| invalid("shared snapshot file path"))?,
        )
    }

    pub fn open_file(&self, path: &Path) -> StorageResult<File> {
        if self
            .source
            .files
            .binary_search_by(|file| file.path.as_path().cmp(path))
            .is_err()
        {
            return Err(invalid("shared snapshot file is outside its inventory"));
        }
        self.source.reader(path)
    }
}

pub(in crate::storage::disk_backup::byte_backup) async fn enter_clean_cut(
    runtime: &Arc<PackRuntime>,
) -> StorageResult<WriteGuard> {
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
        let runtime = Arc::clone(runtime);
        run_blocking(move || merge_round(&runtime)).await?;
        tokio::task::yield_now().await;
    }
    Err(StorageError::SnapshotCompactionPending)
}

fn clean(runtime: &PackRuntime) -> bool {
    runtime.physical.home.snapshot_has_clean_runs()
        && runtime.physical.cas.snapshot_has_clean_runs()
}

fn merge_round(runtime: &PackRuntime) -> StorageResult<()> {
    let cas_policy = laststore::space::SpacePolicy::default();
    for (home, policy) in [
        (&runtime.physical.home, &runtime.space_policy),
        (&runtime.physical.cas, &cas_policy),
    ] {
        if !home.snapshot_has_clean_runs()
            && home.snapshot_cleanup_step_with_space(policy).map_err(map)?
        {
            runtime.adopt_merged_root()?;
        }
    }
    Ok(())
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

pub(in crate::storage::disk_backup::byte_backup) fn publish_hold(root: &Path) -> StorageResult<()> {
    File::open(root)?.sync_all()?;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(root.join("pair-ready.pending"))?;
    file.write_all(READY)?;
    file.sync_all()?;
    fs::rename(root.join("pair-ready.pending"), root.join("pair-ready"))?;
    File::open(root)?.sync_all()?;
    Ok(())
}

pub(in crate::storage::disk_backup::byte_backup) fn hold_complete(
    root: &Path,
) -> StorageResult<bool> {
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
    if !meta.is_file()
        || meta.uid() != unsafe { libc::geteuid() }
        || meta.mode() & 0o077 != 0
        || meta.nlink() != 1
        || meta.len() != READY.len() as u64
    {
        return Err(invalid("run paired completion marker"));
    }
    let mut bytes = [0u8; 9];
    file.read_exact(&mut bytes)?;
    if bytes.as_slice() != READY {
        return Err(invalid("run paired completion marker"));
    }
    Ok(true)
}

pub(in crate::storage::disk_backup::byte_backup) async fn cancel(
    runtime: &Arc<PackRuntime>,
    begin: &Begin,
    key: &[u8; 32],
) -> StorageResult<()> {
    begin.validate().map_err(protocol)?;
    runtime.require_ordinary_backup_home()?;
    let admission = NativeCloudWorkGuard::acquire()?;
    let runtime = Arc::clone(runtime);
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
            inner: Arc::new(make_inner(runtime, store, state, None, admission)),
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
