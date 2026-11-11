//! Clean-cut compaction, paired hold markers, and cancel for a personal capture.

use super::{
    invalid, protocol,
    run_capture::{self, Intent},
    run_source::RunSource,
    store::Store,
    ByteBackupCapture, PackRuntime, State, HOLDS_DIR, RUN_INTENT_FILE,
};
use crate::storage::disk_backup::NativeCloudWorkGuard;
use crate::storage::disk_pack::{map, run_blocking, WriteGuard};
use crate::storage::error::{StorageError, StorageResult};
use cloud_backup_protocol::{bytes_sha256, v2::Begin};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use zeroize::Zeroizing;

const PASS_BUDGET: Duration = Duration::from_secs(15);
const MAX_ROUNDS: usize = 64;
const READY: &[u8] = b"ready-v1\n";

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

pub(in crate::storage::disk_backup::byte_backup) fn hold_complete(root: &Path) -> StorageResult<bool> {
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
            inner: Arc::new(run_capture::make_inner(runtime, store, state, None, admission)),
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
