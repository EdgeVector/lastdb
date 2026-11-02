//! Owner-operated local snapshots use the real byte planner without cloud work.

mod intent;
mod worker;

use super::{source::Source, store::Store, BackupError, Result};
use crate::backup_runtime::{InvocationOutcome, RunOrigin};
use cloud_backup_protocol::{Id, NativeRoot};
use fold_db::storage::disk_pack::byte_backup::RunBackupProfile;
use fold_db::storage::disk_pack::PackRuntime;
use serde::Serialize;
use std::path::Path;
use std::sync::Arc;

pub const LOCAL_INTENT_FILE: &str = "logical-snapshot-local-v1.json";

#[derive(Clone, Copy)]
pub enum LocalSnapshotAction {
    Prepare,
    Inspect,
    Resume,
    Release,
}

#[derive(Debug, Serialize)]
pub struct LocalSnapshotReport {
    pub request_id: Id,
    pub released: bool,
    pub remote_submitted: bool,
    pub root_object_index: Option<u64>,
    pub root_ciphertext_sha256: Option<cloud_backup_protocol::Digest>,
    pub root_ciphertext_bytes: Option<u64>,
    pub native_root: Option<NativeRoot>,
    pub run_profile: Option<RunBackupProfile>,
}

struct Inputs {
    source: Source,
    runtime: Arc<PackRuntime>,
    home: std::path::PathBuf,
    intent: intent::Intent,
    action: LocalSnapshotAction,
}

/// Admission is synchronous. No standard mutex guard survives the async wait.
pub async fn local_snapshot(
    host: &crate::host::Host,
    action: LocalSnapshotAction,
    request_id: Id,
) -> Result<LocalSnapshotReport> {
    let mut changes = host.byte_backup.subscribe();
    let receiver = admit(host, action, request_id.clone())?;
    let result = receiver.await.map_err(|_| BackupError::WorkerPanic)?;
    loop {
        let active = changes
            .borrow_and_update()
            .active
            .as_ref()
            .is_some_and(|job| job.request_id == request_id);
        if !active {
            return result;
        }
        changes
            .changed()
            .await
            .map_err(|_| BackupError::WorkerPanic)?;
    }
}

fn admit(
    host: &crate::host::Host,
    action: LocalSnapshotAction,
    request_id: Id,
) -> Result<tokio::sync::oneshot::Receiver<Result<LocalSnapshotReport>>> {
    let backup = &host.byte_backup;
    let _transition = backup.transition()?;
    backup.check_admission()?;
    if backup.snapshot().active.is_some() || super::status(host)?.automatic_backup_enabled {
        return Err(BackupError::Busy);
    }
    let permit = Store::lock(&host.home)?;
    intent::require_no_remote(&host.home)?;
    let source = Source::from_host(host)?;
    let runtime = host.db.db_ops().namespaced_store().disk_pack_runtime();
    let intent = intent::admit(&host.home, &source, action, &request_id)?;
    let inputs = Inputs {
        source,
        runtime,
        home: host.home.clone(),
        intent,
        action,
    };
    let (sender, receiver) = tokio::sync::oneshot::channel();
    backup.start(request_id, RunOrigin::Manual, move |pause| {
        let result = invoke(inputs, &pause);
        let outcome = match &result {
            Ok(_) => InvocationOutcome::Complete,
            Err(error) => InvocationOutcome::Paused(error.pause_state()),
        };
        // The file lock and every source reader drain before runtime completion.
        drop(permit);
        let _ = sender.send(result);
        outcome
    })?;
    Ok(receiver)
}

fn invoke(inputs: Inputs, pause: &std::sync::atomic::AtomicBool) -> Result<LocalSnapshotReport> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| BackupError::Source)?;
        runtime.block_on(worker::run(inputs, pause))
    }))
    .unwrap_or(Err(BackupError::WorkerPanic))
}

/// Cloud admission must never take over a still-owned local-only source.
pub(super) fn require_inactive(home: &Path) -> Result<()> {
    if intent::load(home)?.is_some_and(|intent| !intent.released) {
        return Err(BackupError::Busy);
    }
    Ok(())
}
