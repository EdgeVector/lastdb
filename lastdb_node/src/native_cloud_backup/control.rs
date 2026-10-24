//! Saved owner requests precede a tracked host-owned worker.
use super::{
    identity::Source,
    model::{CloseIntent, Saved},
    runtime::{BackupRuntime, RunOrigin},
    store::{self, Store},
    *,
};
use cloud_backup_protocol::{
    jobs::{CloseReason, JobResult},
    Id,
};
use fold_db::sync::auth::native_backup::NativeBackupClient;
use std::path::Path;

pub async fn request(
    host: &crate::host::Host,
    action: NativeBackupRequest,
) -> Result<NativeBackupStatus> {
    if matches!(action, NativeBackupRequest::Status) {
        return super::status::status(&host.home, &host.native_backup);
    }
    let client = if matches!(action, NativeBackupRequest::Close)
        || (matches!(action, NativeBackupRequest::Resume) && active_terminal(&host.home)?)
    {
        crate::native_backup_access::client_for_recovery(&host.home).ok()
    } else {
        Some(
            crate::native_backup_access::client_from_home(&host.home)
                .map_err(|_| BackupError::Identity)?,
        )
    };
    // Preserve the public async owner API around synchronous local admission.
    std::future::ready(request_with(host, action, RunOrigin::Manual, client)).await
}

pub(super) fn dispatch(
    host: &crate::host::Host,
    action: NativeBackupRequest,
    origin: RunOrigin,
    client: NativeBackupClient,
) -> Result<NativeBackupStatus> {
    request_with(host, action, origin, Some(client))
}

fn request_with(
    host: &crate::host::Host,
    action: NativeBackupRequest,
    origin: RunOrigin,
    client: Option<NativeBackupClient>,
) -> Result<NativeBackupStatus> {
    let runtime = &host.native_backup;
    let _transition = runtime.transition()?;
    runtime.check_admission()?;
    if matches!(action, NativeBackupRequest::Close) {
        super::automatic::disable_for_close(&host.home)?;
        runtime.wake();
    }
    super::automatic::check_origin(&host.home, &origin)?;
    let terminal_resume =
        matches!(action, NativeBackupRequest::Resume) && active_terminal(&host.home)?;
    if !matches!(action, NativeBackupRequest::Close) && !terminal_resume {
        require_enabled()?;
    }
    let source = Source::from_host(host)?;
    let closing = matches!(action, NativeBackupRequest::Close);
    let id = match action {
        NativeBackupRequest::Start { request_id } => request_id,
        NativeBackupRequest::Resume | NativeBackupRequest::Close => {
            Store::active(&host.home)?.ok_or(BackupError::Missing)?
        }
        NativeBackupRequest::Status => return super::status::status(&host.home, runtime),
    };
    if let Some(value) = archived_terminal(&host.home, &id, runtime)? {
        return Ok(value);
    }
    // Closing records its durable intent even while the owned worker is active.
    if closing {
        let store = Store::open(&host.home, &id)?;
        let saved = store.state()?.ok_or(BackupError::Missing)?;
        source.validate(&saved)?;
        store.write(
            "close.json",
            &CloseIntent {
                request_id: id.clone(),
                reason: CloseReason::UserRequested,
            },
        )?;
    }
    if let Some(active) = runtime.snapshot().active {
        return if active.request_id == id {
            super::status::status(&host.home, runtime)
        } else {
            Err(BackupError::Busy)
        };
    }
    let permit = Store::lock(&source.home)?;
    ensure_previous(&source.home, &id)?;
    let store = Store::open(&source.home, &id)?;
    let previous = store.state()?;
    if matches!(origin, RunOrigin::Auto { .. })
        && previous
            .as_ref()
            .is_some_and(|saved| saved.endpoint.is_none())
    {
        return Err(BackupError::Identity);
    }
    let mut saved = previous.unwrap_or_else(|| source.saved(id.clone()));
    source.validate(&saved)?;
    if let Some(client) = &client {
        let endpoint = client.canonical_endpoint();
        if saved
            .endpoint
            .as_ref()
            .is_some_and(|prior| prior != &endpoint)
        {
            return Err(BackupError::Identity);
        }
        saved.endpoint = Some(endpoint);
    }
    store.save(&saved)?;
    store.select(&id)?;
    runtime.start(id, origin, move |pause| {
        super::invocation::run(source, store, saved, client, permit, pause, origin)
    })?;
    super::status::status(&host.home, runtime)
}

fn ensure_previous(home: &Path, id: &Id) -> Result<()> {
    let Some(active) = Store::active(home)? else {
        return Ok(());
    };
    if &active == id {
        return Ok(());
    }
    let previous = Store::open(home, &active)?;
    let saved = previous.state()?.ok_or(BackupError::Invalid)?;
    if saved.locally_closed() {
        return validate_local_close(&previous, &saved);
    }
    let result: JobResult = previous.read("terminal.json")?.ok_or(BackupError::Busy)?;
    saved.validate_terminal(&result).and_then(|()| {
        if saved.retained_source_bytes == Some(0) {
            Ok(())
        } else {
            Err(BackupError::Busy)
        }
    })
}
fn active_terminal(home: &Path) -> Result<bool> {
    let Some(id) = Store::active(home)? else {
        return Ok(false);
    };
    let root = Store::base(home).join("jobs").join(id.as_str());
    if store::read::<JobResult>(&root.join("terminal.json"))?.is_some() {
        return Ok(true);
    }
    Ok(store::read::<Saved>(&root.join("state.json"))?.is_some_and(|saved| saved.locally_closed()))
}
fn archived_terminal(
    home: &Path,
    id: &Id,
    runtime: &BackupRuntime,
) -> Result<Option<NativeBackupStatus>> {
    if Store::active(home)?.as_ref() == Some(id) {
        return Ok(None);
    }
    let path = Store::base(home).join("jobs").join(id.as_str());
    let Some(saved) = store::read::<Saved>(&path.join("state.json"))? else {
        return Ok(None);
    };
    if let Some(result) = store::read::<JobResult>(&path.join("terminal.json"))? {
        saved.validate_terminal(&result)?;
    } else if saved.locally_closed() {
        let close: CloseIntent =
            store::read(&path.join("close.json"))?.ok_or(BackupError::Invalid)?;
        if &close.request_id != id {
            return Err(BackupError::Identity);
        }
    } else {
        return Ok(None);
    }
    if saved.retained_source_bytes != Some(0) {
        return Ok(None);
    }
    super::status::status_for(home, Some(id.clone()), runtime).map(Some)
}
fn validate_local_close(store: &Store, saved: &Saved) -> Result<()> {
    let close: CloseIntent = store.read("close.json")?.ok_or(BackupError::Invalid)?;
    if close.request_id != saved.request_id {
        return Err(BackupError::Identity);
    }
    Ok(())
}
