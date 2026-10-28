use super::{
    model::Saved,
    source::Source,
    store::{self, Store},
    *,
};
use crate::backup_runtime::RunOrigin;
use cloud_backup_protocol::Id;

pub async fn request(
    host: &crate::host::Host,
    action: ByteBackupRequest,
) -> Result<ByteBackupStatus> {
    std::future::ready(request_now(host, &action)).await
}

fn request_now(host: &crate::host::Host, action: &ByteBackupRequest) -> Result<ByteBackupStatus> {
    if matches!(action, ByteBackupRequest::Status) {
        return status(host);
    }
    let runtime = &host.byte_backup;
    let _transition = runtime.transition()?;
    runtime.check_admission()?;
    let id = match action {
        ByteBackupRequest::Start { request_id } => request_id.clone(),
        ByteBackupRequest::Resume | ByteBackupRequest::Close => {
            Store::active(&host.home)?.ok_or(BackupError::Missing)?
        }
        ByteBackupRequest::Status => return status(host),
    };
    let previous = store::state(&host.home, &id)?;
    if previous.as_ref().is_some_and(Saved::terminal) {
        return status_for(host, Some(id));
    }
    let known_terminal = store::read::<cloud_backup_protocol::v2::jobs::JobResult>(
        &Store::base(&host.home)
            .join("jobs")
            .join(id.as_str())
            .join("terminal.json"),
    )?;
    if let Some(result) = &known_terminal {
        super::worker::validate_terminal(previous.as_ref().ok_or(BackupError::Invalid)?, result)?;
    }
    if known_terminal.is_none() && !matches!(action, ByteBackupRequest::Close) {
        super::connection::require_admission(host)?;
    }
    let source = Source::from_host(host)?;
    if matches!(action, ByteBackupRequest::Close) {
        let saved = previous.as_ref().ok_or(BackupError::Missing)?;
        source.validate(saved)?;
        store::close(&host.home, &id)?;
    }
    if let Some(active) = runtime.snapshot().active {
        return if active.request_id == id {
            status(host)
        } else {
            Err(BackupError::Busy)
        };
    }
    let permit = Store::lock(&host.home)?;
    ensure_previous(&host.home, &id)?;
    if let Some(expected) = known_terminal {
        let store = Store::open(&host.home, &id)?;
        let saved = store.state()?.ok_or(BackupError::Invalid)?;
        source.validate(&saved)?;
        let result = store
            .read::<cloud_backup_protocol::v2::jobs::JobResult>("terminal.json")?
            .ok_or(BackupError::Invalid)?;
        if result != expected {
            return Err(BackupError::Identity);
        }
        super::worker::validate_terminal(&saved, &result)?;
        store.select(&id)?;
        runtime.start(id, RunOrigin::Manual, move |pause| {
            super::worker::terminal_invocation(source, store, saved, permit, pause)
        })?;
        return status(host);
    }
    let client = if matches!(action, ByteBackupRequest::Close) {
        super::credentials::client_for_recovery(&host.home).map_err(|_| BackupError::Identity)?
    } else {
        super::connection::client_for_host(host)?
    };
    let store = Store::open(&host.home, &id)?;
    let mut saved = store
        .state()?
        .unwrap_or_else(|| source.saved(id.clone(), client.canonical_endpoint()));
    source.validate(&saved)?;
    if saved.endpoint != client.canonical_endpoint() {
        return Err(BackupError::Identity);
    }
    saved.blocked_reason = None;
    store.save(&saved)?;
    store.select(&id)?;
    runtime.start(id, RunOrigin::Manual, move |pause| {
        super::invocation::run(source, store, saved, client, permit, pause)
    })?;
    status(host)
}

fn ensure_previous(home: &std::path::Path, id: &Id) -> Result<()> {
    let Some(previous) = Store::active(home)? else {
        return Ok(());
    };
    if &previous == id {
        return Ok(());
    }
    let saved = store::state(home, &previous)?.ok_or(BackupError::Invalid)?;
    if !saved.terminal() {
        return Err(BackupError::Busy);
    }
    Ok(())
}

pub(crate) fn status(host: &crate::host::Host) -> Result<ByteBackupStatus> {
    let id = Store::active(&host.home)?;
    status_for(host, id)
}

fn status_for(host: &crate::host::Host, id: Option<Id>) -> Result<ByteBackupStatus> {
    let saved: Option<Saved> = id
        .as_ref()
        .map(|id| store::state(&host.home, id))
        .transpose()?
        .flatten();
    if saved
        .as_ref()
        .is_some_and(|s| s.account.as_str() != host.user_hash)
    {
        return Err(BackupError::Identity);
    }
    if let Some(saved) = &saved {
        let runtime = host
            .db
            .db_ops()
            .namespaced_store()
            .disk_pack_runtime()
            .ok_or(BackupError::Source)?;
        if saved.source_home.as_str() != runtime.native_home_id().to_string() {
            return Err(BackupError::Identity);
        }
    }
    let accepted: Option<cloud_backup_protocol::v2::AcceptedBackup> =
        store::read(&Store::base(&host.home).join("accepted.json"))?;
    if let Some(accepted) = &accepted {
        accepted.validate()?;
        if accepted.job().stream.scope.account_id.as_str() != host.user_hash {
            return Err(BackupError::Identity);
        }
    }
    let running = host
        .byte_backup
        .snapshot()
        .active
        .is_some_and(|active| Some(active.request_id) == id);
    let connection = super::connection::status(host)?;
    Ok(ByteBackupStatus {
        backup_kind: saved
            .as_ref()
            .map_or("snapshot", |saved| saved.work.label()),
        enabled: require_enabled().is_ok() || connection.is_some(),
        automatic_backup_enabled: false,
        manual_connection: connection,
        running,
        request_id: id,
        phase: saved.as_ref().map(|s| s.phase),
        objects_verified: saved.as_ref().map_or(0, |s| s.objects_verified),
        metadata_pack_objects: saved.as_ref().map_or(0, |s| s.metadata_pack_objects),
        multi_file_pack_objects: saved.as_ref().map_or(0, |s| s.multi_file_pack_objects),
        native_files: saved.as_ref().map_or(0, |s| s.native_files),
        native_bytes: saved.as_ref().map_or(0, |s| s.native_bytes),
        retained_source_bytes: saved.as_ref().and_then(|s| s.retained_source_bytes),
        blocked_reason: saved.and_then(|s| s.blocked_reason),
        last_accepted_backup: accepted,
    })
}
