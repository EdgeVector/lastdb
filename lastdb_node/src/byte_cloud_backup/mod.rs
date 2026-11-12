//! Explicit V2 byte backup. The native format remains a client responsibility.
mod automatic;
pub(crate) mod automatic_publication;
mod blobs;
pub(crate) mod connection;
mod log_capture;
mod publication;
mod source;
mod store;
mod transfer;
mod work;
mod worker;

pub use crate::backup_runtime::{BackupError, BackupRuntime};
pub use automatic::{spawn_automatic, AutomaticBackupStatus};
pub use connection::{
    connect_existing, share_write_gate, ManualConnectionStatus, SensitiveConnectionRequest,
};
pub use control::request;
pub(crate) use control::status;
pub use model::{ByteBackupRequest, ByteBackupStatus, Phase};
pub use source::model;
pub(crate) use source::PublicationSource;
pub use transfer::register_blob_source;
pub(crate) use work::recovery;
pub type Result<T> = std::result::Result<T, BackupError>;

pub const PUBLICATION_MARKER: &str = "share-publication.json";

/// Read saved recovery credentials without enrolling or enabling backup.
pub fn client_from_home(
    home: &std::path::Path,
) -> std::result::Result<crate::cloud_backup_client::byte_backup::ByteBackupClient, String> {
    crate::cloud_backup_client::byte_backup::ByteBackupClient::from_auth(
        crate::cloud::auth_for_recovery(home)?,
    )
    .map_err(|e| e.to_string())
}

pub(crate) fn is_publication_home(home: &std::path::Path) -> std::io::Result<bool> {
    match std::fs::symlink_metadata(home.join(PUBLICATION_MARKER)) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

pub fn require_enabled() -> Result<()> {
    fold_db::env_flag::var_truthy("LASTDB_BYTE_BACKUP_V2_ENABLED")
        .then_some(())
        .ok_or(BackupError::Disabled)
}

fn execute(
    source: &source::Source,
    store: &store::Store,
    mut saved: model::Saved,
    client: Option<&crate::cloud_backup_client::byte_backup::ByteBackupClient>,
    _permit: std::fs::File,
    pause: &std::sync::atomic::AtomicBool,
) -> crate::backup_runtime::InvocationOutcome {
    use crate::backup_runtime::InvocationOutcome;
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| BackupError::Source)?;
        if let Some(c) = client {
            rt.block_on(worker::run(source, store, &mut saved, c, pause))
        } else {
            transfer::check_pause(pause)?;
            rt.block_on(worker::reconcile(source, store, &mut saved))
        }
    }))
    .unwrap_or(Err(BackupError::WorkerPanic));
    let Err(error) = res else {
        return InvocationOutcome::Complete;
    };
    let pause_state = error.pause_state();
    saved.blocked_reason = Some(error.to_string());
    let persisted = store.reopen().and_then(|fresh| {
        fresh.write("pause.json", &pause_state)?;
        fresh.save(&saved)
    });
    InvocationOutcome::Paused(
        persisted.map_or_else(|e| BackupError::pause_state(&e), |()| pause_state),
    )
}

mod control {
    use crate::backup_runtime::{BackupError, PauseState};
    use crate::byte_cloud_backup::{
        automatic, client_from_home, connection, execute, is_publication_home,
        model::{ByteBackupRequest, ByteBackupStatus, Saved},
        require_enabled,
        source::Source,
        store::{self, Store},
        work::Work,
        worker, Result,
    };
    use cloud_backup_protocol::v2::{jobs::JobResult, AcceptedBackup};
    use cloud_backup_protocol::Id;

    pub async fn request(
        host: &crate::host::Host,
        action: ByteBackupRequest,
    ) -> Result<ByteBackupStatus> {
        std::future::ready(request_now(host, &action)).await
    }

    fn request_now(
        host: &crate::host::Host,
        action: &ByteBackupRequest,
    ) -> Result<ByteBackupStatus> {
        if matches!(action, ByteBackupRequest::Status) {
            return status(host);
        }
        if is_publication_home(&host.home)? {
            return Err(BackupError::Identity);
        }
        let _transition = host.byte_backup.transition()?;
        host.byte_backup.check_admission()?;
        match action {
            ByteBackupRequest::EnableAutomatic {
                log_interval_seconds: log,
                snapshot_interval_seconds: snap,
            } => automatic::configure(host, true, *log, *snap).and_then(|()| status(host)),
            ByteBackupRequest::DisableAutomatic => {
                automatic::configure(host, false, 0, 0).and_then(|()| status(host))
            }
            _ => request_locked(host, action, None),
        }
    }

    pub(crate) fn request_automatic(
        host: &crate::host::Host,
        request_id: Id,
        consent_revision: u64,
        work: &Work,
    ) -> Result<ByteBackupStatus> {
        request_locked(
            host,
            &ByteBackupRequest::Start { request_id },
            Some((consent_revision, work)),
        )
    }

    fn request_locked(
        host: &crate::host::Host,
        action: &ByteBackupRequest,
        auto: Option<(u64, &Work)>,
    ) -> Result<ByteBackupStatus> {
        let rt = &host.byte_backup;
        rt.check_admission()?;
        let base = Store::base(&host.home);
        let id = match action {
            ByteBackupRequest::Start { request_id } => request_id.clone(),
            ByteBackupRequest::Resume | ByteBackupRequest::Close => {
                Store::active_at(&base)?.ok_or(BackupError::Missing)?
            }
            _ => return Err(BackupError::Invalid),
        };
        let prev = store::state_at(&base, &id)?;
        if automatic::reserved(&id) && prev.is_none() && auto.is_none() {
            return Err(BackupError::Missing);
        }
        if prev.as_ref().is_some_and(Saved::terminal) {
            return status_for(host, Some(id));
        }
        let known_term =
            store::read::<JobResult>(&base.join("jobs").join(id.as_str()).join("terminal.json"))?;
        let closing = matches!(action, ByteBackupRequest::Close);
        if let Some(res) = &known_term {
            worker::validate_terminal(prev.as_ref().ok_or(BackupError::Invalid)?, res)?;
        } else if !closing {
            connection::require_admission(host)?;
        }
        let source = Source::from_host(host)?;
        if closing {
            source.validate(prev.as_ref().ok_or(BackupError::Missing)?)?;
            store::close_at(&base, &id)?;
        }
        if let Some(active) = rt.snapshot().active {
            return if active.request_id == id {
                status(host)
            } else {
                Err(BackupError::Busy)
            };
        }
        let permit = Store::lock_at(&base)?;
        ensure_previous_at(&base, &id)?;
        let store = Store::open_at(&base, &id)?;
        let (saved, client) = prepare_saved(host, closing, &id, auto, &source, &store, known_term)?;
        store.select(&id)?;
        rt.start(id, move |pause| {
            execute(&source, &store, saved, client.as_ref(), permit, &pause)
        })?;
        status(host)
    }

    fn prepare_saved(
        host: &crate::host::Host,
        closing: bool,
        id: &Id,
        auto: Option<(u64, &Work)>,
        source: &Source,
        store: &Store,
        known_term: Option<JobResult>,
    ) -> Result<(
        Saved,
        Option<crate::cloud_backup_client::byte_backup::ByteBackupClient>,
    )> {
        if let Some(expected) = known_term {
            let saved = store.state()?.ok_or(BackupError::Invalid)?;
            source.validate(&saved)?;
            let res: JobResult = store.read("terminal.json")?.ok_or(BackupError::Invalid)?;
            if res != expected {
                return Err(BackupError::Identity);
            }
            return worker::validate_terminal(&saved, &res).map(|()| (saved, None));
        }
        let client = if closing {
            client_from_home(&host.home).map_err(|_| BackupError::Identity)?
        } else {
            connection::client_for_host(host)?
        };
        let existing = store.state()?;
        let mut saved = existing
            .clone()
            .unwrap_or_else(|| source.saved(id.clone(), client.canonical_endpoint()));
        if let Some((rev, work)) = auto {
            if saved
                .automatic_revision
                .is_some_and(|r| r != rev || saved.work != *work)
                || (saved.automatic_revision.is_none() && existing.is_some())
            {
                return Err(BackupError::Identity);
            }
            (saved.automatic_revision, saved.work) = (Some(rev), work.clone());
        }
        source.validate(&saved)?;
        if saved.endpoint != client.canonical_endpoint() {
            return Err(BackupError::Identity);
        }
        saved.blocked_reason = None;
        store.write("pause.json", &Option::<PauseState>::None)?;
        store.save(&saved).map(|()| (saved, Some(client)))
    }

    pub(crate) fn ensure_previous_at(base: &std::path::Path, id: &Id) -> Result<()> {
        let Some(prev) = Store::active_at(base)? else {
            return Ok(());
        };
        (&prev == id
            || store::state_at(base, &prev)?
                .ok_or(BackupError::Invalid)?
                .terminal())
        .then_some(())
        .ok_or(BackupError::Busy)
    }

    pub(crate) fn status(host: &crate::host::Host) -> Result<ByteBackupStatus> {
        status_for(host, Store::active(&host.home)?)
    }

    fn status_for(host: &crate::host::Host, id: Option<Id>) -> Result<ByteBackupStatus> {
        let saved = id
            .as_ref()
            .map(|id| store::state(&host.home, id))
            .transpose()?
            .flatten();
        let (account, source_home) = connection::identity(host)?;
        if saved
            .as_ref()
            .is_some_and(|s| s.account != account || s.source_home != source_home)
        {
            return Err(BackupError::Identity);
        }
        let accepted: Option<AcceptedBackup> =
            store::read(&Store::base(&host.home).join("accepted.json"))?;
        if let Some(acc) = &accepted {
            acc.validate()?;
            (acc.job().stream.scope.account_id == account)
                .then_some(())
                .ok_or(BackupError::Identity)?;
        }
        let running = host
            .byte_backup
            .snapshot()
            .active
            .is_some_and(|a| Some(a.request_id) == id);
        let (connection, automatic) = (connection::status(host)?, automatic::status(host)?);
        let s = saved.as_ref();
        Ok(ByteBackupStatus {
            backup_kind: s.map_or("snapshot", |s| s.work.label()),
            enabled: require_enabled().is_ok() || connection.is_some(),
            automatic_backup_enabled: automatic.as_ref().is_some_and(|a| a.enabled),
            automatic_backup: automatic,
            mutation_log: host.db.db_ops().native_runtime().mutation_log_status()?,
            manual_connection: connection,
            running,
            request_id: id,
            phase: s.map(|s| s.phase),
            objects_verified: s.map_or(0, |s| s.objects_verified),
            metadata_pack_objects: s.map_or(0, |s| s.metadata_pack_objects),
            multi_file_pack_objects: s.map_or(0, |s| s.multi_file_pack_objects),
            native_files: s.map_or(0, |s| s.native_files),
            native_bytes: s.map_or(0, |s| s.native_bytes),
            retained_source_bytes: s.and_then(|s| s.retained_source_bytes),
            blocked_reason: saved.and_then(|s| s.blocked_reason),
            last_accepted_backup: accepted,
        })
    }
}
