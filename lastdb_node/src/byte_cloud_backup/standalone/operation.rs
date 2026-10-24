use super::{identity::Identity, paths::Paths, report, transport, Action, Options};
use crate::byte_cloud_backup::{
    model::Saved,
    source::Source,
    store::{self, Store},
    worker, *,
};
use cloud_backup_protocol::{v2::jobs::JobResult, Id};
use fold_db::storage::disk_pack::byte_backup::StandaloneByteBackupSource;
use std::sync::atomic::AtomicBool;

pub(super) async fn run(
    options: &Options,
    action: Action,
    identity: Identity,
    paths: Paths,
    pause: &AtomicBool,
) -> Result<super::StandaloneReport> {
    let id = super::select(&paths.coordinator, &action)?;
    let previous = store::state_at(&paths.coordinator, &id)?;
    let terminal = read_terminal(&paths, &id, previous.as_ref())?;
    if previous.as_ref().is_some_and(Saved::terminal) {
        return report::read_for(&identity, &paths, options.environment, Some(&id));
    }
    if terminal.is_none() && !matches!(action, Action::Close) {
        require_enabled()?;
    }
    let paths = Paths::open(identity.home(), &paths.root, true)?;
    if matches!(action, Action::Close) {
        previous.as_ref().ok_or(BackupError::Missing)?;
        report::read_for(&identity, &paths, options.environment, Some(&id))?;
        store::close_at(&paths.coordinator, &id)?;
        paths.check()?;
    }
    let _permit = match Store::lock_at(&paths.coordinator) {
        Err(BackupError::Busy) if matches!(action, Action::Close) => {
            return report::read_for(&identity, &paths, options.environment, Some(&id));
        }
        result => result?,
    };
    ensure_previous(&paths, &id)?;
    let store = Store::open_at(&paths.coordinator, &id)?;
    let previous = store.state()?;
    let terminal = read_terminal(&paths, &id, previous.as_ref())?;
    if previous.as_ref().is_some_and(Saved::terminal) {
        return report::read_for(&identity, &paths, options.environment, Some(&id));
    }
    let native = identity.home().join("data/disk-pack").canonicalize()?;
    let core = StandaloneByteBackupSource::open(
        &native,
        &paths.source,
        lastdb_storage_policy::BACKUP_RESOURCES,
        laststore::disk_pack::maintenance::SpacePolicy::default(),
    )?;
    let source = Source::standalone(identity, core)?;
    let client = if terminal.is_none() && needs_client(&action, previous.as_ref()) {
        Some(transport::client(options, &source)?)
    } else {
        None
    };
    let endpoint = transport::endpoint(options.environment)?;
    let mut saved = previous.unwrap_or_else(|| source.saved(id.clone(), endpoint));
    source.validate(&saved)?;
    if saved.endpoint != transport::endpoint(options.environment)? {
        return Err(BackupError::Identity);
    }
    if !matches!(action, Action::Start { .. }) && store.state()?.is_none() {
        return Err(BackupError::Missing);
    }
    saved.blocked_reason = None;
    store.save(&saved)?;
    store.select(&id)?;
    let result = execute(
        &source,
        &store,
        &mut saved,
        client.as_ref(),
        terminal,
        pause,
    )
    .await;
    if let Err(error) = result {
        saved.blocked_reason = Some(error.to_string());
        store.reopen()?.save(&saved)?;
        return Err(error);
    }
    source.validate(&saved)?;
    paths.check()?;
    let identity = Identity::open(&options.source_home)?;
    report::read_for(&identity, &paths, options.environment, Some(&id))
}

fn needs_client(action: &Action, saved: Option<&Saved>) -> bool {
    !matches!(action, Action::Close) || saved.is_some_and(|s| s.begin_submitted)
}

fn read_terminal(paths: &Paths, id: &Id, saved: Option<&Saved>) -> Result<Option<JobResult>> {
    let result = store::read(
        &paths
            .coordinator
            .join("jobs")
            .join(id.as_str())
            .join("terminal.json"),
    )?;
    if let Some(result) = &result {
        worker::validate_terminal(saved.ok_or(BackupError::Invalid)?, result)?;
    }
    Ok(result)
}

fn ensure_previous(paths: &Paths, id: &Id) -> Result<()> {
    if let Some(previous) = Store::active_at(&paths.coordinator)? {
        if &previous != id
            && !store::state_at(&paths.coordinator, &previous)?
                .ok_or(BackupError::Invalid)?
                .terminal()
        {
            return Err(BackupError::Busy);
        }
    }
    Ok(())
}

async fn execute(
    source: &Source,
    store: &Store,
    saved: &mut Saved,
    client: Option<&fold_db::sync::auth::byte_backup::ByteBackupClient>,
    terminal: Option<JobResult>,
    pause: &AtomicBool,
) -> Result<()> {
    if terminal.is_some() {
        return worker::reconcile_terminal(source, store, saved).await;
    }
    let close = store.read::<Id>("close.json")?;
    if close.as_ref().is_some_and(|id| id != &saved.request_id) {
        return Err(BackupError::Identity);
    }
    if close.is_some() && !saved.begin_submitted {
        if let Some(begin) = &saved.begin {
            source.cancel(begin).await?;
        }
        saved.phase = Phase::Closed;
        saved.retained_source_bytes = Some(0);
        return store.save(saved);
    }
    let client = client.ok_or(BackupError::Identity)?;
    if close.is_none() {
        transport::revalidate(source, saved, client, pause).await?;
    }
    worker::run(source, store, saved, client, pause).await
}
