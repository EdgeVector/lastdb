//! Independent Backup V2 snapshots of one running publication home.
use super::{
    automatic, control, invocation,
    recovery::RecoveryPoint,
    source::{PublicationSource, Source},
    store::{self, Store},
    work::Work,
    worker, BackupError, Phase, Result,
};
use crate::backup_runtime::{InvocationOutcome, RunOrigin};
use crate::host::Host;
use cloud_backup_client::byte_backup::ByteBackupClient;
use cloud_backup_protocol::{v2::jobs::JobResult, Id};
use fold_db::storage::disk_pack::mutation_log::Frontiers;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::time::Instant;

const POLICY: &str = "share-automatic.json";
const REVISION: u64 = 1;
type Client = dyn Fn() -> std::result::Result<ByteBackupClient, String> + Send + Sync;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Policy {
    version: u8,
    source_home: Id,
    stream_id: Id,
    pending: Option<Pending>,
    cleanup: Option<Id>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pending {
    id: Id,
    minimum: Frontiers,
}

struct Publisher {
    host: Arc<Host>,
    source: Source,
    base: PathBuf,
}

pub(crate) fn preflight(home: &Path) -> Result<()> {
    if Store::active(home)?.is_some()
        || store::read::<serde_json::Value>(&Store::base(home).join(store::AUTOMATIC_FILE))?
            .is_some()
    {
        return Err(BackupError::Identity);
    }
    Ok(())
}

pub(super) fn has_pending(base: &Path) -> Result<bool> {
    Ok(store::read::<Policy>(&base.join(POLICY))?.is_some_and(|policy| policy.pending.is_some()))
}

pub(crate) fn spawn(
    host: Arc<Host>,
    shutdown: Arc<AtomicBool>,
    identity: PublicationSource,
    client: impl Fn() -> std::result::Result<ByteBackupClient, String> + Send + Sync + 'static,
) -> Result<tokio::task::JoinHandle<()>> {
    let source = Source::from_publication(
        &host.home,
        host.db.db_ops().native_runtime(),
        identity,
        false,
    )?;
    let publisher = Publisher {
        base: Store::publication_base(&host.home),
        host,
        source,
    };
    publisher.policy()?;
    Ok(tokio::spawn(async move {
        publisher.run(&shutdown, &client).await;
    }))
}

impl Publisher {
    async fn run(&self, shutdown: &AtomicBool, client: &Client) {
        let mut jobs = self.host.byte_backup.subscribe();
        let mut writes = self.source.runtime().subscribe_durable_writes();
        let mut retry = Retry::default();
        let mut last_completion = 0;
        loop {
            let current = jobs.borrow_and_update().clone();
            if shutdown.load(Ordering::Acquire) || current.shutting_down {
                break;
            }
            if let Some(completion) = current.last_completion {
                if completion.invocation.token > last_completion {
                    last_completion = completion.invocation.token;
                    match completion.outcome {
                        InvocationOutcome::Complete => retry = Retry::default(),
                        InvocationOutcome::Paused(state) => {
                            retry.schedule(format_args!("{:?}", state.reason))
                        }
                    }
                }
            }
            if retry.ready() {
                retry.until = None;
                match self.tick(client) {
                    Ok(true) => continue,
                    Ok(false) => {}
                    Err(error) => retry.schedule(error),
                }
            }
            tokio::select! {
                result = jobs.changed() => { if result.is_err() { break; } }
                result = writes.changed(), if retry.until.is_none() => {
                    if result.is_err() { break; }
                }
                _ = retry.wait() => {}
            }
        }
    }

    fn tick(&self, client: &Client) -> Result<bool> {
        let runtime = &self.host.byte_backup;
        let _transition = runtime.transition()?;
        runtime.check_admission()?;
        if runtime.snapshot().active.is_some() {
            return Ok(false);
        }
        if self.source.runtime().requires_recovery() {
            return Err(BackupError::Source);
        }
        let mut policy = self.policy()?;
        let active = Store::active_at(&self.base)?;
        if let Some(old) = policy
            .cleanup
            .as_ref()
            .filter(|old| Some(*old) != active.as_ref())
        {
            automatic::retire_at(&self.base, old)?;
            policy.cleanup = None;
            self.save(&policy)?;
        }
        if policy.pending.is_some() {
            return self.advance(&mut policy, active.as_ref(), client);
        }
        if let Some(id) = &active {
            let saved = store::state_at(&self.base, id)?.ok_or(BackupError::Missing)?;
            self.source.validate(&saved)?;
            if !saved.terminal() {
                return Err(BackupError::Busy);
            }
        }
        let frontier = self.source.runtime().durable_main_frontier();
        if let Some(point) = self.point()? {
            if point.frontiers.main > frontier {
                return Err(BackupError::Identity);
            }
            if point.frontiers.main == frontier {
                return Ok(false);
            }
        }
        policy.pending = Some(Pending {
            id: automatic::request_id()?,
            minimum: Frontiers { main: frontier },
        });
        policy.cleanup = active.filter(automatic::reserved);
        self.save(&policy)?;
        Ok(true)
    }

    fn advance(&self, policy: &mut Policy, active: Option<&Id>, client: &Client) -> Result<bool> {
        let pending = policy.pending.as_ref().ok_or(BackupError::Missing)?;
        let id = &pending.id;
        let permit = Store::lock_at(&self.base)?;
        control::ensure_previous_at(&self.base, id)?;
        let store = Store::open_at(&self.base, id)?;
        let existing = store.state()?;
        let work = Work::Snapshot {
            minimum: Some(pending.minimum),
        };
        if let Some(saved) = &existing {
            self.source.validate(saved)?;
            if saved.automatic_revision != Some(REVISION) || saved.work != work {
                return Err(BackupError::Identity);
            }
            if saved.terminal() {
                if active != Some(id) {
                    return Err(BackupError::Identity);
                }
                let published = match saved.phase {
                    Phase::Published => {
                        let point = self.point()?.ok_or(BackupError::Missing)?;
                        if saved.accepted.as_ref() != Some(&point.accepted)
                            || point.frontiers.main < pending.minimum.main
                        {
                            return Err(BackupError::Identity);
                        }
                        true
                    }
                    Phase::Closed => {
                        let terminal: JobResult =
                            store.read("terminal.json")?.ok_or(BackupError::Missing)?;
                        worker::validate_terminal(saved, &terminal)?;
                        if !worker::source_unavailable(&terminal)
                            || store.read::<Id>("close.json")?.is_some()
                        {
                            return Err(BackupError::Authority);
                        }
                        false
                    }
                    _ => return Err(BackupError::Invalid),
                };
                policy.pending = None;
                self.save(policy)?;
                return if published {
                    Ok(true)
                } else {
                    Err(BackupError::Waiting)
                };
            }
        }
        let id = id.clone();
        let origin = RunOrigin::Auto {
            consent_revision: REVISION,
        };
        let source = self.source.clone();
        if let Some(terminal) = store.read::<JobResult>("terminal.json")? {
            let saved = existing.ok_or(BackupError::Missing)?;
            worker::validate_terminal(&saved, &terminal)?;
            store.select(&id)?;
            self.host.byte_backup.start(id, origin, move |pause| {
                worker::terminal_invocation(source, store, saved, permit, pause)
            })?;
            return Ok(false);
        }
        let client = client().map_err(|_| BackupError::Authority)?;
        let endpoint = client.canonical_endpoint();
        let mut saved = existing.unwrap_or_else(|| {
            let mut saved = source.saved(id.clone(), endpoint.clone());
            saved.work = work;
            saved.automatic_revision = Some(REVISION);
            saved
        });
        if saved.endpoint != endpoint {
            return Err(BackupError::Identity);
        }
        saved.blocked_reason = None;
        store.save(&saved)?;
        store.select(&id)?;
        self.host.byte_backup.start(id, origin, move |pause| {
            invocation::run(source, store, saved, client, permit, pause)
        })?;
        Ok(false)
    }

    fn point(&self) -> Result<Option<RecoveryPoint>> {
        let point: Option<RecoveryPoint> = store::read(&self.base.join("recovery.json"))?;
        if let Some(point) = &point {
            point.validate()?;
            let stream = &point.accepted.job().stream;
            if Some(&stream.stream_id) != self.source.publication_stream.as_ref()
                || stream.scope.account_id != self.source.account
                || point.base != point.accepted
                || !point.accepted.begin.recovery_dependencies.is_empty()
            {
                return Err(BackupError::Identity);
            }
        }
        Ok(point)
    }

    fn policy(&self) -> Result<Policy> {
        let stream_id = self
            .source
            .publication_stream
            .as_ref()
            .ok_or(BackupError::Identity)?;
        let policy: Policy = store::read(&self.base.join(POLICY))?.unwrap_or_else(|| Policy {
            version: 1,
            source_home: self.source.source_home.clone(),
            stream_id: stream_id.clone(),
            pending: None,
            cleanup: None,
        });
        if policy.version != 1
            || policy.source_home != self.source.source_home
            || &policy.stream_id != stream_id
            || policy
                .pending
                .as_ref()
                .is_some_and(|p| !automatic::reserved(&p.id))
            || policy
                .cleanup
                .as_ref()
                .is_some_and(|id| !automatic::reserved(id))
        {
            return Err(BackupError::Identity);
        }
        Ok(policy)
    }

    fn save(&self, policy: &Policy) -> Result<()> {
        Store::write_configuration_at(&self.base, POLICY, policy)
    }
}

#[derive(Default)]
struct Retry {
    until: Option<Instant>,
    attempts: u32,
}

impl Retry {
    fn schedule(&mut self, error: impl std::fmt::Display) {
        if self.attempts == 0 {
            tracing::warn!(target: "lastdbd::share", %error, "share publication will retry");
        }
        let delay = (5u64 << self.attempts.min(4)).min(60);
        self.until = Some(Instant::now() + Duration::from_secs(delay));
        self.attempts = self.attempts.saturating_add(1);
    }

    fn ready(&self) -> bool {
        self.until.is_none_or(|at| at <= Instant::now())
    }

    async fn wait(&self) {
        if let Some(at) = self.until {
            tokio::time::sleep_until(at).await;
        } else {
            std::future::pending().await
        }
    }
}
