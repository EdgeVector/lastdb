//! Backup V2 recovery points for one running publication home.
use super::{
    automatic, control, execute,
    model::Saved,
    publication,
    source::{PublicationSource, Source},
    store::{self, Store},
    work::{LogContext, Work},
    worker, BackupError, Result,
};
use crate::backup_runtime::InvocationOutcome;
use crate::cloud_backup_client::byte_backup::ByteBackupClient;
use crate::host::Host;
use cloud_backup_protocol::v2::{jobs::JobResult, Stream};
use cloud_backup_protocol::Id;
use fold_db::storage::disk_pack::mutation_log::Frontiers;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::time::Instant;
use tracing::Instrument;

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    log: Option<Box<LogContext>>,
}

impl Pending {
    fn work(&self) -> Work {
        match &self.log {
            Some(ctx) => Work::MutationLog {
                context: ctx.clone(),
            },
            None => Work::Snapshot {
                minimum: Some(self.minimum),
            },
        }
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
        let secs = (5u64 << self.attempts.min(4)).min(60);
        self.until = Some(Instant::now() + Duration::from_secs(secs));
        self.attempts = self.attempts.saturating_add(1);
    }

    async fn wait(&self) {
        match self.until {
            Some(at) => tokio::time::sleep_until(at).await,
            None => std::future::pending::<()>().await,
        }
    }
}

struct Publisher {
    host: Arc<Host>,
    source: Source,
    base: PathBuf,
    stream_id: Id,
}

pub(crate) fn preflight(home: &Path) -> Result<()> {
    let auto = store::read::<serde_json::Value>(&Store::base(home).join(store::AUTOMATIC_FILE))?;
    if auto.is_some() || Store::active(home)?.is_some() {
        return Err(BackupError::Identity);
    }
    Ok(())
}

pub(crate) fn spawn(
    host: Arc<Host>,
    shutdown: Arc<AtomicBool>,
    identity: PublicationSource,
    client: impl Fn() -> std::result::Result<ByteBackupClient, String> + Send + Sync + 'static,
) -> Result<tokio::task::JoinHandle<()>> {
    let stream_id = identity.id.clone();
    let source = Source::from_publication(&host.home, host.db.db_ops().native_runtime(), identity)?;
    let publisher = Publisher {
        base: Store::publication_base(&host.home),
        host,
        source,
        stream_id,
    };
    publisher.policy()?;
    if !publisher.source.runtime.mutation_log_status()?.enabled {
        publisher.source.runtime.configure_mutation_log(true)?;
    }
    Ok(tokio::spawn(
        async move { publisher.run(&shutdown, &client).await }.in_current_span(),
    ))
}

impl Publisher {
    async fn run(&self, shutdown: &AtomicBool, client: &Client) {
        let mut jobs = self.host.byte_backup.subscribe();
        let mut writes = self.source.runtime.subscribe_durable_writes();
        let (mut retry, mut cached, mut last_token) = (Retry::default(), None, 0);
        loop {
            let current = jobs.borrow_and_update().clone();
            if shutdown.load(Ordering::Acquire) || current.shutting_down {
                break;
            }
            if let Some(done) = current
                .last_completion
                .filter(|d| d.invocation.token > last_token)
            {
                last_token = done.invocation.token;
                match done.outcome {
                    InvocationOutcome::Complete => retry = Retry::default(),
                    InvocationOutcome::Paused(s) => retry.schedule(format_args!("{:?}", s.reason)),
                }
            }
            if retry.until.is_none_or(|at| at <= Instant::now()) {
                retry.until = None;
                if current.active.is_none() {
                    match self
                        .limit(&mut cached, client)
                        .await
                        .and_then(|l| self.tick(client, l))
                    {
                        Ok(true) => continue,
                        Ok(false) => {}
                        Err(e) => retry.schedule(e),
                    }
                }
            }
            tokio::select! {
                r = jobs.changed() => if r.is_err() { break; },
                r = writes.changed(), if retry.until.is_none() => if r.is_err() { break; },
                _ = retry.wait() => {}
            }
        }
    }

    async fn limit(
        &self,
        cached: &mut Option<(Stream, String, Option<u8>, Instant)>,
        client: &Client,
    ) -> Result<Option<u8>> {
        let need = if let Some(p) = self.policy()?.pending {
            p.log.is_some()
                && store::state_at(&self.base, &p.id)?
                    .is_none_or(|s| !s.begin_submitted && s.local_action.is_none() && !s.terminal())
        } else if let Some(id) = Store::active_at(&self.base)? {
            store::state_at(&self.base, &id)?
                .ok_or(BackupError::Missing)?
                .terminal()
        } else {
            true
        };
        if !need {
            return Ok(None);
        }
        let c = client().map_err(|_| BackupError::Authority)?;
        publication::share_capability_limit(&self.base, &self.source, &self.stream_id, cached, &c)
            .await
    }

    fn tick(&self, client: &Client, limit: Option<u8>) -> Result<bool> {
        let rt = &self.host.byte_backup;
        let _transition = rt.transition()?;
        rt.check_admission()?;
        if rt.snapshot().active.is_some() {
            return Ok(false);
        }
        if self.source.runtime.requires_recovery() {
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
            Store::write_configuration_at(&self.base, POLICY, &policy)?;
        }
        if policy.pending.is_some() {
            return self.advance(&mut policy, active.as_ref(), client, limit);
        }
        if let Some(id) = &active {
            let saved = store::state_at(&self.base, id)?.ok_or(BackupError::Missing)?;
            self.source.validate(&saved)?;
            if !saved.terminal() {
                return self.close_legacy(saved, client);
            }
        }
        let Some((minimum, log)) =
            publication::next_share_work(&self.base, &self.source, &self.stream_id, limit)?
        else {
            return Ok(false);
        };
        policy.pending = Some(Pending {
            id: automatic::request_id()?,
            minimum,
            log,
        });
        policy.cleanup = active.filter(automatic::reserved);
        Store::write_configuration_at(&self.base, POLICY, &policy).map(|()| true)
    }

    fn start_worker(
        &self,
        store: Store,
        saved: Saved,
        client: Option<ByteBackupClient>,
        permit: std::fs::File,
        select: bool,
    ) -> Result<bool> {
        let id = saved.request_id.clone();
        if select {
            store.select(&id)?;
        }
        let source = self.source.clone();
        self.host
            .byte_backup
            .start(id, move |pause| {
                execute(&source, &store, saved, client.as_ref(), permit, &pause)
            })
            .map(|()| false)
    }

    fn close_legacy(&self, saved: Saved, client: &Client) -> Result<bool> {
        let id = saved.request_id.clone();
        if automatic::reserved(&id) || saved.automatic_revision.is_some() {
            return Err(BackupError::Busy);
        }
        let permit = Store::lock_at(&self.base)?;
        let store = Store::open_at(&self.base, &id)?;
        let client = if let Some(term) = store.read::<JobResult>("terminal.json")? {
            worker::validate_terminal(&saved, &term).map(|()| None)?
        } else {
            let c = client().map_err(|_| BackupError::Authority)?;
            if saved.endpoint != c.canonical_endpoint() {
                return Err(BackupError::Identity);
            }
            store::close_at(&self.base, &id).map(|()| Some(c))?
        };
        self.start_worker(store, saved, client, permit, false)
    }

    fn advance(
        &self,
        policy: &mut Policy,
        active: Option<&Id>,
        client: &Client,
        limit: Option<u8>,
    ) -> Result<bool> {
        let pending = policy.pending.as_ref().ok_or(BackupError::Missing)?;
        let (id, minimum, work) = (pending.id.clone(), pending.minimum, pending.work());
        let permit = Store::lock_at(&self.base)?;
        control::ensure_previous_at(&self.base, &id)?;
        let store = Store::open_at(&self.base, &id)?;
        let mut existing = store.state()?;
        if let Some(saved) = &existing {
            self.source.validate(saved)?;
            if saved.automatic_revision != Some(REVISION) || saved.work != work {
                return Err(BackupError::Identity);
            }
            if saved.terminal() {
                if active != Some(&id) {
                    return Err(BackupError::Identity);
                }
                let published = publication::verify_publication_terminal(
                    saved,
                    &store,
                    &self.base,
                    &self.stream_id,
                    &self.source.account,
                    minimum,
                )?;
                policy.pending = None;
                Store::write_configuration_at(&self.base, POLICY, policy)?;
                return published.then_some(true).ok_or(BackupError::Waiting);
            }
        }
        if let Some(term) = store.read::<JobResult>("terminal.json")? {
            let saved = existing.ok_or(BackupError::Missing)?;
            worker::validate_terminal(&saved, &term)?;
            return self.start_worker(store, saved, None, permit, true);
        }
        if let Some(saved) = existing.as_mut().filter(|s| s.local_action.is_some()) {
            return publication::replace_unmaterialized(&self.source, &store, saved, None);
        }
        let client = client().map_err(|_| BackupError::Authority)?;
        let ep = client.canonical_endpoint();
        let mut saved = existing.unwrap_or_else(|| {
            let mut s = self.source.saved(id, ep.clone());
            (s.work, s.automatic_revision) = (work, Some(REVISION));
            s
        });
        if saved.endpoint != ep {
            return Err(BackupError::Identity);
        }
        if publication::replace_unmaterialized(&self.source, &store, &mut saved, limit)? {
            return Ok(true);
        }
        saved.blocked_reason = None;
        store.save(&saved)?;
        self.start_worker(store, saved, Some(client), permit, true)
    }

    fn policy(&self) -> Result<Policy> {
        let p: Policy = store::read(&self.base.join(POLICY))?.unwrap_or_else(|| Policy {
            version: 1,
            source_home: self.source.source_home.clone(),
            stream_id: self.stream_id.clone(),
            pending: None,
            cleanup: None,
        });
        (p.version == 1
            && p.source_home == self.source.source_home
            && p.stream_id == self.stream_id
            && p.pending
                .as_ref()
                .is_none_or(|x| automatic::reserved(&x.id))
            && p.cleanup.as_ref().is_none_or(automatic::reserved))
        .then_some(p)
        .ok_or(BackupError::Identity)
    }
}
