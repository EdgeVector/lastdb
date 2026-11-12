//! Backup V2 recovery points for one running publication home.
use super::{
    connection,
    source::{PublicationSource, Saved, Source},
    store::{self, request_id, reserved, retire_cleanup, Store},
    work::{LogContext, RecoveryPoint, Work},
    worker, BackupError, Phase, Result,
};
use crate::backup_runtime::InvocationOutcome;
use crate::cloud_backup_client::byte_backup::ByteBackupClient;
use crate::host::Host;
use cloud_backup_protocol::{v2::Stream, Id};
use fold_db::storage::disk_pack::mutation_log::Frontiers;
use serde::{Deserialize, Serialize};
use std::fs::File;
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
const CAPABILITY_TTL: Duration = Duration::from_secs(15);
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

struct Publisher {
    host: Arc<Host>,
    source: Source,
    base: PathBuf,
    stream_id: Id,
}

pub(crate) fn preflight(home: &Path) -> Result<()> {
    let base = Store::base(home);
    let auto = store::read::<serde_json::Value>(&base.join(store::AUTOMATIC_FILE))?;
    (auto.is_none() && Store::active(&base)?.is_none())
        .then_some(())
        .ok_or(BackupError::Identity)
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
    fn start(
        &self,
        (store, permit): (Store, File),
        saved: Saved,
        client: Option<ByteBackupClient>,
        select: bool,
    ) -> Result<bool> {
        let src = self.source.clone();
        super::spawn_job(&self.host, src, store, saved, client, permit, select).map(|()| false)
    }

    async fn run(&self, shutdown: &AtomicBool, client: &Client) {
        let mut jobs = self.host.byte_backup.subscribe();
        let mut writes = self.source.runtime.subscribe_durable_writes();
        let (mut until, mut attempts, mut cached, mut last_token) = (None, 0u32, None, 0);
        let retry = |until: &mut Option<Instant>, n: &mut u32, err: &dyn std::fmt::Display| {
            if *n == 0 {
                tracing::warn!(target: "lastdbd::share", error = %err, "share publication will retry");
            }
            *until = Some(Instant::now() + Duration::from_secs((5u64 << (*n).min(4)).min(60)));
            *n = n.saturating_add(1);
        };
        loop {
            let current = jobs.borrow_and_update().clone();
            if shutdown.load(Ordering::Acquire) || current.shutting_down {
                break;
            }
            if let Some((_, tok, out)) = current.last_completion.filter(|(_, t, _)| *t > last_token)
            {
                last_token = tok;
                match out {
                    InvocationOutcome::Complete => (until, attempts) = (None, 0),
                    InvocationOutcome::Paused(s) => {
                        retry(&mut until, &mut attempts, &format_args!("{:?}", s.reason));
                    }
                }
            }
            if until.is_none_or(|at| at <= Instant::now()) {
                until = None;
                if current.active.is_none() {
                    match self
                        .limit(&mut cached, client)
                        .await
                        .and_then(|l| self.tick(client, l))
                    {
                        Ok(true) => continue,
                        Ok(false) => {}
                        Err(e) => retry(&mut until, &mut attempts, &e),
                    }
                }
            }
            tokio::select! {
                r = jobs.changed() => if r.is_err() { break; },
                r = writes.changed(), if until.is_none() => if r.is_err() { break; },
                () = tokio::time::sleep_until(until.unwrap_or_else(Instant::now)), if until.is_some() => {}
            }
        }
    }

    async fn limit(
        &self,
        cached: &mut Option<(Stream, String, Option<u8>, Instant)>,
        client: &Client,
    ) -> Result<Option<u8>> {
        let need = match self.policy()?.pending {
            Some(p) => {
                p.log.is_some()
                    && store::state(&self.base, &p.id)?.is_none_or(|s| {
                        !s.begin_submitted && s.local_action.is_none() && !s.terminal()
                    })
            }
            None => !Store::has_active(&self.base)?,
        };
        if !need {
            return Ok(None);
        }
        let c = client().map_err(|_| BackupError::Authority)?;
        let ep = c.canonical_endpoint();
        let stream =
            connection::publication_stream(&self.base, &self.source, &self.stream_id, &c).await?;
        if let Some((s, cached_ep, lim, exp)) = cached.as_ref() {
            if *s == stream && *cached_ep == ep && Instant::now() < *exp {
                return Ok(*lim);
            }
        }
        let lim = c.share_recovery_limit(&stream).await?;
        *cached = Some((stream, ep, lim, Instant::now() + CAPABILITY_TTL));
        Ok(lim)
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
        if retire_cleanup(&self.base, &mut policy.cleanup)? {
            Store::write_config(&self.base, POLICY, &policy)?;
        }
        let active = Store::active(&self.base)?;
        if policy.pending.is_some() {
            return self.advance(&mut policy, active.as_ref(), client, limit);
        }
        if let Some(id) = &active {
            let saved = store::state(&self.base, id)?.ok_or(BackupError::Missing)?;
            self.source.validate(&saved)?;
            if !saved.terminal() {
                return self.close_legacy(saved, client);
            }
        }
        let Some((minimum, log)) = self.next_work(limit)? else {
            return Ok(false);
        };
        policy.pending = Some(Pending {
            id: request_id()?,
            minimum,
            log,
        });
        policy.cleanup = active.filter(reserved);
        Store::write_config(&self.base, POLICY, &policy).map(|()| true)
    }

    fn close_legacy(&self, saved: Saved, client: &Client) -> Result<bool> {
        let id = saved.request_id.clone();
        if reserved(&id) || saved.automatic_revision.is_some() {
            return Err(BackupError::Busy);
        }
        let permit = Store::lock(&self.base)?;
        let store = Store::open(&self.base, &id)?;
        let client = if worker::reconcilable(&self.source, &store)?.is_some() {
            None
        } else {
            let c = client().map_err(|_| BackupError::Authority)?;
            if saved.endpoint != c.canonical_endpoint() {
                return Err(BackupError::Identity);
            }
            store::close(&self.base, &id).map(|()| Some(c))?
        };
        self.start((store, permit), saved, client, false)
    }

    fn advance(
        &self,
        policy: &mut Policy,
        active: Option<&Id>,
        client: &Client,
        limit: Option<u8>,
    ) -> Result<bool> {
        let p = policy.pending.as_ref().ok_or(BackupError::Missing)?;
        let (id, minimum, work) = (p.id.clone(), p.minimum, p.work());
        let job = Store::acquire_job(&self.base, &id)?;
        let mut existing = job.0.state()?;
        if let Some(saved) = &existing {
            self.source.validate(saved)?;
            if saved.automatic_revision != Some(REVISION) || saved.work != work {
                return Err(BackupError::Identity);
            }
            if saved.terminal() {
                if active != Some(&id) {
                    return Err(BackupError::Identity);
                }
                let ok = self.verify_terminal(saved, &job.0, minimum)?;
                policy.pending = None;
                Store::write_config(&self.base, POLICY, policy)?;
                return ok.then_some(true).ok_or(BackupError::Waiting);
            }
        }
        if let Some(saved) = worker::reconcilable(&self.source, &job.0)? {
            return self.start(job, saved, None, true);
        }
        if let Some(saved) = existing.as_mut().filter(|s| s.local_action.is_some()) {
            return self.source.replace_unmaterialized(&job.0, saved, None);
        }
        let c = client().map_err(|_| BackupError::Authority)?;
        let ep = c.canonical_endpoint();
        let mut saved = self
            .source
            .init_or_validate(existing, id, &ep, Some((REVISION, &work)))?;
        if self
            .source
            .replace_unmaterialized(&job.0, &mut saved, limit)?
        {
            return Ok(true);
        }
        saved.blocked_reason = None;
        job.0.save(&saved)?;
        self.start(job, saved, Some(c), true)
    }

    fn verify_terminal(&self, saved: &Saved, store: &Store, minimum: Frontiers) -> Result<bool> {
        match saved.phase {
            Phase::Published => {
                let scope = (&self.stream_id, &self.source.account);
                let pt =
                    RecoveryPoint::read_at(&self.base, Some(scope))?.ok_or(BackupError::Missing)?;
                (saved.accepted.as_ref() == Some(&pt.accepted) && pt.frontiers.main >= minimum.main)
                    .then_some(true)
                    .ok_or(BackupError::Identity)
            }
            Phase::Closed if saved.local_action.is_some() => Ok(true),
            Phase::Closed => worker::unclosed_source_unavailable(store, saved)?
                .ok_or(BackupError::Missing)?
                .then_some(false)
                .ok_or(BackupError::Authority),
            _ => Err(BackupError::Invalid),
        }
    }

    fn next_work(&self, limit: Option<u8>) -> Result<Option<(Frontiers, Option<Box<LogContext>>)>> {
        let rt = &self.source.runtime;
        let scope = (&self.stream_id, &self.source.account);
        let point = RecoveryPoint::read_at(&self.base, Some(scope))?;
        let frontier = rt.durable_main_frontier();
        if point.as_ref().is_some_and(|p| p.frontiers.main > frontier) {
            return Err(BackupError::Identity);
        }
        let (mut minimum, mut log) = (Frontiers { main: frontier }, None);
        let journal = rt.mutation_log_status()?;
        if !journal.enabled {
            rt.configure_mutation_log(true)?;
        }
        let anchored = point.as_ref().filter(|p| {
            journal.enabled
                && !journal.snapshot_required
                && p.anchor.is_some()
                && journal.anchor == p.anchor
        });
        if let (Some(limit), Some(p)) = (limit, anchored) {
            if p.frontiers.main == frontier {
                return Ok(None);
            }
            if p.accepted.ordinal.get() - p.base.ordinal.get() < u64::from(limit) {
                let ctx = p.log_context()?;
                if journal.head.is_some_and(|h| h.sequence > ctx.from.sequence) {
                    minimum.main = p.frontiers.main + 1;
                    log = Some(Box::new(ctx));
                }
            }
        } else if anchored.is_some_and(|p| p.base == p.accepted && p.frontiers.main == frontier) {
            return Ok(None);
        }
        Ok(Some((minimum, log)))
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
            && p.pending.as_ref().is_none_or(|x| reserved(&x.id))
            && p.cleanup.as_ref().is_none_or(reserved))
        .then_some(p)
        .ok_or(BackupError::Identity)
    }
}
