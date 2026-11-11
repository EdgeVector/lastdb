//! Backup V2 recovery points for one running publication home.
mod tail;

use super::{
    automatic, control, invocation,
    model::{Saved, StreamRecord},
    source::{PublicationSource, Source},
    store::{self, Store},
    work::{LogContext, Work},
    worker, BackupError, Result,
};
use crate::backup_runtime::{InvocationOutcome, RunOrigin};
use crate::cloud_backup_client::byte_backup::ByteBackupClient;
use crate::host::Host;
use cloud_backup_protocol::v2::{
    context::ContextReadRequest, jobs::JobResult, Destination, Stream, Version,
};
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
        if let Some(context) = &self.log {
            Work::MutationLog {
                context: context.clone(),
            }
        } else {
            Work::Snapshot {
                minimum: Some(self.minimum),
            }
        }
    }
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

pub(crate) fn spawn(
    host: Arc<Host>,
    shutdown: Arc<AtomicBool>,
    identity: PublicationSource,
    client: impl Fn() -> std::result::Result<ByteBackupClient, String> + Send + Sync + 'static,
) -> Result<tokio::task::JoinHandle<()>> {
    let source = Source::from_publication(&host.home, host.db.db_ops().native_runtime(), identity)?;
    let publisher = Publisher {
        base: Store::publication_base(&host.home),
        host,
        source,
    };
    publisher.policy()?;
    if !publisher.source.runtime().mutation_log_status()?.enabled {
        publisher.source.runtime().configure_mutation_log(true)?;
    }
    Ok(tokio::spawn(
        async move {
            publisher.run(&shutdown, &client).await;
        }
        .in_current_span(),
    ))
}

impl Publisher {
    async fn run(&self, shutdown: &AtomicBool, client: &Client) {
        let mut jobs = self.host.byte_backup.subscribe();
        let mut writes = self.source.runtime().subscribe_durable_writes();
        let mut retry = tail::Retry::default();
        let mut cached_limit: Option<(Stream, String, Option<u8>, Instant)> = None;
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
                        InvocationOutcome::Complete => retry = tail::Retry::default(),
                        InvocationOutcome::Paused(state) => {
                            retry.schedule(format_args!("{:?}", state.reason));
                        }
                    }
                }
            }
            if retry.ready() {
                retry.until = None;
                if current.active.is_none() {
                    let result = self
                        .capability_limit(&mut cached_limit, client)
                        .await
                        .and_then(|limit| self.tick(client, limit));
                    match result {
                        Ok(true) => continue,
                        Ok(false) => {}
                        Err(error) => retry.schedule(error),
                    }
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

    async fn capability_limit(
        &self,
        cached: &mut Option<(Stream, String, Option<u8>, Instant)>,
        client: &Client,
    ) -> Result<Option<u8>> {
        if !self.needs_limit()? {
            return Ok(None);
        }
        let client = client().map_err(|_| BackupError::Authority)?;
        let endpoint = client.canonical_endpoint();
        let stream = self.stream(&client).await?;
        if let Some((saved_stream, saved_endpoint, limit, expires)) = cached.as_ref() {
            if *saved_stream == stream && *saved_endpoint == endpoint && Instant::now() < *expires {
                return Ok(*limit);
            }
        }
        let limit = client.share_recovery_limit(&stream).await?;
        *cached = Some((stream, endpoint, limit, Instant::now() + CAPABILITY_TTL));
        Ok(limit)
    }

    fn needs_limit(&self) -> Result<bool> {
        if let Some(pending) = self.policy()?.pending {
            return Ok(pending.log.is_some()
                && store::state_at(&self.base, &pending.id)?.is_none_or(|saved| {
                    !saved.begin_submitted && saved.local_action.is_none() && !saved.terminal()
                }));
        }
        if let Some(id) = Store::active_at(&self.base)? {
            return Ok(store::state_at(&self.base, &id)?
                .ok_or(BackupError::Missing)?
                .terminal());
        }
        Ok(true)
    }

    async fn stream(&self, client: &ByteBackupClient) -> Result<Stream> {
        let stream_id = self.stream_id()?;
        let stream =
            if let Some(record) = store::read::<StreamRecord>(&self.base.join("stream.json"))? {
                if record.source_home != self.source.source_home {
                    return Err(BackupError::Identity);
                }
                record.stream
            } else {
                let context = client
                    .context(&ContextReadRequest {
                        version: Version::V2,
                        account_id: self.source.account.clone(),
                        required_destinations: vec![Destination::R2, Destination::B2],
                    })
                    .await?;
                Stream {
                    scope: context.scope,
                    stream_id: stream_id.clone(),
                }
            };
        stream.validate()?;
        if &stream.stream_id != stream_id || stream.scope.account_id != self.source.account {
            return Err(BackupError::Identity);
        }
        Ok(stream)
    }

    fn tick(&self, client: &Client, limit: Option<u8>) -> Result<bool> {
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
            return self.advance(&mut policy, active.as_ref(), client, limit);
        }
        if let Some(id) = &active {
            let saved = store::state_at(&self.base, id)?.ok_or(BackupError::Missing)?;
            self.source.validate(&saved)?;
            if !saved.terminal() {
                return self.close_legacy(saved, client);
            }
        }
        let Some(pending) = self.next_pending(limit)? else {
            return Ok(false);
        };
        policy.pending = Some(pending);
        policy.cleanup = active.filter(automatic::reserved);
        self.save(&policy)?;
        Ok(true)
    }

    fn close_legacy(&self, saved: Saved, client: &Client) -> Result<bool> {
        let id = saved.request_id.clone();
        if automatic::reserved(&id) || saved.automatic_revision.is_some() {
            return Err(BackupError::Busy);
        }
        let permit = Store::lock_at(&self.base)?;
        let store = Store::open_at(&self.base, &id)?;
        let source = self.source.clone();
        if let Some(terminal) = store.read::<JobResult>("terminal.json")? {
            worker::validate_terminal(&saved, &terminal)?;
            self.host
                .byte_backup
                .start(id, RunOrigin::Manual, move |pause| {
                    invocation::execute(source, store, saved, None, permit, pause)
                })?;
        } else {
            let client = client().map_err(|_| BackupError::Authority)?;
            if saved.endpoint != client.canonical_endpoint() {
                return Err(BackupError::Identity);
            }
            store::close_at(&self.base, &id)?;
            self.host
                .byte_backup
                .start(id, RunOrigin::Manual, move |pause| {
                    invocation::execute(source, store, saved, Some(client), permit, pause)
                })?;
        }
        Ok(false)
    }

    fn advance(
        &self,
        policy: &mut Policy,
        active: Option<&Id>,
        client: &Client,
        limit: Option<u8>,
    ) -> Result<bool> {
        let pending = policy.pending.as_ref().ok_or(BackupError::Missing)?;
        let id = &pending.id;
        let permit = Store::lock_at(&self.base)?;
        control::ensure_previous_at(&self.base, id)?;
        let store = Store::open_at(&self.base, id)?;
        let mut existing = store.state()?;
        let work = pending.work();
        if let Some(saved) = &existing {
            self.source.validate(saved)?;
            if saved.automatic_revision != Some(REVISION) || saved.work != work {
                return Err(BackupError::Identity);
            }
            if saved.terminal() {
                if active != Some(id) {
                    return Err(BackupError::Identity);
                }
                let published = self.verify_terminal(saved, &store, pending.minimum)?;
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
                invocation::execute(source, store, saved, None, permit, pause)
            })?;
            return Ok(false);
        }
        if let Some(saved) = existing
            .as_mut()
            .filter(|saved| saved.local_action.is_some())
        {
            return self.replace_unmaterialized(&store, saved, None);
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
        if self.replace_unmaterialized(&store, &mut saved, limit)? {
            return Ok(true);
        }
        saved.blocked_reason = None;
        store.save(&saved)?;
        store.select(&id)?;
        self.host.byte_backup.start(id, origin, move |pause| {
            invocation::execute(source, store, saved, Some(client), permit, pause)
        })?;
        Ok(false)
    }
}
