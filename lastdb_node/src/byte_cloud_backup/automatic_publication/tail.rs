use super::{
    automatic, store, BackupError, Client, Pending, Phase, Publisher, Result, Saved, Store,
};
use crate::byte_cloud_backup::{
    model::{LocalAction, StreamRecord},
    recovery::RecoveryPoint,
    work::Work,
};
use cloud_backup_client::byte_backup::ByteBackupClient;
use cloud_backup_protocol::v2::{context::ContextReadRequest, Destination, Stream, Version};
use fold_db::storage::disk_pack::mutation_log::Frontiers;
use std::time::Duration;
use tokio::time::Instant;

const CAPABILITY_TTL: Duration = Duration::from_secs(15);

struct CapabilityResult {
    stream: Stream,
    endpoint: String,
    limit: Option<u8>,
    expires: Instant,
}

#[derive(Default)]
pub(super) struct Capability(Option<CapabilityResult>);

impl Capability {
    pub(super) async fn limit(
        &mut self,
        publisher: &Publisher,
        client: &Client,
    ) -> Result<Option<u8>> {
        if !publisher.needs_limit()? {
            return Ok(None);
        }
        let client = client().map_err(|_| BackupError::Authority)?;
        let endpoint = client.canonical_endpoint();
        let stream = publisher.stream(&client).await?;
        if let Some(result) = &self.0 {
            if result.stream == stream
                && result.endpoint == endpoint
                && Instant::now() < result.expires
            {
                return Ok(result.limit);
            }
        }
        let limit = client.share_recovery_limit(&stream).await?;
        self.0 = Some(CapabilityResult {
            stream,
            endpoint,
            limit,
            expires: Instant::now() + CAPABILITY_TTL,
        });
        Ok(limit)
    }
}

impl Publisher {
    /// Settle existing snapshots and saved terminal authority without capability I/O.
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
        let stream_id = self
            .source
            .publication_stream
            .as_ref()
            .ok_or(BackupError::Identity)?;
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

    pub(super) fn next_pending(&self, limit: Option<u8>) -> Result<Option<Pending>> {
        let runtime = self.source.runtime();
        let frontier = runtime.durable_main_frontier();
        let point = self.point()?;
        if point
            .as_ref()
            .is_some_and(|point| point.frontiers.main > frontier)
        {
            return Err(BackupError::Identity);
        }
        let mut minimum = Frontiers { main: frontier };
        let mut log = None;
        let journal = runtime.mutation_log_status()?;
        if !journal.enabled {
            runtime.configure_mutation_log(true)?;
        }
        let anchored = point.as_ref().filter(|point| {
            journal.enabled
                && !journal.snapshot_required
                && point.anchor.is_some()
                && journal.anchor == point.anchor
        });
        if let (Some(limit), Some(point)) = (limit, anchored) {
            if point.frontiers.main == frontier {
                return Ok(None);
            }
            let checkpoints = point.accepted.ordinal.get() - point.base.ordinal.get();
            if checkpoints < u64::from(limit) {
                let context = point.log_context()?;
                if journal
                    .head
                    .is_some_and(|head| head.sequence > context.from.sequence)
                {
                    // A capture may split a large tail into several checkpoints.
                    minimum.main = point.frontiers.main + 1;
                    log = Some(Box::new(context));
                }
            }
        } else if anchored
            .is_some_and(|point| point.base == point.accepted && point.frontiers.main == frontier)
        {
            return Ok(None);
        }
        Ok(Some(Pending {
            id: automatic::request_id()?,
            minimum,
            log,
        }))
    }

    pub(super) fn point(&self) -> Result<Option<RecoveryPoint>> {
        let point: Option<RecoveryPoint> = store::read(&self.base.join("recovery.json"))?;
        if let Some(point) = &point {
            point.validate()?;
            let stream = &point.accepted.job().stream;
            let dependencies = &point.accepted.begin.recovery_dependencies;
            if Some(&stream.stream_id) != self.source.publication_stream.as_ref()
                || stream.scope.account_id != self.source.account
                || !point.base.begin.recovery_dependencies.is_empty()
                || (point.base != point.accepted
                    && (point.anchor.is_none()
                        || dependencies.first() != Some(&point.base.head())
                        || dependencies.last() != point.accepted.begin.predecessor.as_ref()))
            {
                return Err(BackupError::Identity);
            }
        }
        Ok(point)
    }

    pub(super) fn replace_unmaterialized(
        &self,
        store: &Store,
        saved: &mut Saved,
        limit: Option<u8>,
    ) -> Result<bool> {
        let Work::MutationLog { context } = &saved.work else {
            return Ok(false);
        };
        if saved.begin_submitted {
            return Ok(false);
        }
        let runtime = self.source.runtime();
        let replacing = saved.local_action.is_some();
        if !replacing && limit.is_some() {
            let journal = runtime.mutation_log_status()?;
            if !journal.snapshot_required && journal.anchor.as_ref() == Some(&context.anchor) {
                return Ok(false);
            }
        }
        if runtime.mutation_log_capture_materialized(&saved.request_id)? {
            if replacing {
                return Err(BackupError::Identity);
            }
            // Reuse the captured prefix when the local journal has changed, but
            // only submit a new log to a server that can serve it to recipients.
            return if limit.is_some() {
                Ok(false)
            } else {
                Err(BackupError::AdmissionPending)
            };
        }
        saved.local_action = Some(LocalAction::ReplaceWithSnapshot);
        store.save(saved)?;
        store.select(&saved.request_id)?;
        self.source.cancel_replacement(saved)?;
        saved.phase = Phase::Closed;
        saved.retained_source_bytes = Some(0);
        saved.blocked_reason = None;
        store.save(saved)?;
        Ok(true)
    }
}
