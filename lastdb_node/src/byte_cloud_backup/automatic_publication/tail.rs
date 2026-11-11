//! Pending work selection, terminal checks, policy store, and retry timer for publication.
use super::super::{
    automatic,
    model::Saved,
    recovery::RecoveryPoint,
    store::{self, Store},
    work::Work,
    worker, BackupError, Phase, Result,
};
use super::{Pending, Policy, Publisher, POLICY};
use cloud_backup_protocol::v2::jobs::JobResult;
use cloud_backup_protocol::Id;
use fold_db::storage::disk_pack::mutation_log::Frontiers;
use std::time::Duration;
use tokio::time::Instant;

impl Publisher {
    pub(super) fn verify_terminal(
        &self,
        saved: &Saved,
        store: &Store,
        minimum: Frontiers,
    ) -> Result<bool> {
        match saved.phase {
            Phase::Published => {
                let point = self.point()?.ok_or(BackupError::Missing)?;
                if saved.accepted.as_ref() != Some(&point.accepted)
                    || point.frontiers.main < minimum.main
                {
                    return Err(BackupError::Identity);
                }
                Ok(true)
            }
            Phase::Closed if saved.local_action.is_some() => Ok(true),
            Phase::Closed => {
                let terminal: JobResult =
                    store.read("terminal.json")?.ok_or(BackupError::Missing)?;
                worker::validate_terminal(saved, &terminal)?;
                if !worker::source_unavailable(&terminal)
                    || store.read::<Id>("close.json")?.is_some()
                {
                    return Err(BackupError::Authority);
                }
                Ok(false)
            }
            _ => Err(BackupError::Invalid),
        }
    }

    pub(super) fn next_pending(&self, limit: Option<u8>) -> Result<Option<Pending>> {
        let runtime = self.source.runtime();
        let frontier = runtime.durable_main_frontier();
        let point = self.point()?;
        if point.as_ref().is_some_and(|p| p.frontiers.main > frontier) {
            return Err(BackupError::Identity);
        }
        let mut minimum = Frontiers { main: frontier };
        let mut log = None;
        let journal = runtime.mutation_log_status()?;
        if !journal.enabled {
            runtime.configure_mutation_log(true)?;
        }
        let anchored = point.as_ref().filter(|p| {
            journal.enabled
                && !journal.snapshot_required
                && p.anchor.is_some()
                && journal.anchor == p.anchor
        });
        if let (Some(limit), Some(point)) = (limit, anchored) {
            if point.frontiers.main == frontier {
                return Ok(None);
            }
            if point.accepted.ordinal.get() - point.base.ordinal.get() < u64::from(limit) {
                let context = point.log_context()?;
                if journal
                    .head
                    .is_some_and(|head| head.sequence > context.from.sequence)
                {
                    minimum.main = point.frontiers.main + 1;
                    log = Some(Box::new(context));
                }
            }
        } else if anchored.is_some_and(|p| p.base == p.accepted && p.frontiers.main == frontier) {
            return Ok(None);
        }
        Ok(Some(Pending {
            id: automatic::request_id()?,
            minimum,
            log,
        }))
    }

    fn point(&self) -> Result<Option<RecoveryPoint>> {
        let point: Option<RecoveryPoint> = store::read(&self.base.join("recovery.json"))?;
        if let Some(point) = &point {
            point.validate()?;
            let stream = &point.accepted.job().stream;
            let deps = &point.accepted.begin.recovery_dependencies;
            if &stream.stream_id != self.stream_id()?
                || stream.scope.account_id != self.source.account
                || !point.base.begin.recovery_dependencies.is_empty()
                || (point.base != point.accepted
                    && (point.anchor.is_none()
                        || deps.first() != Some(&point.base.head())
                        || deps.last() != point.accepted.begin.predecessor.as_ref()))
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
            return if limit.is_some() {
                Ok(false)
            } else {
                Err(BackupError::AdmissionPending)
            };
        }
        self.source.replace_with_snapshot(store, saved)?;
        Ok(true)
    }

    pub(super) fn stream_id(&self) -> Result<&Id> {
        self.source
            .publication_stream
            .as_ref()
            .ok_or(BackupError::Identity)
    }

    pub(super) fn policy(&self) -> Result<Policy> {
        let stream_id = self.stream_id()?;
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

    pub(super) fn save(&self, policy: &Policy) -> Result<()> {
        Store::write_configuration_at(&self.base, POLICY, policy)
    }
}

#[derive(Default)]
pub(super) struct Retry {
    pub(super) until: Option<Instant>,
    attempts: u32,
}

impl Retry {
    pub(super) fn schedule(&mut self, error: impl std::fmt::Display) {
        if self.attempts == 0 {
            tracing::warn!(target: "lastdbd::share", %error, "share publication will retry");
        }
        let delay = (5u64 << self.attempts.min(4)).min(60);
        self.until = Some(Instant::now() + Duration::from_secs(delay));
        self.attempts = self.attempts.saturating_add(1);
    }

    pub(super) fn ready(&self) -> bool {
        self.until.is_none_or(|at| at <= Instant::now())
    }

    pub(super) async fn wait(&self) {
        if let Some(at) = self.until {
            tokio::time::sleep_until(at).await;
        } else {
            std::future::pending::<()>().await;
        }
    }
}
