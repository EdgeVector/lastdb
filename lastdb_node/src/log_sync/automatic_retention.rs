//! Resume covered personal delivery and provider retirement work.

use std::fs::{self, File};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use cloud_backup_protocol::{CloudScope, Digest};
use serde::{Deserialize, Serialize};

use super::{
    cloud::CloudLog,
    disk, retention, session,
    snapshot::{transport, Manifest},
};

const RETRY: Duration = Duration::from_secs(30);
const PAUSED: Duration = Duration::from_secs(60);

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Covered {
    snapshot_id: Digest,
    through: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Work {
    version: u8,
    scope: CloudScope,
    environment: String,
    database_id: String,
    device: String,
    active: Option<Covered>,
    planned: Option<Covered>,
}

impl Work {
    fn new(log: &CloudLog) -> Self {
        Self {
            version: 1,
            scope: log.scope.clone(),
            environment: log.environment().to_owned(),
            database_id: log.database_id().to_owned(),
            device: log.device().as_str().to_owned(),
            active: None,
            planned: None,
        }
    }

    fn read(path: &Path) -> Result<Option<Self>, String> {
        let Some(bytes) = disk::read_limited(path, 4096)
            .map_err(|_| "cannot read local snapshot cleanup state")?
        else {
            return Ok(None);
        };
        let work: Self = serde_json::from_slice(&bytes)
            .map_err(|_| "local snapshot cleanup state is invalid")?;
        if work.version != 1
            || work
                .active
                .iter()
                .chain(&work.planned)
                .any(|cut| cut.through == 0)
        {
            return Err("local snapshot cleanup state is invalid".into());
        }
        Ok(Some(work))
    }

    fn matches(&self, log: &CloudLog) -> bool {
        self.scope == log.scope
            && self.environment == log.environment()
            && self.database_id == log.database_id()
            && self.device == log.device().as_str()
    }

    fn save(&self, path: &Path) -> Result<(), String> {
        save_record(
            path,
            (self.active.is_some() || self.planned.is_some()).then_some(self),
        )
    }

    fn finish_through(
        &mut self,
        path: &Path,
        log: &CloudLog,
        provider: &mut Option<ProviderWork>,
        floor: u64,
    ) -> Result<(), String> {
        let remove_active = self.active.as_ref().is_some_and(|cut| cut.through <= floor);
        let remove_planned = self
            .planned
            .as_ref()
            .is_some_and(|cut| cut.through <= floor);
        if !remove_active && !remove_planned {
            return Ok(());
        }
        // Record follow-up work before removing the completed delivery plan.
        ProviderWork::queue(path, log, provider, floor)?;
        if remove_active {
            self.active = None;
        }
        if remove_planned {
            self.planned = None;
        }
        self.save(path)?;
        if remove_active {
            tracing::info!(target: "lastdbd::log_sync", through = floor, "snapshot log cleanup reached cutoff");
        }
        Ok(())
    }

    async fn promote(&mut self, log: &CloudLog, cloud: &Covered) -> bool {
        let Some(planned) = self
            .planned
            .as_ref()
            .filter(|plan| plan.through <= cloud.through)
        else {
            return false;
        };
        let published = planned == cloud
            || transport::exact(log, &planned.snapshot_id)
                .await
                .is_ok_and(|manifest| covers(&manifest, planned.through));
        if !published {
            return false;
        }
        if self
            .active
            .as_ref()
            .is_none_or(|active| active.through <= planned.through)
        {
            self.active = self.planned.take();
        } else {
            self.planned = None;
        }
        true
    }
}

// Keep provider progress separate so older daemons can still read snapshot-reclaim.json.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProviderWork {
    scope: CloudScope,
    environment: String,
    database_id: String,
    device: String,
    through: u64,
}

impl ProviderWork {
    fn path(delivery_path: &Path) -> PathBuf {
        delivery_path.with_file_name("snapshot-provider-cleanup.json")
    }

    fn read(delivery_path: &Path, log: &CloudLog) -> Result<Option<Self>, String> {
        let Some(bytes) = disk::read_limited(&Self::path(delivery_path), 4096)
            .map_err(|_| "cannot read local snapshot provider cleanup state")?
        else {
            return Ok(None);
        };
        let work: Self = serde_json::from_slice(&bytes)
            .map_err(|_| "local snapshot provider cleanup state is invalid")?;
        if work.through == 0 {
            return Err("local snapshot provider cleanup state is invalid".into());
        }
        Ok((work.scope == log.scope
            && work.environment == log.environment()
            && work.database_id == log.database_id()
            && work.device == log.device().as_str())
        .then_some(work))
    }

    fn queue(
        delivery_path: &Path,
        log: &CloudLog,
        work: &mut Option<Self>,
        through: u64,
    ) -> Result<(), String> {
        if work.as_ref().is_some_and(|work| work.through >= through) {
            return Ok(());
        }
        let pending = Self {
            scope: log.scope.clone(),
            environment: log.environment().to_owned(),
            database_id: log.database_id().to_owned(),
            device: log.device().as_str().to_owned(),
            through,
        };
        save_record(&Self::path(delivery_path), Some(&pending))?;
        *work = Some(pending);
        Ok(())
    }
}

fn save_record<T: Serialize>(path: &Path, record: Option<&T>) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or("snapshot cleanup state has no directory")?;
    if let Some(record) = record {
        let bytes =
            serde_json::to_vec(record).map_err(|_| "cannot encode snapshot cleanup state")?;
        disk::write_private(path, &bytes).map_err(|_| "cannot store snapshot cleanup state")?;
    } else {
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
            Err(_) => return Err("cannot remove completed local snapshot cleanup state".into()),
        }
    }
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| "cannot sync snapshot cleanup state directory".into())
}

fn path(home: &Path, log: &CloudLog) -> PathBuf {
    home.join("log-sync")
        .join(log.database_id())
        .join("snapshot-reclaim.json")
}

fn covers(manifest: &Manifest, through: u64) -> bool {
    manifest.name.is_none() && manifest.through.get() == through
}

/// Preserve the candidate before sending its publish request. Local cleanup
/// bookkeeping must never prevent a valid backup from publishing.
pub(super) fn plan(home: &Path, log: &CloudLog, manifest: &Manifest, previous: Option<&Digest>) {
    if let Err(error) = store_plan(home, log, manifest, previous) {
        tracing::warn!(target: "lastdbd::log_sync", %error, "snapshot cleanup could not be queued");
    }
}

fn store_plan(
    home: &Path,
    log: &CloudLog,
    manifest: &Manifest,
    previous: Option<&Digest>,
) -> Result<(), String> {
    let candidate = Covered {
        snapshot_id: manifest.id()?,
        through: manifest.through.get(),
    };
    let path = path(home, log);
    let mut work = Work::read(&path)?
        .filter(|work| work.matches(log))
        .unwrap_or_else(|| Work::new(log));
    if work.active.as_ref() == Some(&candidate) || work.planned.as_ref() == Some(&candidate) {
        return Ok(());
    }
    // A failed replacement never overwrites already active work. Where the
    // prior plan is now the observed backup, retain it across replacement too.
    if work.planned.as_ref().is_some_and(|prior| {
        Some(&prior.snapshot_id) == previous
            && work
                .active
                .as_ref()
                .is_none_or(|active| active.through <= prior.through)
    }) {
        work.active = work.planned.take();
    }
    work.planned = Some(candidate);
    work.save(&path)
}

/// Pace cleanup independently of upload and download. One turn requests at most
/// one bounded delivery or provider cleanup step.
#[derive(Default)]
pub(super) struct Pacing(Option<Instant>);

impl Pacing {
    pub(super) async fn turn(&mut self, home: &Path, log: &CloudLog) {
        if self.0.is_some_and(|next| next > Instant::now()) {
            return;
        }
        let delay = match run(home, log).await {
            Ok(Some(delay)) => delay,
            Ok(None) => {
                self.0 = None;
                return;
            }
            Err(error) => {
                tracing::debug!(target: "lastdbd::log_sync", %error, "snapshot cleanup awaits retry");
                RETRY
            }
        };
        self.0 = Some(Instant::now() + delay);
    }
}

async fn run(home: &Path, log: &CloudLog) -> Result<Option<Duration>, String> {
    let path = path(home, log);
    let mut work = Work::read(&path)?.filter(|work| work.matches(log));
    let mut provider = ProviderWork::read(&path, log)?;
    if work.is_none() && provider.is_none() {
        return Ok(None);
    }
    let work = work.get_or_insert_with(|| Work::new(log));
    let status = retention::automatic_status(log).await?;
    let floor = status.reclaimed_through.get();
    work.finish_through(&path, log, &mut provider, floor)?;
    let Some(snapshot_id) = status
        .snapshot_id
        .filter(|_| status.reclaim_enabled && status.eligible)
    else {
        return Ok(Some(PAUSED));
    };
    let cloud = Covered {
        snapshot_id,
        through: status.snapshot_through.get(),
    };
    if work.promote(log, &cloud).await {
        work.save(&path)?;
    }
    if let Some(floor) = reclaim_pending_log(home, log, work.active.as_ref(), &cloud, floor).await?
    {
        work.finish_through(&path, log, &mut provider, floor)?;
        return Ok(Some(Duration::ZERO));
    }
    if provider.is_none_or(|pending| floor < pending.through) {
        return Ok(Some(PAUSED));
    }
    match retention::cleanup_retired(log, &cloud.snapshot_id, floor, cloud.through).await? {
        Some(retention::ProviderCleanup::Complete) => {
            save_record::<ProviderWork>(&ProviderWork::path(&path), None)?;
            tracing::info!(target: "lastdbd::log_sync", through = floor, "retired snapshot provider cleanup completed");
            Ok(Some(Duration::ZERO))
        }
        Some(retention::ProviderCleanup::Progress) => Ok(Some(Duration::ZERO)),
        Some(retention::ProviderCleanup::Waiting) => Ok(Some(RETRY)),
        None => Ok(Some(PAUSED)),
    }
}

async fn reclaim_pending_log(
    home: &Path,
    log: &CloudLog,
    active: Option<&Covered>,
    cloud: &Covered,
    floor: u64,
) -> Result<Option<u64>, String> {
    let Some(active) = active else {
        return Ok(None);
    };
    let target = active.through.min(cloud.through);
    if floor >= target || session::load_cursor(home, log.database_id())? < active.through {
        return Ok(None);
    }
    if active != cloud
        && !covers(
            &transport::exact(log, &cloud.snapshot_id).await?,
            cloud.through,
        )
    {
        return Err("new cloud snapshot does not cover the pending cleanup".into());
    }
    // The signed recovery cut can differ from the local snapshot. Bound each
    // step by both positions and keep the original local target on disk.
    retention::reclaim_covered(log, &cloud.snapshot_id, floor, cloud.through, target)
        .await
        .map(Some)
}
