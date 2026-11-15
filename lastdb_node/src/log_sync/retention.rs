//! Owner access to bounded reclamation of log entries covered by a native snapshot.

use cloud_backup_protocol::{DecimalU64, Digest, Id};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::Path;

use super::{cloud::CloudLog, maintenance, snapshot::transport};

const MAX_BATCHES: u8 = 128;
const RECLAIM_BATCH: u64 = 8;
const MAX_STEPS: u8 = 8;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Status {
    pub(super) reclaim_enabled: bool,
    pub(super) eligible: bool,
    pub(super) snapshot_id: Option<Digest>,
    pub(super) snapshot_through: DecimalU64,
    pub(super) reclaimed_through: DecimalU64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Reclaimed {
    snapshot_id: Digest,
    reclaimed_through: DecimalU64,
    snapshot_through: DecimalU64,
}

#[derive(Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum ProviderCleanup {
    Progress,
    Waiting,
    Complete,
}

impl ProviderCleanup {
    fn name(&self) -> &'static str {
        match self {
            Self::Progress => "progress",
            Self::Waiting => "waiting",
            Self::Complete => "complete",
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Cleaned {
    snapshot_id: Digest,
    reclaimed_through: DecimalU64,
    snapshot_through: DecimalU64,
    state: ProviderCleanup,
}

/// Signed snapshot and retention positions for the selected cloud database.
#[derive(Serialize)]
pub struct ReclaimReport {
    /// Saved service environment for this home.
    pub environment: &'static str,
    /// Stable identifier of the personal database.
    pub database_id: Id,
    /// Whether the cloud allows physical reclamation in this environment.
    pub reclaim_enabled: bool,
    /// Whether the server can verify this database's writer history.
    pub eligible: bool,
    /// Native snapshot selected as the recovery baseline, if one exists.
    pub snapshot_id: Option<Digest>,
    /// Last delivery sequence covered by that snapshot.
    pub snapshot_through: DecimalU64,
    /// Last delivery sequence whose original log entry has been reclaimed.
    pub reclaimed_through: DecimalU64,
    /// Confirmed delivery reclamation requests in this invocation.
    pub batches: u8,
    /// Provider cleanup state confirmed by this invocation, when supported.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_cleanup: Option<&'static str>,
}

/// Read retention status, or explicitly reclaim a bounded prefix for one snapshot.
/// `reclaim_batches` is absent for a read, or the maximum number of signed requests.
/// `expected_snapshot` pins the recovery snapshot across repeated invocations.
/// `steps_per_request` opts into several bounded transactions per request.
pub async fn reclaim(
    home: &Path,
    database: &str,
    reclaim_batches: Option<u8>,
    expected_snapshot: Option<&Digest>,
    steps_per_request: Option<u8>,
) -> Result<ReclaimReport, String> {
    if reclaim_batches.is_some_and(|limit| !(1..=MAX_BATCHES).contains(&limit)) {
        return Err("reclaim batches must be between 1 and 128".into());
    }
    if steps_per_request.is_some_and(|steps| !(1..=MAX_STEPS).contains(&steps)) {
        return Err("reclaim steps per request must be between 1 and 8".into());
    }
    if steps_per_request.is_some() && (expected_snapshot.is_none() || reclaim_batches.is_none()) {
        return Err(
            "reclaim steps per request require a pinned snapshot and explicit write mode".into(),
        );
    }
    let (database_id, log) = maintenance::personal_log(home, database).await?;
    let status = status(&log, "retention").await?;
    if expected_snapshot.is_some_and(|id| status.snapshot_id.as_ref() != Some(id)) {
        return Err("the cloud recovery snapshot differs from --expect-snapshot".into());
    }
    let mut report = ReclaimReport {
        environment: log.environment(),
        database_id,
        reclaim_enabled: status.reclaim_enabled,
        eligible: status.eligible,
        snapshot_id: status.snapshot_id,
        snapshot_through: status.snapshot_through,
        reclaimed_through: status.reclaimed_through,
        batches: 0,
        provider_cleanup: None,
    };
    if let Some(batches) = reclaim_batches {
        reclaim_batches_for_snapshot(&log, &mut report, batches, steps_per_request).await?;
    }
    Ok(report)
}

pub(super) async fn automatic_status(log: &CloudLog) -> Result<Status, String> {
    status(log, "automatic-retention").await
}

async fn status(log: &CloudLog, action: &str) -> Result<Status, String> {
    let reply = transport::call(
        log,
        json!({"action": action, "database_id": log.database_id()}),
    )
    .await?;
    let status: Status =
        serde_json::from_value(reply).map_err(|_| "invalid signed retention response")?;
    if status.reclaimed_through.get() > status.snapshot_through.get()
        || (status.snapshot_id.is_none() && status.snapshot_through.get() != 0)
    {
        return Err("cloud retention positions are inconsistent".into());
    }
    Ok(status)
}

async fn reclaim_batches_for_snapshot(
    log: &CloudLog,
    report: &mut ReclaimReport,
    batches: u8,
    steps_per_request: Option<u8>,
) -> Result<(), String> {
    if !report.reclaim_enabled {
        return Err("cloud log reclamation is disabled for this environment".into());
    }
    if !report.eligible {
        return Err("this database's cloud history is ineligible for reclamation".into());
    }
    let id = report
        .snapshot_id
        .as_ref()
        .ok_or("cloud log reclamation requires a verified native recovery snapshot")?;
    let through = report.snapshot_through.get();
    let mut floor = report.reclaimed_through.get();
    if through == 0 {
        return Ok(());
    }
    let manifest = transport::exact(log, id).await?;
    if manifest.through.get() != through || manifest.name.is_some() {
        return Err("the cloud could not verify the selected native recovery snapshot".into());
    }
    for _ in 0..batches {
        if floor == through {
            let Some(cleaned) = cleanup_retired(log, id, floor, through).await? else {
                break;
            };
            report.provider_cleanup = Some(cleaned.name());
            if cleaned == ProviderCleanup::Progress {
                continue;
            }
            break;
        }
        let window = Window::Explicit(steps_per_request);
        floor = reclaim_batch(log, id, floor, through, window).await.map_err(|error| {
            format!("{error}. Last confirmed reclamation was through {floor}. Use backup reclaim --status to read the latest position before retrying")
        })?;
        report.reclaimed_through = DecimalU64::new(floor);
        report.batches += 1;
    }
    Ok(())
}

pub(super) async fn cleanup_retired(
    log: &CloudLog,
    id: &Digest,
    floor: u64,
    through: u64,
) -> Result<Option<ProviderCleanup>, String> {
    let response = transport::call_if_supported(
        log,
        json!({
            "action": "cleanup-retired",
            "database_id": log.database_id(),
            "snapshot_id": id,
            "expected_floor": DecimalU64::new(floor),
        }),
    )
    .await?;
    let Some(response) = response else {
        return Ok(None);
    };
    let response: Cleaned =
        serde_json::from_value(response).map_err(|_| "invalid signed provider cleanup response")?;
    if &response.snapshot_id != id
        || response.snapshot_through.get() != through
        || response.reclaimed_through.get() != floor
    {
        return Err(
            "cloud provider cleanup did not confirm the requested snapshot and floor".into(),
        );
    }
    Ok(Some(response.state))
}

enum Window {
    Explicit(Option<u8>),
    Automatic(u64),
}

pub(super) async fn reclaim_covered(
    log: &CloudLog,
    id: &Digest,
    floor: u64,
    through: u64,
    target: u64,
) -> Result<u64, String> {
    reclaim_batch(log, id, floor, through, Window::Automatic(target)).await
}

async fn reclaim_batch(
    log: &CloudLog,
    id: &Digest,
    floor: u64,
    through: u64,
    window: Window,
) -> Result<u64, String> {
    let (action, steps, target) = match window {
        Window::Explicit(steps) => ("reclaim", steps, through),
        Window::Automatic(target) => ("reclaim-covered", Some(MAX_STEPS), target),
    };
    let mut body = json!({
        "action": action,
        "database_id": log.database_id(),
        "snapshot_id": id,
        "expected_floor": DecimalU64::new(floor),
        "require_native_recovery": true,
    });
    if let Some(steps) = steps {
        body["max_steps"] = json!(steps);
        body["target_through"] = json!(DecimalU64::new(target));
    }
    let response = transport::call(log, body).await?;
    let response: Reclaimed =
        serde_json::from_value(response).map_err(|_| "invalid signed reclaim response")?;
    let reclaimed = response.reclaimed_through.get();
    let max_advance = RECLAIM_BATCH * u64::from(steps.unwrap_or(1));
    let bound = floor.saturating_add(max_advance).min(target);
    if &response.snapshot_id != id
        || response.snapshot_through.get() != through
        || reclaimed <= floor
        || reclaimed > bound
        || (steps.is_none() && reclaimed != bound)
    {
        return Err(
            "cloud reclaim response did not confirm the requested snapshot and range".into(),
        );
    }
    Ok(reclaimed)
}
