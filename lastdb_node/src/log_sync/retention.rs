//! Owner access to bounded reclamation of log entries covered by a native snapshot.

use cloud_backup_protocol::{DecimalU64, Digest, Id};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::Path;

use super::{cloud::CloudLog, maintenance, snapshot::transport};

const MAX_BATCHES: u8 = 128;
const RECLAIM_BATCH: u64 = 8;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Status {
    reclaim_enabled: bool,
    eligible: bool,
    snapshot_id: Option<Digest>,
    snapshot_through: DecimalU64,
    reclaimed_through: DecimalU64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Reclaimed {
    snapshot_id: Digest,
    reclaimed_through: DecimalU64,
    snapshot_through: DecimalU64,
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
    /// Successful steps in this invocation. Zero for a status query.
    pub batches: u8,
}

/// Read retention status, or explicitly reclaim a bounded prefix for one snapshot.
/// `reclaim_batches` is absent for a read, or the maximum number of write steps.
pub async fn reclaim(
    home: &Path,
    database: &str,
    reclaim_batches: Option<u8>,
) -> Result<ReclaimReport, String> {
    if reclaim_batches.is_some_and(|limit| !(1..=MAX_BATCHES).contains(&limit)) {
        return Err("reclaim batches must be between 1 and 128".into());
    }
    let (database_id, log) = maintenance::personal_log(home, database).await?;
    let status = transport::call(
        &log,
        json!({"action": "retention", "database_id": database_id}),
    )
    .await?;
    let status: Status =
        serde_json::from_value(status).map_err(|_| "invalid signed retention response")?;
    if status.reclaimed_through.get() > status.snapshot_through.get()
        || (status.snapshot_id.is_none() && status.snapshot_through.get() != 0)
    {
        return Err("cloud retention positions are inconsistent".into());
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
    };
    if let Some(batches) = reclaim_batches {
        reclaim_batches_for_snapshot(&log, &mut report, batches).await?;
    }
    Ok(report)
}

async fn reclaim_batches_for_snapshot(
    log: &CloudLog,
    report: &mut ReclaimReport,
    batches: u8,
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
    if floor == through {
        return Ok(());
    }
    let manifest = transport::exact(log, id).await?;
    if manifest.through.get() != through || manifest.name.is_some() {
        return Err("the cloud could not verify the selected native recovery snapshot".into());
    }
    for _ in 0..batches {
        let response = reclaim_batch(log, id, floor, through).await.map_err(|error| {
            format!("{error}. Last confirmed reclamation was through {floor}. Use backup reclaim --status to read the latest position before retrying")
        })?;
        floor = response.reclaimed_through.get();
        report.reclaimed_through = response.reclaimed_through;
        report.batches += 1;
        if floor == through {
            break;
        }
    }
    Ok(())
}

async fn reclaim_batch(
    log: &CloudLog,
    id: &Digest,
    floor: u64,
    through: u64,
) -> Result<Reclaimed, String> {
    let body = json!({
        "action": "reclaim",
        "database_id": log.database_id(),
        "snapshot_id": id,
        "expected_floor": DecimalU64::new(floor),
        "require_native_recovery": true,
    });
    let response = transport::call(log, body).await?;
    let response: Reclaimed =
        serde_json::from_value(response).map_err(|_| "invalid signed reclaim response")?;
    if &response.snapshot_id != id
        || response.snapshot_through.get() != through
        || response.reclaimed_through.get() != floor.saturating_add(RECLAIM_BATCH).min(through)
    {
        return Err(
            "cloud reclaim response did not confirm the requested snapshot and range".into(),
        );
    }
    Ok(response)
}
