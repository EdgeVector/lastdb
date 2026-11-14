//! Read and extend the immutable acceptance index for one personal cloud log.

use std::path::Path;
use std::time::Duration;

use cloud_backup_protocol::{DecimalU64, Id};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{cloud::CloudLog, maintenance, snapshot::transport};

const MAX_BATCHES: u16 = 256;
const MAX_BATCH_ENTRIES: u64 = 32;
const PROGRESS_INTERVAL: u16 = 16;
const BATCH_DELAY: Duration = Duration::from_millis(100);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Status {
    database_id: Id,
    index_enabled: bool,
    eligible: bool,
    first_indexed_delivery: Option<DecimalU64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Indexed {
    database_id: Id,
    first_indexed_delivery: DecimalU64,
    indexed_entries: DecimalU64,
}

/// Last confirmed acceptance-index position for the requested personal log.
#[derive(Serialize)]
pub struct IndexHistoryReport {
    /// The cloud environment selected by the home's saved connection.
    pub environment: &'static str,
    /// The exact personal database selected in this home.
    pub database_id: Id,
    /// Whether indexing is allowed for this cloud scope.
    pub index_enabled: bool,
    /// Whether the log has a modern index and retained legacy history.
    pub eligible: bool,
    /// The first indexed delivery. A position of one means indexing is complete.
    pub first_indexed_delivery: Option<DecimalU64>,
    /// Requests confirmed during this invocation.
    pub batches: u16,
    /// Entries indexed during this invocation.
    pub indexed_entries: DecimalU64,
}

/// Read the signed position, or index a bounded number of contiguous batches.
/// `progress` receives each completed wave and the last partial wave on failure.
pub async fn index_history(
    home: &Path,
    selector: &str,
    batches: Option<u16>,
    progress: impl Fn(&IndexHistoryReport),
) -> Result<IndexHistoryReport, String> {
    if batches.is_some_and(|count| !(1..=MAX_BATCHES).contains(&count)) {
        return Err("index batches must be between 1 and 256".into());
    }
    let (database, log) = maintenance::personal_log(home, selector).await?;
    let status = index_status(&log, &database).await?;
    let mut report = IndexHistoryReport {
        environment: log.environment(),
        database_id: database,
        index_enabled: status.index_enabled,
        eligible: status.eligible,
        first_indexed_delivery: status.first_indexed_delivery,
        batches: 0,
        indexed_entries: DecimalU64::new(0),
    };
    if let Some(batches) = batches {
        index_batches(&log, &mut report, batches, progress).await?;
    }
    Ok(report)
}

async fn index_status(log: &CloudLog, database: &Id) -> Result<Status, String> {
    let reply = transport::call(
        log,
        json!({"action": "index-status", "database_id": database}),
    )
    .await?;
    let status: Status =
        serde_json::from_value(reply).map_err(|_| "invalid signed index status response")?;
    if &status.database_id != database
        || status
            .first_indexed_delivery
            .is_some_and(|value| value.get() == 0)
        || (status.eligible && status.first_indexed_delivery.is_none())
    {
        return Err(
            "cloud index status did not identify a valid position in the selected database".into(),
        );
    }
    Ok(status)
}

async fn index_batches(
    log: &CloudLog,
    report: &mut IndexHistoryReport,
    limit: u16,
    progress: impl Fn(&IndexHistoryReport),
) -> Result<(), String> {
    if !report.index_enabled {
        return Err("log history indexing is disabled for this cloud database".into());
    }
    if !report.eligible {
        return Err("this database's retained log is ineligible for indexing".into());
    }
    let expected = report
        .first_indexed_delivery
        .ok_or("cloud returned no index position")?
        .get();
    let mut position = expected;
    for _ in 0..limit {
        if position == 1 {
            break;
        }
        position = match index_batch(log, &report.database_id, position).await {
            Ok(next) => next,
            Err(error) => {
                progress(report);
                return Err(format!("{error}. Last confirmed first indexed delivery was {position}. Read index-log-history --status before retrying"));
            }
        };
        report.batches += 1;
        report.first_indexed_delivery = Some(DecimalU64::new(position));
        report.indexed_entries = DecimalU64::new(expected - position);
        if report.batches.is_multiple_of(PROGRESS_INTERVAL)
            && position != 1
            && report.batches < limit
        {
            progress(report);
        }
        if position != 1 && report.batches < limit {
            tokio::time::sleep(BATCH_DELAY).await;
        }
    }
    Ok(())
}

async fn index_batch(log: &CloudLog, database: &Id, expected: u64) -> Result<u64, String> {
    let reply = transport::call(
        log,
        json!({
            "action": "index-history",
            "database_id": database,
            "expected_first_indexed_delivery": DecimalU64::new(expected),
        }),
    )
    .await?;
    let indexed: Indexed =
        serde_json::from_value(reply).map_err(|_| "invalid signed index response")?;
    let first = indexed.first_indexed_delivery.get();
    let count = indexed.indexed_entries.get();
    if &indexed.database_id != database
        || first == 0
        || expected.checked_sub(first) != Some(count)
        || !(1..=MAX_BATCH_ENTRIES).contains(&count)
    {
        return Err(
            "cloud index response did not confirm the requested database and progress".into(),
        );
    }
    Ok(first)
}
