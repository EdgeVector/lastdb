//! Numeric request clocks for explicitly isolated write investigations.

use super::DataRoute;
use crate::request_telemetry::OpSample;
use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub(super) struct Clock {
    started_us: u64,
    started: Instant,
}

fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("LASTDB_SNAPSHOT_TIMING").as_deref() == Ok("1")
            && std::env::var("LASTDB_ISOLATED_COPY").as_deref() == Ok("1")
    })
}

fn unix_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            elapsed.as_micros().try_into().unwrap_or(u64::MAX)
        })
}

pub(super) fn start(route: DataRoute) -> Option<Clock> {
    (matches!(route, DataRoute::Mutation | DataRoute::MutationBatch) && enabled()).then(|| Clock {
        started_us: unix_us(),
        started: Instant::now(),
    })
}

pub(super) fn finish(clock: Option<Clock>, sample: &OpSample) {
    let Some(clock) = clock else { return };
    eprintln!(
        "LASTDB_WRITE_REQUEST_TIMING {}",
        serde_json::json!({
            "kind": "request", "started_us": clock.started_us, "ended_us": unix_us(),
            "total_us": clock.started.elapsed().as_micros().min(u64::MAX as u128) as u64,
            "queue_wait_us": sample.phases.queue_wait_us, "status": sample.status,
            "body_bytes": sample.body_bytes, "rows": sample.rows,
            "phases_us": sample.phases,
            "counts": {
                "molecules_persisted": sample.molecules_persisted,
                "molecule_store_commits": sample.molecule_store_commits,
                "resident_commits": sample.resident_commits,
                "resident_operations": sample.resident_operations,
                "partition_read_rejections": sample.partition_read_rejections,
                "all_group_walks": sample.all_group_walks,
            },
        })
    );
}
