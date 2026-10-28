//! Opt-in preparation phases inside the native write gate, with no user data.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::time::Instant;

#[derive(Default)]
struct Report {
    phases_us: BTreeMap<&'static str, u64>,
    counts: BTreeMap<String, u64>,
}

tokio::task_local! {
    static ACTIVE: RefCell<Report>;
}

/// Capture the logical preparation only; admission and disk waits stay outside.
pub(crate) async fn capture<F: Future>(work: F) -> F::Output {
    if !laststore::commit_timing::enabled() || ACTIVE.try_with(|_| ()).is_ok() {
        return work.await;
    }
    ACTIVE
        .scope(RefCell::new(Report::default()), async move {
            let started_us = laststore::commit_timing::unix_us();
            let started = Instant::now();
            let result = work.await;
            let report = ACTIVE.with(|active| active.take());
            laststore::commit_timing::emit(
                "LASTDB_SNAPSHOT_TIMING",
                &serde_json::json!({
                    "kind": "prepare", "started_us": started_us,
                    "ended_us": laststore::commit_timing::unix_us(),
                    "total_us": micros(started.elapsed()),
                    "phases_us": report.phases_us, "counts": report.counts,
                }),
            );
            result
        })
        .await
}

/// Child stages overlap their parents. Read counts include OS-cache reads.
pub(crate) async fn measure<F: Future>(name: &'static str, work: F) -> F::Output {
    if ACTIVE.try_with(|_| ()).is_err() {
        return work.await;
    }
    let started = Instant::now();
    let (result, reads) = crate::read_receipts::observe(work).await;
    ACTIVE.with(|active| {
        let mut report = active.borrow_mut();
        *report.phases_us.entry(name).or_default() += micros(started.elapsed());
        for (field, value) in [
            ("calls", 1),
            ("keys", reads.requested_keys),
            ("file_opens", reads.file_opens),
            ("reads", reads.read_operations),
            ("payload_bytes", reads.payload_bytes),
            ("decode_us", reads.decode_nanos / 1000),
            ("lock_us", reads.lock_wait_nanos / 1000),
            ("worker_wait_us", reads.queue_wait_nanos / 1000),
        ] {
            *report.counts.entry(format!("{name}_{field}")).or_default() += value;
        }
    });
    result
}

fn micros(duration: std::time::Duration) -> u64 {
    duration.as_micros().try_into().unwrap_or(u64::MAX)
}
