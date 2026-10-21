//! Background dwell pass: delete the local file blobs whose count is zero and
//! whose dwell has passed.
//!
//! The blob counter (`fold_db::db_operations::AtomStore::blob_dwell_step`)
//! keeps a time-bucketed queue of the blobs at zero (`docs/lastdb-time-queue.md`).
//! Each sampler tick starts at most one pass. A pass keeps the reader of the
//! queue (its cursors over the buckets) from one pass to the next, and deletes
//! the due blobs in chunks. A chunk takes the native write gate for its own
//! reads and its one batch, and the gate is free between chunks. The pass takes the local mutation admission per chunk, as the
//! TTL sweep does, so a backup cut never starts inside a chunk. After each chunk
//! the pass rests for a share of the time the chunk kept writers waiting
//! (`pace.rs`: duty 10 percent by default), so a writer waits for at most one
//! chunk and its flush and then runs free.
//!
//! The dwell is `LASTDB_BLOB_DWELL_SECS` (one hour by default, 120 s at least
//! unless `LASTDB_BLOB_DWELL_UNSAFE_SHORT_FOR_DEV=1`). Kill switch for the pass:
//! `LASTDB_BLOB_DWELL_PASS=0`. Default **on**. With the pass off no blob leaves
//! the store, and the queue grows until the pass runs again.

use fold_db::db_operations::{DwellCursor, WriterWatch};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tracing::Instrument;

use crate::host::Host;

mod pace;
use pace::Pace;
pub use pace::{
    BLOB_DWELL_CHUNK_ENV, BLOB_DWELL_DUTY_PCT_ENV, BLOB_DWELL_MIN_PAUSE_MS_ENV,
    BLOB_DWELL_NET_SECS_ENV,
};

/// Default-on kill switch. Off only for the explicit falsey values.
pub const BLOB_DWELL_PASS_ENV: &str = "LASTDB_BLOB_DWELL_PASS";

/// Steps of one pass, at most. A longer queue continues on the next tick.
const MAX_STEPS_PER_PASS: usize = 2_048;

/// Steps between two reads of the directory of the queue. A bucket that starts
/// while a long pass runs then gets its turn.
const REFRESH_EVERY: usize = 16;

#[must_use]
pub fn blob_dwell_pass_enabled() -> bool {
    !matches!(
        std::env::var(BLOB_DWELL_PASS_ENV)
            .ok()
            .as_deref()
            .map(str::trim),
        Some("0" | "false" | "no" | "off")
    )
}

/// Spawn one dwell pass off the sampler tick, at most one at a time. The pass
/// stops between chunks once `shutdown` is set.
pub fn maybe_spawn_blob_dwell_pass(
    host: &std::sync::Arc<Host>,
    shutdown: &std::sync::Arc<AtomicBool>,
) {
    if !blob_dwell_pass_enabled() {
        return;
    }
    let Some(guard) = host.db.db_ops().atoms().begin_blob_dwell_pass() else {
        return;
    };
    let host = std::sync::Arc::clone(host);
    let shutdown = std::sync::Arc::clone(shutdown);
    tokio::spawn(
        async move {
            let mut cursor = guard.take_cursor(Pace::from_env().chunk);
            let result = run_pass(&host, &shutdown, &mut cursor).await;
            if let Err(error) = &result {
                tracing::warn!(target: "lastdbd::blob_dwell", error = %error, "blob dwell pass failed");
            }
            guard.put_cursor(cursor);
            guard.finish(result.err());
        }
        .in_current_span(),
    );
}

/// Run one bounded dwell pass with the reader that the last pass left. A step
/// reads one page of due entries and deletes it in one chunk, or retires one
/// bucket. After each step the pass rests. An error ends the pass, and the next
/// pass waits longer (30 s, doubling, up to 10 min).
pub async fn run_pass(
    host: &Host,
    shutdown: &AtomicBool,
    cursor: &mut DwellCursor,
) -> Result<(), String> {
    // The watch remembers the last write of another task from one pass to the next.
    let mut watch = WATCH
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
        .unwrap_or_default();
    let result = run_steps(host, shutdown, cursor, &mut watch).await;
    *WATCH
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(watch);
    result
}

/// The writer watch of the dwell pass. At most one pass runs at a time.
static WATCH: std::sync::Mutex<Option<WriterWatch>> = std::sync::Mutex::new(None);

async fn run_steps(
    host: &Host,
    shutdown: &AtomicBool,
    cursor: &mut DwellCursor,
    watch: &mut WriterWatch,
) -> Result<(), String> {
    let atoms = host.db.db_ops().atoms();
    let pace = Pace::from_env();
    let failed = |error: fold_db::schema::SchemaError| error.to_string();
    atoms.blob_dwell_begin(cursor).await.map_err(failed)?;
    cursor.set_net_every(pace::net_every_from_env());
    if sweep_net(host, shutdown, cursor, &pace, watch).await > 0 {
        // A bucket that the net registered again is in the directory now.
        atoms.blob_dwell_refresh(cursor).await.map_err(failed)?;
    }
    for number in 0..MAX_STEPS_PER_PASS {
        if shutdown.load(Ordering::Acquire) {
            return Ok(());
        }
        if number > 0 && number % REFRESH_EVERY == 0 {
            atoms.blob_dwell_refresh(cursor).await.map_err(failed)?;
        }
        let started = Instant::now();
        let step = atoms.blob_dwell_step(cursor, pace.chunk);
        let step = match crate::maintenance_write_gate::run(host, step).await {
            Some(Ok(Some(step))) => step,
            // Nothing is due, or local writes are paused for a retained cloud
            // source.
            Some(Ok(None)) | None => return Ok(()),
            Some(Err(error)) => return Err(failed(error)),
        };
        let took = started.elapsed();
        let pause = pace.pause_after(step.blocked, watch.writer_present(step.scopes));
        tracing::debug!(
            target: "lastdbd::blob_dwell",
            chunk = step.handled(),
            retired = step.retired,
            buckets = cursor.buckets(),
            took_ms = u64::try_from(took.as_millis()).unwrap_or(u64::MAX),
            blocked_us = u64::try_from(step.blocked.as_micros()).unwrap_or(u64::MAX),
            rest_ms = u64::try_from(pause.as_millis()).unwrap_or(u64::MAX),
            "blob dwell chunk done; resting"
        );
        rest(pause, shutdown, watch).await;
    }
    Ok(())
}

/// Rest between two chunks, in short slices so that a shutdown ends it at once. A
/// long rest ends early when no other task has opened a write scope within the
/// quiet period (`WriterWatch::rest_may_end`): the writers went away.
async fn rest(pause: Duration, shutdown: &AtomicBool, watch: &mut WriterWatch) {
    const SLICE: Duration = Duration::from_millis(250);
    let end = Instant::now() + pause;
    loop {
        if shutdown.load(Ordering::Acquire) {
            return;
        }
        let left = end.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return;
        }
        tokio::time::sleep(left.min(SLICE)).await;
        if left > SLICE && watch.rest_may_end() {
            return;
        }
    }
}

/// The sweeps of the safety net that are due: chunks of slices, each paced like a
/// step. Returns the number of buckets that the net registered again. A sweep that
/// fails ends; it does not fail the pass, and the next sweep comes at its time.
async fn sweep_net(
    host: &Host,
    shutdown: &AtomicBool,
    cursor: &mut DwellCursor,
    pace: &Pace,
    watch: &mut WriterWatch,
) -> u64 {
    let atoms = host.db.db_ops().atoms();
    let mut repaired = 0;
    loop {
        if shutdown.load(Ordering::Acquire) {
            return repaired;
        }
        let started = Instant::now();
        let step = atoms.blob_dwell_net_step(cursor);
        match crate::maintenance_write_gate::run(host, step).await {
            Some(Ok(Some(step))) => {
                repaired += step.repaired;
                tracing::debug!(
                    target: "lastdbd::blob_dwell",
                    probed = step.probed,
                    repaired = step.repaired,
                    took_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                    blocked_us = u64::try_from(step.blocked.as_micros()).unwrap_or(u64::MAX),
                    "blob dwell net chunk"
                );
                rest(
                    pace.pause_after(step.blocked, watch.writer_present(step.scopes)),
                    shutdown,
                    watch,
                )
                .await;
            }
            // No sweep is due, or local writes are paused for a retained cloud
            // source.
            Some(Ok(None)) | None => return repaired,
            Some(Err(error)) => {
                tracing::warn!(
                    target: "lastdbd::blob_dwell",
                    error = %error,
                    "blob dwell net sweep failed"
                );
                return repaired;
            }
        }
    }
}
