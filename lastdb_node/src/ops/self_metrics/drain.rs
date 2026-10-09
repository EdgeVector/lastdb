use super::*;

pub(super) async fn record_sample(
    host: &Host,
    cpu: &mut CpuProbe,
    retention_cap: usize,
) -> Result<(u64, usize), String> {
    // Build vitals without writing to the DB first — process/RSS/sync health
    // must not require a healthy mutation path.
    let snapshot = build_snapshot(host, cpu, true).await;
    host.self_metrics.publish_status(snapshot.clone());

    // PRIMARY sink: JSONL on disk under the node home. If LastDB is down or
    // SelfMetricSample mutations fail (atom-missing, timeout), we still keep
    // a reviewable trail. Tom 2026-07-19: prefer log over LastDB for heartbeats.
    let log_path = self_metrics_log_path(&host.home);
    append_self_metrics_log_line(&log_path, &snapshot)?;

    let mut db_count = 0usize;
    let mut db_error = None;
    if self_metrics_db_write_enabled() {
        match record_sample_to_db(host, &snapshot, retention_cap).await {
            Ok(n) => db_count = n,
            Err(e) => {
                // Log already succeeded, but the status surface must still
                // show that durable telemetry persistence is unhealthy.
                tracing::warn!(
                    target: "lastdbd::self_metrics",
                    error = %e,
                    "self-metrics LastDB write failed (JSONL log ok); set LASTDB_SELF_METRICS_TO_DB=0 to silence"
                );
                db_error = Some(e);
            }
        }
    }

    if let Some(e) = db_error {
        Err(e)
    } else {
        Ok((snapshot.sampled_at, db_count))
    }
}

/// Drain telemetry rows left behind by a sink that is now switched off.
///
/// This is the half of retention that must not live inside the writer. It runs
/// on the `LASTDB_SELF_METRICS_TO_DB` **off** path — the default — and brings
/// each telemetry series down to the same cap the writer-on path enforces.
///
/// Four properties, each of which is a way this could have gone wrong:
///
/// - **It never creates the schema.** A node that has never written telemetry
///   must stay a node that has never written telemetry; a reclaim pass that
///   materialises `lastdb_telemetry/*` on every daemon in the fleet would cost
///   more storage than it returns. Absent schema is a silent no-op, and it is
///   checked before the plane is queried so the common case costs one metadata
///   lookup.
/// - **It drains in one batch.** See [`purge_series_rows`]: a drain's cost
///   tracks plane size, so slicing it across ticks multiplies the cost by the
///   number of slices. There is no per-tick work budget here on purpose.
/// - **It settles.** With the sink off nothing adds rows, so once a pass finds
///   nothing above the cap the plane is done forever (for this process) and
///   further ticks would pay a full plane scan every minute to learn the same
///   thing. Settling makes the steady-state cost zero rather than perpetual.
/// - **It gives up loudly rather than retrying forever.** A drain that fails
///   every tick is a full plane scan every tick. After
///   `MAX_DRAIN_ATTEMPTS` failures it settles and says so; an operator who
///   wants another go restarts the daemon or turns the sink on.
///
/// Kill switch: `LASTDB_SELF_METRICS_RETENTION_DRAIN=0`. It is default-ON
/// deliberately. Shipping a reclaim path that is off by default would recreate
/// the exact defect this fixes — a reaper nobody calls — one level out.
/// How a drain pass ended, and what it moved.
///
/// The pass used to return `()`. Its five endings — kill switch off, already
/// settled, no telemetry plane, a failed pass, and a real prune — were then
/// indistinguishable to every caller, so the only thing a test could observe
/// was the row count afterwards. Every occurrence of
/// `papercut-fold-mini-lane-orphaned-telemetry-order-log-row-count-flake`
/// (six between 2026-08-22 and 2026-08-30) reported the same `left: 4,
/// right: 2` and none of them said which ending ran or carried the error
/// string the pass had swallowed into a `tracing::warn`. Naming the ending is
/// what makes the next occurrence readable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum DrainOutcome {
    /// Kill switch off, or this process already settled its drain.
    Skipped,
    /// Neither telemetry schema exists, so there is nothing to drain and the
    /// pass settled rather than re-learn that every tick.
    NoPlane,
    /// The pass ran to completion. Counts are what the prune reported.
    Pruned {
        samples: PruneOutcome,
        rollups: PruneOutcome,
    },
    /// The pass could not finish, carrying the error it also logged.
    Failed(String),
}

pub(super) async fn drain_orphaned_telemetry(host: &Host, retention_cap: usize) -> DrainOutcome {
    if !retention_drain_enabled() || host.self_metrics.drain_settled() {
        return DrainOutcome::Skipped;
    }

    // Cheapest possible answer first: a daemon that never turned the sink on
    // has no plane to drain, and must not be given one.
    //
    // Checked PER SCHEMA, not "either one exists". The two series are written
    // by the same tick but they are separate schemas, and a store can hold one
    // without the other (the rollup schema post-dates the sample schema). An
    // either/or check would send a prune at a schema that is not there, whose
    // query errors, which would count as a drain failure and — three ticks
    // later — permanently settle a node that had a perfectly drainable plane.
    let (samples_present, rollups_present) = match (
        schema_present(host, SELF_METRIC_SCHEMA),
        schema_present(host, REQUEST_OPS_ROLLUP_SCHEMA),
    ) {
        (Ok(s), Ok(r)) => (s, r),
        (Err(e), _) | (_, Err(e)) => {
            tracing::warn!(
                target: "lastdbd::self_metrics",
                error = %e,
                "orphaned-telemetry drain could not read schema metadata"
            );
            host.self_metrics.record_drain_failure();
            return DrainOutcome::Failed(e);
        }
    };
    if !samples_present && !rollups_present {
        host.self_metrics.settle_drain();
        return DrainOutcome::NoPlane;
    }

    let started = Instant::now();
    let drained = async {
        let samples = if samples_present {
            prune_retention(host, retention_cap).await?
        } else {
            PruneOutcome::NOTHING
        };
        let rollups = if rollups_present {
            prune_request_ops_rollup_retention(host, request_ops_rollup_retention_from_env())
                .await?
        } else {
            PruneOutcome::NOTHING
        };
        Ok::<_, String>((samples, rollups))
    }
    .await;

    match drained {
        Ok((samples, rollups)) => {
            if samples.removed + rollups.removed > 0 {
                tracing::info!(
                    target: "lastdbd::self_metrics",
                    samples_removed = samples.removed,
                    samples_remaining = samples.remaining,
                    rollups_removed = rollups.removed,
                    rollups_remaining = rollups.remaining,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "drained orphaned telemetry left behind by a disabled sink"
                );
            } else {
                // Nothing above the cap, and with the sink off nothing will add
                // any. Stop paying a plane scan a minute to re-learn this.
                host.self_metrics.settle_drain();
            }
            DrainOutcome::Pruned { samples, rollups }
        }
        Err(error) => {
            tracing::warn!(
                target: "lastdbd::self_metrics",
                error = %error,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "orphaned-telemetry drain failed"
            );
            host.self_metrics.record_drain_failure();
            DrainOutcome::Failed(error)
        }
    }
}

/// Whether a schema already exists. Read through `get_schema_metadata`, which
/// — unlike `ensure_schema` — does not create what it fails to find.
pub(super) fn schema_present(host: &Host, name: &str) -> Result<bool, String> {
    host.db
        .schema_manager()
        .get_schema_metadata(name)
        .map(|meta| meta.is_some())
        .map_err(|e| e.to_string())
}

/// Default **on** — see [`drain_orphaned_telemetry`] for why an off-by-default
/// reclaim path would reproduce the bug it fixes.
pub fn retention_drain_enabled() -> bool {
    !matches!(
        std::env::var("LASTDB_SELF_METRICS_RETENTION_DRAIN")
            .ok()
            .as_deref()
            .map(str::trim),
        Some("0" | "false" | "no" | "off")
    )
}
