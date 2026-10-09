use super::*;

pub fn spawn_sampler(
    host: std::sync::Arc<Host>,
    interval: Duration,
    shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut cpu = CpuProbe::default();
        // Once per boot, before the first tick. Field drift is introduced only
        // by a new binary, and a new binary is a new process, so boot is the
        // exact moment the registered schema can have fallen behind the code.
        heal_registered_telemetry_schemas(&host).await;
        while !shutdown.load(std::sync::atomic::Ordering::Acquire) {
            let cap = retention_max_samples_from_env();
            match record_sample(&host, &mut cpu, cap).await {
                Ok((sampled_at, count)) => host.self_metrics.record_success(sampled_at, count, cap),
                Err(e) => {
                    tracing::warn!(target: "lastdbd::self_metrics", error = %e, "self-metrics sample failed");
                    host.self_metrics.record_error(e, cap);
                }
            }
            // Retention must not be a step inside the writer — that is the bug
            // the drain exists to fix — but it must not be a step inside the
            // SAMPLER either, and that had to be measured rather than argued.
            //
            // Measured on the real 51-field schema (debug build, see
            // `telemetry_drain_cost_on_the_real_schema`): draining a 100-row
            // plane to 25 took **97 s**, a 200-row plane **196 s** — linear in
            // plane size, ~1.1 s per removed row. Whatever the release-build
            // constant turns out to be, a drain of any realistic plane runs far
            // longer than the 60 s sample interval, so inline it would stop the
            // vitals log for the whole drain. That log is the PRIMARY sink
            // precisely because it survives when LastDB is unhappy, and a large
            // purge is exactly when someone wants to read it.
            if shutdown.load(std::sync::atomic::Ordering::Acquire) {
                break;
            }
            maybe_spawn_drain(&host, cap);
            crate::ttl_sweep::maybe_spawn_ttl_sweep(&host);
            crate::atom_ref_backfill::maybe_spawn_atom_ref_backfill(&host);
            let until = tokio::time::Instant::now() + interval;
            while !shutdown.load(std::sync::atomic::Ordering::Acquire)
                && tokio::time::Instant::now() < until
            {
                tokio::time::sleep(
                    std::time::Duration::from_millis(250)
                        .min(until.saturating_duration_since(tokio::time::Instant::now())),
                )
                .await;
            }
        }
    })
}

/// Bring an ALREADY-REGISTERED telemetry schema up to this binary's field list.
///
/// This is the other half of the writer that must not live inside the writer.
/// `ensure_schema` — which carries the additive migration — is reachable only
/// through `record_sample_to_db`, and that is gated on
/// `LASTDB_SELF_METRICS_TO_DB`, off by default. So a store whose sink was on
/// under an older binary and is off now keeps that older binary's field list
/// forever, while every newer reader widens its projection past it.
///
/// The consequence is not a degraded read, it is no read at all: a field the
/// schema does not declare is a hard `400 Invalid field` for the whole query,
/// so `lastdb ops --since` fails on EVERY window. Measured on the primary
/// 2026-08-08 — registered schema 12 fields, binary projecting 39, 400 on every
/// `--since`. The schema is a READ contract; it cannot be maintained only on a
/// write path that the default configuration never executes.
///
/// Two properties, both borrowed from [`drain_orphaned_telemetry`] because they
/// are the same hazard:
///
/// - **It never creates a schema.** A node that has never written telemetry
///   stays one. Materialising `lastdb_telemetry/*` on every daemon in the fleet
///   would cost storage to fix a problem those daemons do not have — an absent
///   schema is already unambiguous to the reader, and reads as "never enabled"
///   rather than as an error.
/// - **It is checked per schema.** The rollup schema post-dates the sample
///   schema, so a store can hold either without the other.
///
/// Failure is logged and swallowed: this runs on the path that must keep the
/// JSONL vitals log alive, and a schema that cannot be upgraded is strictly no
/// worse than the state before this function existed.
pub(super) async fn heal_registered_telemetry_schemas(host: &Host) {
    for (name, build) in [
        (
            SELF_METRIC_SCHEMA,
            self_metric_schema as fn() -> Result<DeclarativeSchemaDefinition, String>,
        ),
        (REQUEST_OPS_ROLLUP_SCHEMA, request_ops_rollup_schema),
    ] {
        match schema_present(host, name) {
            Ok(false) => continue,
            Ok(true) => {}
            Err(e) => {
                tracing::warn!(
                    target: "lastdbd::self_metrics",
                    schema = name,
                    error = %e,
                    "telemetry schema heal could not read schema metadata"
                );
                continue;
            }
        }
        let current = match build() {
            Ok(schema) => schema,
            Err(e) => {
                tracing::warn!(
                    target: "lastdbd::self_metrics",
                    schema = name,
                    error = %e,
                    "telemetry schema heal could not build the current schema"
                );
                continue;
            }
        };
        let schema_manager = host.db.schema_manager();
        let existing = match schema_manager.get_schema_metadata(name) {
            Ok(Some(existing)) => existing,
            // Raced with a removal between the presence check and here.
            Ok(None) => continue,
            Err(e) => {
                tracing::warn!(
                    target: "lastdbd::self_metrics",
                    schema = name,
                    error = %e,
                    "telemetry schema heal could not load schema metadata"
                );
                continue;
            }
        };
        let missing = if name == SELF_METRIC_SCHEMA {
            missing_self_metric_fields(&existing)
        } else {
            missing_request_ops_rollup_fields(&existing)
        };
        if missing.is_empty() {
            continue;
        }
        tracing::info!(
            target: "lastdbd::self_metrics",
            schema = name,
            missing_fields = ?missing,
            "upgrading stale telemetry schema on the read path (sink may be off)"
        );
        if let Err(e) = schema_manager.update_schema(&current).await {
            tracing::warn!(
                target: "lastdbd::self_metrics",
                schema = name,
                error = %e,
                "telemetry schema heal failed; `--since` reads will stay narrowed"
            );
        }
    }
}

/// Start the orphaned-telemetry drain in its own task, at most one at a time.
///
/// The in-flight guard is not belt-and-braces: the drain outlives its tick by
/// design (see the measurement above), so without it a slow drain would be
/// relaunched every sample interval and the purge work would pile up on the
/// schema's durable mutation lane.
pub(super) fn maybe_spawn_drain(host: &std::sync::Arc<Host>, retention_cap: usize) {
    if !retention_drain_enabled() || self_metrics_db_write_enabled() {
        return;
    }
    if !host.self_metrics.begin_drain() {
        return;
    }
    let host = std::sync::Arc::clone(host);
    tokio::spawn(async move {
        drain_orphaned_telemetry(&host, retention_cap).await;
        host.self_metrics.end_drain();
    });
}
