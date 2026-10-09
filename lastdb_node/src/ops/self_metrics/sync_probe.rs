use super::*;

pub(super) const MUTATION_LOG_CAPTURE_MISSING_REASON: &str = "mutation_log_capture_missing";

pub(super) fn apply_mutation_log_capture_honesty(sync: &mut SyncHealth, local_mutation_tip: u64) {
    let capture_missing = sync.enabled
        && local_mutation_tip > 0
        && sync.recording_local_changes == Some(true)
        && sync.mutation_log_active == Some(true)
        && sync.mutation_log_capture_registered == Some(false);
    if !capture_missing {
        return;
    }

    sync.sync_degraded = Some(true);
    let reasons = sync.degraded_reasons.get_or_insert_with(Vec::new);
    if !reasons
        .iter()
        .any(|reason| reason == MUTATION_LOG_CAPTURE_MISSING_REASON)
    {
        reasons.push(MUTATION_LOG_CAPTURE_MISSING_REASON.to_string());
    }
}

pub(super) fn cached_sync_health(host: &Host) -> SyncHealth {
    if let Some((mut health, age)) = host.self_metrics.cached_sync_health() {
        if !cloud_sync_intent_is_on(host) {
            health.enabled = false;
        }
        let max_age = sample_interval_from_env()
            .checked_mul(2)
            .unwrap_or(Duration::MAX);
        if age > max_age {
            health.snapshot_unavailable_reason = Some("sync_snapshot_stale".to_string());
        }
        return health;
    }
    let configured = host.db.sync_engine().is_some();
    let enabled = configured && cloud_sync_intent_is_on(host);
    SyncHealth {
        enabled,
        snapshot_unavailable_reason: configured.then(|| "sync_snapshot_not_sampled".to_string()),
        local_writable: Some(true),
        sync_degraded: (!enabled).then_some(false),
        ..SyncHealth::default()
    }
}

pub(super) const SYNC_STATUS_BUDGET_EXCEEDED: &str = "sync_status_budget_exceeded";

/// Keep last-known sync gauges when the live probe misses. Empty defaults
/// after a timeout are what printed `last_success=never` / "log plane not
/// recording" on a node whose previous sample was healthy.
pub(super) fn sync_health_on_probe_timeout(host: &Host) -> SyncHealth {
    overlay_sync_probe_miss(
        host.self_metrics
            .cached_sync_health()
            .map(|(health, _age)| health),
        host.db.sync_engine().is_some() && cloud_sync_intent_is_on(host),
    )
}

pub(super) fn cloud_sync_intent_is_on(host: &Host) -> bool {
    crate::cloud::cloud_sync_file_state(&host.home) == "on"
        && !crate::cloud::cloud_resume_required_path(&host.home).exists()
}

pub(super) fn overlay_sync_probe_miss(
    cached: Option<SyncHealth>,
    operational_enabled: bool,
) -> SyncHealth {
    if let Some(mut health) = cached {
        if !operational_enabled {
            health.enabled = false;
        }
        health.snapshot_unavailable_reason = Some(SYNC_STATUS_BUDGET_EXCEEDED.to_string());
        return health;
    }
    SyncHealth {
        enabled: operational_enabled,
        snapshot_unavailable_reason: Some(SYNC_STATUS_BUDGET_EXCEEDED.to_string()),
        // Cloud lag never makes local storage read-only. This fact does not
        // depend on the unavailable sync snapshot.
        local_writable: Some(true),
        ..SyncHealth::default()
    }
}

/// `capture` for a node with no sync engine: the automatic-compaction reports
/// and nothing else.
///
/// Every other `capture` field is a cloud-capture counter that genuinely does
/// not exist without an engine. `capture_status_line` renders those as
/// `unavailable` / `none`, which is the honest answer, and still prints the
/// `automatic_compactions=` the operator came for. Returns `None` when no
/// cadence has run, so a node with the cadence disabled prints no line rather
/// than an empty one.
pub(super) async fn local_only_capture_status(host: &Host) -> Option<Value> {
    let planes = host.db.automatic_compaction_status().await?;
    if planes.is_empty() {
        return None;
    }
    serde_json::to_value(planes)
        .ok()
        .map(|planes| json!({ "automatic_compactions": planes }))
}

pub(super) fn sync_snapshot_complete(sync: &SyncHealth) -> bool {
    sync.snapshot_unavailable_reason.is_none()
}

pub(super) async fn sync_health(host: &Host) -> SyncHealth {
    // lint:fn-size-ok moved verbatim from self_metrics.rs; splitting this function is separate work.
    let status = tokio::time::timeout(SAMPLER_SYNC_SNAPSHOT_BUDGET, host.db.sync_status()).await;
    let mut health = match status {
        Err(_) => sync_health_on_probe_timeout(host),
        Ok(Some(status)) => SyncHealth {
            // Honest Off: engine may still exist, but intentional pause means
            // the cloud plane is not enabled for upload. Do not report
            // enabled=true merely because a SyncEngine was constructed.
            enabled: status.cloud_sync_disabled_at.is_none() && cloud_sync_intent_is_on(host),
            snapshot_unavailable_reason: None,
            local_writable: Some(status.local_writable),
            sync_degraded: Some(status.sync_degraded),
            state: Some(
                if status.cloud_sync_disabled_at.is_some() || !cloud_sync_intent_is_on(host) {
                    "Paused".to_string()
                } else {
                    format!("{:?}", status.state)
                },
            ),
            last_success_ts: g_instant_wire(status.last_sync_at, Unit::Named("unix_s")),
            pending_count: g_instant(status.pending_count as u64, Unit::Named("entry(ies)")),
            durable_outbox_count: g_instant(
                status.durable_outbox_count as u64,
                Unit::Named("entry(ies)"),
            ),
            upload_queue_count: g_instant(
                status.upload_queue_count as u64,
                Unit::Named("entry(ies)"),
            ),
            upload_queue_max: g_instant(status.upload_queue_max as u64, Unit::Named("entry(ies)")),
            durable_outbox_max: g_instant(
                status.durable_outbox_max as u64,
                Unit::Named("entry(ies)"),
            ),
            last_error: status.last_error,
            last_error_at: g_instant_wire(status.last_error_at, Unit::Named("unix_s")),
            consecutive_sync_failures: g_lifetime(status.consecutive_sync_failures, Unit::Events),
            failing_since: g_instant_wire(status.failing_since, Unit::Named("unix_s")),
            degraded_reasons: Some(status.degraded_reasons),
            replay_blocker: status
                .replay_blocker
                .and_then(|blocker| serde_json::to_value(blocker).ok()),
            last_download: status
                .last_download
                .and_then(|stats| serde_json::to_value(stats).ok()),
            upload_policy: status
                .upload_policy
                .and_then(|p| serde_json::to_value(p).ok()),
            backup_upload_concurrency: serde_json::to_value(status.backup_upload_concurrency).ok(),
            recording_local_changes: Some(status.recording_local_changes),
            sync_off_grace_expired: Some(status.sync_off_grace_expired),
            reenable_strategy: status.reenable_strategy,
            cloud_sync_disabled_at: g_instant_wire(
                status.cloud_sync_disabled_at,
                Unit::Named("unix_s"),
            ),
            sync_off_grace_secs: g_instant(status.sync_off_grace_secs, Unit::Named("sec(s)")),
            mutation_log_active: status.mutation_log.as_ref().map(|m| m.active),
            mutation_log_writer_id: status.mutation_log.as_ref().map(|m| m.writer_id.clone()),
            mutation_log_frontier_f: g_instant_wire(
                status.mutation_log.as_ref().map(|m| m.frontier_f),
                Unit::Named("seq"),
            ),
            mutation_log_published_through: g_instant_wire(
                status.mutation_log.as_ref().map(|m| m.published_through),
                Unit::Named("seq"),
            ),
            mutation_log_last_durable_frontier: g_instant_wire(
                status
                    .mutation_log
                    .as_ref()
                    .map(|m| m.last_durable_frontier),
                Unit::Named("seq"),
            ),
            mutation_log_recovery_point_age_secs: g_instant_wire(
                status
                    .mutation_log
                    .as_ref()
                    .and_then(|m| m.recovery_point_age_secs),
                Unit::Named("sec(s)"),
            ),
            mutation_log_lag: g_instant_wire(
                status.mutation_log.as_ref().map(|m| m.log_lag),
                Unit::Named("seq"),
            ),
            mutation_log_segments_uploaded: g_lifetime_wire(
                status.mutation_log.as_ref().map(|m| m.segments_uploaded),
                Unit::Named("segment(s)"),
            ),
            mutation_log_lag_degraded: status.mutation_log.as_ref().map(|m| m.lag_degraded),
            mutation_log_capture_registered: status
                .mutation_log
                .as_ref()
                .map(|m| m.capture_registered),
            capture_reexport_pending_count_estimate: g_instant(
                status.capture_reexport_pending_count,
                Unit::Named("entry(ies)"),
            ),
            capture_reexport_pending_known_nonempty: status.capture_reexport_pending_known_nonempty,
            mutation_log_peer_segments_applied: status
                .mutation_log
                .as_ref()
                .map(|m| m.peer_segments_applied),
            mutation_log_peer_records_applied: status
                .mutation_log
                .as_ref()
                .map(|m| m.peer_records_applied),
            mutation_log_records_quarantined: status
                .mutation_log
                .as_ref()
                .map(|m| m.records_quarantined),
            mutation_log_last_quarantine_reason: status
                .mutation_log
                .as_ref()
                .and_then(|m| m.last_quarantine_reason.clone()),
            capture: serde_json::to_value(status.capture).ok(),
        },
        Ok(None) => SyncHealth {
            enabled: false,
            snapshot_unavailable_reason: None,
            local_writable: Some(true),
            sync_degraded: Some(false),
            // Plane self-compaction is not a cloud feature and runs here too.
            // Without this the `Capture:` line vanished on exactly the node
            // shape whose reclaim nobody could otherwise observe.
            capture: local_only_capture_status(host).await,
            ..SyncHealth::default()
        },
    };
    // `LocalOutbox` advances only after a successful product mutation and is
    // independent of the sync engine. A fresh process therefore stays healthy
    // at tip 0, while accepted writes plus no capture runtime are unambiguous.
    apply_mutation_log_capture_honesty(&mut health, host.local_outbox.tip_seq());
    health
}
