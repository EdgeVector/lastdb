//! Additive self-describing gauge contract for `/api/status` (design PR-5).
//!
//! Every operator-visible [`Gauge`](crate::ops::gauge::Gauge) on a
//! [`StatusSnapshot`](crate::ops::self_metrics::StatusSnapshot) is listed with
//! its unit, window, optional since, and availability. Existing top-level
//! `/api/status` field names and numeric shapes are **unchanged** — the
//! contract is a sibling `contract` object, never a retype of the gauges.
//!
//! Ground truth: brain `design-lastdb-status-gauge-contract` (PR-5);
//! North Star terminal criteria on `north-star-lastdb-status-gauge-contract`.

use serde::Serialize;
use serde_json::{json, Value};

use std::collections::HashMap;

use super::gauge::Gauge;
use super::self_metrics::{
    AtomRefEdgeHealth, BackupProgressHealth, BackupStorageHealth, BuildHealth, CodecPolicyHealth,
    DualReadHealth, DurabilityHealth, FileBlobHealth, IntegrityHealth, LimitsHealth,
    LocatorOnlyHealth, LogStreamHealth, MemoryBudgetHealth, MemoryGuardMetric, MoleculeGateHealth,
    QosHealth, ReadCostHealth, ResidentHealth, SamplerStatus, StatusSnapshot, SyncHealth,
    UdsPoolHealth, WatchersHealth,
};
use crate::request_telemetry::RequestTelemetrySnapshot;

/// One gauge declaration in the additive contract block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GaugeContractEntry {
    /// Dotted path relative to the status object (e.g. `integrity.unresolved_atom_distinct`).
    pub path: String,
    /// Closed-set unit id or Named noun (`rows`, `edges`, `hold(s)`, …).
    pub unit: String,
    /// Display noun (same as `Unit::noun()` / Named string).
    pub unit_noun: String,
    /// Window kind: `instant` | `interval` | `process_lifetime` | `cumulative`.
    pub window: String,
    /// Display qualifier (`now`, `this process`, …).
    pub window_qualifier: String,
    /// Unix-seconds window start when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub since_unix: Option<u64>,
    /// `measured` or `unavailable`.
    pub availability: String,
    /// Present when availability is unavailable (e.g. `field_not_served`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unavailable_reason: Option<String>,
}

/// Full additive contract payload attached to `/api/status` as `contract`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StatusGaugeContract {
    /// Schema version for machine consumers. Bump only on breaking changes.
    pub version: u32,
    /// Typed gauges declared on this snapshot (unit + window + availability).
    pub gauges: Vec<GaugeContractEntry>,
}

impl StatusGaugeContract {
    pub const VERSION: u32 = 1;

    pub fn len(&self) -> usize {
        self.gauges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.gauges.is_empty()
    }
}

fn push_gauge(out: &mut Vec<GaugeContractEntry>, path: impl Into<String>, g: &Gauge) {
    out.push(GaugeContractEntry {
        path: path.into(),
        unit: g.unit.contract_id().to_string(),
        unit_noun: g.unit.noun().to_string(),
        window: g.window.contract_kind().to_string(),
        window_qualifier: g.window.qualifier().to_string(),
        since_unix: g.window.since_unix(),
        availability: g.availability_token().to_string(),
        unavailable_reason: g.unavailable_reason_token().map(str::to_string),
    });
}

fn collect_sync(out: &mut Vec<GaugeContractEntry>, s: &SyncHealth) {
    push_gauge(out, "sync.last_success_ts", &s.last_success_ts);
    push_gauge(out, "sync.pending_count", &s.pending_count);
    push_gauge(out, "sync.durable_outbox_count", &s.durable_outbox_count);
    push_gauge(out, "sync.upload_queue_count", &s.upload_queue_count);
    push_gauge(out, "sync.upload_queue_max", &s.upload_queue_max);
    push_gauge(out, "sync.durable_outbox_max", &s.durable_outbox_max);
    push_gauge(out, "sync.last_error_at", &s.last_error_at);
    push_gauge(
        out,
        "sync.consecutive_sync_failures",
        &s.consecutive_sync_failures,
    );
    push_gauge(out, "sync.failing_since", &s.failing_since);
    push_gauge(
        out,
        "sync.cloud_sync_disabled_at",
        &s.cloud_sync_disabled_at,
    );
    push_gauge(out, "sync.sync_off_grace_secs", &s.sync_off_grace_secs);
    push_gauge(
        out,
        "sync.mutation_log_frontier_f",
        &s.mutation_log_frontier_f,
    );
    push_gauge(
        out,
        "sync.mutation_log_published_through",
        &s.mutation_log_published_through,
    );
    push_gauge(
        out,
        "sync.mutation_log_last_durable_frontier",
        &s.mutation_log_last_durable_frontier,
    );
    push_gauge(
        out,
        "sync.mutation_log_recovery_point_age_secs",
        &s.mutation_log_recovery_point_age_secs,
    );
    push_gauge(out, "sync.mutation_log_lag", &s.mutation_log_lag);
    push_gauge(
        out,
        "sync.mutation_log_segments_uploaded",
        &s.mutation_log_segments_uploaded,
    );
    push_gauge(
        out,
        "sync.capture_reexport_pending_count_estimate",
        &s.capture_reexport_pending_count_estimate,
    );
}

fn collect_qos(out: &mut Vec<GaugeContractEntry>, q: &QosHealth) {
    push_gauge(out, "qos.total_permits", &q.total_permits);
    push_gauge(out, "qos.bulk_permits", &q.bulk_permits);
    push_gauge(out, "qos.total_in_use", &q.total_in_use);
    push_gauge(out, "qos.bulk_in_use", &q.bulk_in_use);
    push_gauge(out, "qos.interactive_sheds", &q.interactive_sheds);
    push_gauge(out, "qos.bulk_sheds", &q.bulk_sheds);
}

fn collect_uds(out: &mut Vec<GaugeContractEntry>, u: &UdsPoolHealth) {
    push_gauge(out, "uds.workers", &u.workers);
    push_gauge(out, "uds.queue_capacity", &u.queue_capacity);
    push_gauge(out, "uds.in_flight", &u.in_flight);
    push_gauge(out, "uds.submitted", &u.submitted);
    push_gauge(out, "uds.queue_full_rejects", &u.queue_full_rejects);
}

fn collect_watchers(out: &mut Vec<GaugeContractEntry>, w: &WatchersHealth) {
    push_gauge(out, "watchers.max", &w.max);
    push_gauge(out, "watchers.active", &w.active);
    push_gauge(out, "watchers.peak", &w.peak);
    push_gauge(out, "watchers.sheds", &w.sheds);
}

fn collect_sampler(out: &mut Vec<GaugeContractEntry>, s: &SamplerStatus) {
    push_gauge(out, "sampler.sample_count", &s.sample_count);
    push_gauge(
        out,
        "sampler.retention_max_samples",
        &s.retention_max_samples,
    );
}

fn collect_molecule_gate(out: &mut Vec<GaugeContractEntry>, m: &MoleculeGateHealth) {
    push_gauge(out, "molecule_gate.hold_total_us", &m.hold_total_us);
    push_gauge(out, "molecule_gate.hold_count", &m.hold_count);
    push_gauge(out, "molecule_gate.hold_max_us", &m.hold_max_us);
}

fn collect_resident(out: &mut Vec<GaugeContractEntry>, r: &ResidentHealth) {
    push_gauge(
        out,
        "resident.deferred_persist_completed",
        &r.deferred_persist_completed,
    );
    push_gauge(out, "resident.deferred_persist_us", &r.deferred_persist_us);
    push_gauge(out, "resident.resident_key_count", &r.resident_key_count);
    push_gauge(out, "resident.resident_held_keys", &r.resident_held_keys);
    push_gauge(out, "resident.resident_dirty_keys", &r.resident_dirty_keys);
    push_gauge(out, "resident.resident_key_budget", &r.resident_key_budget);
    push_gauge(
        out,
        "resident.resident_purged_keys",
        &r.resident_purged_keys,
    );
    push_gauge(out, "resident.loader_pin_bytes", &r.loader_pin_bytes);
    push_gauge(
        out,
        "resident.loader_groups_open_now",
        &r.loader_groups_open_now,
    );
    push_gauge(out, "resident.resident_point_hits", &r.resident_point_hits);
    push_gauge(
        out,
        "resident.resident_point_misses",
        &r.resident_point_misses,
    );
    push_gauge(out, "resident.resident_purge_runs", &r.resident_purge_runs);
    push_gauge(
        out,
        "resident.resident_over_cap_stalls",
        &r.resident_over_cap_stalls,
    );
    push_gauge(
        out,
        "resident.resident_over_cap_keys",
        &r.resident_over_cap_keys,
    );
    push_gauge(out, "resident.loader_loads", &r.loader_loads);
    push_gauge(out, "resident.loader_load_us", &r.loader_load_us);
}

fn collect_integrity(out: &mut Vec<GaugeContractEntry>, i: &IntegrityHealth) {
    push_gauge(
        out,
        "integrity.unresolved_atom_skips",
        &i.unresolved_atom_skips,
    );
    push_gauge(
        out,
        "integrity.unresolved_atom_distinct",
        &i.unresolved_atom_distinct,
    );
    push_gauge(
        out,
        "integrity.unresolved_atom_rows",
        &i.unresolved_atom_rows,
    );
}

fn collect_atom_ref_edges(out: &mut Vec<GaugeContractEntry>, a: &AtomRefEdgeHealth) {
    push_gauge(out, "atom_ref_edges.v1_bytes", &a.v1_bytes);
    push_gauge(out, "atom_ref_edges.v2_bytes", &a.v2_bytes);
    push_gauge(out, "atom_ref_edges.active_edges", &a.active_edges);
    push_gauge(out, "atom_ref_edges.inactive_keys", &a.inactive_keys);
    push_gauge(out, "atom_ref_edges.bytes_per_edge", &a.bytes_per_edge);
    push_gauge(
        out,
        "atom_ref_edges.projected_final_bytes",
        &a.projected_final_bytes,
    );
}

fn collect_file_blob(out: &mut Vec<GaugeContractEntry>, f: &FileBlobHealth) {
    push_gauge(out, "file_blob.absent_with_memo", &f.absent_with_memo);
    push_gauge(
        out,
        "file_blob.absent_with_memo_distinct",
        &f.absent_with_memo_distinct,
    );
    push_gauge(out, "file_blob.absent_without_memo", &f.absent_without_memo);
}

fn collect_locator_only(out: &mut Vec<GaugeContractEntry>, l: &LocatorOnlyHealth) {
    push_gauge(out, "locator_only.tips_sampled", &l.tips_sampled);
    push_gauge(out, "locator_only.max_tips", &l.max_tips);
    push_gauge(out, "locator_only.strata", &l.strata);
    push_gauge(out, "locator_only.strata_exhausted", &l.strata_exhausted);
    push_gauge(out, "locator_only.locator_only", &l.locator_only);
    push_gauge(
        out,
        "locator_only.body_at_derived_or_flat",
        &l.body_at_derived_or_flat,
    );
    push_gauge(out, "locator_only.dangling", &l.dangling);
    push_gauge(out, "locator_only.other_unresolved", &l.other_unresolved);
    push_gauge(
        out,
        "locator_only.locator_only_per_mille",
        &l.locator_only_per_mille,
    );
    push_gauge(
        out,
        "locator_only.dangling_per_mille",
        &l.dangling_per_mille,
    );
    push_gauge(out, "locator_only.prev_dangling", &l.prev_dangling);
    push_gauge(
        out,
        "locator_only.dangling_recurrence_per_hour",
        &l.dangling_recurrence_per_hour,
    );
}

fn collect_dual_read(out: &mut Vec<GaugeContractEntry>, d: &DualReadHealth) {
    push_gauge(out, "dual_read.gets", &d.gets);
    push_gauge(out, "dual_read.target_hits", &d.target_hits);
    push_gauge(out, "dual_read.legacy_hits", &d.legacy_hits);
    push_gauge(out, "dual_read.by_design_hits", &d.by_design_hits);
    push_gauge(out, "dual_read.misses", &d.misses);
    for plane in &d.legacy_hits_by_plane {
        push_gauge(
            out,
            format!("dual_read.legacy_hits_by_plane.{}.legacy_hits", plane.role),
            &plane.legacy_hits,
        );
    }
}

fn collect_read_cost(out: &mut Vec<GaugeContractEntry>, r: &ReadCostHealth) {
    push_gauge(out, "read_cost.cold_shard_loads", &r.cold_shard_loads);
    push_gauge(
        out,
        "read_cost.warm_resident_groups",
        &r.warm_resident_groups,
    );
    push_gauge(out, "read_cost.warm_resident_bytes", &r.warm_resident_bytes);
    push_gauge(out, "read_cost.warm_budget_bytes", &r.warm_budget_bytes);
    push_gauge(out, "read_cost.warm_budget_handles", &r.warm_budget_handles);
    push_gauge(out, "read_cost.open_append_handles", &r.open_append_handles);
    push_gauge(
        out,
        "read_cost.torn_transaction_rollbacks",
        &r.torn_transaction_rollbacks,
    );
    push_gauge(
        out,
        "read_cost.torn_transaction_rollback_failures",
        &r.torn_transaction_rollback_failures,
    );
    push_gauge(
        out,
        "read_cost.transaction_residency_refresh_failures",
        &r.transaction_residency_refresh_failures,
    );
}

fn collect_limits(out: &mut Vec<GaugeContractEntry>, l: &LimitsHealth) {
    push_gauge(
        out,
        "limits.max_request_body_bytes",
        &l.max_request_body_bytes,
    );
    push_gauge(
        out,
        "limits.max_atom_content_bytes",
        &l.max_atom_content_bytes,
    );
    push_gauge(
        out,
        "limits.max_atom_content_bytes_default",
        &l.max_atom_content_bytes_default,
    );
    push_gauge(
        out,
        "limits.max_atom_content_bytes_absolute_max",
        &l.max_atom_content_bytes_absolute_max,
    );
}

fn collect_durability(out: &mut Vec<GaugeContractEntry>, d: &DurabilityHealth) {
    push_gauge(
        out,
        "durability.backup_manifest_counter",
        &d.backup_manifest_counter,
    );
    push_gauge(
        out,
        "durability.last_backup_commit_ts",
        &d.last_backup_commit_ts,
    );
    push_gauge(out, "durability.backup_age_secs", &d.backup_age_secs);
    push_gauge(out, "durability.max_age_secs", &d.max_age_secs);
}

fn collect_backup_storage(out: &mut Vec<GaugeContractEntry>, b: &BackupStorageHealth) {
    push_gauge(out, "backup_storage.referenced_bytes", &b.referenced_bytes);
    push_gauge(out, "backup_storage.billed_bytes", &b.billed_bytes);
    push_gauge(
        out,
        "backup_storage.reclaimable_bytes",
        &b.reclaimable_bytes,
    );
    push_gauge(
        out,
        "backup_storage.referenced_chunks",
        &b.referenced_chunks,
    );
    push_gauge(out, "backup_storage.billed_chunks", &b.billed_chunks);
    push_gauge(
        out,
        "backup_storage.reclaimable_chunks",
        &b.reclaimable_chunks,
    );
}

fn collect_backup(out: &mut Vec<GaugeContractEntry>, b: &BackupProgressHealth) {
    push_gauge(out, "backup.chunks_total", &b.chunks_total);
    push_gauge(out, "backup.chunks_present", &b.chunks_present);
    push_gauge(out, "backup.chunks_remaining", &b.chunks_remaining);
    push_gauge(out, "backup.bytes_remaining", &b.bytes_remaining);
    push_gauge(out, "backup.elapsed_secs", &b.elapsed_secs);
    push_gauge(out, "backup.eta_secs", &b.eta_secs);
    push_gauge(out, "backup.last_cycle_uploaded", &b.last_cycle_uploaded);
    push_gauge(
        out,
        "backup.last_cycle_already_present",
        &b.last_cycle_already_present,
    );
    push_gauge(
        out,
        "backup.last_cycle_bytes_uploaded",
        &b.last_cycle_bytes_uploaded,
    );
    push_gauge(out, "backup.last_cycle_failed", &b.last_cycle_failed);
    push_gauge(
        out,
        "backup.chunks_source_missing",
        &b.chunks_source_missing,
    );
    push_gauge(
        out,
        "backup.chunks_unbackable_manifest",
        &b.chunks_unbackable_manifest,
    );
    push_gauge(out, "backup.cas_counter", &b.cas_counter);
    push_gauge(out, "backup.last_success_unix", &b.last_success_unix);
    push_gauge(
        out,
        "backup.last_success_age_secs",
        &b.last_success_age_secs,
    );
    push_gauge(out, "backup.consecutive_failures", &b.consecutive_failures);
    push_gauge(out, "backup.chunks_gained", &b.chunks_gained);
    push_gauge(out, "backup.chunks_erased", &b.chunks_erased);
    push_gauge(out, "backup.recent_gained", &b.recent_gained);
    push_gauge(out, "backup.recent_erased", &b.recent_erased);
    push_gauge(
        out,
        "backup.net_progress_window_cycles",
        &b.net_progress_window_cycles,
    );
    push_gauge(
        out,
        "backup.cycles_since_net_gain",
        &b.cycles_since_net_gain,
    );
    push_gauge(out, "backup.target_generation", &b.target_generation);
    push_gauge(out, "backup.last_publish_unix", &b.last_publish_unix);
    push_gauge(
        out,
        "backup.last_publish_age_secs",
        &b.last_publish_age_secs,
    );
}

fn collect_codec_policy(out: &mut Vec<GaugeContractEntry>, _c: &CodecPolicyHealth) {
    out.push(GaugeContractEntry {
        path: "codec_policy.writer_policy".to_string(),
        unit: "text".to_string(),
        unit_noun: "text".to_string(),
        window: "instant".to_string(),
        window_qualifier: "now".to_string(),
        since_unix: None,
        availability: "measured".to_string(),
        unavailable_reason: None,
    });
    out.push(GaugeContractEntry {
        path: "codec_policy.supported_read_formats".to_string(),
        unit: "text_array".to_string(),
        unit_noun: "text_array".to_string(),
        window: "instant".to_string(),
        window_qualifier: "now".to_string(),
        since_unix: None,
        availability: "measured".to_string(),
        unavailable_reason: None,
    });
}

fn push_process_counter(
    out: &mut Vec<GaugeContractEntry>,
    path: &str,
    unit: &'static str,
    since_boot_unix: u64,
) {
    out.push(GaugeContractEntry {
        path: path.to_string(),
        unit: unit.to_string(),
        unit_noun: unit.to_string(),
        window: "process_lifetime".to_string(),
        window_qualifier: "this process".to_string(),
        since_unix: (since_boot_unix > 0).then_some(since_boot_unix),
        availability: "measured".to_string(),
        unavailable_reason: None,
    });
}

fn collect_at_rest_compression(out: &mut Vec<GaugeContractEntry>, since_boot_unix: u64) {
    for (path, unit) in [
        ("at_rest_compression.compression_attempts", "attempts"),
        (
            "at_rest_compression.compression_attempt_cpu_ns",
            "nanoseconds",
        ),
        ("at_rest_compression.bytes_saved", "bytes"),
        ("at_rest_compression.skipped_below_min_bytes", "skips"),
        ("at_rest_compression.skipped_above_inflate_ceiling", "skips"),
        ("at_rest_compression.skipped_output_not_smaller", "skips"),
        ("at_rest_compression.skipped_compression_disabled", "skips"),
    ] {
        push_process_counter(out, path, unit, since_boot_unix);
    }
}

/// Build the additive contract from every typed gauge on the snapshot.
pub fn gauge_contract(snapshot: &StatusSnapshot) -> StatusGaugeContract {
    let mut gauges = Vec::with_capacity(128);
    collect_sync(&mut gauges, &snapshot.sync);
    if let Some(ref b) = snapshot.backup {
        collect_backup(&mut gauges, b);
    }
    if let Some(ref bs) = snapshot.backup_storage {
        collect_backup_storage(&mut gauges, bs);
    }
    collect_durability(&mut gauges, &snapshot.durability);
    collect_sampler(&mut gauges, &snapshot.sampler);
    collect_qos(&mut gauges, &snapshot.qos);
    collect_uds(&mut gauges, &snapshot.uds);
    collect_watchers(&mut gauges, &snapshot.watchers);
    collect_limits(&mut gauges, &snapshot.limits);
    if let Some(ref rc) = snapshot.read_cost {
        collect_read_cost(&mut gauges, rc);
    }
    collect_resident(&mut gauges, &snapshot.resident);
    collect_dual_read(&mut gauges, &snapshot.dual_read);
    collect_integrity(&mut gauges, &snapshot.integrity);
    collect_atom_ref_edges(&mut gauges, &snapshot.atom_ref_edges);
    collect_file_blob(&mut gauges, &snapshot.file_blob);
    if let Some(ref lo) = snapshot.locator_only {
        collect_locator_only(&mut gauges, lo);
    }
    collect_molecule_gate(&mut gauges, &snapshot.molecule_gate);
    if snapshot.at_rest_compression.is_some() {
        collect_at_rest_compression(&mut gauges, snapshot.process_start_ts);
    }
    if let Some(ref cp) = snapshot.codec_policy {
        collect_codec_policy(&mut gauges, cp);
    }
    StatusGaugeContract {
        version: StatusGaugeContract::VERSION,
        gauges,
    }
}

/// Serialize a status snapshot for `/api/status` with the additive
/// `capabilities` and `contract` siblings. Existing field names/types are
/// preserved (wire freeze).
pub fn status_value_with_contract(snapshot: &StatusSnapshot) -> Result<Value, String> {
    let mut value = serde_json::to_value(snapshot).map_err(|e| format!("status serialize: {e}"))?;
    let capabilities = json!({
        "atomic_aggregate_set_v1": true,
    });
    let contract = gauge_contract(snapshot);
    let contract_value =
        serde_json::to_value(&contract).map_err(|e| format!("contract serialize: {e}"))?;
    match value.as_object_mut() {
        Some(map) => {
            map.insert("capabilities".to_string(), capabilities);
            map.insert("contract".to_string(), contract_value);
            Ok(value)
        }
        None => Err("status serialize produced non-object".into()),
    }
}

/// Operator-facing lines for `lastdb status --contract`.
pub fn contract_lines(contract: &StatusGaugeContract) -> Vec<String> {
    let mut lines = Vec::with_capacity(contract.gauges.len() + 2);
    lines.push(format!(
        "Gauge contract v{} — {} typed gauge(s)",
        contract.version,
        contract.gauges.len()
    ));
    for g in &contract.gauges {
        let since = g
            .since_unix
            .map(|s| format!(" since={s}"))
            .unwrap_or_default();
        let reason = g
            .unavailable_reason
            .as_ref()
            .map(|r| format!(" reason={r}"))
            .unwrap_or_default();
        lines.push(format!(
            "  {path}: unit={unit} ({noun}) window={window} ({qual}){since} availability={avail}{reason}",
            path = g.path,
            unit = g.unit,
            noun = g.unit_noun,
            window = g.window,
            qual = g.window_qualifier,
            since = since,
            avail = g.availability,
            reason = reason,
        ));
    }
    lines
}

/// Top-level JSON keys that are **not** allowed to change type/shape under
/// the wire-freeze rule. Used by PR-5 tests; the `contract` key is additive.
pub fn wire_freeze_top_level_keys(status_json: &Value) -> Vec<String> {
    match status_json.as_object() {
        Some(map) => map
            .keys()
            .filter(|k| k.as_str() != "contract")
            .cloned()
            .collect(),
        None => Vec::new(),
    }
}

/// Compare two status JSON objects for wire-freeze: every non-`contract` key
/// present on `before` must exist on `after` with the same JSON type tag.
pub fn wire_freeze_types_compatible(before: &Value, after: &Value) -> Result<(), Vec<String>> {
    let (Some(b), Some(a)) = (before.as_object(), after.as_object()) else {
        return Err(vec!["both sides must be objects".into()]);
    };
    let mut errs = Vec::new();
    for (k, bv) in b {
        if k == "contract" {
            continue;
        }
        match a.get(k) {
            None => errs.push(format!("missing key after change: {k}")),
            Some(av) if json_type_tag(bv) != json_type_tag(av) => {
                errs.push(format!(
                    "type change at {k}: {} -> {}",
                    json_type_tag(bv),
                    json_type_tag(av)
                ));
            }
            Some(_) => {}
        }
    }
    if errs.is_empty() {
        Ok(())
    } else {
        Err(errs)
    }
}

fn json_type_tag(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Minimal typed snapshot for offline fixtures / wire-freeze tests (no daemon).
fn default_status_snapshot(
    sampled_at: u64,
    rss_bytes: Option<u64>,
    cpu_percent: Option<f64>,
) -> StatusSnapshot {
    StatusSnapshot {
        logs: LogStreamHealth::default(),
        sampled_at,
        process_start_ts: sampled_at,
        uptime_secs: 0,
        rss_bytes,
        phys_footprint_bytes: None,
        phys_footprint_peak_bytes: None,
        memory_limit_bytes: None,
        memory_guard_metric: MemoryGuardMetric::default(),
        memory_budget: MemoryBudgetHealth::default(),
        cpu_percent,
        home_bytes: Some(0),
        home_size_walks: 1,
        home: None,
        home_path_disclosed: false,
        data_dir_bytes: Some(0),
        data_dir_size_walks: 1,
        data_dir: None,
        data_dir_path_disclosed: false,
        sync: SyncHealth::default(),
        backup: None,
        backup_storage: None,
        durability: DurabilityHealth::default(),
        sampler: SamplerStatus::default(),
        qos: QosHealth::default(),
        uds: UdsPoolHealth::default(),
        watchers: WatchersHealth::default(),
        request_ops: RequestTelemetrySnapshot::default(),
        limits: LimitsHealth::default(),
        read_cost: None,
        resident: ResidentHealth::default(),
        dual_read: DualReadHealth::default(),
        build: BuildHealth::default(),
        integrity: IntegrityHealth::default(),
        atom_ref_edges: AtomRefEdgeHealth::default(),
        file_blob: FileBlobHealth::default(),
        locator_only: None,
        purge_stats: HashMap::new(),
        local_retention: crate::self_metrics::LocalRetentionHealth::default(),
        molecule_gate: MoleculeGateHealth::default(),
        at_rest_compression: None,
        codec_policy: None,
    }
}

/// Minimal offline fixture for North Star proof harnesses (no daemon).
///
/// Shape matches `/api/status` envelope body under `status` when a live node
/// serves PR-5+: gauges stay bare numbers; `contract` lists unit/window.
pub fn offline_proof_fixture() -> Value {
    let snapshot = default_status_snapshot(1_700_000_000, Some(1), None);
    let status = status_value_with_contract(&snapshot).expect("fixture status");
    json!({
        "status": status,
        "notes": [
            "offline fixture for north-star-lastdb-status-gauge-contract proof",
            "never point proof harnesses at the primary lastdbd",
            "live mode: CoW ephemeral node + GET /api/status; assert contract.count == typed gauges",
        ],
    })
}
