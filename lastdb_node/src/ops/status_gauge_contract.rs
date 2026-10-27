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

use super::gauge::Gauge;
use super::self_metrics::{
    IntegrityHealth, LimitsHealth, QosHealth, ResidentHealth, SamplerStatus, StatusSnapshot,
    UdsPoolHealth, WatchersHealth,
};

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

fn collect_resident(out: &mut Vec<GaugeContractEntry>, r: &ResidentHealth) {
    push_gauge(
        out,
        "resident.resident_tip_only_keys",
        &r.resident_tip_only_keys,
    );
    push_gauge(out, "resident.warm_lookup_hits", &r.warm_lookup_hits);
    push_gauge(out, "resident.warm_lookup_misses", &r.warm_lookup_misses);
    push_gauge(out, "resident.resident_key_count", &r.resident_key_count);
    push_gauge(out, "resident.resident_held_keys", &r.resident_held_keys);
    push_gauge(out, "resident.resident_dirty_keys", &r.resident_dirty_keys);
    push_gauge(out, "resident.resident_key_budget", &r.resident_key_budget);
    push_gauge(
        out,
        "resident.resident_purged_keys",
        &r.resident_purged_keys,
    );
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
    collect_sampler(&mut gauges, &snapshot.sampler);
    collect_qos(&mut gauges, &snapshot.qos);
    collect_uds(&mut gauges, &snapshot.uds);
    collect_watchers(&mut gauges, &snapshot.watchers);
    collect_limits(&mut gauges, &snapshot.limits);
    collect_resident(&mut gauges, &snapshot.resident);
    collect_integrity(&mut gauges, &snapshot.integrity);
    if snapshot.at_rest_compression.is_some() {
        collect_at_rest_compression(&mut gauges, snapshot.process_start_ts);
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
