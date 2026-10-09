//! Machine gate for enabling local `enforce_existing_only` reuse.
//!
//! The install/operator switch is not sufficient by itself. Enforce mode is
//! eligible only when an automated report proves the adversarial, precision,
//! fallback, LKG, latency, and kill-switch criteria are green.

use serde::{Deserialize, Serialize};

pub const DEFAULT_ENFORCE_MIN_MATCH_PRECISION: f32 = 0.99;
pub const DEFAULT_ENFORCE_MIN_FIELD_COVERAGE: f32 = 0.95;
pub const DEFAULT_ENFORCE_MIN_REQUIRED_FIELD_COVERAGE: f32 = 1.0;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct SchemaResolverEnforceGateReport {
    /// Unsafe local reuse disagreements from the shadow/eval window.
    pub unsafe_reuse_count: u64,
    /// Explicit adversarial note/event/trip fixtures all fell back or matched safely.
    pub adversarial_note_event_trip_passed: bool,
    /// Measured precision for local reuse decisions.
    pub match_precision: f32,
    /// Required precision floor for local reuse decisions.
    pub match_precision_floor: f32,
    /// Measured field coverage for local reuse/component decisions.
    pub field_coverage: f32,
    /// Required field coverage floor.
    pub field_coverage_floor: f32,
    /// Measured required-field coverage.
    pub required_field_coverage: f32,
    /// Required required-field coverage floor.
    pub required_field_coverage_floor: f32,
    /// p95 proposal embed + resolve latency, when a budget is configured.
    pub p95_latency_ms: Option<u64>,
    /// p95 latency budget. `None` means latency-only gating is skipped.
    pub p95_latency_budget_ms: Option<u64>,
    /// Ambiguity and miss fixtures forced live fallback.
    pub live_fallback_on_ambiguity_or_miss: bool,
    /// Bad latest manifest/artifact fell back to last-known-good.
    pub bad_latest_lkg_drill_passed: bool,
    /// Unit/integration evidence that kill switch forces LiveOnly.
    pub kill_switch_forces_live_only: bool,
}

impl Default for SchemaResolverEnforceGateReport {
    fn default() -> Self {
        Self {
            unsafe_reuse_count: 1,
            adversarial_note_event_trip_passed: false,
            match_precision: 0.0,
            match_precision_floor: DEFAULT_ENFORCE_MIN_MATCH_PRECISION,
            field_coverage: 0.0,
            field_coverage_floor: DEFAULT_ENFORCE_MIN_FIELD_COVERAGE,
            required_field_coverage: 0.0,
            required_field_coverage_floor: DEFAULT_ENFORCE_MIN_REQUIRED_FIELD_COVERAGE,
            p95_latency_ms: None,
            p95_latency_budget_ms: None,
            live_fallback_on_ambiguity_or_miss: false,
            bad_latest_lkg_drill_passed: false,
            kill_switch_forces_live_only: false,
        }
    }
}

pub fn schema_resolver_enforce_gate_pass(report: &SchemaResolverEnforceGateReport) -> bool {
    schema_resolver_enforce_gate_failures(report).is_empty()
}

pub fn schema_resolver_enforce_gate_failures(
    report: &SchemaResolverEnforceGateReport,
) -> Vec<&'static str> {
    let mut failures = Vec::new();

    if report.unsafe_reuse_count != 0 {
        failures.push("unsafe_reuse_count");
    }
    if !report.adversarial_note_event_trip_passed {
        failures.push("adversarial_note_event_trip");
    }
    if !finite_unit_at_least(report.match_precision, report.match_precision_floor) {
        failures.push("match_precision");
    }
    if !finite_unit_at_least(report.field_coverage, report.field_coverage_floor) {
        failures.push("field_coverage");
    }
    if !finite_unit_at_least(
        report.required_field_coverage,
        report.required_field_coverage_floor,
    ) {
        failures.push("required_field_coverage");
    }
    if let (Some(p95), Some(budget)) = (report.p95_latency_ms, report.p95_latency_budget_ms) {
        if p95 > budget {
            failures.push("p95_latency");
        }
    }
    if !report.live_fallback_on_ambiguity_or_miss {
        failures.push("live_fallback_on_ambiguity_or_miss");
    }
    if !report.bad_latest_lkg_drill_passed {
        failures.push("bad_latest_lkg_drill");
    }
    if !report.kill_switch_forces_live_only {
        failures.push("kill_switch_forces_live_only");
    }

    failures
}

fn finite_unit_at_least(value: f32, floor: f32) -> bool {
    value.is_finite()
        && floor.is_finite()
        && (0.0..=1.0).contains(&value)
        && (0.0..=1.0).contains(&floor)
        && value >= floor
}
