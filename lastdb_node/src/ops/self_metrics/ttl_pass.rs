//! What the last TTL pass did, for the status.

use serde::{Deserialize, Serialize};

use super::sampler_state::{SamplerRuntimeState, MAX_DRAIN_ATTEMPTS};

/// The backlog signal of the last TTL sweep pass.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TtlPassReport {
    /// True when the pass erased or dropped every key that it listed, in every
    /// schema, and no schema failed.
    pub finished_backlog: bool,
    /// Keys that the pass listed and had not reached when it ended.
    pub keys_left: u64,
    /// True when the pass ended early: the node was stopping, a backup cut was
    /// held, or local writes were paused.
    pub cut: bool,
    /// Schemas that the pass skipped because their listing or a batch failed.
    pub schemas_failed: u32,
}

impl SamplerRuntimeState {
    /// Keep the report of a finished pass and add its skipped schemas to the
    /// count for the process. An unfinished pass is not settled, unless the sweep gave up.
    pub(crate) fn record_ttl_pass(&self, report: TtlPassReport) {
        let mut ttl = self
            .ttl
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ttl.schema_failures = ttl
            .schema_failures
            .saturating_add(u64::from(report.schemas_failed));
        // A sweep that gave up for this process stays settled.
        ttl.settled =
            (ttl.settled && report.finished_backlog) || ttl.failures >= MAX_DRAIN_ATTEMPTS;
        ttl.last_pass = report;
    }
}
