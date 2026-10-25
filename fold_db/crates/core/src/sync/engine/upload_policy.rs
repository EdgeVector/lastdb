use serde::{Deserialize, Serialize};

pub const DEFAULT_RSS_LIMIT_MB: u64 = 6144;
/// How upload caps are chosen each cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum UploadPolicyMode {
    /// Derive byte budget + concurrency from live resources.
    #[default]
    Auto,
    /// Use static [`crate::sync::engine::SyncConfig`] / env fixed knobs only.
    Fixed,
}

/// Stable coarse source of the active upload throttle (status/ops field).
///
/// Fine-grained detail stays in [`UploadPolicySnapshot::throttle_reason`]
/// (e.g. `rss_near_limit`, `high_cpu`). This enum is the operator-facing
/// bucket contract from the cloud-sync-off milestone: one of
/// `interactive_busy` | `cpu_percent` | `rss` | `network_ewma` | `catch_up` | `none`.
///
/// Derived from the **same** reason string written by [`compute_upload_policy`]
/// — never a second ad-hoc guess at status-serialization time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum UploadThrottleSource {
    /// No resource throttle engaged (or Fixed mode — not a live resource brake).
    #[default]
    None,
    InteractiveBusy,
    CpuPercent,
    Rss,
    NetworkEwma,
    /// Reserved / backup catch-up regime when that path stamps a reason.
    CatchUp,
}

impl UploadThrottleSource {
    /// Map a fine-grained `throttle_reason` to the stable source bucket.
    pub fn from_reason(reason: Option<&str>) -> Self {
        match reason {
            None | Some("fixed") => Self::None,
            Some("interactive_busy") => Self::InteractiveBusy,
            Some("high_cpu") => Self::CpuPercent,
            Some(r) if r.starts_with("rss_") => Self::Rss,
            Some("slow_network") => Self::NetworkEwma,
            Some("catch_up") => Self::CatchUp,
            // Unknown future reason: stay honest — not "none" (that means no brake).
            // Prefer `rss` only for rss_* prefixes above; everything else stays
            // None so operators see an unexpected reason without a false source.
            Some(_) => Self::None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::InteractiveBusy => "interactive_busy",
            Self::CpuPercent => "cpu_percent",
            Self::Rss => "rss",
            Self::NetworkEwma => "network_ewma",
            Self::CatchUp => "catch_up",
        }
    }
}

/// Caps applied for one sync cycle (and surfaced on status).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UploadPolicySnapshot {
    pub mode: UploadPolicyMode,
    /// In-memory upload queue depth for this cycle.
    pub max_pending: usize,
    /// Max entries sealed/uploaded this cycle.
    pub max_upload_entries: usize,
    /// Soft byte budget for selection this cycle.
    pub max_upload_bytes: usize,
    /// Concurrent S3 PUTs for this cycle's upload partition.
    pub concurrency: usize,
    /// Same as `max_upload_bytes` when auto (explicit for operators).
    pub budget_bytes: usize,
    pub headroom_rss_bytes: Option<u64>,
    pub rss_bytes: Option<u64>,
    pub rss_limit_bytes: u64,
    pub ewma_upload_bps: f64,
    pub cpu_percent: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub foreground_pressure: Option<ForegroundPressure>,
    /// Short machine-readable reason when throttled below max budget.
    pub throttle_reason: Option<String>,
    /// Stable coarse source of the throttle (always present; `none` when idle).
    #[serde(default)]
    pub throttle_source: UploadThrottleSource,
}

impl Default for UploadPolicySnapshot {
    fn default() -> Self {
        Self {
            mode: UploadPolicyMode::Auto,
            max_pending: 8,
            max_upload_entries: 8,
            max_upload_bytes: 8 * 1024 * 1024,
            concurrency: 2,
            budget_bytes: 8 * 1024 * 1024,
            headroom_rss_bytes: None,
            rss_bytes: None,
            rss_limit_bytes: DEFAULT_RSS_LIMIT_MB * 1024 * 1024,
            ewma_upload_bps: 0.0,
            cpu_percent: None,
            foreground_pressure: None,
            throttle_reason: None,
            throttle_source: UploadThrottleSource::None,
        }
    }
}

/// Node-supplied foreground pressure sample used by adaptive upload policy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForegroundPressure {
    /// Source label so status can prove the throttle is from live telemetry,
    /// not the `LASTDB_SYNC_INTERACTIVE_BUSY` test override.
    pub source: String,
    /// Worst recent foreground app/verb p95 latency in milliseconds.
    pub foreground_p95_ms: Option<u64>,
    /// P95 threshold used to classify the foreground as busy.
    pub foreground_busy_ms: u64,
    /// QoS total permit occupancy at sample time.
    pub qos_total_in_use: usize,
    pub qos_total_permits: usize,
    /// Interactive QoS sheds observed since the previous sample.
    pub qos_interactive_shed_delta: u64,
    /// Process CPU percent from the node sampler.
    pub cpu_percent: Option<f64>,
}

impl ForegroundPressure {
    pub fn interactive_busy(&self) -> bool {
        self.foreground_p95_ms
            .is_some_and(|p95| p95 >= self.foreground_busy_ms)
            || self.qos_interactive_shed_delta > 0
            || (self.qos_total_permits > 0 && self.qos_total_in_use >= self.qos_total_permits)
    }
}
