//! The resolved process memory budget.

use super::*;

/// Where a resolved [`ProcessMemoryBudget::deferred_cap_bytes`] came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeferCapSource {
    /// Derived from headroom under the guard limit.
    Derived,
    /// Set explicitly via [`DEFERRED_BYTES_ENV`].
    EnvOverride,
    /// No headroom: the charged budgets alone project over the guard limit, or
    /// what is left cannot fund [`MIN_DEFERRED_BYTES`]. Deferral is off.
    NoHeadroom,
}

impl DeferCapSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Derived => "derived",
            Self::EnvOverride => "env_override",
            Self::NoHeadroom => "no_headroom",
        }
    }
}

/// Inputs for the pure budget computation (unit-testable without `std::env`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MemoryBudgetInputs {
    pub warm_bytes: u64,
    pub key_cache_bytes: u64,
    pub resident_graph_bytes: u64,
    pub rss_limit_bytes: u64,
    pub rss_multiplier: f64,
    /// Explicit deferred cap from env, if set.
    pub deferred_cap_override: Option<u64>,
}

/// The process's whole memory charge, its RSS projection, and what that leaves
/// for the deferred-persist window.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProcessMemoryBudget {
    pub warm_bytes: u64,
    pub key_cache_bytes: u64,
    pub resident_graph_bytes: u64,
    /// Sum of the budgets this process chose.
    pub charged_bytes: u64,
    pub rss_multiplier: f64,
    /// `charged_bytes * rss_multiplier` — what the charge is expected to cost
    /// in real RSS, before any deferred-write pressure.
    pub projected_rss_bytes: u64,
    pub rss_limit_bytes: u64,
    /// `rss_limit - projected_rss`, saturating at zero.
    pub headroom_bytes: u64,
    pub deferred_cap_bytes: u64,
    pub deferred_cap_source: DeferCapSource,
    /// `projected_rss + deferred_cap` — the whole accounted number.
    pub projected_total_bytes: u64,
    /// Whether the charged budgets alone fit under the guard limit.
    pub fits: bool,
}

/// Result of comparing one live physical-footprint sample with the immutable
/// boot budget. The self-metrics sampler records every field; the two `*_now`
/// flags are edge-triggered so recurring one-minute samples do not spam logs.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RuntimeMemoryBudgetObservation {
    pub measured_footprint_bytes: u64,
    pub projected_total_bytes: u64,
    pub projection_tolerance_bytes: u64,
    pub projection_diverged: bool,
    pub footprint_over_limit: bool,
    pub runtime_degraded: bool,
    pub effective_deferred_cap_bytes: u64,
    pub projection_warning_now: bool,
    pub over_limit_alarm_now: bool,
    /// This sample reopened a hard-latched defer window: the footprint stayed
    /// under [`footprint_latch_recovery_line`] for
    /// [`FOOTPRINT_LATCH_RECOVERY_HOLD_SECS`]. Edge-triggered like the alarm.
    pub runtime_recovered_now: bool,
}

impl ProcessMemoryBudget {
    /// Pure computation from explicit inputs.
    #[must_use]
    pub fn compute(inputs: MemoryBudgetInputs) -> Self {
        let MemoryBudgetInputs {
            warm_bytes,
            key_cache_bytes,
            resident_graph_bytes,
            rss_limit_bytes,
            rss_multiplier,
            deferred_cap_override,
        } = inputs;

        let charged_bytes = warm_bytes
            .saturating_add(key_cache_bytes)
            .saturating_add(resident_graph_bytes)
            .saturating_add(logical_resident_set_charged_bytes());
        let multiplier = rss_multiplier.clamp(MIN_RSS_MULTIPLIER, MAX_RSS_MULTIPLIER);
        let projected_rss_bytes = scale_bytes(charged_bytes, multiplier);
        let headroom_bytes = rss_limit_bytes.saturating_sub(projected_rss_bytes);
        let fits = projected_rss_bytes <= rss_limit_bytes;

        // An explicit cap is an operator decision; honour it even when it does
        // not fit, but the boot log still states the total.
        let (deferred_cap_bytes, deferred_cap_source) =
            if let Some(explicit) = deferred_cap_override {
                (explicit, DeferCapSource::EnvOverride)
            } else {
                let derived = scale_bytes(headroom_bytes, DEFER_HEADROOM_FRACTION);
                if derived < MIN_DEFERRED_BYTES {
                    (0, DeferCapSource::NoHeadroom)
                } else {
                    let floored = derived.max(FLOOR_DEFERRED_BYTES);
                    (
                        floored.min(MAX_DEFERRED_BYTES).min(headroom_bytes),
                        DeferCapSource::Derived,
                    )
                }
            };

        Self {
            warm_bytes,
            key_cache_bytes,
            resident_graph_bytes,
            charged_bytes,
            rss_multiplier: multiplier,
            projected_rss_bytes,
            rss_limit_bytes,
            headroom_bytes,
            deferred_cap_bytes,
            deferred_cap_source,
            projected_total_bytes: projected_rss_bytes.saturating_add(deferred_cap_bytes),
            fits,
        }
    }

    /// Resolve from environment. The resident graph budget comes from the same
    /// [`ResidentPolicy`] the node runs on, so the two can never disagree.
    #[must_use]
    pub fn from_env() -> Self {
        let policy = ResidentPolicy::from_env();
        Self::compute(MemoryBudgetInputs {
            warm_bytes: parse_bytes(std::env::var(WARM_BYTES_ENV).ok(), PRESET_WARM_BYTES),
            key_cache_bytes: parse_bytes(
                std::env::var(KEY_CACHE_BYTES_ENV).ok(),
                PRESET_KEY_CACHE_BYTES,
            ),
            // The resident graph fills even in mode=off (tips are published
            // after every durable store), so it is charged unconditionally.
            resident_graph_bytes: policy.budget_bytes,
            rss_limit_bytes: parse_rss_limit_bytes(std::env::var(RSS_LIMIT_MB_ENV).ok()),
            rss_multiplier: parse_rss_multiplier(std::env::var(RSS_MULTIPLIER_ENV).ok()),
            deferred_cap_override: parse_deferred_cap_override(
                std::env::var(DEFERRED_BYTES_ENV).ok(),
            ),
        })
    }

    /// Log the whole accounted number once at boot — `INFO` when it fits,
    /// `ERROR` when it cannot, so a configuration that will kill the node is
    /// visible before it does rather than after.
    pub fn log_at_boot(&self) {
        if self.fits {
            tracing::info!(
                target: "fold_node::database",
                warm_mb = self.warm_bytes / MIB,
                key_cache_mb = self.key_cache_bytes / MIB,
                resident_graph_mb = self.resident_graph_bytes / MIB,
                charged_mb = self.charged_bytes / MIB,
                rss_multiplier = self.rss_multiplier,
                projected_rss_mb = self.projected_rss_bytes / MIB,
                deferred_cap_mb = self.deferred_cap_bytes / MIB,
                deferred_cap_source = self.deferred_cap_source.as_str(),
                projected_total_mb = self.projected_total_bytes / MIB,
                rss_limit_mb = self.rss_limit_bytes / MIB,
                headroom_mb = self.headroom_bytes / MIB,
                "process memory budget (one accounted number: warm + key cache + \
                 resident graph, projected to RSS, plus the deferred-write window)"
            );
        } else {
            tracing::error!(
                target: "fold_node::database",
                warm_mb = self.warm_bytes / MIB,
                key_cache_mb = self.key_cache_bytes / MIB,
                resident_graph_mb = self.resident_graph_bytes / MIB,
                charged_mb = self.charged_bytes / MIB,
                rss_multiplier = self.rss_multiplier,
                projected_rss_mb = self.projected_rss_bytes / MIB,
                rss_limit_mb = self.rss_limit_bytes / MIB,
                over_by_mb = self.projected_rss_bytes.saturating_sub(self.rss_limit_bytes) / MIB,
                warm_env = WARM_BYTES_ENV,
                limit_env = RSS_LIMIT_MB_ENV,
                "process memory budget CANNOT FIT the RSS guard: the chosen budgets \
                 project over the limit before any write pressure, so the memory guard \
                 will kill this node. Deferred writes are disabled (every batch persists \
                 inline). Lower a budget or raise the guard deliberately."
            );
        }
    }
}

pub(super) const MIB: u64 = 1024 * 1024;

/// Process-wide budget, resolved once. Same idiom as
/// [`crate::atom::max_atom_content_bytes`]: one env read, one answer, so the
/// boot log and every runtime consumer quote the same number.
pub fn process_memory_budget() -> &'static ProcessMemoryBudget {
    static BUDGET: OnceLock<ProcessMemoryBudget> = OnceLock::new();
    BUDGET.get_or_init(ProcessMemoryBudget::from_env)
}

#[allow(clippy::cast_precision_loss, clippy::cast_sign_loss)]
pub(super) fn scale_bytes(bytes: u64, factor: f64) -> u64 {
    if factor <= 0.0 {
        return 0;
    }
    let scaled = bytes as f64 * factor;
    if scaled >= u64::MAX as f64 {
        u64::MAX
    } else {
        scaled as u64
    }
}
