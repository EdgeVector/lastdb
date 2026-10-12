//! Pure helpers for [`super::upload_policy::compute_upload_policy`].

use super::upload_policy::*;

/// Fixed-mode caps: config/env values pass through, concurrency stays clamped.
pub(super) fn fixed_upload_policy(inputs: &UploadPolicyInputs) -> UploadPolicySnapshot {
    // Same global S3 concurrency safety ceiling as Auto — Fixed mode is
    // not a license to open unbounded concurrent PUTs.
    let concurrency = inputs
        .config_concurrency
        .clamp(MIN_CONCURRENCY, MAX_CONCURRENCY);
    let reason = Some("fixed".into());
    UploadPolicySnapshot {
        mode: UploadPolicyMode::Fixed,
        max_pending: inputs.config_max_pending,
        max_upload_entries: inputs.config_max_upload_entries,
        max_upload_bytes: inputs.config_max_upload_bytes,
        concurrency,
        budget_bytes: inputs.config_max_upload_bytes,
        headroom_rss_bytes: inputs.rss_bytes.map(|r| {
            inputs
                .rss_limit_bytes
                .saturating_sub(r)
                .saturating_sub(inputs.reserved_bytes)
        }),
        rss_bytes: inputs.rss_bytes,
        rss_limit_bytes: inputs.rss_limit_bytes,
        ewma_upload_bps: inputs.ewma_upload_bps,
        cpu_percent: inputs.cpu_percent,
        foreground_pressure: inputs.foreground_pressure.clone(),
        throttle_source: UploadThrottleSource::from_reason(reason.as_deref()),
        throttle_reason: reason,
    }
}

/// Auto-mode byte budget and the throttle reason that shaped it.
pub(super) fn auto_budget(
    inputs: &UploadPolicyInputs,
    rss: u64,
    headroom: u64,
) -> (u64, Option<String>) {
    let mut reason: Option<String> = None;
    // Usage-ratio throttle (primary control):
    //   < 85% of RSS limit → full budget_max (upload is not the risk)
    //   85% → 98%         → linear scale budget_max → budget_min
    //   ≥ 98%             → budget_min only (near memory-guard kill)
    //
    // Absolute headroom (limit − rss − reserve) alone is wrong for Mini: the
    // multi-GB Last Store + embeddings baseline already sits near the guard without
    // any upload thrash. The kill line is the circuit breaker; below it we
    // should use the network, not starve at 1 MiB/cycle.
    let mut budget = inputs.budget_max_bytes;
    if inputs.rss_bytes.is_none() {
        budget = (inputs.budget_min_bytes + inputs.budget_max_bytes) / 2;
        reason = Some("rss_unknown".into());
    } else if inputs.rss_limit_bytes > 0 {
        let usage = rss as f64 / inputs.rss_limit_bytes as f64;
        if usage >= 0.98 {
            budget = inputs.budget_min_bytes;
            reason = Some("rss_near_limit".into());
        } else if usage >= 0.85 {
            let t = ((usage - 0.85) / (0.98 - 0.85)).clamp(0.0, 1.0);
            budget = ((inputs.budget_max_bytes as f64) * (1.0 - t)
                + (inputs.budget_min_bytes as f64) * t) as u64;
            reason = Some("rss_pressure".into());
        }
        // Also never claim more absolute free space than we have (minus
        // reserve) — caps a runaway max on a tiny free margin.
        if headroom > 0 {
            budget = budget.min(headroom.saturating_mul(3).max(inputs.budget_min_bytes));
        } else {
            budget = inputs.budget_min_bytes;
            reason = Some("rss_at_limit".into());
        }
    }

    // Network feedback: if EWMA is healthy, don't sit below ~3s of pipe;
    // if EWMA is tiny, avoid filling RAM faster than HTTPS drains.
    if inputs.ewma_upload_bps > 0.0 {
        let net_suggest = (inputs.ewma_upload_bps * 3.0) as u64;
        if net_suggest > budget && reason.is_none() {
            budget = budget.max(net_suggest.min(inputs.budget_max_bytes));
        } else if net_suggest > 0 && net_suggest < budget / 4 {
            budget = budget.min(net_suggest.saturating_mul(2).max(inputs.budget_min_bytes));
            if reason.is_none() {
                reason = Some("slow_network".into());
            }
        }
    }

    // CPU / interactive: shrink when hot.
    if inputs.interactive_busy {
        budget = budget.min(inputs.budget_min_bytes.saturating_mul(4));
        reason = Some("interactive_busy".into());
    } else if let Some(cpu) = inputs.cpu_percent {
        if cpu > 85.0 {
            budget = (budget as f64 * 0.5) as u64;
            reason = Some("high_cpu".into());
        }
    }

    budget = budget.clamp(inputs.budget_min_bytes, inputs.budget_max_bytes);
    (budget, reason)
}

/// Auto-mode upload concurrency: CPU-shaped, reduced under pressure.
pub(super) fn auto_concurrency(inputs: &UploadPolicyInputs, rss: u64) -> usize {
    let mut concurrency = inputs
        .available_parallelism
        .clamp(MIN_CONCURRENCY, MAX_CONCURRENCY);
    if inputs.config_concurrency > 0 {
        concurrency = concurrency.max(inputs.config_concurrency.min(MAX_CONCURRENCY));
    }
    if inputs.interactive_busy {
        concurrency = MIN_CONCURRENCY;
    } else if let Some(cpu) = inputs.cpu_percent {
        if cpu > 85.0 {
            concurrency = concurrency.min(2);
        }
    }
    if inputs.rss_bytes.is_some() && inputs.rss_limit_bytes > 0 {
        let usage = rss as f64 / inputs.rss_limit_bytes as f64;
        if usage >= 0.95 {
            concurrency = concurrency.min(2);
        } else if usage >= 0.90 {
            concurrency = concurrency.min(4);
        }
    }
    concurrency.clamp(MIN_CONCURRENCY, MAX_CONCURRENCY)
}
