//! Adaptive upload throughput policy.
//!
//! # Why not a hard-coded entry count?
//!
//! After the 2026-07-14 re-enable thrash, emergency caps pinned upload to
//! 1 (then 8) entries per cycle. That is the same class of mistake as treating
//! “max UDS connections” as the product limiter: it is not the real resource.
//!
//! The real resources are:
//! - **RAM headroom** vs the memory-guard RSS ceiling (default 6 GiB)
//! - **CPU / interactive load** (don't steal the agent query path)
//! - **Measured network** (EWMA of successful upload bytes/sec)
//!
//! This module turns those into a **byte budget + concurrency** for the next
//! sync cycle. Entry count is only a derived sanity ceiling so we do not hold
//! tens of thousands of tiny stubs in the in-memory upload queue.
//!
//! # Live CPU sample cadence
//!
//! `sample_process_cpu_percent` is called **once per upload-policy refresh**
//! (sync cycle / backup drain tick, typically every few seconds — not per
//! chunk PUT). It reads process user+sys CPU via `getrusage(RUSAGE_SELF)` and
//! computes percent over the wall time since the previous sample. Operators
//! reading `throttle_reason=high_cpu` on status therefore see a cycle-rate
//! signal, not a per-chunk spike. Cost is one syscall + a process-wide mutex
//! per refresh; first sample after process start returns `None` until a second
//! refresh establishes a delta.
//!
//! When the embedding node also publishes [`ForegroundPressure`] (status
//! sampler), its `cpu_percent` is preferred; the process sample is the
//! always-on fallback so a normally launched primary does not need a special
//! env var for the `cpu_percent > 85` throttle branch to be reachable.
//!
//! # Env (optional overrides)
//!
//! | Variable | Meaning |
//! |----------|---------|
//! | `LASTDB_SYNC_UPLOAD_MODE` | `auto` (default) or `fixed` |
//! | `LASTDB_SYNC_RSS_LIMIT_MB` | RSS ceiling used for headroom (default 6144) |
//! | `LASTDB_SYNC_UPLOAD_BUDGET_MIN_MB` | Floor budget per cycle (default 1) |
//! | `LASTDB_SYNC_UPLOAD_BUDGET_MAX_MB` | Ceiling budget per cycle (default 256) |
//! | `LASTDB_SYNC_UPLOAD_RESERVED_MB` | RSS reserved for interactive (default 512) |
//! | `LASTDB_SYNC_CONCURRENCY` | Fixed: hard concurrency (clamped to 1..=16). Auto: optional floor (still clamped to 1..=16); `auto` means derive |
//! | `LASTDB_SYNC_MAX_UPLOAD_ENTRIES` | Fixed: hard entry cap. Auto: **floor** on the RSS-derived entry count (still clamped to `DEFAULT_ENTRY_CEILING`) |
//! | `LASTDB_SYNC_MAX_UPLOAD_BYTES` | Fixed: hard byte budget. Auto: **ignored** — Auto derives the byte budget from RSS headroom / EWMA / CPU |
//! | `LASTDB_SYNC_INTERACTIVE_BUSY` | Force interactive throttle (`1`/`true`) for tests / emergency |

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

/// Default RSS limit matching `lastdbd-memory-guard` (`LASTDBD_RSS_LIMIT_MB=6144`).
pub const DEFAULT_RSS_LIMIT_MB: u64 = 6144;
/// Keep this much RSS free for interactive work before claiming upload budget.
pub const DEFAULT_RESERVED_MB: u64 = 512;
pub const DEFAULT_BUDGET_MIN_MB: u64 = 1;
pub const DEFAULT_BUDGET_MAX_MB: u64 = 256;
/// Sanity entry ceiling when auto-deriving from budget (tiny stubs).
///
/// This must stay comfortably above the write volume one dirty sync interval can
/// generate on the primary fleet. At ~730 writes/sec and a 30s interval, a
/// 4,096-entry ceiling guarantees backlog growth even with ample RSS headroom.
pub const DEFAULT_ENTRY_CEILING: usize = 32_768;
pub const MIN_CONCURRENCY: usize = 1;
pub const MAX_CONCURRENCY: usize = 16;

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

/// Inputs for pure policy computation (unit-testable).
#[derive(Debug, Clone)]
pub struct UploadPolicyInputs {
    pub mode: UploadPolicyMode,
    /// Config defaults / fixed-mode knobs.
    pub config_max_pending: usize,
    pub config_max_upload_entries: usize,
    pub config_max_upload_bytes: usize,
    pub config_concurrency: usize,
    pub rss_bytes: Option<u64>,
    pub rss_limit_bytes: u64,
    pub reserved_bytes: u64,
    pub budget_min_bytes: u64,
    pub budget_max_bytes: u64,
    /// EWMA of successful upload throughput (bytes/sec). 0 = unknown.
    pub ewma_upload_bps: f64,
    /// Process CPU percent if known (0..=N*100).
    pub cpu_percent: Option<f64>,
    /// True when interactive QoS is busy (shed or high in-use).
    pub interactive_busy: bool,
    pub foreground_pressure: Option<ForegroundPressure>,
    /// Available logical CPUs (for concurrency default).
    pub available_parallelism: usize,
}

/// Runtime state retained across sync cycles.
pub(crate) struct UploadPolicyRuntime {
    /// Last computed snapshot (for status + next-cycle concurrency).
    pub last: std::sync::Mutex<UploadPolicySnapshot>,
    /// EWMA upload bytes/sec × 1000 (fixed-point) for lock-free updates.
    ewma_bps_milli: AtomicU64,
}

impl Default for UploadPolicyRuntime {
    fn default() -> Self {
        Self {
            last: std::sync::Mutex::new(UploadPolicySnapshot::default()),
            ewma_bps_milli: AtomicU64::new(0),
        }
    }
}

impl UploadPolicyRuntime {
    pub fn ewma_bps(&self) -> f64 {
        self.ewma_bps_milli.load(Ordering::Relaxed) as f64 / 1000.0
    }

    /// Update EWMA after a successful upload of `bytes` taking `elapsed_secs`.
    pub fn record_upload_sample(&self, bytes: u64, elapsed_secs: f64) {
        if bytes == 0 || elapsed_secs <= 0.0 {
            return;
        }
        let instant = bytes as f64 / elapsed_secs;
        let prev = self.ewma_bps();
        // α = 0.3 — responsive but not noisy.
        let next = if prev <= 0.0 {
            instant
        } else {
            0.3 * instant + 0.7 * prev
        };
        self.ewma_bps_milli
            .store((next * 1000.0) as u64, Ordering::Relaxed);
    }

    pub fn store_snapshot(&self, snap: UploadPolicySnapshot) {
        if let Ok(mut g) = self.last.lock() {
            *g = snap;
        }
    }
}

/// Resolve mode from env (`LASTDB_SYNC_UPLOAD_MODE`).
pub fn mode_from_env() -> UploadPolicyMode {
    match std::env::var("LASTDB_SYNC_UPLOAD_MODE") {
        Ok(s) if s.eq_ignore_ascii_case("fixed") => UploadPolicyMode::Fixed,
        Ok(s) if s.eq_ignore_ascii_case("auto") => UploadPolicyMode::Auto,
        _ => UploadPolicyMode::Auto,
    }
}

/// Live samples that feed auto-mode budget computation.
#[derive(Debug, Clone, Default)]
pub struct UploadPolicyLiveSamples {
    pub rss_bytes: Option<u64>,
    pub ewma_upload_bps: f64,
    pub cpu_percent: Option<f64>,
    pub interactive_busy: bool,
    pub foreground_pressure: Option<ForegroundPressure>,
}

/// Build policy inputs from config + live samples + env.
pub fn policy_inputs_from_env_and_config(
    config_max_pending: usize,
    config_max_upload_entries: usize,
    config_max_upload_bytes: usize,
    config_concurrency: usize,
    live: &UploadPolicyLiveSamples,
) -> UploadPolicyInputs {
    let rss_bytes = live.rss_bytes;
    let ewma_upload_bps = live.ewma_upload_bps;
    let cpu_percent = live
        .foreground_pressure
        .as_ref()
        .and_then(|p| p.cpu_percent)
        .or(live.cpu_percent);
    let interactive_busy = live.interactive_busy
        || live
            .foreground_pressure
            .as_ref()
            .is_some_and(ForegroundPressure::interactive_busy);
    let rss_limit_mb = env_flag::var_parsed::<u64>("LASTDB_SYNC_RSS_LIMIT_MB")
        .or_else(|| env_flag::var_parsed::<u64>("LASTDBD_RSS_LIMIT_MB"))
        .unwrap_or(DEFAULT_RSS_LIMIT_MB);
    let reserved_mb = env_flag::var_parsed::<u64>("LASTDB_SYNC_UPLOAD_RESERVED_MB")
        .unwrap_or(DEFAULT_RESERVED_MB);
    let budget_min_mb = env_flag::var_parsed::<u64>("LASTDB_SYNC_UPLOAD_BUDGET_MIN_MB")
        .unwrap_or(DEFAULT_BUDGET_MIN_MB);
    let budget_max_mb = env_flag::var_parsed::<u64>("LASTDB_SYNC_UPLOAD_BUDGET_MAX_MB")
        .unwrap_or(DEFAULT_BUDGET_MAX_MB);

    // Env overrides for the config-shaped knobs. Semantics differ by mode
    // (see module docs): Fixed treats entries/bytes as hard caps; Auto uses
    // entries as a floor on the derived entry count and ignores the byte knob.
    let fixed_entries = env_flag::var_parsed::<usize>("LASTDB_SYNC_MAX_UPLOAD_ENTRIES")
        .unwrap_or(config_max_upload_entries);
    let fixed_bytes = env_flag::var_parsed::<usize>("LASTDB_SYNC_MAX_UPLOAD_BYTES")
        .unwrap_or(config_max_upload_bytes);
    let fixed_pending =
        env_flag::var_parsed::<usize>("LASTDB_SYNC_MAX_PENDING").unwrap_or(config_max_pending);

    let concurrency = match std::env::var("LASTDB_SYNC_CONCURRENCY") {
        Ok(s) if s.eq_ignore_ascii_case("auto") => 0, // 0 = derive in auto
        Ok(s) => s.parse().unwrap_or(config_concurrency),
        Err(_) => config_concurrency,
    };

    let cpus = std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get);

    UploadPolicyInputs {
        mode: mode_from_env(),
        config_max_pending: fixed_pending,
        config_max_upload_entries: fixed_entries,
        config_max_upload_bytes: fixed_bytes,
        config_concurrency: if concurrency == 0 {
            config_concurrency
        } else {
            concurrency
        },
        rss_bytes,
        rss_limit_bytes: rss_limit_mb.saturating_mul(1024 * 1024),
        reserved_bytes: reserved_mb.saturating_mul(1024 * 1024),
        budget_min_bytes: budget_min_mb.saturating_mul(1024 * 1024).max(64 * 1024),
        budget_max_bytes: budget_max_mb
            .saturating_mul(1024 * 1024)
            .max(budget_min_mb.saturating_mul(1024 * 1024)),
        ewma_upload_bps,
        cpu_percent,
        interactive_busy,
        foreground_pressure: live.foreground_pressure.clone(),
        available_parallelism: cpus,
    }
}

/// Pure policy: map resources → cycle caps.
pub fn compute_upload_policy(inputs: &UploadPolicyInputs) -> UploadPolicySnapshot {
    if inputs.mode == UploadPolicyMode::Fixed {
        // Same global S3 concurrency safety ceiling as Auto — Fixed mode is
        // not a license to open unbounded concurrent PUTs.
        let concurrency = inputs
            .config_concurrency
            .clamp(MIN_CONCURRENCY, MAX_CONCURRENCY);
        let reason = Some("fixed".into());
        return UploadPolicySnapshot {
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
        };
    }

    let mut reason: Option<String> = None;
    let rss = inputs.rss_bytes.unwrap_or(0);
    // Absolute free space under the memory-guard kill line (for status).
    let free_under_limit = inputs.rss_limit_bytes.saturating_sub(rss);
    // Soft reserve only when there is room for it — a 5.5 GiB steady-state
    // process against a 6 GiB guard must not look like "0 headroom forever".
    let effective_reserve = if free_under_limit > inputs.reserved_bytes.saturating_mul(2) {
        inputs.reserved_bytes
    } else {
        free_under_limit / 4
    };
    let headroom = free_under_limit.saturating_sub(effective_reserve);

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

    // Concurrency: CPU-shaped, reduced under pressure.
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
    concurrency = concurrency.clamp(MIN_CONCURRENCY, MAX_CONCURRENCY);

    // Entry ceiling: derive from budget assuming ~4 KiB typical sealed entry,
    // clamp to DEFAULT_ENTRY_CEILING; never below config when config is higher
    // emergency minimum for drain (use at least 8).
    let typical = 4 * 1024u64;
    let derived_entries = ((budget / typical) as usize).clamp(8, DEFAULT_ENTRY_CEILING);
    // Prefer derived over the old static 8. Config/env entry count is a floor
    // in auto mode when explicitly raised; the byte budget remains the primary
    // RAM guard for fat entries.
    let max_entries = derived_entries
        .max(inputs.config_max_upload_entries)
        .clamp(8, DEFAULT_ENTRY_CEILING);

    // In-memory queue matches per-cycle selection so we don't deserialize
    // more than we'll upload (re-enable thrash lesson).
    let max_pending = max_entries;

    let throttle_source = UploadThrottleSource::from_reason(reason.as_deref());
    UploadPolicySnapshot {
        mode: UploadPolicyMode::Auto,
        max_pending,
        max_upload_entries: max_entries,
        max_upload_bytes: budget as usize,
        concurrency,
        budget_bytes: budget as usize,
        headroom_rss_bytes: Some(headroom),
        rss_bytes: inputs.rss_bytes,
        rss_limit_bytes: inputs.rss_limit_bytes,
        ewma_upload_bps: inputs.ewma_upload_bps,
        cpu_percent: inputs.cpu_percent,
        foreground_pressure: inputs.foreground_pressure.clone(),
        throttle_reason: reason,
        throttle_source,
    }
}

/// Best-effort process CPU percent since the previous sample (0..=N×100).
///
/// See module docs for cadence. Safe to call from `refresh_upload_policy` once
/// per cycle; do **not** call from per-chunk upload paths.
pub fn sample_process_cpu_percent() -> Option<f64> {
    #[cfg(unix)]
    {
        sample_process_cpu_percent_unix()
    }
    #[cfg(not(unix))]
    {
        None
    }
}

#[cfg(unix)]
fn sample_process_cpu_percent_unix() -> Option<f64> {
    struct Probe {
        last_wall: Instant,
        last_cpu_secs: f64,
    }
    static PROBE: OnceLock<Mutex<Option<Probe>>> = OnceLock::new();
    let state = PROBE.get_or_init(|| Mutex::new(None));
    let Ok(mut guard) = state.lock() else {
        return None;
    };
    let wall = Instant::now();
    let cpu = process_cpu_secs()?;
    let out = match guard.as_ref() {
        Some(prev) => {
            let wall_delta = wall.duration_since(prev.last_wall).as_secs_f64();
            let cpu_delta = cpu - prev.last_cpu_secs;
            if wall_delta > 0.0 && cpu_delta >= 0.0 {
                // Round to hundredths so status JSON stays stable.
                Some(((cpu_delta / wall_delta) * 100.0 * 100.0).round() / 100.0)
            } else {
                None
            }
        }
        None => None,
    };
    *guard = Some(Probe {
        last_wall: wall,
        last_cpu_secs: cpu,
    });
    out
}

#[cfg(unix)]
fn process_cpu_secs() -> Option<f64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the provided rusage struct on success.
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    // SAFETY: rc == 0 means getrusage initialized `usage`.
    let usage = unsafe { usage.assume_init() };
    Some(timeval_secs(usage.ru_utime) + timeval_secs(usage.ru_stime))
}

#[cfg(unix)]
fn timeval_secs(tv: libc::timeval) -> f64 {
    tv.tv_sec as f64 + (tv.tv_usec as f64 / 1_000_000.0)
}

/// Best-effort process RSS in bytes (macOS / Linux). `None` if unavailable.
pub fn sample_rss_bytes() -> Option<u64> {
    #[cfg(target_os = "macos")]
    {
        // Prefer live resident size (matches `ps` RSS).
        use std::mem::MaybeUninit;
        // task_info path via libc if linked; fallback: parse `ps`.
        if let Some(v) = sample_rss_bytes_macos() {
            return Some(v);
        }
        let _ = MaybeUninit::<u8>::uninit();
    }
    #[cfg(target_os = "linux")]
    {
        if let Ok(s) = std::fs::read_to_string("/proc/self/status") {
            for line in s.lines() {
                if let Some(rest) = line.strip_prefix("VmRSS:") {
                    let kb: u64 = rest
                        .split_whitespace()
                        .next()
                        .and_then(|t| t.parse().ok())?;
                    return Some(kb.saturating_mul(1024));
                }
            }
        }
    }
    // Portable fallback: `ps -o rss=` for this pid (kb).
    sample_rss_via_ps()
}

#[cfg(target_os = "macos")]
fn sample_rss_bytes_macos() -> Option<u64> {
    // mach task_info is awkward without extra crates; use ps.
    sample_rss_via_ps()
}

fn sample_rss_via_ps() -> Option<u64> {
    let pid = std::process::id().to_string();
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout);
    let kb: u64 = s.trim().parse().ok()?;
    Some(kb.saturating_mul(1024))
}
