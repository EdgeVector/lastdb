use super::*;

#[derive(Debug, Default)]
pub(super) struct CpuProbe {
    pub(super) last_wall: Option<Instant>,
    pub(super) last_cpu_secs: Option<f64>,
}

impl CpuProbe {
    pub(super) fn sample_percent(&mut self) -> Option<f64> {
        let wall = Instant::now();
        let cpu = process_cpu_secs()?;
        let out = match (self.last_wall, self.last_cpu_secs) {
            (Some(prev_wall), Some(prev_cpu)) => {
                let wall_delta = wall.duration_since(prev_wall).as_secs_f64();
                let cpu_delta = cpu - prev_cpu;
                if wall_delta > 0.0 && cpu_delta >= 0.0 {
                    Some(((cpu_delta / wall_delta) * 100.0 * 100.0).round() / 100.0)
                } else {
                    None
                }
            }
            _ => None,
        };
        self.last_wall = Some(wall);
        self.last_cpu_secs = Some(cpu);
        out
    }
}

pub(super) fn sample_id(sampled_at: u64) -> String {
    let nanos = fold_db::clock::unix_nanos() % 1_000_000_000;
    format!("{sampled_at:020}-{nanos:09}-{}", std::process::id())
}

pub(super) fn process_cpu_secs() -> Option<f64> {
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

pub(super) fn timeval_secs(tv: libc::timeval) -> f64 {
    tv.tv_sec as f64 + (tv.tv_usec as f64 / 1_000_000.0)
}

#[cfg(target_os = "linux")]
pub(super) fn current_rss_bytes() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let resident_pages = statm.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    // SAFETY: sysconf is read-only; a positive page size is expected on Linux.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    (page_size > 0).then_some(resident_pages.saturating_mul(page_size as u64))
}

#[cfg(target_os = "macos")]
#[allow(deprecated)]
pub(super) fn current_rss_bytes() -> Option<u64> {
    // Prefer live resident size (matches `ps` RSS). `getrusage(RUSAGE_SELF).ru_maxrss`
    // on Darwin is a high-water mark in bytes and misled operators during the
    // 2026-07-14 re-enable (status showed 18 GiB peak while current was ~4–8 GiB).
    // SAFETY: mach task_info writes into a stack buffer of the declared size.
    unsafe {
        let mut info = std::mem::MaybeUninit::<libc::mach_task_basic_info>::uninit();
        let mut count = libc::MACH_TASK_BASIC_INFO_COUNT;
        let kr = libc::task_info(
            mach2::traps::mach_task_self(),
            libc::MACH_TASK_BASIC_INFO,
            info.as_mut_ptr() as *mut _,
            &mut count,
        );
        if kr == libc::KERN_SUCCESS {
            let info = info.assume_init();
            return Some(info.resident_size);
        }
    }
    None
}

#[cfg(all(not(target_os = "linux"), not(target_os = "macos")))]
pub(super) fn current_rss_bytes() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the provided rusage struct on success.
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    // SAFETY: rc == 0 means getrusage initialized `usage`.
    let usage = unsafe { usage.assume_init() };
    Some(usage.ru_maxrss as u64)
}

/// Current and lifetime-peak physical footprint of this process.
#[derive(Debug, Clone, Copy)]
pub(super) struct PhysFootprint {
    pub(super) current_bytes: u64,
    pub(super) peak_bytes: u64,
}

/// Read `phys_footprint` in-process, the same quantity `/usr/bin/footprint -p`
/// reports and the one the kernel's own memory accounting bills.
///
/// `mach_task_basic_info.resident_size` (what [`current_rss_bytes`] returns and
/// what `ps` prints) omits compressed pages. On a node whose working set is
/// mostly compressible that gap is not a rounding error: measured 1.39 GiB RSS
/// against 9,547 MB footprint on the primary, 2026-08-03.
///
/// `proc_pid_rusage(RUSAGE_INFO_V4)` is used rather than shelling out to
/// `/usr/bin/footprint`, which costs a process spawn per status read.
#[cfg(target_os = "macos")]
pub(super) fn current_phys_footprint() -> Option<PhysFootprint> {
    let mut info = std::mem::MaybeUninit::<libc::rusage_info_v4>::uninit();
    // SAFETY: proc_pid_rusage writes a `rusage_info_v4` for flavor
    // RUSAGE_INFO_V4 into the provided buffer, which is sized for exactly that.
    let rc = unsafe {
        libc::proc_pid_rusage(
            std::process::id() as libc::c_int,
            libc::RUSAGE_INFO_V4,
            info.as_mut_ptr().cast::<libc::rusage_info_t>(),
        )
    };
    if rc != 0 {
        return None;
    }
    // SAFETY: rc == 0 means the buffer was initialized.
    let info = unsafe { info.assume_init() };
    // A zero footprint is not a real reading — treat it as unavailable rather
    // than reporting a node that uses no memory.
    (info.ri_phys_footprint > 0).then_some(PhysFootprint {
        current_bytes: info.ri_phys_footprint,
        // The lifetime max can lag the current sample on the first reads;
        // never report a peak below what we just measured.
        peak_bytes: info
            .ri_lifetime_max_phys_footprint
            .max(info.ri_phys_footprint),
    })
}

/// No footprint accounting outside macOS. Linux RSS from `/proc/self/statm` is
/// genuinely resident — it does not hide a compressed working set — so callers
/// fall back to naming RSS rather than implying a footprint they never read.
#[cfg(not(target_os = "macos"))]
pub(super) fn current_phys_footprint() -> Option<PhysFootprint> {
    None
}

/// The ceiling `lastdbd-memory-guard` will restart this process at.
///
/// Read from the environment on every sample rather than cached at boot: the
/// guard reads it the same way, so a limit changed under a running node is
/// reported as the guard would actually enforce it.
pub(super) fn memory_limit_bytes() -> Option<u64> {
    let limit = fold_db::memory_budget::parse_rss_limit_bytes(
        std::env::var(fold_db::memory_budget::RSS_LIMIT_MB_ENV).ok(),
    );
    (limit > 0).then_some(limit)
}

/// Which gauge `lastdbd-memory-guard` samples when it decides to restart.
///
/// A ceiling does not say what it is a ceiling *on*, and on macOS the two
/// candidates have measured 6x apart on the live primary. Pairing the limit
/// with the wrong one turns the restart distance into a number that is
/// confidently wrong in the safe direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryGuardMetric {
    /// `ps -o rss=` — the legacy gauge. Excludes compressed and swapped pages,
    /// so it sits far below what the process holds.
    Rss,
    /// `proc_pid_rusage` phys_footprint — what macOS jetsam counts, and what
    /// the guard has enforced since the 2026-08-08 decision.
    #[default]
    PhysFootprint,
}

impl MemoryGuardMetric {
    /// The gauge's name as the guard logs it, so one string greps across the
    /// status line and `lastdbd-memory-guard.log`.
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Rss => "RSS",
            Self::PhysFootprint => "phys_footprint",
        }
    }
}

/// The gauge the installed guard enforces [`memory_limit_bytes`] on.
///
/// Read from the environment on every sample, for the same reason the limit is:
/// the status line must describe the guard that is actually installed, not the
/// one this binary was written against. Between 2026-08-23 and 2026-09-06 it
/// described neither — it asserted RSS as a literal while the guard enforced
/// phys_footprint, and four routine runs re-confirmed a node as healthy at 29%
/// of a ceiling it was really at 40% of.
///
/// The residual exposure is shared with the limit and is worth naming: both are
/// read from *this* process's environment, so a guard configured differently in
/// its own LaunchAgent is not observable here. Mirroring the guard's default is
/// still strictly better than hard-coding the gauge it stopped using.
pub(super) fn memory_guard_metric() -> MemoryGuardMetric {
    match std::env::var(fold_db::memory_budget::GUARD_METRIC_ENV)
        .ok()
        .as_deref()
    {
        Some("rss") => MemoryGuardMetric::Rss,
        // The guard validates this env var at startup and refuses anything but
        // `rss`/`footprint`, so an unset or unrecognised value here means the
        // guard is running its own default.
        _ => MemoryGuardMetric::PhysFootprint,
    }
}

/// Disk consumed by the node home, in allocated blocks.
///
/// This reports what the volume has actually given the store, not the summed
/// length of its records — see
/// [`fold_db::mini_cutover::plane_roles::allocated_bytes`] for why those differ
/// (measured 17.5% on the primary) and why a capacity gauge must use the
/// former.
pub(super) fn dir_size_bytes(path: &Path) -> std::io::Result<u64> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Ok(0);
    }
    if metadata.is_file() {
        return Ok(fold_db::mini_cutover::plane_roles::allocated_bytes(
            &metadata,
        ));
    }
    if !metadata.is_dir() {
        return Ok(0);
    }
    let mut total = 0u64;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        total = total.saturating_add(dir_size_bytes(&entry.path()).unwrap_or(0));
    }
    Ok(total)
}

pub(super) struct DataDirSizeState {
    /// Last completed measurement. Meaningless until `measured_at` is set —
    /// read it only through [`DataDirSizeCache::get`], which returns `None`
    /// rather than handing back this placeholder.
    pub(super) bytes: u64,
    pub(super) measured_at: Option<Instant>,
    pub(super) refreshing: bool,
    /// Path this cache entry describes. Changing path invalidates the value so
    /// multi-home tests (and rare multi-home processes) never serve another
    /// tree's size under a different root.
    pub(super) path: Option<std::path::PathBuf>,
}

/// Caches a recursive directory walk behind a TTL, refreshing off-runtime.
///
/// [`dir_size_bytes`] stats every entry under the measured root — ~132k syscalls
/// on Tom's multi-GiB primary, measured at ~600ms per call and 67s under
/// page-cache pressure. It used to run inline on *every* `/api/status`, which
/// made `kanban ping` — the documented cheap health check — the single most
/// expensive operation on the node, and let a health check time out and read
/// as a dead node.
///
/// The value is a display gauge (status line + telemetry field); nothing gates
/// behavior on it, so serving a slightly stale number is a strictly better
/// trade than re-walking the store per request.
///
/// **The walk never runs on the request path — not even the first one.** A TTL
/// covers every call except the cold one, and the cold one is not the rare
/// case: it is every restart, which is exactly when every agent's health check
/// arrives at once. Measured on the primary after the 2026-08-17T19:40Z
/// restart, `client=kanban kind=status` n=29 spent 179.95 s of 187.11 s in
/// `status_data_dir`, with one call at **36,985 ms**. `/api/status` is what
/// `kanban ping` and `lastdb status` both ride, and a 37-second liveness probe
/// reads as a dead node — which invites exactly the doctor/restart loops the
/// standing rules forbid. Single-flighting the cold walk (#1542) removed the
/// duplicate walks but not the wait; the caller still paid for one.
///
/// So a cold [`get`](Self::get) returns [`None`] and kicks off the walk in the
/// background. `None` must reach the operator as "measuring…", never as `0`:
/// a wrong number is worse than an absent one for a gauge read to judge disk
/// pressure, and [`crate::ops::gauge`] already makes that rule explicit
/// ("a missing field becomes `Unavailable`, never `Measured(0)`").
///
/// Two process-global instances are used: [`NODE_HOME_SIZE`] (primary operator
/// gauge) and [`DATA_DIR_SIZE`] (data-only secondary).
pub(super) struct DataDirSizeCache {
    pub(super) state: Mutex<DataDirSizeState>,
    /// Completed walks of this tree, since process start.
    ///
    /// Readable on a live node (rendered on the status line, published as
    /// `*_size_walks`) because the property that matters here — N concurrent
    /// cold callers cost ONE walk — is not observable in any other gauge. The
    /// only other candidate, the `status_data_dir` request phase, measures
    /// *wait*: a caller that blocks behind another caller's walk records that
    /// walk's full duration, so one walk with N waiters and N real walks are
    /// byte-identical in it. That is why #1542's acceptance criterion ("summed
    /// `status_data_dir` in the first minute is one walk's worth, not N")
    /// could never pass or fail. This counter can.
    pub(super) walks: AtomicU64,
}

pub(super) static DATA_DIR_SIZE: DataDirSizeCache = DataDirSizeCache::new();
pub(super) static NODE_HOME_SIZE: DataDirSizeCache = DataDirSizeCache::new();

impl DataDirSizeCache {
    pub(super) const fn new() -> Self {
        Self {
            state: Mutex::new(DataDirSizeState {
                bytes: 0,
                measured_at: None,
                refreshing: false,
                path: None,
            }),
            walks: AtomicU64::new(0),
        }
    }

    /// Completed directory walks performed by this cache since process start.
    pub(super) fn walks(&self) -> u64 {
        self.walks.load(Ordering::Relaxed)
    }

    /// A poisoned lock must never take down the status surface.
    pub(super) fn lock(&self) -> std::sync::MutexGuard<'_, DataDirSizeState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Release the single-flight without recording a measurement, so a walk
    /// that failed is retried rather than remembered as `0`.
    pub(super) fn abandon_refresh(&self) {
        self.lock().refreshing = false;
    }

    pub(super) fn store(&self, path: std::path::PathBuf, bytes: u64) {
        self.walks.fetch_add(1, Ordering::Relaxed);
        let mut state = self.lock();
        state.bytes = bytes;
        state.measured_at = Some(Instant::now());
        state.refreshing = false;
        state.path = Some(path);
    }

    /// Size of `path`, or `None` when no walk has completed for it yet.
    ///
    /// Never walks inline. Every call is O(1): it either serves a cached
    /// number (fresh or stale) or reports "not measured yet", and in both of
    /// the latter cases arranges for at most one background walk. `refreshing`
    /// is the single-flight, and it is taken under the state lock, so N
    /// concurrent cold callers spawn one walk between them.
    pub(super) fn get(&'static self, path: &Path, ttl: Duration) -> Option<u64> {
        let mut state = self.lock();
        let same_path = state.path.as_ref().is_some_and(|p| p.as_path() == path);
        if !same_path {
            // Different root than last measure — do not serve another tree.
            state.measured_at = None;
            state.refreshing = false;
            state.bytes = 0;
            state.path = Some(path.to_path_buf());
        }
        // Fresh enough — serve it and walk nothing.
        if let Some(at) = state.measured_at {
            if at.elapsed() < ttl {
                return Some(state.bytes);
            }
        }
        // Cold or stale. Both are served without blocking; they differ only in
        // whether there is a previous number to hand back meanwhile.
        if !state.refreshing {
            // A caller outside a tokio runtime cannot spawn. Leave `refreshing`
            // false so a later in-runtime call still schedules the walk, rather
            // than latching the cache into a permanent "measuring" state.
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                state.refreshing = true;
                let path = path.to_path_buf();
                handle.spawn(async move {
                    match measure_dir_size(path.clone()).await {
                        Some(bytes) => self.store(path, bytes),
                        // A failed walk must not publish a measured `0`; drop
                        // the flag so the next call retries instead of the
                        // cache latching a lie.
                        None => self.abandon_refresh(),
                    }
                });
            }
        }
        state.measured_at.map(|_| state.bytes)
    }
}

/// Allocated bytes under `path`, or `None` when the walk could not run.
///
/// The `None` is load-bearing: [`DataDirSizeCache`] stores only real
/// measurements, so a failed walk leaves the gauge unmeasured instead of
/// publishing a `0` that reads as an empty store.
pub(super) async fn measure_dir_size(path: std::path::PathBuf) -> Option<u64> {
    tokio::task::spawn_blocking(move || dir_size_bytes(&path).ok())
        .await
        .ok()
        .flatten()
}

// lint:file-size-ok moved verbatim from self_metrics.rs; cohesive unit, split further in a later pass
