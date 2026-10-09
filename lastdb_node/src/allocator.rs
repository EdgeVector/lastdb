//! Process allocator configuration and accounting for the `lastdbd` daemon.

use fold_db::memory_budget::MallocZoneStats;

/// The environment variable that mimalloc reads before `main` starts.
pub const PURGE_DELAY_ENV: &str = "MIMALLOC_PURGE_DELAY";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AllocatorTuning {
    pub name: &'static str,
    pub purge_delay_ms: Option<i64>,
    pub purge_delay_from_env: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AllocatorMetrics {
    pub name: &'static str,
    pub bytes_in_use: Option<u64>,
    pub bytes_held_free: Option<u64>,
    pub committed_bytes: Option<u64>,
    pub reserved_bytes: Option<u64>,
}

#[cfg(feature = "purging-allocator")]
const MI_OPTION_PURGE_DELAY: libmimalloc_sys::mi_option_t = 15;

#[cfg(feature = "purging-allocator")]
mod tracked {
    use std::alloc::{GlobalAlloc, Layout};
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};

    #[repr(align(64))]
    struct Counter(AtomicI64);
    static BYTES: [Counter; 32] = [const { Counter(AtomicI64::new(0)) }; 32];
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    static ACTIVE: AtomicBool = AtomicBool::new(false);
    thread_local! {
        static SLOT: usize = {
            ACTIVE.store(true, Ordering::Relaxed);
            NEXT.fetch_add(1, Ordering::Relaxed) % BYTES.len()
        };
    }

    pub(super) fn active() -> bool {
        ACTIVE.load(Ordering::Relaxed)
    }

    fn adjust(delta: i64) {
        let slot = SLOT.try_with(|slot| *slot).unwrap_or(0);
        BYTES[slot].0.fetch_add(delta, Ordering::Relaxed);
    }

    pub(super) fn live_bytes() -> u64 {
        BYTES
            .iter()
            .map(|n| n.0.load(Ordering::Relaxed))
            .sum::<i64>()
            .max(0) as u64
    }

    /// Requested Rust allocation bytes, including allocations freed by another
    /// thread. Sharded signed deltas avoid one contended counter per allocation.
    pub struct TrackedMiMalloc;

    // SAFETY: all pointer and layout operations delegate unchanged to MiMalloc.
    // Accounting performs no allocation and never accesses the allocation.
    unsafe impl GlobalAlloc for TrackedMiMalloc {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let ptr = unsafe { mimalloc::MiMalloc.alloc(layout) };
            if !ptr.is_null() {
                adjust(layout.size() as i64);
            }
            ptr
        }
        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            let ptr = unsafe { mimalloc::MiMalloc.alloc_zeroed(layout) };
            if !ptr.is_null() {
                adjust(layout.size() as i64);
            }
            ptr
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { mimalloc::MiMalloc.dealloc(ptr, layout) };
            adjust(-(layout.size() as i64));
        }
        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            let ptr = unsafe { mimalloc::MiMalloc.realloc(ptr, layout, size) };
            if !ptr.is_null() {
                adjust(size as i64 - layout.size() as i64);
            }
            ptr
        }
    }
}

#[cfg(feature = "purging-allocator")]
pub use tracked::TrackedMiMalloc;

/// Configure the allocator before the daemon starts worker threads.
///
/// The pinned mimalloc v3 header defines `mi_option_purge_delay` as option 15.
/// The sys crate does not export experimental option constants. Keep this
/// index beside the exact dependency pins and the round-trip test below.
#[cfg(feature = "purging-allocator")]
pub fn configure() -> AllocatorTuning {
    if !tracked::active() {
        return AllocatorTuning {
            name: "system",
            purge_delay_ms: None,
            purge_delay_from_env: false,
        };
    }
    let purge_delay_from_env = std::env::var_os(PURGE_DELAY_ENV).is_some();
    if !purge_delay_from_env {
        // SAFETY: mimalloc documents option writes as process-global setup.
        // This runs once, at the start of `main`, before worker threads start.
        unsafe { libmimalloc_sys::mi_option_set(MI_OPTION_PURGE_DELAY, 0) };
    }
    // SAFETY: the option index matches the pinned mimalloc v3 header.
    let purge_delay_ms = unsafe { libmimalloc_sys::mi_option_get(MI_OPTION_PURGE_DELAY) };
    AllocatorTuning {
        name: "mimalloc",
        purge_delay_ms: Some(purge_delay_ms),
        purge_delay_from_env,
    }
}

#[cfg(not(feature = "purging-allocator"))]
pub fn configure() -> AllocatorTuning {
    AllocatorTuning {
        name: "system",
        purge_delay_ms: None,
        purge_delay_from_env: false,
    }
}

#[cfg(feature = "purging-allocator")]
fn nonnegative_current(value: &serde_json::Value, field: &str) -> Option<u64> {
    value.get(field)?.get("current")?.as_i64()?.try_into().ok()
}

#[cfg(feature = "purging-allocator")]
fn parse_mimalloc_stats(raw: &str, live_bytes: u64) -> Option<AllocatorMetrics> {
    let value: serde_json::Value = serde_json::from_str(raw).ok()?;
    let reserved_bytes = nonnegative_current(&value, "reserved");
    let committed_bytes = nonnegative_current(&value, "committed");
    // The upstream release build disables MI_STAT allocation counters. Track
    // requested Rust bytes ourselves; committed-minus-requested is an upper
    // bound on free retention that also includes allocator metadata/slack.
    let bytes_in_use = live_bytes;
    Some(AllocatorMetrics {
        name: "mimalloc",
        bytes_in_use: Some(bytes_in_use),
        bytes_held_free: committed_bytes.map(|bytes| bytes.saturating_sub(bytes_in_use)),
        committed_bytes,
        reserved_bytes,
    })
}

#[cfg(feature = "purging-allocator")]
#[must_use]
pub fn metrics() -> AllocatorMetrics {
    if !tracked::active() {
        return system_metrics();
    }
    mimalloc::MiMalloc::stats_json()
        .ok()
        .and_then(|stats| {
            stats
                .to_str()
                .ok()
                .and_then(|raw| parse_mimalloc_stats(raw, tracked::live_bytes()))
        })
        .unwrap_or(AllocatorMetrics {
            name: "mimalloc",
            ..AllocatorMetrics::default()
        })
}

fn system_metrics() -> AllocatorMetrics {
    let system = fold_db::memory_budget::malloc_zone_stats();
    AllocatorMetrics {
        name: "system",
        bytes_in_use: system.map(|stats| stats.bytes_in_use),
        bytes_held_free: system.map(|stats| stats.bytes_held_free),
        committed_bytes: None,
        reserved_bytes: None,
    }
}

#[cfg(not(feature = "purging-allocator"))]
#[must_use]
pub fn metrics() -> AllocatorMetrics {
    system_metrics()
}

#[must_use]
pub fn occupancy() -> Option<MallocZoneStats> {
    let metrics = metrics();
    Some(MallocZoneStats {
        bytes_in_use: metrics.bytes_in_use?,
        bytes_held_free: metrics.bytes_held_free?,
    })
}

/// Bumped by every `purge()`. A thread compares it to the last epoch it
/// collected for and runs `mi_collect` on its own heap when it is behind.
/// Tokio workers do that from `on_thread_park`. A UDS worker does it at
/// request end when the last tick was over the slack line. `mi_collect`
/// only reaches the caller's heap.
#[cfg(feature = "purging-allocator")]
static COLLECT_EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Last footprint tick sat at or over
/// [`fold_db::memory_budget::ALLOCATOR_SLACK_PURGE_BYTES`] of
/// measured footprint minus `footprint_net`. The park hook does not read this. Request-end
/// collect does, so a quiet node does not collect on every request.
static LAST_TICK_OVER_SLACK: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Record the slack-line result of one governor tick.
pub fn note_tick_over_slack_line(over: bool) {
    LAST_TICK_OVER_SLACK.store(over, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(feature = "purging-allocator")]
thread_local! {
    static COLLECTED_EPOCH: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Tokio `on_thread_park` hook: collect this worker's heap once per purge request.
#[cfg(feature = "purging-allocator")]
pub fn collect_thread_heap_if_requested() {
    use std::sync::atomic::Ordering;
    if !tracked::active() {
        return;
    }
    let wanted = COLLECT_EPOCH.load(Ordering::Relaxed);
    let _ = COLLECTED_EPOCH.try_with(|seen| {
        if seen.get() != wanted {
            seen.set(wanted);
            // SAFETY: collects only the calling thread's own heap and shared arenas.
            unsafe { libmimalloc_sys::mi_collect(true) };
        }
    });
}

#[cfg(not(feature = "purging-allocator"))]
pub fn collect_thread_heap_if_requested() {}

/// Request-end hook for the worker that just finished the request.
///
/// UDS workers poll the handler on their own thread and never run the tokio
/// park hook, so a purge on the sampler leaves their freed pages in place
/// (papercut-lastdb-footprint-governor-eviction-frees-kilobytes-mi-collect-one-thread-20260929).
/// Collect only this thread, and only when the last tick was over the slack
/// line. [`collect_thread_heap_if_requested`] still ignores a thread that has
/// already caught `COLLECT_EPOCH`.
pub fn collect_request_heap_if_over_slack() {
    if !LAST_TICK_OVER_SLACK.load(std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    collect_thread_heap_if_requested();
}

/// Ask the configured allocator to return unused pages to the kernel.
#[cfg(feature = "purging-allocator")]
#[must_use]
pub fn purge() -> u64 {
    if !tracked::active() {
        return fold_db::memory_budget::malloc_zone_pressure_relief(0);
    }
    COLLECT_EPOCH.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let before = metrics().committed_bytes;
    // SAFETY: collect visits the caller's thread heap plus shared arenas.
    // Other worker heaps collect on their next park, or at request end when
    // the last tick was over the slack line (see COLLECT_EPOCH). Their
    // release shows up in a later committed reading, not this one.
    unsafe { libmimalloc_sys::mi_collect(true) };
    before
        .zip(metrics().committed_bytes)
        .map_or(0, |(before, after)| before.saturating_sub(after))
}

#[cfg(not(feature = "purging-allocator"))]
#[must_use]
pub fn purge() -> u64 {
    fold_db::memory_budget::malloc_zone_pressure_relief(0)
}
