//! Settings for how a flush syncs its groups.

/// Wide transaction scopes can sync independent group files in parallel.
/// Keep the worker count small so a flush cannot exhaust append descriptors.
const DEFAULT_PARALLEL_SCOPE_MIN_GROUPS: usize = 64;
const DEFAULT_PARALLEL_SCOPE_WORKERS: usize = 8;
const MAX_PARALLEL_SCOPE_WORKERS: usize = 64;
/// Env: the narrowest scope (in groups) that syncs with parallel workers.
const PARALLEL_FLUSH_MIN_GROUPS_ENV: &str = "LASTSTORE_PARALLEL_FLUSH_MIN_GROUPS";
/// Env: workers for a parallel sync. Clamped to 1..=64.
const PARALLEL_FLUSH_WORKERS_ENV: &str = "LASTSTORE_PARALLEL_FLUSH_WORKERS";
/// Env: when truthy, the whole-store flush (the periodic tick, shutdown)
/// also syncs its groups with parallel workers. Default off.
const PARALLEL_WHOLE_FLUSH_ENV: &str = "LASTSTORE_PARALLEL_WHOLE_FLUSH";

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.trim().parse::<usize>().ok()
}

pub(super) fn parallel_scope_min_groups() -> usize {
    static VALUE: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *VALUE.get_or_init(|| {
        env_usize(PARALLEL_FLUSH_MIN_GROUPS_ENV)
            .filter(|n| *n >= 1)
            .unwrap_or(DEFAULT_PARALLEL_SCOPE_MIN_GROUPS)
    })
}

pub(super) fn parallel_scope_workers() -> usize {
    static VALUE: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *VALUE.get_or_init(|| {
        env_usize(PARALLEL_FLUSH_WORKERS_ENV)
            .map(|n| n.clamp(1, MAX_PARALLEL_SCOPE_WORKERS))
            .unwrap_or(DEFAULT_PARALLEL_SCOPE_WORKERS)
    })
}

pub(super) fn parallel_whole_flush() -> bool {
    static VALUE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *VALUE.get_or_init(|| {
        std::env::var(PARALLEL_WHOLE_FLUSH_ENV)
            .map(|raw| {
                matches!(
                    raw.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
    })
}
