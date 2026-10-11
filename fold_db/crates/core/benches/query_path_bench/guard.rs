use std::time::Duration;

use criterion::measurement::WallTime;
use criterion::BenchmarkGroup;

pub(crate) const SCHEMA: &str = "Contacts";

/// Cardinalities swept by the point-lookup / full-read groups. Extended to
/// 100k so the O(1)-vs-O(field) divergence is unmistakable at scale: a flat
/// `indexed_point_lookup` here is the headline result of the per-key chain.
pub(crate) const CARDINALITIES: [usize; 4] = [1_000, 10_000, 50_000, 100_000];

pub(crate) fn requested_benchmark_filter_matches(group_name: &str) -> bool {
    let filters: Vec<String> = std::env::args()
        .skip(1)
        .take_while(|arg| !arg.starts_with("--"))
        .collect();
    filters.is_empty()
        || filters
            .iter()
            .any(|filter| group_name.contains(filter) || filter.contains(group_name))
}

pub(crate) fn fast_guard_enabled() -> bool {
    env_flag::var_truthy("FOLD_BENCH_FAST_GUARD")
        || env_flag::var_truthy("FOLD_DISABLE_NATIVE_INDEX")
}

pub(crate) fn configure_guard_group(group: &mut BenchmarkGroup<'_, WallTime>, sample_size: usize) {
    group.sample_size(sample_size);
    if fast_guard_enabled() {
        group.sample_size(10);
        group.warm_up_time(Duration::from_millis(250));
        group.measurement_time(Duration::from_millis(750));
    }
}
