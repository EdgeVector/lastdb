//! The existing LastStore file barriers, exposed without a flush or file read.

/// Cumulative completed barrier attempts for every LastStore home in this process.
/// Counts include failed attempts. Durations sum each attempt's elapsed time and
/// can overlap across threads; they are not request latency or total wall time.
/// Each atomic counter is sampled independently, so a concurrent barrier can
/// finish between its count and duration samples.
#[derive(serde::Serialize)]
pub struct NativeFileBarrierStats {
    pub scope: &'static str,
    /// All dirty-file barriers, including native journal, map, and root files.
    /// These use F_FULLFSYNC on macOS and sync_data on other platforms.
    pub data_file_calls: u64,
    pub data_file_micros: u64,
    /// Key sidecar barriers used by the separate hash-group storage backend.
    pub sidecar_file_calls: u64,
    pub sidecar_file_micros: u64,
    /// Directory sync_all attempts for durable file names and renames.
    pub directory_calls: u64,
    pub directory_micros: u64,
}

impl NativeFileBarrierStats {
    pub(super) fn capture() -> Self {
        let stats = laststore::sync_stats();
        Self {
            scope: "process_lifetime_all_laststore_homes",
            data_file_calls: stats.data_syncs,
            data_file_micros: stats.data_sync_micros,
            sidecar_file_calls: stats.sidecar_syncs,
            sidecar_file_micros: stats.sidecar_sync_micros,
            directory_calls: stats.dir_syncs,
            directory_micros: stats.dir_sync_micros,
        }
    }
}
