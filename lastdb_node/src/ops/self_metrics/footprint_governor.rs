//! The operator view of the measured-footprint governor state.
//!
//! The governor in [`crate::jobs::footprint`] keeps the latest copy of this
//! snapshot; the memory budget status reads it. The type sits here, below the
//! host, so the status model does not reach up into the governor job.

/// Cheap operator view of the current measured-footprint governor state.
#[derive(Debug, Clone, Copy)]
pub struct FootprintGovernorSnapshot {
    pub footprint_sticky: bool,
    pub footprint_delta_per_evicted_byte: Option<f64>,
    pub footprint_net_bytes: u64,
    pub governor_state: &'static str,
    /// Unix seconds when `governor_state` last CHANGED value; 0 before the
    /// first tick stamps it.
    ///
    /// The state string alone cannot separate a one-tick blip from a latch
    /// that has held for hours, and `classify_governor_state` returns
    /// `purge-failed` at the HIGHEST precedence -- so the one label most
    /// likely to be read as news is also the one that masks every other
    /// state while it holds. Measured on the primary 2026-10-07:
    /// `governor_state=purge-failed` across every sample of a 13h episode,
    /// with the only duration evidence two `became_failed` WARN lines in a
    /// rotating log. Stamp the transition so the age is in the reading.
    pub governor_state_since_epoch_secs: u64,
    /// Latched host pressure: `high` or `clear`. Not the raw sample.
    pub host_pressure: &'static str,
    pub swap_used_bytes: u64,
    pub compressor_bytes: u64,
    /// Latest scored purge failed the 512 MiB slack predicate.
    pub purge_failed: bool,
    /// Drop in allocator `committed` measured ACROSS the last `purge()`
    /// call. Not the predicate, and not every release: `purge()` collects
    /// the calling thread's heap plus shared arenas, so a worker heap's
    /// release lands in a later `committed` reading and is never counted
    /// here. A 0 therefore means "this call reclaimed nothing", never "the
    /// allocator reclaimed nothing". Measured on the primary 2026-10-07:
    /// `committed` held 2732589056 B byte-identical across five forced
    /// purges, so the 0 was honest there -- but only a reading of
    /// `allocator_committed_bytes` over time can establish that.
    pub malloc_bytes_released_last_purge: u64,
    /// Lifetime warm bytes released by eviction steps. The footprint proof
    /// scores each increase against the `phys_footprint` drop.
    pub warm_bytes_freed: u64,
}

impl Default for FootprintGovernorSnapshot {
    fn default() -> Self {
        Self {
            footprint_sticky: false,
            footprint_delta_per_evicted_byte: None,
            footprint_net_bytes: 0,
            governor_state: "under",
            governor_state_since_epoch_secs: 0,
            host_pressure: "clear",
            swap_used_bytes: 0,
            compressor_bytes: 0,
            purge_failed: false,
            malloc_bytes_released_last_purge: 0,
            warm_bytes_freed: 0,
        }
    }
}
