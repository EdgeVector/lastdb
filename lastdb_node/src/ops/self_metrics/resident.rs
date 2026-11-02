use super::*;

/// Warm set records plus background work. Historical wire fields that installed
/// clients require remain; the native sampler does not maintain retired counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResidentHealth {
    /// Historical graph-cache wire field. Native values use core_model counters.
    pub resident_bytes: u64,
    /// Historical graph-cache wire field; always zero.
    pub resident_entries: u64,
    /// Historical graph budget wire field; zero does not describe native limits.
    pub budget_bytes: u64,
    /// Retired cache-kind counters, retained as zero for installed clients.
    pub schema_hits: u64,
    pub schema_rehydrates: u64,
    pub field_key_hits: u64,
    pub field_key_rehydrates: u64,
    pub record_hits: u64,
    pub record_rehydrates: u64,
    pub file_blob_hits: u64,
    pub file_blob_rehydrates: u64,
    /// Retired background totals. Live queue measurements use persist_lane_*.
    pub persist_enqueued: u64,
    pub persist_flushed: u64,
    pub deferred_persist_failed: u64,
    /// Retired graph counters. Current purge counts use resident_purged_keys.
    pub evicted: u64,
    pub evict_refused_dirty: u64,
    /// Current FIFO depth across schema persist lanes.
    #[serde(default)]
    pub persist_lane_depth: u64,
    /// Age of the oldest queued persist envelope, milliseconds.
    #[serde(default)]
    pub persist_lane_oldest_age_ms: u64,
    /// Persist-lane write failures.
    #[serde(default)]
    pub persist_lane_failures: u64,
    /// Used records currently in the warm set.
    #[serde(default = "default_keys_instant_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_keys_instant")]
    pub resident_key_count: Gauge,
    /// Used records a call still holds.
    #[serde(default = "default_keys_instant_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_keys_instant")]
    pub resident_held_keys: Gauge,
    /// Retired path-count budget; not served. The warm set limit is
    /// `memory_budget.core_warm.limit_bytes`.
    #[serde(default = "default_keys_instant_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_keys_instant")]
    pub resident_key_budget: Gauge,
    /// LRU purge removals of used records since process start.
    #[serde(default = "default_keys_lifetime_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_keys_lifetime")]
    pub resident_purged_keys: Gauge,
    /// Warm set lookups; these are not query counts.
    #[serde(default = "default_events_lifetime_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_events_lifetime")]
    pub warm_lookup_hits: Gauge,
    #[serde(default = "default_events_lifetime_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_events_lifetime")]
    pub warm_lookup_misses: Gauge,
}

impl Default for ResidentHealth {
    fn default() -> Self {
        Self {
            resident_bytes: 0,
            resident_entries: 0,
            budget_bytes: 0,
            schema_hits: 0,
            schema_rehydrates: 0,
            field_key_hits: 0,
            field_key_rehydrates: 0,
            record_hits: 0,
            record_rehydrates: 0,
            file_blob_hits: 0,
            file_blob_rehydrates: 0,
            persist_enqueued: 0,
            persist_flushed: 0,
            deferred_persist_failed: 0,
            evicted: 0,
            evict_refused_dirty: 0,
            persist_lane_depth: 0,
            persist_lane_oldest_age_ms: 0,
            persist_lane_failures: 0,
            resident_key_count: default_keys_instant_gauge(),
            resident_held_keys: default_keys_instant_gauge(),
            resident_key_budget: default_keys_instant_gauge(),
            resident_purged_keys: default_keys_lifetime_gauge(),
            warm_lookup_hits: default_events_lifetime_gauge(),
            warm_lookup_misses: default_events_lifetime_gauge(),
        }
    }
}

impl ResidentHealth {
    pub(crate) fn from_native(
        core: &fold_db::storage::disk_pack::NativeDiskStatus,
        work: fold_db::resident::PersistLaneMetricsSnapshot,
    ) -> Self {
        Self {
            resident_key_count: g_instant(core.warm_records as u64, Unit::Keys),
            resident_held_keys: g_instant(core.core_warm_held as u64, Unit::Keys),
            resident_key_budget: Gauge::field_not_served(Unit::Keys, GAUGE_INSTANT),
            resident_purged_keys: g_lifetime(core.core_warm_purged, Unit::Keys),
            warm_lookup_hits: g_lifetime(core.core_warm_hits, Unit::Events),
            warm_lookup_misses: g_lifetime(core.core_warm_misses, Unit::Events),
            persist_lane_depth: work.depth,
            persist_lane_oldest_age_ms: work.oldest_age_ms,
            persist_lane_failures: work.failures,
            ..Self::default()
        }
    }
}
