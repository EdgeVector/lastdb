use super::*;

/// Logical records plus background work. Historical wire fields remain for
/// installed clients; the native sampler does not maintain retired counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResidentHealth {
    /// Historical graph-cache wire field. Native values use core_model counters.
    pub resident_bytes: u64,
    /// Historical graph-cache wire field; always zero.
    pub resident_entries: u64,
    /// Historical graph revision wire field; always zero.
    #[serde(
        default = "default_keys_instant_gauge",
        serialize_with = "serialize_gauge_wire",
        deserialize_with = "deserialize_keys_instant"
    )]
    pub revision_slots: Gauge,
    /// Historical graph budget wire field; zero does not describe native limits.
    pub budget_bytes: u64,
    /// Retired cache-kind counters, retained as zero for installed clients.
    pub schema_hits: u64,
    pub schema_rehydrates: u64,
    pub molecule_hits: u64,
    pub molecule_rehydrates: u64,
    pub atom_hits: u64,
    pub atom_rehydrates: u64,
    pub file_blob_hits: u64,
    pub file_blob_rehydrates: u64,
    pub protein_hits: u64,
    pub protein_rehydrates: u64,
    /// Retired background totals. Live queue measurements use persist_lane_*.
    pub persist_enqueued: u64,
    pub persist_flushed: u64,
    pub deferred_persist_failed: u64,
    /// Retired completion gauge; unavailable from the native sampler.
    #[serde(default = "default_deferred_persist_completed_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_deferred_persist_completed")]
    pub deferred_persist_completed: Gauge,
    /// Retired duration gauge; unavailable from the native sampler.
    #[serde(default = "default_deferred_persist_us_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_deferred_persist_us")]
    pub deferred_persist_us: Gauge,
    /// Retired graph counters. Current purge counts use resident_purged_keys.
    pub evicted: u64,
    pub evict_refused_dirty: u64,
    #[serde(default)]
    pub key_set_hits: u64,
    /// Retired key-set counter, retained as zero for installed clients.
    #[serde(default)]
    pub key_set_misses: u64,
    #[serde(default)]
    pub key_set_overlays: u64,
    #[serde(default)]
    pub key_set_unknowns: u64,
    #[serde(default)]
    pub key_set_demotes: u64,
    /// Retired key-set counter, retained as zero for installed clients.
    #[serde(default)]
    pub key_set_marked_complete: u64,
    /// Current FIFO depth across schema persist lanes.
    #[serde(default)]
    pub persist_lane_depth: u64,
    /// Age of the oldest queued persist envelope, milliseconds.
    #[serde(default)]
    pub persist_lane_oldest_age_ms: u64,
    /// Persist-lane write failures.
    #[serde(default)]
    pub persist_lane_failures: u64,
    /// Reserved resident-minus-durable revision lag gauge. It is zero.
    #[serde(default)]
    pub resident_minus_durable_revision: u64,
    /// Used logical records currently in the logical resident set.
    #[serde(default = "default_keys_instant_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_keys_instant")]
    pub resident_key_count: Gauge,
    /// Used records a call still holds.
    #[serde(default = "default_keys_instant_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_keys_instant")]
    pub resident_held_keys: Gauge,
    /// Used records with an uncovered durability token.
    #[serde(default = "default_keys_instant_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_keys_instant")]
    pub resident_dirty_keys: Gauge,
    /// Used-record budget (`RESIDENT_KEY_CAP`, 10000).
    #[serde(default = "default_keys_instant_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_keys_instant")]
    pub resident_key_budget: Gauge,
    /// LRU purge removals of used records since process start.
    #[serde(default = "default_keys_lifetime_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_keys_lifetime")]
    pub resident_purged_keys: Gauge,
    /// Retired loader gauge; unavailable from the native sampler.
    #[serde(default = "default_bytes_instant_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_bytes_instant")]
    pub loader_pin_bytes: Gauge,
    /// Retired loader gauge; unavailable from the native sampler.
    #[serde(default = "default_groups_instant_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_groups_instant")]
    pub loader_groups_open_now: Gauge,
    /// Retired point-read gauge; current lookups use warm_lookup_hits.
    #[serde(default = "default_events_lifetime_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_events_lifetime")]
    pub resident_point_hits: Gauge,
    /// Retired point-read gauge; current lookups use warm_lookup_misses.
    #[serde(default = "default_events_lifetime_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_events_lifetime")]
    pub resident_point_misses: Gauge,
    /// Retired graph gauge; unavailable from the native sampler.
    #[serde(default = "default_events_lifetime_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_events_lifetime")]
    pub resident_purge_runs: Gauge,
    /// Retired graph gauge; unavailable from the native sampler.
    #[serde(default = "default_events_lifetime_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_events_lifetime")]
    pub resident_over_cap_stalls: Gauge,
    /// Retired graph gauge; unavailable from the native sampler.
    #[serde(default = "default_keys_instant_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_keys_instant")]
    pub resident_over_cap_keys: Gauge,
    /// Retired loader gauge; unavailable from the native sampler.
    #[serde(default = "default_events_lifetime_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_events_lifetime")]
    pub loader_loads: Gauge,
    /// Retired loader gauge; unavailable from the native sampler.
    #[serde(default = "default_micros_lifetime_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_micros_lifetime")]
    pub loader_load_us: Gauge,
    /// Logical records with a known tip and no full value.
    #[serde(default = "default_keys_instant_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_keys_instant")]
    pub resident_tip_only_keys: Gauge,
    /// Core lookups, including tip-only lookups; these are not query counts.
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
            revision_slots: default_keys_instant_gauge(),
            budget_bytes: 0,
            schema_hits: 0,
            schema_rehydrates: 0,
            molecule_hits: 0,
            molecule_rehydrates: 0,
            atom_hits: 0,
            atom_rehydrates: 0,
            file_blob_hits: 0,
            file_blob_rehydrates: 0,
            protein_hits: 0,
            protein_rehydrates: 0,
            persist_enqueued: 0,
            persist_flushed: 0,
            deferred_persist_failed: 0,
            deferred_persist_completed: default_deferred_persist_completed_gauge(),
            deferred_persist_us: default_deferred_persist_us_gauge(),
            evicted: 0,
            evict_refused_dirty: 0,
            key_set_hits: 0,
            key_set_misses: 0,
            key_set_overlays: 0,
            key_set_unknowns: 0,
            key_set_demotes: 0,
            key_set_marked_complete: 0,
            persist_lane_depth: 0,
            persist_lane_oldest_age_ms: 0,
            persist_lane_failures: 0,
            resident_minus_durable_revision: 0,
            resident_key_count: default_keys_instant_gauge(),
            resident_held_keys: default_keys_instant_gauge(),
            resident_dirty_keys: default_keys_instant_gauge(),
            resident_key_budget: default_keys_instant_gauge(),
            resident_purged_keys: default_keys_lifetime_gauge(),
            loader_pin_bytes: default_bytes_instant_gauge(),
            loader_groups_open_now: default_groups_instant_gauge(),
            resident_point_hits: default_events_lifetime_gauge(),
            resident_point_misses: default_events_lifetime_gauge(),
            resident_purge_runs: default_events_lifetime_gauge(),
            resident_over_cap_stalls: default_events_lifetime_gauge(),
            resident_over_cap_keys: default_keys_instant_gauge(),
            loader_loads: default_events_lifetime_gauge(),
            loader_load_us: default_micros_lifetime_gauge(),
            resident_tip_only_keys: default_keys_instant_gauge(),
            warm_lookup_hits: default_events_lifetime_gauge(),
            warm_lookup_misses: default_events_lifetime_gauge(),
        }
    }
}

impl ResidentHealth {
    pub(super) fn from_native(
        core: &fold_db::storage::disk_pack::NativeDiskStatus,
        work: fold_db::resident::PersistLaneMetricsSnapshot,
    ) -> Self {
        Self {
            resident_key_count: g_instant(core.core_warm_paths as u64, Unit::Keys),
            resident_held_keys: g_instant(core.core_warm_held as u64, Unit::Keys),
            resident_dirty_keys: g_instant(core.core_warm_dirty as u64, Unit::Keys),
            resident_key_budget: g_instant(core.warm_record_limit as u64, Unit::Keys),
            resident_purged_keys: g_lifetime(core.core_warm_purged, Unit::Keys),
            resident_tip_only_keys: g_instant(core.core_warm_tip_only_paths as u64, Unit::Keys),
            warm_lookup_hits: g_lifetime(core.core_warm_hits, Unit::Events),
            warm_lookup_misses: g_lifetime(core.core_warm_misses, Unit::Events),
            persist_lane_depth: work.depth,
            persist_lane_oldest_age_ms: work.oldest_age_ms,
            persist_lane_failures: work.failures,
            ..Self::default()
        }
    }
}
