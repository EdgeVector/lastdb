use super::*;

/// Warm set records, read from the native core.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResidentHealth {
    /// Used records currently in the warm set.
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_keys_instant")]
    pub resident_key_count: Gauge,
    /// Used records a call still holds.
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_keys_instant")]
    pub resident_held_keys: Gauge,
    /// LRU purge removals of used records since process start.
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_keys_lifetime")]
    pub resident_purged_keys: Gauge,
    /// Warm set lookups; these are not query counts.
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_events_lifetime")]
    pub warm_lookup_hits: Gauge,
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_events_lifetime")]
    pub warm_lookup_misses: Gauge,
}

impl ResidentHealth {
    pub(crate) fn from_native(core: &fold_db::storage::disk_pack::NativeDiskStatus) -> Self {
        Self {
            resident_key_count: g_instant(core.warm_records as u64, Unit::Keys),
            resident_held_keys: g_instant(core.core_warm_held as u64, Unit::Keys),
            resident_purged_keys: g_lifetime(core.core_warm_purged, Unit::Keys),
            warm_lookup_hits: g_lifetime(core.core_warm_hits, Unit::Events),
            warm_lookup_misses: g_lifetime(core.core_warm_misses, Unit::Events),
        }
    }
}
