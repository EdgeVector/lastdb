use super::*;

/// QoS gate occupancy + cumulative shed counters (from [`lastdb_host::QosGate`]).
///
/// Wire-transparent gauges (bare u64 / null). Permits and in-use are Instant;
/// shed counters are process-lifetime totals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QosHealth {
    pub total_permits: Gauge,
    pub bulk_permits: Gauge,
    pub total_in_use: Gauge,
    pub bulk_in_use: Gauge,
    pub interactive_sheds: Gauge,
    pub bulk_sheds: Gauge,
}

impl Default for QosHealth {
    fn default() -> Self {
        Self {
            total_permits: Gauge::field_not_served(Unit::Named("permit(s)"), GAUGE_INSTANT),
            bulk_permits: Gauge::field_not_served(Unit::Named("permit(s)"), GAUGE_INSTANT),
            total_in_use: Gauge::field_not_served(Unit::Named("permit(s)"), GAUGE_INSTANT),
            bulk_in_use: Gauge::field_not_served(Unit::Named("permit(s)"), GAUGE_INSTANT),
            interactive_sheds: Gauge::field_not_served(Unit::Events, GAUGE_PROCESS_LIFETIME),
            bulk_sheds: Gauge::field_not_served(Unit::Events, GAUGE_PROCESS_LIFETIME),
        }
    }
}

impl From<lastdb_host::QosSnapshot> for QosHealth {
    fn from(s: lastdb_host::QosSnapshot) -> Self {
        Self {
            total_permits: g_instant(s.total_permits as u64, Unit::Named("permit(s)")),
            bulk_permits: g_instant(s.bulk_permits as u64, Unit::Named("permit(s)")),
            total_in_use: g_instant(s.total_in_use as u64, Unit::Named("permit(s)")),
            bulk_in_use: g_instant(s.bulk_in_use as u64, Unit::Named("permit(s)")),
            interactive_sheds: g_lifetime(s.interactive_sheds, Unit::Events),
            bulk_sheds: g_lifetime(s.bulk_sheds, Unit::Events),
        }
    }
}

impl Serialize for QosHealth {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("QosHealth", 6)?;
        s.serialize_field("total_permits", &wire_u64(&self.total_permits))?;
        s.serialize_field("bulk_permits", &wire_u64(&self.bulk_permits))?;
        s.serialize_field("total_in_use", &wire_u64(&self.total_in_use))?;
        s.serialize_field("bulk_in_use", &wire_u64(&self.bulk_in_use))?;
        s.serialize_field("interactive_sheds", &wire_u64(&self.interactive_sheds))?;
        s.serialize_field("bulk_sheds", &wire_u64(&self.bulk_sheds))?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for QosHealth {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(default)]
            total_permits: Option<u64>,
            #[serde(default)]
            bulk_permits: Option<u64>,
            #[serde(default)]
            total_in_use: Option<u64>,
            #[serde(default)]
            bulk_in_use: Option<u64>,
            #[serde(default)]
            interactive_sheds: Option<u64>,
            #[serde(default)]
            bulk_sheds: Option<u64>,
        }
        let raw = Raw::deserialize(deserializer)?;
        Ok(Self {
            total_permits: g_instant_wire(raw.total_permits, Unit::Named("permit(s)")),
            bulk_permits: g_instant_wire(raw.bulk_permits, Unit::Named("permit(s)")),
            total_in_use: g_instant_wire(raw.total_in_use, Unit::Named("permit(s)")),
            bulk_in_use: g_instant_wire(raw.bulk_in_use, Unit::Named("permit(s)")),
            interactive_sheds: g_lifetime_wire(raw.interactive_sheds, Unit::Events),
            bulk_sheds: g_lifetime_wire(raw.bulk_sheds, Unit::Events),
        })
    }
}

/// UDS worker pool occupancy (primary concurrency control).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UdsPoolHealth {
    pub workers: Gauge,
    pub queue_capacity: Gauge,
    pub in_flight: Gauge,
    pub submitted: Gauge,
    pub queue_full_rejects: Gauge,
}

impl Default for UdsPoolHealth {
    fn default() -> Self {
        Self {
            workers: Gauge::field_not_served(Unit::Named("worker(s)"), GAUGE_INSTANT),
            queue_capacity: Gauge::field_not_served(Unit::Named("slot(s)"), GAUGE_INSTANT),
            in_flight: Gauge::field_not_served(Unit::Named("request(s)"), GAUGE_INSTANT),
            submitted: Gauge::field_not_served(Unit::Events, GAUGE_PROCESS_LIFETIME),
            queue_full_rejects: Gauge::field_not_served(Unit::Events, GAUGE_PROCESS_LIFETIME),
        }
    }
}

impl From<lastdb_uds::UdsPoolSnapshot> for UdsPoolHealth {
    fn from(s: lastdb_uds::UdsPoolSnapshot) -> Self {
        Self {
            workers: g_instant(s.workers as u64, Unit::Named("worker(s)")),
            queue_capacity: g_instant(s.queue_capacity as u64, Unit::Named("slot(s)")),
            in_flight: g_instant(s.in_flight as u64, Unit::Named("request(s)")),
            submitted: g_lifetime(s.submitted, Unit::Events),
            queue_full_rejects: g_lifetime(s.queue_full_rejects, Unit::Events),
        }
    }
}

impl Serialize for UdsPoolHealth {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("UdsPoolHealth", 5)?;
        s.serialize_field("workers", &wire_u64(&self.workers))?;
        s.serialize_field("queue_capacity", &wire_u64(&self.queue_capacity))?;
        s.serialize_field("in_flight", &wire_u64(&self.in_flight))?;
        s.serialize_field("submitted", &wire_u64(&self.submitted))?;
        s.serialize_field("queue_full_rejects", &wire_u64(&self.queue_full_rejects))?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for UdsPoolHealth {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(default)]
            workers: Option<u64>,
            #[serde(default)]
            queue_capacity: Option<u64>,
            #[serde(default)]
            in_flight: Option<u64>,
            #[serde(default)]
            submitted: Option<u64>,
            #[serde(default)]
            queue_full_rejects: Option<u64>,
        }
        let raw = Raw::deserialize(deserializer)?;
        Ok(Self {
            workers: g_instant_wire(raw.workers, Unit::Named("worker(s)")),
            queue_capacity: g_instant_wire(raw.queue_capacity, Unit::Named("slot(s)")),
            in_flight: g_instant_wire(raw.in_flight, Unit::Named("request(s)")),
            submitted: g_lifetime_wire(raw.submitted, Unit::Events),
            queue_full_rejects: g_lifetime_wire(raw.queue_full_rejects, Unit::Events),
        })
    }
}

/// Blocking `/api/local-watch` occupancy (see [`crate::watch_gate`]).
///
/// A parked long-poll consumes a UDS worker and no QoS permit, so neither
/// [`QosHealth`] nor [`UdsPoolHealth`] alone answers "how much of the pool is
/// asleep on a watch?". This does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchersHealth {
    /// Concurrent blocking watchers allowed (sized from the worker pool).
    pub max: Gauge,
    /// Blocking watchers holding a worker right now.
    pub active: Gauge,
    /// High-water mark since boot — says whether the cap was ever approached.
    pub peak: Gauge,
    /// Watchers refused because the cap was full.
    pub sheds: Gauge,
}

impl Default for WatchersHealth {
    fn default() -> Self {
        Self {
            max: Gauge::field_not_served(Unit::Named("watcher(s)"), GAUGE_INSTANT),
            active: Gauge::field_not_served(Unit::Named("watcher(s)"), GAUGE_INSTANT),
            peak: Gauge::field_not_served(Unit::Named("watcher(s)"), GAUGE_PROCESS_LIFETIME),
            sheds: Gauge::field_not_served(Unit::Events, GAUGE_PROCESS_LIFETIME),
        }
    }
}

impl From<crate::watch_gate::WatchGateSnapshot> for WatchersHealth {
    fn from(s: crate::watch_gate::WatchGateSnapshot) -> Self {
        Self {
            max: g_instant(s.max as u64, Unit::Named("watcher(s)")),
            active: g_instant(s.active as u64, Unit::Named("watcher(s)")),
            peak: g_lifetime(s.peak as u64, Unit::Named("watcher(s)")),
            sheds: g_lifetime(s.sheds, Unit::Events),
        }
    }
}

impl Serialize for WatchersHealth {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("WatchersHealth", 4)?;
        s.serialize_field("max", &wire_u64(&self.max))?;
        s.serialize_field("active", &wire_u64(&self.active))?;
        s.serialize_field("peak", &wire_u64(&self.peak))?;
        s.serialize_field("sheds", &wire_u64(&self.sheds))?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for WatchersHealth {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(default)]
            max: Option<u64>,
            #[serde(default)]
            active: Option<u64>,
            #[serde(default)]
            peak: Option<u64>,
            #[serde(default)]
            sheds: Option<u64>,
        }
        let raw = Raw::deserialize(deserializer)?;
        Ok(Self {
            max: g_instant_wire(raw.max, Unit::Named("watcher(s)")),
            active: g_instant_wire(raw.active, Unit::Named("watcher(s)")),
            peak: g_lifetime_wire(raw.peak, Unit::Named("watcher(s)")),
            sheds: g_lifetime_wire(raw.sheds, Unit::Events),
        })
    }
}
