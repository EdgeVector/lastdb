use super::*;

gauge_health! {
    /// QoS gate occupancy + cumulative shed counters (from [`lastdb_host::QosGate`]).
    ///
    /// Wire-transparent gauges (bare u64 / null). Permits and in-use are Instant;
    /// shed counters are process-lifetime totals.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct QosHealth {
        pub total_permits: (Unit::Named("permit(s)"), GAUGE_INSTANT),
        pub bulk_permits: (Unit::Named("permit(s)"), GAUGE_INSTANT),
        pub total_in_use: (Unit::Named("permit(s)"), GAUGE_INSTANT),
        pub bulk_in_use: (Unit::Named("permit(s)"), GAUGE_INSTANT),
        pub interactive_sheds: (Unit::Events, GAUGE_PROCESS_LIFETIME),
        pub bulk_sheds: (Unit::Events, GAUGE_PROCESS_LIFETIME),
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

gauge_health! {
    /// UDS worker pool occupancy (primary concurrency control).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct UdsPoolHealth {
        pub workers: (Unit::Named("worker(s)"), GAUGE_INSTANT),
        pub queue_capacity: (Unit::Named("slot(s)"), GAUGE_INSTANT),
        pub in_flight: (Unit::Named("request(s)"), GAUGE_INSTANT),
        pub submitted: (Unit::Events, GAUGE_PROCESS_LIFETIME),
        pub queue_full_rejects: (Unit::Events, GAUGE_PROCESS_LIFETIME),
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

gauge_health! {
    /// Blocking `/api/local-watch` occupancy (see [`crate::watch_gate`]).
    ///
    /// A parked long-poll consumes a UDS worker and no QoS permit, so neither
    /// [`QosHealth`] nor [`UdsPoolHealth`] alone answers "how much of the pool is
    /// asleep on a watch?". This does.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct WatchersHealth {
        /// Concurrent blocking watchers allowed (sized from the worker pool).
        pub max: (Unit::Named("watcher(s)"), GAUGE_INSTANT),
        /// Blocking watchers holding a worker right now.
        pub active: (Unit::Named("watcher(s)"), GAUGE_INSTANT),
        /// High-water mark since boot — says whether the cap was ever approached.
        pub peak: (Unit::Named("watcher(s)"), GAUGE_PROCESS_LIFETIME),
        /// Watchers refused because the cap was full.
        pub sheds: (Unit::Events, GAUGE_PROCESS_LIFETIME),
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
