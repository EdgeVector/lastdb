use super::*;

/// Process-lifetime window used when the producer does not attach a boot ts to
/// the gauge (unit tests, Default, wire restore). Display only uses the
/// qualifier ("this process"); the unix field is metadata for later tooling.
pub(super) const GAUGE_PROCESS_LIFETIME: Window = Window::ProcessLifetime { since_boot_unix: 0 };
/// Point-in-time occupancy / config gauges (queue depth "now", permit caps, …).
pub(super) const GAUGE_INSTANT: Window = Window::Instant;

pub(super) fn gauge_measured(n: u64, unit: Unit, window: Window) -> Gauge {
    Gauge::measured(n, unit, window)
}

pub(super) fn gauge_from_wire(opt: Option<u64>, unit: Unit, window: Window) -> Gauge {
    match opt {
        Some(n) => Gauge::measured(n, unit, window),
        None => Gauge::field_not_served(unit, window),
    }
}

pub(super) fn wire_u64(g: &Gauge) -> Option<u64> {
    g.as_wire_u64()
}

/// Extract a measured number for consumers that still speak bare `u64`
/// (JSONL fields, pressure samples, format strings that predate Display).
/// Unavailable deserializations become `0` only at that non-Gauge boundary.
pub(super) fn gauge_or_zero(g: &Gauge) -> u64 {
    g.as_wire_u64().unwrap_or(0)
}

pub(super) fn gauge_or_zero_usize(g: &Gauge) -> usize {
    usize::try_from(gauge_or_zero(g)).unwrap_or(usize::MAX)
}

/// Instant occupancy/config gauge (permits, queue depth, caps).
pub(super) fn g_instant(n: u64, unit: Unit) -> Gauge {
    gauge_measured(n, unit, GAUGE_INSTANT)
}

/// Process-lifetime cumulative counter.
pub(super) fn g_lifetime(n: u64, unit: Unit) -> Gauge {
    gauge_measured(n, unit, GAUGE_PROCESS_LIFETIME)
}

pub(super) fn g_instant_wire(opt: Option<u64>, unit: Unit) -> Gauge {
    gauge_from_wire(opt, unit, GAUGE_INSTANT)
}

pub(super) fn g_lifetime_wire(opt: Option<u64>, unit: Unit) -> Gauge {
    gauge_from_wire(opt, unit, GAUGE_PROCESS_LIFETIME)
}

pub(super) fn default_deferred_persist_completed_gauge() -> Gauge {
    Gauge::field_not_served(Unit::Events, GAUGE_PROCESS_LIFETIME)
}

pub(super) fn default_deferred_persist_us_gauge() -> Gauge {
    Gauge::field_not_served(Unit::Micros, GAUGE_PROCESS_LIFETIME)
}

pub(super) fn serialize_gauge_wire<S: serde::Serializer>(
    g: &Gauge,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match g.as_wire_u64() {
        Some(n) => serializer.serialize_u64(n),
        None => serializer.serialize_none(),
    }
}

pub(super) fn deserialize_deferred_persist_completed<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Gauge, D::Error> {
    let opt = Option::<u64>::deserialize(deserializer)?;
    Ok(gauge_from_wire(opt, Unit::Events, GAUGE_PROCESS_LIFETIME))
}

pub(super) fn deserialize_deferred_persist_us<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Gauge, D::Error> {
    let opt = Option::<u64>::deserialize(deserializer)?;
    Ok(gauge_from_wire(opt, Unit::Micros, GAUGE_PROCESS_LIFETIME))
}

pub(super) fn default_events_lifetime_gauge() -> Gauge {
    Gauge::field_not_served(Unit::Events, GAUGE_PROCESS_LIFETIME)
}

pub(super) fn deserialize_events_lifetime<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Gauge, D::Error> {
    crate::ops::gauge::serde_u64::deserialize_as(deserializer, Unit::Events, GAUGE_PROCESS_LIFETIME)
}

pub(super) fn default_micros_lifetime_gauge() -> Gauge {
    Gauge::field_not_served(Unit::Micros, GAUGE_PROCESS_LIFETIME)
}

pub(super) fn deserialize_micros_lifetime<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Gauge, D::Error> {
    crate::ops::gauge::serde_u64::deserialize_as(deserializer, Unit::Micros, GAUGE_PROCESS_LIFETIME)
}

pub(super) fn default_keys_instant_gauge() -> Gauge {
    Gauge::field_not_served(Unit::Keys, GAUGE_INSTANT)
}

pub(super) fn default_keys_lifetime_gauge() -> Gauge {
    Gauge::field_not_served(Unit::Keys, GAUGE_PROCESS_LIFETIME)
}

pub(super) fn default_bytes_instant_gauge() -> Gauge {
    Gauge::field_not_served(Unit::Bytes, GAUGE_INSTANT)
}

pub(super) fn default_groups_instant_gauge() -> Gauge {
    Gauge::field_not_served(Unit::Named("group(s)"), GAUGE_INSTANT)
}

pub(super) fn deserialize_keys_instant<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Gauge, D::Error> {
    crate::ops::gauge::serde_u64::deserialize_as(deserializer, Unit::Keys, GAUGE_INSTANT)
}

pub(super) fn deserialize_keys_lifetime<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Gauge, D::Error> {
    crate::ops::gauge::serde_u64::deserialize_as(deserializer, Unit::Keys, GAUGE_PROCESS_LIFETIME)
}

pub(super) fn deserialize_bytes_instant<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Gauge, D::Error> {
    crate::ops::gauge::serde_u64::deserialize_as(deserializer, Unit::Bytes, GAUGE_INSTANT)
}

pub(super) fn deserialize_groups_instant<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Gauge, D::Error> {
    crate::ops::gauge::serde_u64::deserialize_as(
        deserializer,
        Unit::Named("group(s)"),
        GAUGE_INSTANT,
    )
}
