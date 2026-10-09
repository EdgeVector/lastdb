//! Self-describing operator gauge: measurement + unit + window + availability.
//!
//! Health gauges used to be bare `u64` fields whose unit, window, and
//! absent-vs-zero semantics lived only in field names and doc comments. This
//! module puts those claims in the type so `Display` cannot invent a noun that
//! contradicts the unit, and a missing JSON field deserializes to
//! [`Availability::Unavailable`] instead of a measured zero.
//!
//! ## Wire compatibility (PR-1 contract)
//!
//! No existing `/api/status` field is retyped in this PR. Later conversion PRs
//! keep the bare-numeric wire shape via [`as_wire_u64`] / [`from_wire_u64`]
//! and the [`serde_u64`] module (`#[serde(with = "gauge::serde_u64")]` or
//! `from`/`into` on a thin wrapper). A missing or `null` field becomes
//! [`UnavailableReason::FieldNotServed`], never `Measured(0)`.
//!
//! Ground truth: brain `design-lastdb-status-gauge-contract` (PR-1).

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Why a gauge value is not a trustworthy measurement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnavailableReason {
    /// JSON field was absent or null — daemon predates the field, or the
    /// producer declined to publish it. Not the same as measured zero.
    FieldNotServed,
}

impl UnavailableReason {
    fn display_phrase(self) -> &'static str {
        match self {
            Self::FieldNotServed => "unavailable (daemon predates this field)",
        }
    }
}

/// Measured count or an explicit non-measurement.
///
/// Deserializing a missing status field must land here as
/// [`Unavailable`](Availability::Unavailable), never as `Measured(0)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Availability {
    Measured(u64),
    Unavailable(UnavailableReason),
}

impl Availability {
    pub const fn measured(n: u64) -> Self {
        Self::Measured(n)
    }

    pub const fn field_not_served() -> Self {
        Self::Unavailable(UnavailableReason::FieldNotServed)
    }

    pub const fn is_measured(self) -> bool {
        matches!(self, Self::Measured(_))
    }

    /// Wire number when measured; `None` when unavailable (so serde can omit
    /// or emit null instead of lying with `0`).
    pub const fn as_wire_u64(self) -> Option<u64> {
        match self {
            Self::Measured(n) => Some(n),
            Self::Unavailable(_) => None,
        }
    }
}

impl Default for Availability {
    fn default() -> Self {
        Self::field_not_served()
    }
}

/// What the integer counts.
///
/// The noun in [`Display`] is derived from this enum — `Unit::Edges` can never
/// print as `row(s)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Unit {
    Rows,
    Edges,
    Keys,
    Bytes,
    Chunks,
    Events,
    Micros,
    /// Escape hatch for operator nouns that are not yet in the closed set.
    Named(&'static str),
}

impl Unit {
    /// Singular/plural-friendly noun fragment used by [`Display`].
    pub const fn noun(self) -> &'static str {
        match self {
            Self::Rows => "row(s)",
            Self::Edges => "edge(s)",
            Self::Keys => "key(s)",
            Self::Bytes => "byte(s)",
            Self::Chunks => "chunk(s)",
            Self::Events => "event(s)",
            Self::Micros => "µs",
            Self::Named(s) => s,
        }
    }

    /// Stable machine token for the additive `/api/status` `contract` block.
    ///
    /// Closed-set units use snake_case names; [`Unit::Named`] uses the noun
    /// string as-is so operator-specific units remain identifiable.
    pub const fn contract_id(self) -> &'static str {
        match self {
            Self::Rows => "rows",
            Self::Edges => "edges",
            Self::Keys => "keys",
            Self::Bytes => "bytes",
            Self::Chunks => "chunks",
            Self::Events => "events",
            Self::Micros => "micros",
            Self::Named(s) => s,
        }
    }
}

/// Time scope of the measurement.
///
/// [`Display`] qualifies the value from this enum so a process-lifetime total
/// cannot read as an instant "now".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Window {
    /// Point-in-time sample (queue depth "now", current RSS, …).
    Instant,
    /// Rolling or fixed interval starting at `since_unix` (seconds since epoch).
    Interval { since_unix: u64 },
    /// Totals accumulated since this process booted (`since_boot_unix` is the
    /// process start time as unix seconds when known).
    ProcessLifetime { since_boot_unix: u64 },
    /// Lifetime/cumulative counter with no process-bound window claim.
    Cumulative,
}

impl Window {
    /// Short qualifier appended by [`Display`].
    pub const fn qualifier(self) -> &'static str {
        match self {
            Self::Instant => "now",
            Self::Interval { .. } => "over interval",
            Self::ProcessLifetime { .. } => "this process",
            Self::Cumulative => "cumulative",
        }
    }

    /// Machine window kind for the contract block (`instant` / `interval` / …).
    pub const fn contract_kind(self) -> &'static str {
        match self {
            Self::Instant => "instant",
            Self::Interval { .. } => "interval",
            Self::ProcessLifetime { .. } => "process_lifetime",
            Self::Cumulative => "cumulative",
        }
    }

    /// Unix-seconds start of the window when the kind carries one; else `None`.
    ///
    /// `ProcessLifetime { since_boot_unix: 0 }` means "this process, boot ts
    /// not attached" and is treated as absent so consumers do not invent epoch.
    pub const fn since_unix(self) -> Option<u64> {
        match self {
            Self::Interval { since_unix } => Some(since_unix),
            Self::ProcessLifetime { since_boot_unix } if since_boot_unix > 0 => {
                Some(since_boot_unix)
            }
            Self::Instant | Self::Cumulative | Self::ProcessLifetime { .. } => None,
        }
    }
}

/// Operator-visible gauge: value + unit + window in one type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Gauge {
    pub value: Availability,
    pub unit: Unit,
    pub window: Window,
}

impl Gauge {
    pub const fn new(value: Availability, unit: Unit, window: Window) -> Self {
        Self {
            value,
            unit,
            window,
        }
    }

    pub const fn measured(n: u64, unit: Unit, window: Window) -> Self {
        Self::new(Availability::Measured(n), unit, window)
    }

    pub const fn unavailable(reason: UnavailableReason, unit: Unit, window: Window) -> Self {
        Self::new(Availability::Unavailable(reason), unit, window)
    }

    pub const fn field_not_served(unit: Unit, window: Window) -> Self {
        Self::unavailable(UnavailableReason::FieldNotServed, unit, window)
    }

    /// Bare-numeric wire when measured; `None` when unavailable.
    pub const fn as_wire_u64(self) -> Option<u64> {
        self.value.as_wire_u64()
    }

    /// Restore a gauge from a bare-numeric wire value plus unit/window metadata
    /// that lives in the type (not on the wire in the transparent path).
    pub const fn from_wire_u64(n: u64, unit: Unit, window: Window) -> Self {
        Self::measured(n, unit, window)
    }
}

impl fmt::Display for Gauge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.value {
            Availability::Measured(n) => {
                write!(f, "{n} {} {}", self.unit.noun(), self.window.qualifier())
            }
            Availability::Unavailable(reason) => {
                // Never print a bare measured 0. Keep unit/window so the
                // operator still sees *what* was not served.
                write!(
                    f,
                    "{} [{} / {}]",
                    reason.display_phrase(),
                    self.unit.noun(),
                    self.window.qualifier()
                )
            }
        }
    }
}

/// Transparent bare-`u64` wire helpers for later `*Health` field conversion.
///
/// # Shape
///
/// - Serialize `Measured(n)` as the JSON number `n`.
/// - Serialize `Unavailable` as JSON `null`.
/// - Deserialize a JSON number as `Measured(n)`.
/// - Deserialize JSON `null` as `Unavailable(FieldNotServed)`.
/// - Pair with `#[serde(default = "...")]` on the field so a **missing** key
///   also becomes `Unavailable(FieldNotServed)` (see tests).
///
/// Unit and window are **not** on this wire path — they stay in the Rust type
/// via constructors / field defaults in the converting PR.
pub mod serde_u64 {
    use super::*;

    pub fn serialize<S>(gauge: &Gauge, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match gauge.value {
            Availability::Measured(n) => serializer.serialize_u64(n),
            Availability::Unavailable(_) => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Gauge, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Transparent path only carries the number (or null). Callers that
        // need real unit/window attach them after deserialize via
        // `with_unit_window` or by constructing the field with known metadata.
        // Here we use placeholder Instant/Rows only when this free function is
        // used without a wrapper; production conversion uses typed helpers
        // below (`deserialize_as`).
        let opt = Option::<u64>::deserialize(deserializer)?;
        Ok(match opt {
            Some(n) => Gauge::measured(n, Unit::Rows, Window::Instant),
            None => Gauge::field_not_served(Unit::Rows, Window::Instant),
        })
    }

    /// Deserialize a bare number / null into a gauge with known unit + window.
    pub fn deserialize_as<'de, D>(
        deserializer: D,
        unit: Unit,
        window: Window,
    ) -> Result<Gauge, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt = Option::<u64>::deserialize(deserializer)?;
        Ok(match opt {
            Some(n) => Gauge::measured(n, unit, window),
            None => Gauge::field_not_served(unit, window),
        })
    }
}

/// Attach unit/window after a transparent wire round-trip that only carried the
/// number. Prefer constructing with the correct unit/window up front.
impl Gauge {
    pub const fn with_unit_window(self, unit: Unit, window: Window) -> Self {
        Self {
            value: self.value,
            unit,
            window,
        }
    }

    /// Availability token for the contract block (`measured` / `unavailable`).
    pub const fn availability_token(self) -> &'static str {
        match self.value {
            Availability::Measured(_) => "measured",
            Availability::Unavailable(_) => "unavailable",
        }
    }

    /// Optional reason when unavailable (e.g. `field_not_served`).
    pub const fn unavailable_reason_token(self) -> Option<&'static str> {
        match self.value {
            Availability::Measured(_) => None,
            Availability::Unavailable(UnavailableReason::FieldNotServed) => {
                Some("field_not_served")
            }
        }
    }
}

/// Thin newtype used only in tests / conversion examples: serde as bare u64
/// while preserving a fixed unit/window pair for Display.
///
/// Production `*Health` fields can follow the same pattern with
/// `#[serde(from = "WireU64", into = "WireU64")]` once conversion PRs land.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransparentGauge {
    pub gauge: Gauge,
}

/// Wire form: optional bare integer (`None` / missing ⇒ unavailable).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WireU64(pub Option<u64>);

impl TransparentGauge {
    pub const fn measured(n: u64, unit: Unit, window: Window) -> Self {
        Self {
            gauge: Gauge::measured(n, unit, window),
        }
    }

    pub const fn field_not_served(unit: Unit, window: Window) -> Self {
        Self {
            gauge: Gauge::field_not_served(unit, window),
        }
    }
}

impl From<WireU64> for TransparentGauge {
    fn from(w: WireU64) -> Self {
        // Unit/window are not on the wire; conversion sites override with
        // `with_unit_window` after `from`. Default Instant/Rows is intentional
        // only for the round-trip helper path tested below.
        match w.0 {
            Some(n) => Self::measured(n, Unit::Rows, Window::Instant),
            None => Self::field_not_served(Unit::Rows, Window::Instant),
        }
    }
}

impl From<TransparentGauge> for WireU64 {
    fn from(t: TransparentGauge) -> Self {
        Self(t.gauge.as_wire_u64())
    }
}

impl fmt::Display for TransparentGauge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.gauge.fmt(f)
    }
}
