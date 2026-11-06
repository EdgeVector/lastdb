//! Self-describing operator gauge: measurement + unit + window + availability.
//!
//! Health gauges used to be bare `u64` fields whose unit, window, and
//! absent-vs-zero semantics lived only in field names and doc comments. This
//! module puts those claims in the type so `Display` cannot invent a noun that
//! contradicts the unit, and a missing JSON field deserializes to
//! [`Availability::Unavailable`] instead of a measured zero.
//!
//! ## Wire compatibility
//!
//! No existing `/api/status` field is retyped. Callers that still speak a bare
//! number use [`Gauge::as_wire_u64`] on the way out and
//! [`serde_u64::deserialize_as`] on the way in, with the unit and window the
//! field already declared. A missing or `null` field becomes
//! [`UnavailableReason::FieldNotServed`], never `Measured(0)`.
//!
//! Ground truth: brain `design-lastdb-status-gauge-contract` (PR-1).

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};

/// Why a gauge value is not a trustworthy measurement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnavailableReason {
    /// JSON field was null: the producer did not publish a value. Not the
    /// same as measured zero.
    FieldNotServed,
}

impl UnavailableReason {
    fn display_phrase(self) -> &'static str {
        match self {
            Self::FieldNotServed => "unavailable (not served)",
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
/// The noun in [`Display`] is derived from this enum — `Unit::Keys` can never
/// print as `byte(s)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Unit {
    Keys,
    Bytes,
    Events,
    /// Escape hatch for operator nouns that are not yet in the closed set.
    Named(&'static str),
}

impl Unit {
    /// Singular/plural-friendly noun fragment used by [`Display`].
    pub const fn noun(self) -> &'static str {
        match self {
            Self::Keys => "key(s)",
            Self::Bytes => "byte(s)",
            Self::Events => "event(s)",
            Self::Named(s) => s,
        }
    }

    /// Stable machine token for the additive `/api/status` `contract` block.
    ///
    /// Closed-set units use snake_case names; [`Unit::Named`] uses the noun
    /// string as-is so operator-specific units remain identifiable.
    pub const fn contract_id(self) -> &'static str {
        match self {
            Self::Keys => "keys",
            Self::Bytes => "bytes",
            Self::Events => "events",
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
    /// Totals accumulated since this process booted (`since_boot_unix` is the
    /// process start time as unix seconds when known).
    ProcessLifetime { since_boot_unix: u64 },
}

impl Window {
    /// Short qualifier appended by [`Display`].
    pub const fn qualifier(self) -> &'static str {
        match self {
            Self::Instant => "now",
            Self::ProcessLifetime { .. } => "this process",
        }
    }

    /// Machine window kind for the contract block (`instant` / `process_lifetime`).
    pub const fn contract_kind(self) -> &'static str {
        match self {
            Self::Instant => "instant",
            Self::ProcessLifetime { .. } => "process_lifetime",
        }
    }

    /// Unix-seconds start of the window when the kind carries one; else `None`.
    ///
    /// `ProcessLifetime { since_boot_unix: 0 }` means "this process, boot ts
    /// not attached" and is treated as absent so consumers do not invent epoch.
    pub const fn since_unix(self) -> Option<u64> {
        match self {
            Self::ProcessLifetime { since_boot_unix } if since_boot_unix > 0 => {
                Some(since_boot_unix)
            }
            Self::Instant | Self::ProcessLifetime { .. } => None,
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

/// Bare-`u64` deserialize helper for status fields that already know their unit
/// and window.
///
/// A JSON number becomes [`Availability::Measured`]. JSON `null` becomes
/// [`UnavailableReason::FieldNotServed`]. Unit and window are arguments, not
/// wire fields.
pub mod serde_u64 {
    use super::*;

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

impl Gauge {
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
