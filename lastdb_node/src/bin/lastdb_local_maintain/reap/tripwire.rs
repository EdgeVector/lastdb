//! The post-drop tripwire of the tip pass.
//!
//! The plan calls a molecule dead when a drop receipt or a dropped name names
//! it and no live schema uses it. A live schema can still be missing from the
//! catalog. A client can then write a tip under the "dead" molecule after the
//! drop. No catalog gate sees such a write. The tripwire reads `written_at` of
//! each doomed tip and stops the plan when the tip is newer than the drop of
//! its molecule, plus a slack.

use std::collections::BTreeMap;

use fold_db::db_operations::SchemaDropReceipt;
use serde::{Deserialize, Serialize};

use super::identities::Identities;
use super::keys::{mol_key, MolKey};
use super::ReapError;

/// 2026-10-08T00:00:00Z in milliseconds. The drop time of a name that has no
/// receipt.
pub(crate) const RECEIPTLESS_DROP_MS: u64 = 1_791_417_600_000;

/// The default slack in milliseconds. It covers the clock skew between the
/// receipt and the tip.
pub(crate) const DEFAULT_SLACK_MS: u64 = 60_000;

/// A `written_at` below this value is in milliseconds. 1e14 ms is the year 5138.
const MILLIS_BELOW: u64 = 100_000_000_000_000;

/// A `written_at` from this value is in nanoseconds. 1e17 ns is the year 1973.
/// A value between the two limits is in microseconds.
const NANOS_FROM: u64 = 100_000_000_000_000_000;

/// The unit in which a writer stored `written_at`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Unit {
    Millis,
    Micros,
    Nanos,
}

impl Unit {
    fn name(self) -> &'static str {
        match self {
            Self::Millis => "ms",
            Self::Micros => "us",
            Self::Nanos => "ns",
        }
    }
}

/// Read a `written_at` value as milliseconds. The size of the value gives the
/// unit, because production wrote nanoseconds and older writers wrote less
/// precise units.
pub(crate) fn to_millis(raw: u64) -> (u64, Unit) {
    if raw < MILLIS_BELOW {
        (raw, Unit::Millis)
    } else if raw < NANOS_FROM {
        (raw / 1_000, Unit::Micros)
    } else {
        (raw / 1_000_000, Unit::Nanos)
    }
}

/// What the tripwire saw. The plan file prints it, so a reader can check the
/// unit that the data used.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TripwireStats {
    pub slack_ms: u64,
    pub checked_tips: u64,
    pub read_as_ms: u64,
    pub read_as_us: u64,
    pub read_as_ns: u64,
    /// The newest `written_at` of a doomed tip, in milliseconds.
    pub newest_written_at_ms: u64,
}

/// The check of one doomed tip against the drop time of its molecule.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Tripwire<'a> {
    drops: &'a BTreeMap<MolKey, u64>,
    slack_ms: u64,
}

impl<'a> Tripwire<'a> {
    /// `drops` gives the drop time in milliseconds of each molecule of E1.
    pub(crate) fn new(drops: &'a BTreeMap<MolKey, u64>, slack_ms: u64) -> Self {
        Self { drops, slack_ms }
    }

    pub(crate) fn slack_ms(&self) -> u64 {
        self.slack_ms
    }

    /// Gate: a doomed tip must not be newer than the drop plus the slack.
    ///
    /// A molecule with no drop time stops the plan too. Every dead molecule
    /// comes from E1, so a missing time is a defect of the plan.
    pub(crate) fn check(
        &self,
        molecule: &MolKey,
        written_at: u64,
        tip_key: &str,
        stats: &mut TripwireStats,
    ) -> Result<(), ReapError> {
        let (written_ms, unit) = to_millis(written_at);
        stats.checked_tips += 1;
        match unit {
            Unit::Millis => stats.read_as_ms += 1,
            Unit::Micros => stats.read_as_us += 1,
            Unit::Nanos => stats.read_as_ns += 1,
        }
        stats.newest_written_at_ms = stats.newest_written_at_ms.max(written_ms);
        let shown: String = tip_key.replace('\0', "\\0").chars().take(120).collect();
        let Some(&dropped_ms) = self.drops.get(molecule) else {
            return Err(ReapError::abort(
                "POST_DROP_WRITE",
                format!("the molecule of tip {shown} has no drop time"),
            ));
        };
        let latest_ok = dropped_ms.saturating_add(self.slack_ms);
        if written_ms > latest_ok {
            return Err(ReapError::abort(
                "POST_DROP_WRITE",
                format!(
                    "tip {shown} was written at {written_ms} ms (raw {written_at}, read as {}). \
                     Its molecule was dropped at {dropped_ms} ms, slack {} ms. A write after the \
                     drop means the molecule may be live.",
                    unit.name(),
                    self.slack_ms
                ),
            ));
        }
        Ok(())
    }
}

/// Raise the drop time of the molecule `id` to at least `ms`.
///
/// A molecule that two receipts name was live until the later drop. The later
/// time is then the one that counts.
pub(crate) fn note_drop(drops: &mut BTreeMap<MolKey, u64>, id: &str, ms: u64) {
    let slot = drops.entry(mol_key(id)).or_insert(0);
    *slot = (*slot).max(ms);
}

/// The drop time of every spelling of every listed name.
///
/// A name with a receipt has the latest `dropped_at_unix_ms` of the receipts
/// that name one of its spellings. A name with no receipt has
/// [`RECEIPTLESS_DROP_MS`].
pub(crate) fn name_drop_times(
    receipts: &[&SchemaDropReceipt],
    ids: &Identities,
) -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();
    for line in &ids.listed {
        let spellings = Identities::spellings_of(line);
        let dropped = receipts
            .iter()
            .filter(|receipt| spellings.contains(&receipt.identity))
            .map(|receipt| receipt.dropped_at_unix_ms)
            .max()
            .unwrap_or(RECEIPTLESS_DROP_MS);
        for spelling in spellings {
            let slot = out.entry(spelling).or_insert(0);
            *slot = (*slot).max(dropped);
        }
    }
    out
}
