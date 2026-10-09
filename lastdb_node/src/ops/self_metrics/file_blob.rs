use super::*;

/// File-blob durability counters — the other graceful degradation.
///
/// [`IntegrityHealth`] above exists because skipping an unreadable row turns a
/// loud failure into a `200` with fewer rows than the index claims. The file
/// blob plane has the same shape one layer out: a pointer persists on the row
/// and reads back perfectly, and only a fetch discovers the bytes are gone.
///
/// The node already knows which of the two it is looking at. `download_file_blob`
/// clears the `sync_file_blob_known` memo when a presigned GET *proves* the
/// object absent, and it reads that memo before clearing it — so at exactly
/// that moment it can tell "a write we accepted and cannot serve back" from "a
/// blob we never had". It spent that answer on a `WARN` line.
///
/// What that cost: measuring the durability class needed one CAS fetch per
/// pointer from outside the node — 356 fetches across 57 repos on 2026-08-17 to
/// find 23 dangling. The request-layer error count cannot substitute, because
/// both classes are the same `404` by the time it sees them.
///
/// Gauges are process-lifetime and wire-transparent: a daemon predating these
/// fields deserializes to [`Availability::Unavailable`], never to a measured
/// zero, so "this daemon does not count it" cannot read as "nothing is wrong".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileBlobHealth {
    /// Proven-absent fetch events for blobs this node recorded as uploaded —
    /// loss *discoveries*, not retries.
    ///
    /// The fetch that proves absence also clears the memo, so a client
    /// hammering one lost blob books it once. Exceeding
    /// [`Self::absent_with_memo_distinct`] therefore means a blob was
    /// re-uploaded and lost again, which is worse than the same count spread
    /// over distinct blobs.
    pub absent_with_memo: Gauge,
    /// Distinct blobs behind those events — how many objects are actually gone.
    pub absent_with_memo_distinct: Gauge,
    /// Proven-absent fetch events with no memo held: an ordinary miss for bytes
    /// this node does not claim to have uploaded.
    ///
    /// Carried so the durability figure has a denominator. Absent it, a reader
    /// cannot tell a node that lost blobs from a node that is merely being
    /// asked for blobs it never had. Note it also absorbs re-fetches of blobs
    /// already reported above, because that first report cleared their memo —
    /// a denominator, not a diagnosis.
    pub absent_without_memo: Gauge,
    /// True when the node stopped retaining new identities at the cap, so
    /// [`Self::absent_with_memo_distinct`] is a floor rather than a total.
    pub absent_distinct_capped: bool,
}

impl Default for FileBlobHealth {
    fn default() -> Self {
        Self {
            absent_with_memo: Gauge::field_not_served(Unit::Events, GAUGE_PROCESS_LIFETIME),
            absent_with_memo_distinct: Gauge::field_not_served(
                Unit::Named("blob(s)"),
                GAUGE_PROCESS_LIFETIME,
            ),
            absent_without_memo: Gauge::field_not_served(Unit::Events, GAUGE_PROCESS_LIFETIME),
            absent_distinct_capped: false,
        }
    }
}

impl From<fold_db::sync::engine::FileBlobDurability> for FileBlobHealth {
    fn from(d: fold_db::sync::engine::FileBlobDurability) -> Self {
        Self {
            absent_with_memo: gauge_measured(
                d.absent_with_memo,
                Unit::Events,
                GAUGE_PROCESS_LIFETIME,
            ),
            absent_with_memo_distinct: gauge_measured(
                d.distinct_with_memo,
                Unit::Named("blob(s)"),
                GAUGE_PROCESS_LIFETIME,
            ),
            absent_without_memo: gauge_measured(
                d.absent_without_memo,
                Unit::Events,
                GAUGE_PROCESS_LIFETIME,
            ),
            absent_distinct_capped: d.distinct_capped,
        }
    }
}

impl Serialize for FileBlobHealth {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("FileBlobHealth", 4)?;
        s.serialize_field("absent_with_memo", &wire_u64(&self.absent_with_memo))?;
        s.serialize_field(
            "absent_with_memo_distinct",
            &wire_u64(&self.absent_with_memo_distinct),
        )?;
        s.serialize_field("absent_without_memo", &wire_u64(&self.absent_without_memo))?;
        s.serialize_field("absent_distinct_capped", &self.absent_distinct_capped)?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for FileBlobHealth {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(default)]
            absent_with_memo: Option<u64>,
            #[serde(default)]
            absent_with_memo_distinct: Option<u64>,
            #[serde(default)]
            absent_without_memo: Option<u64>,
            #[serde(default)]
            absent_distinct_capped: bool,
        }
        let raw = Raw::deserialize(deserializer)?;
        Ok(Self {
            absent_with_memo: gauge_from_wire(
                raw.absent_with_memo,
                Unit::Events,
                GAUGE_PROCESS_LIFETIME,
            ),
            absent_with_memo_distinct: gauge_from_wire(
                raw.absent_with_memo_distinct,
                Unit::Named("blob(s)"),
                GAUGE_PROCESS_LIFETIME,
            ),
            absent_without_memo: gauge_from_wire(
                raw.absent_without_memo,
                Unit::Events,
                GAUGE_PROCESS_LIFETIME,
            ),
            absent_distinct_capped: raw.absent_distinct_capped,
        })
    }
}

/// Cached result of `lastdb db probe-locator-only` for this process.
///
/// Population sample (tips), not event counters. After dual-write completion
/// the dual-write residual (`would_dual_write`) must stay empty; locator-only
/// growth is a separate reachability class and is what this gauge names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocatorOnlyHealth {
    pub tips_sampled: Gauge,
    pub max_tips: Gauge,
    /// Contiguous `mk:` windows the budget was spread across, and how many the
    /// walk ran to the end of. Equal means the sample was a full walk.
    pub strata: Gauge,
    pub strata_exhausted: Gauge,
    pub locator_only: Gauge,
    pub body_at_derived_or_flat: Gauge,
    /// Tips whose atom body no reader route can reach — the class that makes a
    /// query return 200 with fewer rows than its index claims. Carved out of
    /// `other_unresolved`; a daemon predating the split serves it
    /// `Unavailable`, never a measured zero.
    pub dangling: Gauge,
    pub other_unresolved: Gauge,
    pub completed: bool,
    pub locator_only_per_mille: Gauge,
    pub dangling_per_mille: Gauge,
    pub probed_at_unix: u64,
    /// `dangling` from the previous comparable probe of this process, and when
    /// it ran. `Unavailable` on the first probe after a restart.
    pub prev_dangling: Gauge,
    pub prev_probed_at_unix: u64,
    /// Narrowest contiguous band of `mk:`, in per mille, this sample could have
    /// detected with 95% confidence.
    ///
    /// Read it before any rate above. A `0` rate under a 162‰ floor and a `0`
    /// rate under a 6‰ floor are different claims, and the status line rendered
    /// them identically until 2026-09-07. A daemon predating this field serves
    /// `Unavailable`, never a measured zero.
    pub detection_floor_per_mille: Gauge,
    /// New dangling tips per hour since the previous probe — the recurrence
    /// signal. `dangling` says how much damage the store carries; this says how
    /// fast it is making more, which is the only way an operator can see that a
    /// repair did not hold.
    pub dangling_recurrence_per_hour: Gauge,
}

impl From<fold_db::db_operations::LocatorOnlyPopulationReport> for LocatorOnlyHealth {
    fn from(r: fold_db::db_operations::LocatorOnlyPopulationReport) -> Self {
        Self {
            tips_sampled: g_instant(r.tips_sampled, Unit::Named("tip(s)")),
            max_tips: g_instant(r.max_tips, Unit::Named("tip(s)")),
            strata: g_instant(r.strata, Unit::Named("window(s)")),
            strata_exhausted: g_instant(r.strata_exhausted, Unit::Named("window(s)")),
            locator_only: g_instant(r.locator_only, Unit::Named("tip(s)")),
            body_at_derived_or_flat: g_instant(r.body_at_derived_or_flat, Unit::Named("tip(s)")),
            dangling: g_instant(r.dangling, Unit::Named("tip(s)")),
            other_unresolved: g_instant(r.other_unresolved, Unit::Named("tip(s)")),
            completed: r.completed,
            locator_only_per_mille: match r.locator_only_per_mille {
                Some(n) => g_instant(n, Unit::Named("‰")),
                None => Gauge::field_not_served(Unit::Named("‰"), GAUGE_INSTANT),
            },
            dangling_per_mille: match r.dangling_per_mille {
                Some(n) => g_instant(n, Unit::Named("‰")),
                None => Gauge::field_not_served(Unit::Named("‰"), GAUGE_INSTANT),
            },
            probed_at_unix: r.probed_at_unix,
            prev_dangling: match r.prev_dangling {
                Some(n) => g_instant(n, Unit::Named("tip(s)")),
                None => Gauge::field_not_served(Unit::Named("tip(s)"), GAUGE_INSTANT),
            },
            prev_probed_at_unix: r.prev_probed_at_unix.unwrap_or(0),
            detection_floor_per_mille: match r.detection_floor_per_mille {
                Some(n) => g_instant(n, Unit::Named("‰")),
                None => Gauge::field_not_served(Unit::Named("‰"), GAUGE_INSTANT),
            },
            dangling_recurrence_per_hour: match r.dangling_recurrence_per_hour {
                Some(n) => g_instant(n, Unit::Named("tip(s)/h")),
                None => Gauge::field_not_served(Unit::Named("tip(s)/h"), GAUGE_INSTANT),
            },
        }
    }
}

impl Serialize for LocatorOnlyHealth {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("LocatorOnlyHealth", 15)?;
        s.serialize_field("tips_sampled", &wire_u64(&self.tips_sampled))?;
        s.serialize_field("max_tips", &wire_u64(&self.max_tips))?;
        s.serialize_field("strata", &wire_u64(&self.strata))?;
        s.serialize_field("strata_exhausted", &wire_u64(&self.strata_exhausted))?;
        s.serialize_field("locator_only", &wire_u64(&self.locator_only))?;
        s.serialize_field(
            "body_at_derived_or_flat",
            &wire_u64(&self.body_at_derived_or_flat),
        )?;
        s.serialize_field("dangling", &wire_u64(&self.dangling))?;
        s.serialize_field("other_unresolved", &wire_u64(&self.other_unresolved))?;
        s.serialize_field("completed", &self.completed)?;
        s.serialize_field(
            "locator_only_per_mille",
            &wire_u64(&self.locator_only_per_mille),
        )?;
        s.serialize_field("dangling_per_mille", &wire_u64(&self.dangling_per_mille))?;
        s.serialize_field("probed_at_unix", &self.probed_at_unix)?;
        s.serialize_field("prev_dangling", &wire_u64(&self.prev_dangling))?;
        s.serialize_field("prev_probed_at_unix", &self.prev_probed_at_unix)?;
        s.serialize_field(
            "detection_floor_per_mille",
            &wire_u64(&self.detection_floor_per_mille),
        )?;
        s.serialize_field(
            "dangling_recurrence_per_hour",
            &wire_u64(&self.dangling_recurrence_per_hour),
        )?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for LocatorOnlyHealth {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(default)]
            tips_sampled: Option<u64>,
            #[serde(default)]
            max_tips: Option<u64>,
            #[serde(default)]
            strata: Option<u64>,
            #[serde(default)]
            strata_exhausted: Option<u64>,
            #[serde(default)]
            locator_only: Option<u64>,
            #[serde(default)]
            body_at_derived_or_flat: Option<u64>,
            #[serde(default)]
            dangling: Option<u64>,
            #[serde(default)]
            other_unresolved: Option<u64>,
            #[serde(default)]
            completed: bool,
            #[serde(default)]
            locator_only_per_mille: Option<u64>,
            #[serde(default)]
            dangling_per_mille: Option<u64>,
            #[serde(default)]
            probed_at_unix: u64,
            #[serde(default)]
            prev_dangling: Option<u64>,
            #[serde(default)]
            prev_probed_at_unix: u64,
            #[serde(default)]
            detection_floor_per_mille: Option<u64>,
            #[serde(default)]
            dangling_recurrence_per_hour: Option<u64>,
        }
        let raw = Raw::deserialize(deserializer)?;
        Ok(Self {
            tips_sampled: g_instant_wire(raw.tips_sampled, Unit::Named("tip(s)")),
            max_tips: g_instant_wire(raw.max_tips, Unit::Named("tip(s)")),
            strata: g_instant_wire(raw.strata, Unit::Named("window(s)")),
            strata_exhausted: g_instant_wire(raw.strata_exhausted, Unit::Named("window(s)")),
            locator_only: g_instant_wire(raw.locator_only, Unit::Named("tip(s)")),
            body_at_derived_or_flat: g_instant_wire(
                raw.body_at_derived_or_flat,
                Unit::Named("tip(s)"),
            ),
            dangling: g_instant_wire(raw.dangling, Unit::Named("tip(s)")),
            other_unresolved: g_instant_wire(raw.other_unresolved, Unit::Named("tip(s)")),
            completed: raw.completed,
            locator_only_per_mille: g_instant_wire(raw.locator_only_per_mille, Unit::Named("‰")),
            dangling_per_mille: g_instant_wire(raw.dangling_per_mille, Unit::Named("‰")),
            probed_at_unix: raw.probed_at_unix,
            prev_dangling: g_instant_wire(raw.prev_dangling, Unit::Named("tip(s)")),
            prev_probed_at_unix: raw.prev_probed_at_unix,
            detection_floor_per_mille: g_instant_wire(
                raw.detection_floor_per_mille,
                Unit::Named("‰"),
            ),
            dangling_recurrence_per_hour: g_instant_wire(
                raw.dangling_recurrence_per_hour,
                Unit::Named("tip(s)/h"),
            ),
        })
    }
}
