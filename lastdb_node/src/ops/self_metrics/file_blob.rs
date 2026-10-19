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
