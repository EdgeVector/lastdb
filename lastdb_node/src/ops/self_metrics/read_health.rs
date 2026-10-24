use super::*;

gauge_health! {
    /// Operator-facing read cost — what a read *costs*, not how long it took.
    ///
    /// Cold shard loads (segment parse + `BTreeMap` rebuild, ~5.3 MB per `atoms`
    /// group) are the unit of cost for every read regression this node has chased.
    /// Wall time alone cannot separate "this query loaded 625 groups" from "this
    /// query waited on a lock"; this can.
    /// Wire-transparent read-cost gauges (cold loads + warm-set occupancy).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ReadCostHealth {
        /// Cold hash-group shard loads since the store was opened (monotonic).
        pub cold_shard_loads: (Unit::Events, GAUGE_PROCESS_LIFETIME),
        /// Hash-group handles currently resident in memory.
        pub warm_resident_groups: (Unit::Named("group(s)"), GAUGE_INSTANT),
        /// Approximate bytes held by those handles.
        pub warm_resident_bytes: (Unit::Bytes, GAUGE_INSTANT),
        /// Configured warm-set byte budget. `0` means eviction is disabled.
        pub warm_budget_bytes: (Unit::Bytes, GAUGE_INSTANT),
        /// Configured append-descriptor cap. `0` means the handle cap is off.
        ///
        /// `open_append_handles` over this is the node's descriptor headroom.
        /// `warm_resident_groups` over this is **not** — see that field.
        pub warm_budget_handles: (Unit::Named("handle(s)"), GAUGE_INSTANT),
        /// Open append descriptors across all resident hash groups.
        ///
        /// The numerator of the descriptor budget. A group holds one of these only
        /// after it has taken a write, so it sits below `warm_resident_groups`:
        /// 5,048 against 13,818 in steady state on the primary on 2026-07-30, and
        /// 2 against 4,915 while a cap on the group count was churning that same
        /// home — eviction closes the file it evicts.
        ///
        /// Unavailable means the daemon does not report this field. A measured
        /// zero would render a staged-behind daemon as having full descriptor
        /// headroom, exactly the reassuring-zero failure this gauge was added to
        /// prevent.
        pub open_append_handles: (Unit::Named("handle(s)"), GAUGE_INSTANT),
        /// Multi-op LastStore transactions that restored prior values after a
        /// mid-batch error.
        pub torn_transaction_rollbacks: (Unit::Events, GAUGE_PROCESS_LIFETIME),
        /// Transactions whose restore or rollback flush failed.
        pub torn_transaction_rollback_failures: (Unit::Events, GAUGE_PROCESS_LIFETIME),
        /// Committed transactions whose post-commit cache refresh failed.
        pub transaction_residency_refresh_failures: (Unit::Events, GAUGE_PROCESS_LIFETIME),
        /// Keys-only group resolutions that found the group already warm.
        ///
        /// This and the six fields below are the id tiers beneath the warm body
        /// set. Before them the body tier was the only instrumented one, so a
        /// read-thrash diagnosis could only reach for the warm byte budget — the
        /// most expensive knob on the node, because it is charged 1:1 into the
        /// process memory budget and multiplied into the projection against the
        /// guard ceiling. `id_tier_key_cache_hits` and `id_tier_sidecar_hits`
        /// against `id_tier_live_scans` say whether the two cheap tiers are
        /// working before that knob is the answer.
        ///
        /// The four `id_tier_*` counters sum to the number of keys-only
        /// resolutions.
        pub id_tier_resident: (Unit::Events, GAUGE_PROCESS_LIFETIME),
        /// Keys-only resolutions served by the in-memory key-index cache.
        pub id_tier_key_cache_hits: (Unit::Events, GAUGE_PROCESS_LIFETIME),
        /// Keys-only resolutions served by the on-disk id sidecar.
        pub id_tier_sidecar_hits: (Unit::Events, GAUGE_PROCESS_LIFETIME),
        /// Keys-only resolutions that missed both id tiers and scanned a group.
        pub id_tier_live_scans: (Unit::Events, GAUGE_PROCESS_LIFETIME),
        /// Groups whose ids the key-index cache retains right now.
        pub key_cache_groups: (Unit::Named("group(s)"), GAUGE_INSTANT),
        /// Charged bytes of those retained id lists.
        pub key_cache_bytes: (Unit::Bytes, GAUGE_INSTANT),
        /// Configured key-index cache budget. `0` means the cache is disabled.
        pub key_cache_budget_bytes: (Unit::Bytes, GAUGE_INSTANT),
    }
}

impl From<fold_db::storage::traits::ReadCostStats> for ReadCostHealth {
    fn from(s: fold_db::storage::traits::ReadCostStats) -> Self {
        Self {
            cold_shard_loads: g_lifetime(s.cold_shard_loads, Unit::Events),
            warm_resident_groups: g_instant(s.warm_resident_groups, Unit::Named("group(s)")),
            warm_resident_bytes: g_instant(s.warm_resident_bytes, Unit::Bytes),
            warm_budget_bytes: g_instant(s.warm_budget_bytes, Unit::Bytes),
            warm_budget_handles: g_instant(s.warm_budget_handles, Unit::Named("handle(s)")),
            open_append_handles: g_instant(s.open_append_handles, Unit::Named("handle(s)")),
            torn_transaction_rollbacks: g_lifetime(s.torn_transaction_rollbacks, Unit::Events),
            torn_transaction_rollback_failures: g_lifetime(
                s.torn_transaction_rollback_failures,
                Unit::Events,
            ),
            transaction_residency_refresh_failures: g_lifetime(
                s.transaction_residency_refresh_failures,
                Unit::Events,
            ),
            id_tier_resident: g_lifetime(s.id_tier_resident, Unit::Events),
            id_tier_key_cache_hits: g_lifetime(s.id_tier_key_cache_hits, Unit::Events),
            id_tier_sidecar_hits: g_lifetime(s.id_tier_sidecar_hits, Unit::Events),
            id_tier_live_scans: g_lifetime(s.id_tier_live_scans, Unit::Events),
            key_cache_groups: g_instant(s.key_cache_groups, Unit::Named("group(s)")),
            key_cache_bytes: g_instant(s.key_cache_bytes, Unit::Bytes),
            key_cache_budget_bytes: g_instant(s.key_cache_budget_bytes, Unit::Bytes),
        }
    }
}

impl ReadCostHealth {
    /// Share of keys-only resolutions that MISSED the warm set and were still
    /// answered without a segment read, or `None` when this daemon does not
    /// serve the id-tier counters.
    ///
    /// The denominator deliberately excludes `id_tier_resident`. A resolution
    /// that found the group already warm never consulted an id tier, so
    /// counting it as a hit would make the rate rise as the warm set grew and
    /// read as the id tiers working when they were merely unused. What an
    /// operator wants to know is the opposite: of the resolutions the warm set
    /// did NOT cover, how many still avoided a scan.
    ///
    /// `None` rather than `0` when nothing missed the warm set yet — a rate
    /// over an empty denominator is not zero, it is unmeasured.
    #[must_use]
    pub fn id_tier_hit_percent(&self) -> Option<f64> {
        let key_cache = self.id_tier_key_cache_hits.as_wire_u64()?;
        let sidecar = self.id_tier_sidecar_hits.as_wire_u64()?;
        let scans = self.id_tier_live_scans.as_wire_u64()?;
        let missed_warm = key_cache.saturating_add(sidecar).saturating_add(scans);
        if missed_warm == 0 {
            return None;
        }
        Some((key_cache.saturating_add(sidecar) as f64 / missed_warm as f64) * 100.0)
    }

    /// Key-index cache fill as a percentage of its budget, or `None` when the
    /// cache is disabled (`budget == 0`).
    #[must_use]
    pub fn key_cache_fill_percent(&self) -> Option<f64> {
        let budget = self.key_cache_budget_bytes.as_wire_u64()?;
        if budget == 0 {
            return None;
        }
        let used = self.key_cache_bytes.as_wire_u64()?;
        Some((used as f64 / budget as f64) * 100.0)
    }

    /// Warm-set fill as a percentage of budget, or `None` when eviction is
    /// disabled (`budget == 0`), where a percentage would be meaningless.
    #[must_use]
    pub fn warm_fill_percent(&self) -> Option<f64> {
        let budget = self.warm_budget_bytes.as_wire_u64()?;
        if budget == 0 {
            return None;
        }
        let resident = self.warm_resident_bytes.as_wire_u64()?;
        Some((resident as f64 / budget as f64) * 100.0)
    }

    /// Open append descriptors as a percentage of the descriptor cap, or `None`
    /// when the cap is off.
    ///
    /// This is the number that was missing at 07:48Z on 2026-07-30: the byte
    /// fill read ~69% while the descriptor fill was at 99.7% and about to stop
    /// the data plane. Surfaced separately because the two can diverge by design
    /// — many small groups spend descriptors far faster than bytes.
    ///
    /// The numerator is `open_append_handles` and not `warm_resident_groups`.
    /// Using the group count read 100% for the 48 minutes after the cap went
    /// live while the node held 2 of 4,915 descriptors, which turned a throttle
    /// costing 4.7M cold loads into a dial that looked correctly pegged.
    #[must_use]
    pub fn warm_handle_fill_percent(&self) -> Option<f64> {
        let budget = self.warm_budget_handles.as_wire_u64()?;
        if budget == 0 {
            return None;
        }
        let open = self.open_append_handles.as_wire_u64()?;
        Some((open as f64 / budget as f64) * 100.0)
    }
}

/// Product write limits exposed on `/api/status` and `lastdb status`.
///
/// Byte ceilings are Instant config gauges; the env var name stays a string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LimitsHealth {
    /// Effective maximum HTTP request body on the owner Unix socket (bytes).
    pub max_request_body_bytes: Gauge,
    /// Effective max serialized atom field content (bytes).
    pub max_atom_content_bytes: Gauge,
    /// Compiled default (256 KiB) when env is unset.
    pub max_atom_content_bytes_default: Gauge,
    /// Absolute max even if env is higher.
    pub max_atom_content_bytes_absolute_max: Gauge,
    /// Env var name that overrides the default.
    pub max_atom_content_bytes_env: String,
}

impl Default for LimitsHealth {
    fn default() -> Self {
        Self {
            max_request_body_bytes: g_instant(
                lastdb_uds::uds_http::MAX_BODY_LEN as u64,
                Unit::Bytes,
            ),
            max_atom_content_bytes: g_instant(
                fold_db::atom::max_atom_content_bytes() as u64,
                Unit::Bytes,
            ),
            max_atom_content_bytes_default: g_instant(
                fold_db::atom::DEFAULT_MAX_ATOM_CONTENT_BYTES as u64,
                Unit::Bytes,
            ),
            max_atom_content_bytes_absolute_max: g_instant(
                fold_db::atom::ABSOLUTE_MAX_ATOM_CONTENT_BYTES as u64,
                Unit::Bytes,
            ),
            max_atom_content_bytes_env: fold_db::atom::MAX_ATOM_CONTENT_BYTES_ENV.to_string(),
        }
    }
}

impl Serialize for LimitsHealth {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("LimitsHealth", 5)?;
        s.serialize_field(
            "max_request_body_bytes",
            &wire_u64(&self.max_request_body_bytes),
        )?;
        s.serialize_field(
            "max_atom_content_bytes",
            &wire_u64(&self.max_atom_content_bytes),
        )?;
        s.serialize_field(
            "max_atom_content_bytes_default",
            &wire_u64(&self.max_atom_content_bytes_default),
        )?;
        s.serialize_field(
            "max_atom_content_bytes_absolute_max",
            &wire_u64(&self.max_atom_content_bytes_absolute_max),
        )?;
        s.serialize_field(
            "max_atom_content_bytes_env",
            &self.max_atom_content_bytes_env,
        )?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for LimitsHealth {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(default)]
            max_request_body_bytes: Option<u64>,
            #[serde(default)]
            max_atom_content_bytes: Option<u64>,
            #[serde(default)]
            max_atom_content_bytes_default: Option<u64>,
            #[serde(default)]
            max_atom_content_bytes_absolute_max: Option<u64>,
            #[serde(default)]
            max_atom_content_bytes_env: String,
        }
        let raw = Raw::deserialize(deserializer)?;
        Ok(Self {
            max_request_body_bytes: g_instant_wire(raw.max_request_body_bytes, Unit::Bytes),
            max_atom_content_bytes: g_instant_wire(raw.max_atom_content_bytes, Unit::Bytes),
            max_atom_content_bytes_default: g_instant_wire(
                raw.max_atom_content_bytes_default,
                Unit::Bytes,
            ),
            max_atom_content_bytes_absolute_max: g_instant_wire(
                raw.max_atom_content_bytes_absolute_max,
                Unit::Bytes,
            ),
            max_atom_content_bytes_env: raw.max_atom_content_bytes_env,
        })
    }
}

// lint:file-size-ok moved verbatim from self_metrics.rs; cohesive unit, split further in a later pass
