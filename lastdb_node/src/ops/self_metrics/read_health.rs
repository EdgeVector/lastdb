use super::*;

/// Operator view of dual-read residue (`dual_read.legacy_hits`).
///
/// Process-lifetime hit counters are typed gauges so Display cannot invent
/// an instant "now" for a session total.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DualReadHealth {
    pub gets: Gauge,
    pub target_hits: Gauge,
    /// Residue fallthroughs only — the number a cutover probe reads. By-design
    /// planes are in `by_design_hits`; see `dual_read_metrics` module docs.
    pub legacy_hits: Gauge,
    /// Fallthroughs onto planes read by design forever (append-only order log,
    /// mutation history). Reported, never counted as debt.
    pub by_design_hits: Gauge,
    pub misses: Gauge,
    /// Which collections actually served the legacy hits, non-zero only.
    /// Without this, the residue total is a number with no owner.
    pub legacy_hits_by_collection: Vec<(String, u64)>,
    /// Same legacy hits rolled up by ideal-storage plane role (no disk walk).
    pub legacy_hits_by_plane: Vec<DualReadPlaneHitHealth>,
    /// Which by-design planes served reads, non-zero only. Excluded from
    /// `legacy_hits_by_collection` so that list is residue and only residue.
    pub by_design_hits_by_collection: Vec<(String, u64)>,
}

impl Default for DualReadHealth {
    fn default() -> Self {
        let hit = || Gauge::field_not_served(Unit::Events, GAUGE_PROCESS_LIFETIME);
        Self {
            gets: hit(),
            target_hits: hit(),
            legacy_hits: hit(),
            by_design_hits: hit(),
            misses: hit(),
            legacy_hits_by_collection: Vec::new(),
            legacy_hits_by_plane: Vec::new(),
            by_design_hits_by_collection: Vec::new(),
        }
    }
}

/// Dual-read legacy hits for one ideal-storage plane role.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DualReadPlaneHitHealth {
    pub role: String,
    pub label: String,
    pub legacy_hits: Gauge,
}

impl Serialize for DualReadPlaneHitHealth {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("DualReadPlaneHitHealth", 3)?;
        s.serialize_field("role", &self.role)?;
        s.serialize_field("label", &self.label)?;
        s.serialize_field("legacy_hits", &wire_u64(&self.legacy_hits))?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for DualReadPlaneHitHealth {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            role: String,
            label: String,
            #[serde(default)]
            legacy_hits: Option<u64>,
        }
        let raw = Raw::deserialize(deserializer)?;
        Ok(Self {
            role: raw.role,
            label: raw.label,
            legacy_hits: g_lifetime_wire(raw.legacy_hits, Unit::Events),
        })
    }
}

impl From<fold_db::storage::DualReadMetricsSnapshot> for DualReadHealth {
    fn from(s: fold_db::storage::DualReadMetricsSnapshot) -> Self {
        let legacy_hits_by_plane: Vec<DualReadPlaneHitHealth> =
            fold_db::mini_cutover::dual_read_hits_by_plane(&s.legacy_hits_by_collection)
                .into_iter()
                .map(|h| DualReadPlaneHitHealth {
                    role: plane_role_slug(h.role),
                    label: h.label,
                    legacy_hits: g_lifetime(h.legacy_hits, Unit::Events),
                })
                .collect();
        Self {
            gets: g_lifetime(s.gets, Unit::Events),
            target_hits: g_lifetime(s.target_hits, Unit::Events),
            legacy_hits: g_lifetime(s.legacy_hits, Unit::Events),
            by_design_hits: g_lifetime(s.by_design_hits, Unit::Events),
            misses: g_lifetime(s.misses, Unit::Events),
            legacy_hits_by_collection: s.legacy_hits_by_collection,
            legacy_hits_by_plane,
            by_design_hits_by_collection: s.by_design_hits_by_collection,
        }
    }
}

impl Serialize for DualReadHealth {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("DualReadHealth", 16)?;
        s.serialize_field("gets", &wire_u64(&self.gets))?;
        s.serialize_field("target_hits", &wire_u64(&self.target_hits))?;
        s.serialize_field("legacy_hits", &wire_u64(&self.legacy_hits))?;
        s.serialize_field("by_design_hits", &wire_u64(&self.by_design_hits))?;
        s.serialize_field("misses", &wire_u64(&self.misses))?;
        s.serialize_field("legacy_hits_by_collection", &self.legacy_hits_by_collection)?;
        s.serialize_field("legacy_hits_by_plane", &self.legacy_hits_by_plane)?;
        s.serialize_field(
            "by_design_hits_by_collection",
            &self.by_design_hits_by_collection,
        )?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for DualReadHealth {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(default)]
            gets: Option<u64>,
            #[serde(default)]
            target_hits: Option<u64>,
            #[serde(default)]
            legacy_hits: Option<u64>,
            #[serde(default)]
            by_design_hits: Option<u64>,
            #[serde(default)]
            misses: Option<u64>,
            #[serde(default)]
            legacy_hits_by_collection: Vec<(String, u64)>,
            #[serde(default)]
            legacy_hits_by_plane: Vec<DualReadPlaneHitHealth>,
            #[serde(default)]
            by_design_hits_by_collection: Vec<(String, u64)>,
        }
        let raw = Raw::deserialize(deserializer)?;
        Ok(Self {
            gets: g_lifetime_wire(raw.gets, Unit::Events),
            target_hits: g_lifetime_wire(raw.target_hits, Unit::Events),
            legacy_hits: g_lifetime_wire(raw.legacy_hits, Unit::Events),
            by_design_hits: g_lifetime_wire(raw.by_design_hits, Unit::Events),
            misses: g_lifetime_wire(raw.misses, Unit::Events),
            legacy_hits_by_collection: raw.legacy_hits_by_collection,
            legacy_hits_by_plane: raw.legacy_hits_by_plane,
            by_design_hits_by_collection: raw.by_design_hits_by_collection,
        })
    }
}

pub(super) fn plane_role_slug(role: fold_db::mini_cutover::CollectionPlaneRole) -> String {
    use fold_db::mini_cutover::CollectionPlaneRole::*;
    match role {
        Sot => "sot",
        Indexes => "indexes",
        HistoryAdjacent => "history_adjacent",
        TipResidue => "tip_residue",
        Locality => "locality",
        ColdSync => "cold_sync",
        Ops => "ops",
        Aside => "aside",
        Unknown => "unknown",
    }
    .to_string()
}

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
    pub cold_shard_loads: Gauge,
    /// Hash-group handles currently resident in memory.
    pub warm_resident_groups: Gauge,
    /// Approximate bytes held by those handles.
    pub warm_resident_bytes: Gauge,
    /// Configured warm-set byte budget. `0` means eviction is disabled.
    pub warm_budget_bytes: Gauge,
    /// Configured append-descriptor cap. `0` means the handle cap is off.
    ///
    /// `open_append_handles` over this is the node's descriptor headroom.
    /// `warm_resident_groups` over this is **not** — see that field.
    pub warm_budget_handles: Gauge,
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
    pub open_append_handles: Gauge,
    /// Multi-op LastStore transactions that restored prior values after a
    /// mid-batch error.
    pub torn_transaction_rollbacks: Gauge,
    /// Transactions whose restore or rollback flush failed.
    pub torn_transaction_rollback_failures: Gauge,
    /// Committed transactions whose post-commit cache refresh failed.
    pub transaction_residency_refresh_failures: Gauge,
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
    pub id_tier_resident: Gauge,
    /// Keys-only resolutions served by the in-memory key-index cache.
    pub id_tier_key_cache_hits: Gauge,
    /// Keys-only resolutions served by the on-disk id sidecar.
    pub id_tier_sidecar_hits: Gauge,
    /// Keys-only resolutions that missed both id tiers and scanned a group.
    pub id_tier_live_scans: Gauge,
    /// Groups whose ids the key-index cache retains right now.
    pub key_cache_groups: Gauge,
    /// Charged bytes of those retained id lists.
    pub key_cache_bytes: Gauge,
    /// Configured key-index cache budget. `0` means the cache is disabled.
    pub key_cache_budget_bytes: Gauge,
}

impl Default for ReadCostHealth {
    fn default() -> Self {
        Self {
            cold_shard_loads: Gauge::field_not_served(Unit::Events, GAUGE_PROCESS_LIFETIME),
            warm_resident_groups: Gauge::field_not_served(Unit::Named("group(s)"), GAUGE_INSTANT),
            warm_resident_bytes: Gauge::field_not_served(Unit::Bytes, GAUGE_INSTANT),
            warm_budget_bytes: Gauge::field_not_served(Unit::Bytes, GAUGE_INSTANT),
            warm_budget_handles: Gauge::field_not_served(Unit::Named("handle(s)"), GAUGE_INSTANT),
            open_append_handles: Gauge::field_not_served(Unit::Named("handle(s)"), GAUGE_INSTANT),
            torn_transaction_rollbacks: Gauge::field_not_served(
                Unit::Events,
                GAUGE_PROCESS_LIFETIME,
            ),
            torn_transaction_rollback_failures: Gauge::field_not_served(
                Unit::Events,
                GAUGE_PROCESS_LIFETIME,
            ),
            transaction_residency_refresh_failures: Gauge::field_not_served(
                Unit::Events,
                GAUGE_PROCESS_LIFETIME,
            ),
            id_tier_resident: Gauge::field_not_served(Unit::Events, GAUGE_PROCESS_LIFETIME),
            id_tier_key_cache_hits: Gauge::field_not_served(Unit::Events, GAUGE_PROCESS_LIFETIME),
            id_tier_sidecar_hits: Gauge::field_not_served(Unit::Events, GAUGE_PROCESS_LIFETIME),
            id_tier_live_scans: Gauge::field_not_served(Unit::Events, GAUGE_PROCESS_LIFETIME),
            key_cache_groups: Gauge::field_not_served(Unit::Named("group(s)"), GAUGE_INSTANT),
            key_cache_bytes: Gauge::field_not_served(Unit::Bytes, GAUGE_INSTANT),
            key_cache_budget_bytes: Gauge::field_not_served(Unit::Bytes, GAUGE_INSTANT),
        }
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

impl Serialize for ReadCostHealth {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("ReadCostHealth", 16)?;
        s.serialize_field("cold_shard_loads", &wire_u64(&self.cold_shard_loads))?;
        s.serialize_field(
            "warm_resident_groups",
            &wire_u64(&self.warm_resident_groups),
        )?;
        s.serialize_field("warm_resident_bytes", &wire_u64(&self.warm_resident_bytes))?;
        s.serialize_field("warm_budget_bytes", &wire_u64(&self.warm_budget_bytes))?;
        s.serialize_field("warm_budget_handles", &wire_u64(&self.warm_budget_handles))?;
        s.serialize_field("open_append_handles", &wire_u64(&self.open_append_handles))?;
        s.serialize_field(
            "torn_transaction_rollbacks",
            &wire_u64(&self.torn_transaction_rollbacks),
        )?;
        s.serialize_field(
            "torn_transaction_rollback_failures",
            &wire_u64(&self.torn_transaction_rollback_failures),
        )?;
        s.serialize_field(
            "transaction_residency_refresh_failures",
            &wire_u64(&self.transaction_residency_refresh_failures),
        )?;
        s.serialize_field("id_tier_resident", &wire_u64(&self.id_tier_resident))?;
        s.serialize_field(
            "id_tier_key_cache_hits",
            &wire_u64(&self.id_tier_key_cache_hits),
        )?;
        s.serialize_field(
            "id_tier_sidecar_hits",
            &wire_u64(&self.id_tier_sidecar_hits),
        )?;
        s.serialize_field("id_tier_live_scans", &wire_u64(&self.id_tier_live_scans))?;
        s.serialize_field("key_cache_groups", &wire_u64(&self.key_cache_groups))?;
        s.serialize_field("key_cache_bytes", &wire_u64(&self.key_cache_bytes))?;
        s.serialize_field(
            "key_cache_budget_bytes",
            &wire_u64(&self.key_cache_budget_bytes),
        )?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for ReadCostHealth {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(default)]
            cold_shard_loads: Option<u64>,
            #[serde(default)]
            warm_resident_groups: Option<u64>,
            #[serde(default)]
            warm_resident_bytes: Option<u64>,
            #[serde(default)]
            warm_budget_bytes: Option<u64>,
            #[serde(default)]
            warm_budget_handles: Option<u64>,
            #[serde(default)]
            open_append_handles: Option<u64>,
            #[serde(default)]
            torn_transaction_rollbacks: Option<u64>,
            #[serde(default)]
            torn_transaction_rollback_failures: Option<u64>,
            #[serde(default)]
            transaction_residency_refresh_failures: Option<u64>,
            #[serde(default)]
            id_tier_resident: Option<u64>,
            #[serde(default)]
            id_tier_key_cache_hits: Option<u64>,
            #[serde(default)]
            id_tier_sidecar_hits: Option<u64>,
            #[serde(default)]
            id_tier_live_scans: Option<u64>,
            #[serde(default)]
            key_cache_groups: Option<u64>,
            #[serde(default)]
            key_cache_bytes: Option<u64>,
            #[serde(default)]
            key_cache_budget_bytes: Option<u64>,
        }
        let raw = Raw::deserialize(deserializer)?;
        Ok(Self {
            cold_shard_loads: g_lifetime_wire(raw.cold_shard_loads, Unit::Events),
            warm_resident_groups: g_instant_wire(raw.warm_resident_groups, Unit::Named("group(s)")),
            warm_resident_bytes: g_instant_wire(raw.warm_resident_bytes, Unit::Bytes),
            warm_budget_bytes: g_instant_wire(raw.warm_budget_bytes, Unit::Bytes),
            warm_budget_handles: g_instant_wire(raw.warm_budget_handles, Unit::Named("handle(s)")),
            open_append_handles: g_instant_wire(raw.open_append_handles, Unit::Named("handle(s)")),
            torn_transaction_rollbacks: g_lifetime_wire(
                raw.torn_transaction_rollbacks,
                Unit::Events,
            ),
            torn_transaction_rollback_failures: g_lifetime_wire(
                raw.torn_transaction_rollback_failures,
                Unit::Events,
            ),
            transaction_residency_refresh_failures: g_lifetime_wire(
                raw.transaction_residency_refresh_failures,
                Unit::Events,
            ),
            id_tier_resident: g_lifetime_wire(raw.id_tier_resident, Unit::Events),
            id_tier_key_cache_hits: g_lifetime_wire(raw.id_tier_key_cache_hits, Unit::Events),
            id_tier_sidecar_hits: g_lifetime_wire(raw.id_tier_sidecar_hits, Unit::Events),
            id_tier_live_scans: g_lifetime_wire(raw.id_tier_live_scans, Unit::Events),
            key_cache_groups: g_instant_wire(raw.key_cache_groups, Unit::Named("group(s)")),
            key_cache_bytes: g_instant_wire(raw.key_cache_bytes, Unit::Bytes),
            key_cache_budget_bytes: g_instant_wire(raw.key_cache_budget_bytes, Unit::Bytes),
        })
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
    /// Compiled default (64 KiB) when env is unset.
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
