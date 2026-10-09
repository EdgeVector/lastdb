use super::*;

/// Operator-facing resident graph telemetry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResidentHealth {
    /// Approximate resident graph bytes charged against the resident budget.
    pub resident_bytes: u64,
    /// Number of charged resident entries.
    pub resident_entries: u64,
    /// Configured ResidentGraph byte budget (`LASTDB_RESIDENT_BYTES`).
    /// `0` means unbounded. This does not size the logical resident set.
    pub budget_bytes: u64,
    pub schema_hits: u64,
    pub schema_rehydrates: u64,
    pub molecule_hits: u64,
    pub molecule_rehydrates: u64,
    pub atom_hits: u64,
    pub atom_rehydrates: u64,
    pub file_blob_hits: u64,
    pub file_blob_rehydrates: u64,
    pub protein_hits: u64,
    pub protein_rehydrates: u64,
    pub persist_enqueued: u64,
    pub persist_flushed: u64,
    pub deferred_persist_failed: u64,
    /// Deferred durable persists (mode=write background tasks) that finished,
    /// success or failure — the denominator for `deferred_persist_us`.
    ///
    /// [`Availability::Unavailable`] means the daemon does not report the field,
    /// not that nothing has completed. Those two readings demand opposite
    /// responses: under `LASTDB_RESIDENT_MODE=write` the client is acked before
    /// the durable put, so a genuine `Measured(0)` after hours of uptime says no
    /// acked write has ever been made durable — the one reading that would
    /// justify emergency action on a primary. A daemon staged behind the CLI
    /// produced a false zero on the primary on 2026-08-06 while the deferred
    /// path was in fact healthy (historical mislabel #3).
    #[serde(default = "default_deferred_persist_completed_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_deferred_persist_completed")]
    pub deferred_persist_completed: Gauge,
    /// Cumulative wall-clock microseconds spent inside the deferred durable
    /// persist task across all completions. Divide by the measured
    /// `deferred_persist_completed` for an average; this is the cost
    /// `report_phase_totals`'s `apply` residual cannot see because the task
    /// runs off the request's task-local scope after the client is acked.
    ///
    /// Unavailable when the daemon predates the counter — see
    /// [`Self::deferred_persist_completed`].
    #[serde(default = "default_deferred_persist_us_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_deferred_persist_us")]
    pub deferred_persist_us: Gauge,
    pub evicted: u64,
    pub evict_refused_dirty: u64,
    #[serde(default)]
    pub key_set_hits: u64,
    /// `key_set_overlays + key_set_unknowns`. Kept so existing readers of the
    /// wire keep working; prefer the two components below.
    #[serde(default)]
    pub key_set_misses: u64,
    #[serde(default)]
    pub key_set_overlays: u64,
    #[serde(default)]
    pub key_set_unknowns: u64,
    #[serde(default)]
    pub key_set_demotes: u64,
    /// Complete partition intervals certified from required reads.
    #[serde(default)]
    pub key_set_marked_complete: u64,
    /// Current FIFO depth across schema persist lanes.
    #[serde(default)]
    pub persist_lane_depth: u64,
    /// Age of the oldest queued persist envelope, milliseconds.
    #[serde(default)]
    pub persist_lane_oldest_age_ms: u64,
    /// Persist-lane write failures.
    #[serde(default)]
    pub persist_lane_failures: u64,
    /// Reserved resident-minus-durable revision lag gauge. It is zero.
    #[serde(default)]
    pub resident_minus_durable_revision: u64,
    /// Used logical records currently in the logical resident set.
    #[serde(default = "default_keys_instant_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_keys_instant")]
    pub resident_key_count: Gauge,
    /// Used records a call still holds.
    #[serde(default = "default_keys_instant_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_keys_instant")]
    pub resident_held_keys: Gauge,
    /// Used records with an uncovered durability token.
    #[serde(default = "default_keys_instant_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_keys_instant")]
    pub resident_dirty_keys: Gauge,
    /// Used-record budget (`RESIDENT_KEY_CAP`, 10000).
    #[serde(default = "default_keys_instant_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_keys_instant")]
    pub resident_key_budget: Gauge,
    /// LRU purge removals of used records since process start.
    #[serde(default = "default_keys_lifetime_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_keys_lifetime")]
    pub resident_purged_keys: Gauge,
    /// Bytes in the loader pin table. Measurement, not a cap.
    #[serde(default = "default_bytes_instant_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_bytes_instant")]
    pub loader_pin_bytes: Gauge,
    /// Hash groups the loader has open right now. Measurement, not a cap.
    #[serde(default = "default_groups_instant_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_groups_instant")]
    pub loader_groups_open_now: Gauge,
    /// Point reads the logical set served from memory.
    #[serde(default = "default_events_lifetime_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_events_lifetime")]
    pub resident_point_hits: Gauge,
    /// Point reads that went to the loader.
    #[serde(default = "default_events_lifetime_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_events_lifetime")]
    pub resident_point_misses: Gauge,
    /// Purge passes that removed at least one used record.
    #[serde(default = "default_events_lifetime_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_events_lifetime")]
    pub resident_purge_runs: Gauge,
    /// Purge passes that ended over budget (all remaining held or dirty).
    #[serde(default = "default_events_lifetime_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_events_lifetime")]
    pub resident_over_cap_stalls: Gauge,
    /// Used records above the budget right now.
    #[serde(default = "default_keys_instant_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_keys_instant")]
    pub resident_over_cap_keys: Gauge,
    /// Loader point loads since process start.
    #[serde(default = "default_events_lifetime_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_events_lifetime")]
    pub loader_loads: Gauge,
    /// Cumulative microseconds inside loader point loads.
    #[serde(default = "default_micros_lifetime_gauge")]
    #[serde(serialize_with = "serialize_gauge_wire")]
    #[serde(deserialize_with = "deserialize_micros_lifetime")]
    pub loader_load_us: Gauge,
}

impl Default for ResidentHealth {
    fn default() -> Self {
        Self {
            resident_bytes: 0,
            resident_entries: 0,
            budget_bytes: 0,
            schema_hits: 0,
            schema_rehydrates: 0,
            molecule_hits: 0,
            molecule_rehydrates: 0,
            atom_hits: 0,
            atom_rehydrates: 0,
            file_blob_hits: 0,
            file_blob_rehydrates: 0,
            protein_hits: 0,
            protein_rehydrates: 0,
            persist_enqueued: 0,
            persist_flushed: 0,
            deferred_persist_failed: 0,
            deferred_persist_completed: default_deferred_persist_completed_gauge(),
            deferred_persist_us: default_deferred_persist_us_gauge(),
            evicted: 0,
            evict_refused_dirty: 0,
            key_set_hits: 0,
            key_set_misses: 0,
            key_set_overlays: 0,
            key_set_unknowns: 0,
            key_set_demotes: 0,
            key_set_marked_complete: 0,
            persist_lane_depth: 0,
            persist_lane_oldest_age_ms: 0,
            persist_lane_failures: 0,
            resident_minus_durable_revision: 0,
            resident_key_count: default_keys_instant_gauge(),
            resident_held_keys: default_keys_instant_gauge(),
            resident_dirty_keys: default_keys_instant_gauge(),
            resident_key_budget: default_keys_instant_gauge(),
            resident_purged_keys: default_keys_lifetime_gauge(),
            loader_pin_bytes: default_bytes_instant_gauge(),
            loader_groups_open_now: default_groups_instant_gauge(),
            resident_point_hits: default_events_lifetime_gauge(),
            resident_point_misses: default_events_lifetime_gauge(),
            resident_purge_runs: default_events_lifetime_gauge(),
            resident_over_cap_stalls: default_events_lifetime_gauge(),
            resident_over_cap_keys: default_keys_instant_gauge(),
            loader_loads: default_events_lifetime_gauge(),
            loader_load_us: default_micros_lifetime_gauge(),
        }
    }
}

impl ResidentHealth {
    pub(super) fn from_graph(graph: &fold_db::ResidentGraph) -> Self {
        let s = graph.metrics().snapshot();
        Self::from_snapshot(
            s,
            graph.resident_bytes(),
            graph.resident_entries() as u64,
            graph.budget_bytes(),
        )
    }

    pub(crate) fn from_snapshot(
        s: fold_db::resident::ResidentMetricsSnapshot,
        resident_bytes: u64,
        resident_entries: u64,
        budget_bytes: u64,
    ) -> Self {
        Self {
            resident_bytes,
            resident_entries,
            budget_bytes,
            schema_hits: s.schema_hit,
            schema_rehydrates: s.schema_rehydrate,
            molecule_hits: s.molecule_hit,
            molecule_rehydrates: s.molecule_rehydrate,
            atom_hits: s.atom_hit,
            atom_rehydrates: s.atom_rehydrate,
            file_blob_hits: s.file_blob_hit,
            file_blob_rehydrates: s.file_blob_rehydrate,
            protein_hits: s.protein_hit,
            protein_rehydrates: s.protein_rehydrate,
            persist_enqueued: s.persist_enqueued,
            persist_flushed: s.persist_flushed,
            deferred_persist_failed: s.deferred_persist_failed,
            // This process measured them, so they are always Measured here. The
            // Unavailable case is produced only by deserializing an older
            // daemon's payload, never by a live snapshot.
            deferred_persist_completed: gauge_measured(
                s.deferred_persist_completed,
                Unit::Events,
                GAUGE_PROCESS_LIFETIME,
            ),
            deferred_persist_us: gauge_measured(
                s.deferred_persist_us,
                Unit::Micros,
                GAUGE_PROCESS_LIFETIME,
            ),
            evicted: s.evicted,
            evict_refused_dirty: s.evict_refused_dirty,
            key_set_hits: s.key_set_hit,
            key_set_misses: s.key_set_miss,
            key_set_overlays: s.key_set_overlay,
            key_set_unknowns: s.key_set_unknown,
            key_set_demotes: s.key_set_demote,
            key_set_marked_complete: s.key_set_marked_complete,
            persist_lane_depth: s.persist_lane_depth,
            persist_lane_oldest_age_ms: s.persist_lane_oldest_age_ms,
            persist_lane_failures: s.persist_lane_failures,
            resident_minus_durable_revision: s.resident_minus_durable_revision,
            resident_key_count: g_instant(s.resident_key_count, Unit::Keys),
            resident_held_keys: g_instant(s.resident_held_keys, Unit::Keys),
            resident_dirty_keys: g_instant(s.resident_dirty_keys, Unit::Keys),
            resident_key_budget: g_instant(s.resident_key_budget, Unit::Keys),
            resident_purged_keys: g_lifetime(s.resident_purged_keys, Unit::Keys),
            loader_pin_bytes: g_instant(s.loader_pin_bytes, Unit::Bytes),
            loader_groups_open_now: g_instant(s.loader_groups_open_now, Unit::Named("group(s)")),
            resident_point_hits: g_lifetime(s.resident_point_hits, Unit::Events),
            resident_point_misses: g_lifetime(s.resident_point_misses, Unit::Events),
            resident_purge_runs: g_lifetime(s.resident_purge_runs, Unit::Events),
            resident_over_cap_stalls: g_lifetime(s.resident_over_cap_stalls, Unit::Events),
            resident_over_cap_keys: g_instant(s.resident_over_cap_keys, Unit::Keys),
            loader_loads: g_lifetime(s.loader_loads, Unit::Events),
            loader_load_us: g_lifetime(s.loader_load_us, Unit::Micros),
        }
    }
}

/// Read-path integrity counters — the cost of degrading gracefully.
///
/// A tip pointing at an unresolvable atom used to fail the WHOLE hash-key
/// partition with a 400 (79,519 healthy rows were unreachable behind 23 bad
/// ones on the primary). The fix was to skip the broken row and serve the
/// rest, which is right — but it turns a loud failure into a query that
/// returns `200` with silently fewer rows than the data claims.
///
/// So the graceful path has to increment something an operator can read.
/// Otherwise "resilient" and "silently wrong" are the same observation, and
/// the only remaining evidence is a `WARN` line in a log that also carries
/// every mutation at `INFO`.
/// Wire-transparent integrity gauges (bare u64 / null on the wire).
///
/// Edges are typed as [`Unit::Edges`] so a renderer cannot print them as
/// row(s) (historical mislabel #1). Missing fields deserialize to
/// [`Availability::Unavailable`], never measured zero (#3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntegrityHealth {
    /// Node-lifetime count of query rows dropped because the tip's atom did
    /// not resolve. Non-zero means reads are returning short — see the
    /// daemon log's `Skipping unresolved atom ref` warnings for the keys.
    ///
    /// This is **exposure**, not damage size: it counts drop *events*, so one
    /// broken row on a partition a hot client polls will run into the
    /// thousands on its own. Read it with `unresolved_atom_distinct`.
    pub unresolved_atom_skips: Gauge,
    /// Distinct `(atom_uuid, key)` edges behind those events — one per broken
    /// **field** tip, not one per row.
    ///
    /// A record has many fields and each carries its own tip -> atom edge, so
    /// one unreadable row with five dangling field tips is five edges. Read
    /// `unresolved_atom_rows` for how many rows are affected.
    pub unresolved_atom_distinct: Gauge,
    /// Distinct keys among those edges — how many row reads come back short.
    ///
    /// Unavailable means the daemon does not report this field. Rendering that
    /// as `0` would make an older daemon look like it measured "zero affected
    /// rows" while it only measured dangling field-tip edges.
    pub unresolved_atom_rows: Gauge,
    /// True when the node stopped retaining new identities at the cap, so both
    /// distinct figures are floors rather than totals.
    pub unresolved_distinct_capped: bool,
}

impl Default for IntegrityHealth {
    fn default() -> Self {
        Self {
            unresolved_atom_skips: Gauge::field_not_served(Unit::Events, GAUGE_PROCESS_LIFETIME),
            unresolved_atom_distinct: Gauge::field_not_served(Unit::Edges, GAUGE_PROCESS_LIFETIME),
            unresolved_atom_rows: Gauge::field_not_served(Unit::Rows, GAUGE_PROCESS_LIFETIME),
            unresolved_distinct_capped: false,
        }
    }
}

impl IntegrityHealth {
    pub fn measured(skips: u64, edges: u64, rows: Option<u64>, capped: bool) -> Self {
        Self {
            unresolved_atom_skips: gauge_measured(skips, Unit::Events, GAUGE_PROCESS_LIFETIME),
            unresolved_atom_distinct: gauge_measured(edges, Unit::Edges, GAUGE_PROCESS_LIFETIME),
            unresolved_atom_rows: gauge_from_wire(rows, Unit::Rows, GAUGE_PROCESS_LIFETIME),
            unresolved_distinct_capped: capped,
        }
    }
}

impl Serialize for IntegrityHealth {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("IntegrityHealth", 4)?;
        s.serialize_field(
            "unresolved_atom_skips",
            &wire_u64(&self.unresolved_atom_skips),
        )?;
        s.serialize_field(
            "unresolved_atom_distinct",
            &wire_u64(&self.unresolved_atom_distinct),
        )?;
        s.serialize_field(
            "unresolved_atom_rows",
            &wire_u64(&self.unresolved_atom_rows),
        )?;
        s.serialize_field(
            "unresolved_distinct_capped",
            &self.unresolved_distinct_capped,
        )?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for IntegrityHealth {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(default)]
            unresolved_atom_skips: Option<u64>,
            #[serde(default)]
            unresolved_atom_distinct: Option<u64>,
            #[serde(default)]
            unresolved_atom_rows: Option<u64>,
            #[serde(default)]
            unresolved_distinct_capped: bool,
        }
        let raw = Raw::deserialize(deserializer)?;
        Ok(Self {
            unresolved_atom_skips: gauge_from_wire(
                raw.unresolved_atom_skips,
                Unit::Events,
                GAUGE_PROCESS_LIFETIME,
            ),
            unresolved_atom_distinct: gauge_from_wire(
                raw.unresolved_atom_distinct,
                Unit::Edges,
                GAUGE_PROCESS_LIFETIME,
            ),
            unresolved_atom_rows: gauge_from_wire(
                raw.unresolved_atom_rows,
                Unit::Rows,
                GAUGE_PROCESS_LIFETIME,
            ),
            unresolved_distinct_capped: raw.unresolved_distinct_capped,
        })
    }
}

// lint:file-size-ok moved verbatim from self_metrics.rs; cohesive unit, split further in a later pass
