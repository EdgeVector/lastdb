use super::*;

/// Operator view of how long molecule write gates are held.
///
/// `molecule_gate` in the request-phase table is **wait** time — how long
/// writers queued. This is **hold** time. The pair is what makes the number
/// actionable, because the same wait total has two causes with opposite fixes:
///
/// | wait | mean hold | reading |
/// |---|---|---|
/// | high | low | many writers on one key — spread the caller's key layout |
/// | high | high | something stalls inside the guarded region — write-path bug |
///
/// The guarded region includes `restore_missing_molecules`, which does storage
/// IO, so the second case is reachable on a node whose warm set is at its
/// ceiling.
/// Wire-transparent process-lifetime hold gauges (bare u64 / null on the wire).
///
/// Unit/window live in the Rust type so Display cannot claim an instant "now"
/// for a process total (historical mislabel #2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MoleculeGateHealth {
    /// Summed hold time over every released gate, microseconds.
    pub hold_total_us: Gauge,
    /// Released gate acquisitions — the denominator for a mean hold.
    pub hold_count: Gauge,
    /// Longest single hold, microseconds. This is the figure that catches a
    /// one-off multi-second stall that a mean would average away.
    pub hold_max_us: Gauge,
}

impl Default for MoleculeGateHealth {
    fn default() -> Self {
        Self {
            hold_total_us: Gauge::field_not_served(Unit::Micros, GAUGE_PROCESS_LIFETIME),
            hold_count: Gauge::field_not_served(Unit::Named("hold(s)"), GAUGE_PROCESS_LIFETIME),
            hold_max_us: Gauge::field_not_served(Unit::Micros, GAUGE_PROCESS_LIFETIME),
        }
    }
}

impl MoleculeGateHealth {
    pub fn measured(hold_total_us: u64, hold_count: u64, hold_max_us: u64) -> Self {
        Self {
            hold_total_us: gauge_measured(hold_total_us, Unit::Micros, GAUGE_PROCESS_LIFETIME),
            hold_count: gauge_measured(hold_count, Unit::Named("hold(s)"), GAUGE_PROCESS_LIFETIME),
            hold_max_us: gauge_measured(hold_max_us, Unit::Micros, GAUGE_PROCESS_LIFETIME),
        }
    }

    /// Mean hold in microseconds, or `None` when nothing has been released yet
    /// (or when the counters were not served by the daemon).
    #[must_use]
    pub fn mean_hold_us(&self) -> Option<u64> {
        match (self.hold_count.value, self.hold_total_us.value) {
            (Availability::Measured(c), Availability::Measured(t)) if c > 0 => Some(t / c),
            _ => None,
        }
    }
}

impl Serialize for MoleculeGateHealth {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("MoleculeGateHealth", 3)?;
        s.serialize_field("hold_total_us", &wire_u64(&self.hold_total_us))?;
        s.serialize_field("hold_count", &wire_u64(&self.hold_count))?;
        s.serialize_field("hold_max_us", &wire_u64(&self.hold_max_us))?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for MoleculeGateHealth {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(default)]
            hold_total_us: Option<u64>,
            #[serde(default)]
            hold_count: Option<u64>,
            #[serde(default)]
            hold_max_us: Option<u64>,
        }
        let raw = Raw::deserialize(deserializer)?;
        Ok(Self {
            hold_total_us: gauge_from_wire(raw.hold_total_us, Unit::Micros, GAUGE_PROCESS_LIFETIME),
            hold_count: gauge_from_wire(
                raw.hold_count,
                Unit::Named("hold(s)"),
                GAUGE_PROCESS_LIFETIME,
            ),
            hold_max_us: gauge_from_wire(raw.hold_max_us, Unit::Micros, GAUGE_PROCESS_LIFETIME),
        })
    }
}

/// Compact atom reverse-edge cutover gauges.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AtomRefEdgeHealth {
    pub v1_bytes: Gauge,
    pub v2_bytes: Gauge,
    pub active_edges: Gauge,
    pub inactive_keys: Gauge,
    pub bytes_per_edge: Gauge,
    pub projected_final_bytes: Gauge,
    pub phase: String,
    pub molecule_bytes: Gauge,
    pub molecule_complete: Option<bool>,
    pub molecule_phase: String,
    pub blob_bytes: Gauge,
    pub blob_complete: Option<bool>,
    pub blob_phase: String,
}

impl Default for AtomRefEdgeHealth {
    fn default() -> Self {
        Self {
            v1_bytes: Gauge::field_not_served(Unit::Bytes, GAUGE_INSTANT),
            v2_bytes: Gauge::field_not_served(Unit::Bytes, GAUGE_INSTANT),
            active_edges: Gauge::field_not_served(Unit::Edges, GAUGE_INSTANT),
            inactive_keys: Gauge::field_not_served(Unit::Keys, GAUGE_INSTANT),
            bytes_per_edge: Gauge::field_not_served(Unit::Bytes, GAUGE_INSTANT),
            projected_final_bytes: Gauge::field_not_served(Unit::Bytes, GAUGE_INSTANT),
            phase: "unsupported".to_string(),
            molecule_bytes: Gauge::field_not_served(Unit::Bytes, GAUGE_INSTANT),
            molecule_complete: None,
            molecule_phase: "unsupported".to_string(),
            blob_bytes: Gauge::field_not_served(Unit::Bytes, GAUGE_INSTANT),
            blob_complete: None,
            blob_phase: "unsupported".to_string(),
        }
    }
}

impl AtomRefEdgeHealth {
    pub(super) fn default_off(v1_bytes: Option<u64>, v2_bytes: u64) -> Self {
        Self {
            v1_bytes: gauge_from_wire(v1_bytes, Unit::Bytes, GAUGE_INSTANT),
            v2_bytes: g_instant(v2_bytes, Unit::Bytes),
            active_edges: Gauge::field_not_served(Unit::Edges, GAUGE_INSTANT),
            inactive_keys: Gauge::field_not_served(Unit::Keys, GAUGE_INSTANT),
            bytes_per_edge: Gauge::field_not_served(Unit::Bytes, GAUGE_INSTANT),
            projected_final_bytes: Gauge::field_not_served(Unit::Bytes, GAUGE_INSTANT),
            phase: "default_off".to_string(),
            molecule_bytes: Gauge::field_not_served(Unit::Bytes, GAUGE_INSTANT),
            molecule_complete: None,
            molecule_phase: "mixed_write".to_string(),
            blob_bytes: Gauge::field_not_served(Unit::Bytes, GAUGE_INSTANT),
            blob_complete: None,
            blob_phase: "mixed_write".to_string(),
        }
    }

    pub(super) fn dual_write(v1_bytes: Option<u64>, v2_bytes: u64) -> Self {
        Self {
            phase: "dual_write".to_string(),
            ..Self::default_off(v1_bytes, v2_bytes)
        }
    }

    pub(super) fn backfill(
        v1_bytes: Option<u64>,
        v2_bytes: u64,
        status: &fold_db::db_operations::atom_store::AtomRefBackfillStatus,
        history_complete: bool,
    ) -> Self {
        let active_edges = status.edges_written;
        let bytes_per_edge = v2_bytes.checked_div(active_edges).unwrap_or(0);
        let projected_final_bytes =
            crate::atom_ref_backfill::projected_atom_ref_v2_bytes(v2_bytes, active_edges);
        let phase = if projected_final_bytes > crate::atom_ref_backfill::atom_ref_v2_max_bytes() {
            "growth_blocked".to_string()
        } else {
            match status.phase {
                fold_db::db_operations::atom_store::AtomRefBackfillPhase::Backfill => {
                    "backfill".to_string()
                }
                fold_db::db_operations::atom_store::AtomRefBackfillPhase::Replay => {
                    "replay".to_string()
                }
                fold_db::db_operations::atom_store::AtomRefBackfillPhase::Complete => {
                    if history_complete {
                        "complete".to_string()
                    } else {
                        "molecule_audit".to_string()
                    }
                }
                fold_db::db_operations::atom_store::AtomRefBackfillPhase::Blocked => {
                    "blocked".to_string()
                }
            }
        };
        Self {
            v1_bytes: gauge_from_wire(v1_bytes, Unit::Bytes, GAUGE_INSTANT),
            v2_bytes: g_instant(v2_bytes, Unit::Bytes),
            active_edges: g_instant(active_edges, Unit::Edges),
            inactive_keys: g_instant(0, Unit::Keys),
            bytes_per_edge: g_instant(bytes_per_edge, Unit::Bytes),
            projected_final_bytes: g_instant(projected_final_bytes, Unit::Bytes),
            phase,
            molecule_bytes: Gauge::field_not_served(Unit::Bytes, GAUGE_INSTANT),
            molecule_complete: None,
            molecule_phase: "mixed_write".to_string(),
            blob_bytes: Gauge::field_not_served(Unit::Bytes, GAUGE_INSTANT),
            blob_complete: None,
            blob_phase: "mixed_write".to_string(),
        }
    }
}

/// Build a status-safe atom-ref view from configuration and filesystem
/// metadata. This function never opens a LastStore group.
pub(super) fn configured_atom_ref_edge_health(host: &Host) -> AtomRefEdgeHealth {
    let namespaced_store = host.db.db_ops().namespaced_store();
    let v1_bytes = namespaced_store.collection_disk_bytes("atom_ref_edges");
    let v2_bytes = namespaced_store
        .collection_disk_bytes("atom_ref_edges_v2")
        .unwrap_or(0);
    let atom_ref_store = host.db.db_ops().atoms();
    let dual_write = atom_ref_store.atom_ref_v2_dual_write_enabled();
    let reads = atom_ref_store.atom_ref_v2_reads_enabled();
    let only_writes = atom_ref_store.atom_ref_v2_only_writes_enabled();
    let mut health = if dual_write {
        AtomRefEdgeHealth::dual_write(v1_bytes, v2_bytes)
    } else {
        AtomRefEdgeHealth::default_off(v1_bytes, v2_bytes)
    };
    if reads {
        health.phase = if dual_write {
            if only_writes {
                "v2_only_writes"
            } else {
                "v2_reads"
            }
        } else {
            "read_misconfigured"
        }
        .to_string();
    } else if only_writes {
        health.phase = "write_misconfigured".to_string();
    }
    if crate::atom_ref_backfill::atom_ref_v1_drain_enabled() {
        health.phase = if crate::atom_ref_backfill::atom_ref_backfill_enabled() {
            "drain_misconfigured"
        } else {
            "v1_draining"
        }
        .to_string();
    }
    health.molecule_bytes = gauge_from_wire(
        namespaced_store.collection_disk_bytes("molecule_ref_edges"),
        Unit::Bytes,
        GAUGE_INSTANT,
    );
    health.blob_bytes = gauge_from_wire(
        namespaced_store.collection_disk_bytes("blob_ref_edges"),
        Unit::Bytes,
        GAUGE_INSTANT,
    );
    health
}

/// Refresh atom-ref health on the background owner task.
///
/// Durable backfill cursors and readiness markers can populate LastStore's
/// warm set. They belong here, never on `/api/status`.
pub(crate) async fn refresh_atom_ref_edge_health(host: &Host) {
    let namespaced_store = host.db.db_ops().namespaced_store();
    let v1_bytes = namespaced_store.collection_disk_bytes("atom_ref_edges");
    let v2_bytes = namespaced_store
        .collection_disk_bytes("atom_ref_edges_v2")
        .unwrap_or(0);
    let atom_ref_store = host.db.db_ops().atoms();
    let dual_write = atom_ref_store.atom_ref_v2_dual_write_enabled();
    let reads = atom_ref_store.atom_ref_v2_reads_enabled();
    let only_writes = atom_ref_store.atom_ref_v2_only_writes_enabled();
    let mut health = if !dual_write {
        AtomRefEdgeHealth::default_off(v1_bytes, v2_bytes)
    } else if crate::atom_ref_backfill::atom_ref_v2_backfill_enabled() {
        match atom_ref_store.atom_ref_v2_backfill_status(None).await {
            Ok(status) => AtomRefEdgeHealth::backfill(
                v1_bytes,
                v2_bytes,
                &status,
                atom_ref_store
                    .atom_ref_v2_history_complete(None)
                    .await
                    .unwrap_or(false),
            ),
            Err(_) => AtomRefEdgeHealth::dual_write(v1_bytes, v2_bytes),
        }
    } else {
        AtomRefEdgeHealth::dual_write(v1_bytes, v2_bytes)
    };
    if reads {
        health.phase = if dual_write {
            match atom_ref_store.atom_ref_v2_reads_ready(None).await {
                Ok(true) if only_writes => "v2_only_writes",
                Ok(true) => "v2_reads",
                Ok(false) => "read_blocked",
                Err(_) => "read_invalid",
            }
        } else {
            "read_misconfigured"
        }
        .to_string();
    } else if only_writes {
        health.phase = "write_misconfigured".to_string();
    }
    if crate::atom_ref_backfill::atom_ref_v1_drain_enabled() {
        health.phase = if crate::atom_ref_backfill::atom_ref_backfill_enabled() {
            "drain_misconfigured"
        } else {
            match atom_ref_store.atom_ref_v1_drain_status().await {
                Ok(status) if status.completed => "v1_drained",
                Ok(_) => "v1_draining",
                Err(_) => "drain_invalid",
            }
        }
        .to_string();
    }
    health.molecule_bytes = gauge_from_wire(
        namespaced_store.collection_disk_bytes("molecule_ref_edges"),
        Unit::Bytes,
        GAUGE_INSTANT,
    );
    health.molecule_complete = atom_ref_store.molecule_ref_edges_complete(None).await.ok();
    health.molecule_phase = match health.molecule_complete {
        Some(true) => "complete",
        Some(false) => "bootstrap_required",
        None => "read_error",
    }
    .to_string();
    health.blob_bytes = gauge_from_wire(
        namespaced_store.collection_disk_bytes("blob_ref_edges"),
        Unit::Bytes,
        GAUGE_INSTANT,
    );
    health.blob_complete = atom_ref_store.blob_ref_edges_complete(None).await.ok();
    health.blob_phase = match health.blob_complete {
        Some(true) => "complete",
        Some(false) => "bootstrap_required",
        None => "read_error",
    }
    .to_string();
    host.self_metrics.cache_atom_ref_edge_health(health);
}

impl Serialize for AtomRefEdgeHealth {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("AtomRefEdgeHealth", 13)?;
        s.serialize_field("v1_bytes", &wire_u64(&self.v1_bytes))?;
        s.serialize_field("v2_bytes", &wire_u64(&self.v2_bytes))?;
        s.serialize_field("active_edges", &wire_u64(&self.active_edges))?;
        s.serialize_field("inactive_keys", &wire_u64(&self.inactive_keys))?;
        s.serialize_field("bytes_per_edge", &wire_u64(&self.bytes_per_edge))?;
        s.serialize_field(
            "projected_final_bytes",
            &wire_u64(&self.projected_final_bytes),
        )?;
        s.serialize_field("phase", &self.phase)?;
        s.serialize_field("molecule_bytes", &wire_u64(&self.molecule_bytes))?;
        s.serialize_field("molecule_complete", &self.molecule_complete)?;
        s.serialize_field("molecule_phase", &self.molecule_phase)?;
        s.serialize_field("blob_bytes", &wire_u64(&self.blob_bytes))?;
        s.serialize_field("blob_complete", &self.blob_complete)?;
        s.serialize_field("blob_phase", &self.blob_phase)?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for AtomRefEdgeHealth {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(default)]
            v1_bytes: Option<u64>,
            #[serde(default)]
            v2_bytes: Option<u64>,
            #[serde(default)]
            active_edges: Option<u64>,
            #[serde(default)]
            inactive_keys: Option<u64>,
            #[serde(default)]
            bytes_per_edge: Option<u64>,
            #[serde(default)]
            projected_final_bytes: Option<u64>,
            #[serde(default = "default_atom_ref_edge_phase")]
            phase: String,
            #[serde(default)]
            molecule_bytes: Option<u64>,
            #[serde(default)]
            molecule_complete: Option<bool>,
            #[serde(default = "default_derived_edge_phase")]
            molecule_phase: String,
            #[serde(default)]
            blob_bytes: Option<u64>,
            #[serde(default)]
            blob_complete: Option<bool>,
            #[serde(default = "default_derived_edge_phase")]
            blob_phase: String,
        }
        let raw = Raw::deserialize(deserializer)?;
        Ok(Self {
            v1_bytes: gauge_from_wire(raw.v1_bytes, Unit::Bytes, GAUGE_INSTANT),
            v2_bytes: gauge_from_wire(raw.v2_bytes, Unit::Bytes, GAUGE_INSTANT),
            active_edges: gauge_from_wire(raw.active_edges, Unit::Edges, GAUGE_INSTANT),
            inactive_keys: gauge_from_wire(raw.inactive_keys, Unit::Keys, GAUGE_INSTANT),
            bytes_per_edge: gauge_from_wire(raw.bytes_per_edge, Unit::Bytes, GAUGE_INSTANT),
            projected_final_bytes: gauge_from_wire(
                raw.projected_final_bytes,
                Unit::Bytes,
                GAUGE_INSTANT,
            ),
            phase: raw.phase,
            molecule_bytes: gauge_from_wire(raw.molecule_bytes, Unit::Bytes, GAUGE_INSTANT),
            molecule_complete: raw.molecule_complete,
            molecule_phase: raw.molecule_phase,
            blob_bytes: gauge_from_wire(raw.blob_bytes, Unit::Bytes, GAUGE_INSTANT),
            blob_complete: raw.blob_complete,
            blob_phase: raw.blob_phase,
        })
    }
}

pub(super) fn default_atom_ref_edge_phase() -> String {
    "unsupported".to_string()
}

pub(super) fn default_derived_edge_phase() -> String {
    "unsupported".to_string()
}

// lint:file-size-ok moved verbatim from self_metrics.rs; cohesive unit, split further in a later pass
