use super::*;

/// Molecule reverse-edge gauges. A later step replaces the plane with counters.
/// The atom plane and the blob plane have no edges: each has counters
/// (`lastdb status` shows the planner reports on their own lines).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AtomRefEdgeHealth {
    pub molecule_bytes: Gauge,
    pub molecule_complete: Option<bool>,
    pub molecule_phase: String,
}

impl Default for AtomRefEdgeHealth {
    fn default() -> Self {
        Self {
            molecule_bytes: Gauge::field_not_served(Unit::Bytes, GAUGE_INSTANT),
            molecule_complete: None,
            molecule_phase: "unsupported".to_string(),
        }
    }
}

/// Build a status-safe view from filesystem metadata. This function never
/// opens a LastStore group.
pub(super) fn configured_atom_ref_edge_health(host: &Host) -> AtomRefEdgeHealth {
    let namespaced_store = host.db.db_ops().namespaced_store();
    AtomRefEdgeHealth {
        molecule_bytes: gauge_from_wire(
            namespaced_store.collection_disk_bytes("molecule_ref_edges"),
            Unit::Bytes,
            GAUGE_INSTANT,
        ),
        ..AtomRefEdgeHealth::default()
    }
}

/// Refresh the edge health on the background owner task.
///
/// Durable completeness markers can populate LastStore's warm set. They
/// belong here, never on `/api/status`.
pub(crate) async fn refresh_atom_ref_edge_health(host: &Host) {
    let atoms = host.db.db_ops().atoms();
    let mut health = configured_atom_ref_edge_health(host);
    health.molecule_complete = atoms.molecule_ref_edges_complete(None).await.ok();
    health.molecule_phase = phase_of(health.molecule_complete);
    host.self_metrics.cache_atom_ref_edge_health(health);
}

fn phase_of(complete: Option<bool>) -> String {
    match complete {
        Some(true) => "complete",
        Some(false) => "bootstrap_required",
        None => "read_error",
    }
    .to_string()
}

impl Serialize for AtomRefEdgeHealth {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("AtomRefEdgeHealth", 3)?;
        s.serialize_field("molecule_bytes", &wire_u64(&self.molecule_bytes))?;
        s.serialize_field("molecule_complete", &self.molecule_complete)?;
        s.serialize_field("molecule_phase", &self.molecule_phase)?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for AtomRefEdgeHealth {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(default)]
            molecule_bytes: Option<u64>,
            #[serde(default)]
            molecule_complete: Option<bool>,
            #[serde(default = "default_derived_edge_phase")]
            molecule_phase: String,
        }
        let raw = Raw::deserialize(deserializer)?;
        Ok(Self {
            molecule_bytes: gauge_from_wire(raw.molecule_bytes, Unit::Bytes, GAUGE_INSTANT),
            molecule_complete: raw.molecule_complete,
            molecule_phase: raw.molecule_phase,
        })
    }
}

pub(super) fn default_derived_edge_phase() -> String {
    "unsupported".to_string()
}
