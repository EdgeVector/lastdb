use super::*;

/// Molecule and blob reverse-edge gauges. A later step replaces the blob plane
/// with its own counter. The molecule plane keeps its binding records, which a
/// counter in the same batch decides; the bytes gauge sizes them. The atom plane
/// has no edges: an atom has one counter (`lastdb status` shows the planner
/// report on its own line).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AtomRefEdgeHealth {
    pub molecule_bytes: Gauge,
    pub blob_bytes: Gauge,
    pub blob_complete: Option<bool>,
    pub blob_phase: String,
}

impl Default for AtomRefEdgeHealth {
    fn default() -> Self {
        Self {
            molecule_bytes: Gauge::field_not_served(Unit::Bytes, GAUGE_INSTANT),
            blob_bytes: Gauge::field_not_served(Unit::Bytes, GAUGE_INSTANT),
            blob_complete: None,
            blob_phase: "unsupported".to_string(),
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
        blob_bytes: gauge_from_wire(
            namespaced_store.collection_disk_bytes("blob_ref_edges"),
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
    let namespaced_store = host.db.db_ops().namespaced_store();
    let atoms = host.db.db_ops().atoms();
    let mut health = configured_atom_ref_edge_health(host);
    health.blob_bytes = gauge_from_wire(
        namespaced_store.collection_disk_bytes("blob_ref_edges"),
        Unit::Bytes,
        GAUGE_INSTANT,
    );
    health.blob_complete = atoms.blob_ref_edges_complete(None).await.ok();
    health.blob_phase = phase_of(health.blob_complete);
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
        let mut s = serializer.serialize_struct("AtomRefEdgeHealth", 4)?;
        s.serialize_field("molecule_bytes", &wire_u64(&self.molecule_bytes))?;
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
            molecule_bytes: Option<u64>,
            #[serde(default)]
            blob_bytes: Option<u64>,
            #[serde(default)]
            blob_complete: Option<bool>,
            #[serde(default = "default_derived_edge_phase")]
            blob_phase: String,
        }
        let raw = Raw::deserialize(deserializer)?;
        Ok(Self {
            molecule_bytes: gauge_from_wire(raw.molecule_bytes, Unit::Bytes, GAUGE_INSTANT),
            blob_bytes: gauge_from_wire(raw.blob_bytes, Unit::Bytes, GAUGE_INSTANT),
            blob_complete: raw.blob_complete,
            blob_phase: raw.blob_phase,
        })
    }
}

pub(super) fn default_derived_edge_phase() -> String {
    "unsupported".to_string()
}
