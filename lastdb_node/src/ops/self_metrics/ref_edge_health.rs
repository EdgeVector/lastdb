use super::*;

/// Molecule binding record gauge. The molecule plane keeps its binding records,
/// which a counter in the same batch decides; the bytes gauge sizes them. The
/// atom plane and the blob plane have no edges: each has counters, and
/// `lastdb status` shows the planner reports on their own lines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AtomRefEdgeHealth {
    pub molecule_bytes: Gauge,
}

impl Default for AtomRefEdgeHealth {
    fn default() -> Self {
        Self {
            molecule_bytes: Gauge::field_not_served(Unit::Bytes, GAUGE_INSTANT),
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
    }
}

/// Refresh the edge health on the background owner task. The gauge reads file
/// metadata only; no completeness marker is read, because a home holds none.
pub(crate) fn refresh_atom_ref_edge_health(host: &Host) {
    host.self_metrics
        .cache_atom_ref_edge_health(configured_atom_ref_edge_health(host));
}

impl Serialize for AtomRefEdgeHealth {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("AtomRefEdgeHealth", 1)?;
        s.serialize_field("molecule_bytes", &wire_u64(&self.molecule_bytes))?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for AtomRefEdgeHealth {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(default)]
            molecule_bytes: Option<u64>,
        }
        let raw = Raw::deserialize(deserializer)?;
        Ok(Self {
            molecule_bytes: gauge_from_wire(raw.molecule_bytes, Unit::Bytes, GAUGE_INSTANT),
        })
    }
}
