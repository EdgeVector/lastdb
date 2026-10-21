use super::*;

/// Molecule binding record gauge. The molecule plane keeps its binding records,
/// which a counter in the same batch decides; the bytes gauge sizes them. The
/// atom plane and the blob plane have no such records: each has counters, and
/// `lastdb status` shows the planner reports on their own lines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoleculeEdgeHealth {
    pub bytes: Gauge,
}

impl Default for MoleculeEdgeHealth {
    fn default() -> Self {
        Self {
            bytes: Gauge::field_not_served(Unit::Bytes, GAUGE_INSTANT),
        }
    }
}

/// Build a status-safe view from filesystem metadata. This function never
/// opens a LastStore group, and it reads no record.
pub(super) fn configured_molecule_edge_health(host: &Host) -> MoleculeEdgeHealth {
    let namespaced_store = host.db.db_ops().namespaced_store();
    MoleculeEdgeHealth {
        bytes: gauge_from_wire(
            namespaced_store.collection_disk_bytes("molecule_ref_edges"),
            Unit::Bytes,
            GAUGE_INSTANT,
        ),
    }
}

impl Serialize for MoleculeEdgeHealth {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("MoleculeEdgeHealth", 1)?;
        s.serialize_field("bytes", &wire_u64(&self.bytes))?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for MoleculeEdgeHealth {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(default)]
            bytes: Option<u64>,
        }
        let raw = Raw::deserialize(deserializer)?;
        Ok(Self {
            bytes: gauge_from_wire(raw.bytes, Unit::Bytes, GAUGE_INSTANT),
        })
    }
}
