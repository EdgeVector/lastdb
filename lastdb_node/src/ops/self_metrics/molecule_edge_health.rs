use super::*;

gauge_health! {
    /// Molecule binding record gauge. The molecule plane keeps its binding records,
    /// which a counter in the same batch decides; the bytes gauge sizes them. The
    /// atom plane and the blob plane have no such records: each has counters, and
    /// `lastdb status` shows the planner reports on their own lines.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct MoleculeEdgeHealth {
        pub bytes: (Unit::Bytes, GAUGE_INSTANT),
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
