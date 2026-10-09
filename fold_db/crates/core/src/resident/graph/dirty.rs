//! Dirty-set queries for the resident graph.

use super::*;

impl ResidentGraph {
    pub(super) fn mark_dirty(&self, key: DirtyKey) {
        let mut dirty = self.dirty.write().expect("resident dirty lock");
        if dirty.insert(key) {
            self.metrics.record_persist_enqueued();
        }
    }

    pub fn is_dirty(&self, key: &DirtyKey) -> bool {
        self.dirty
            .read()
            .expect("resident dirty lock")
            .contains(key)
    }

    pub fn dirty_count(&self) -> usize {
        self.dirty.read().expect("resident dirty lock").len()
    }

    /// Whether this molecule has any dirty overlay (tip, key-index, tombstone).
    /// Overlay visibility is per-molecule, not process-global dirty_count.
    pub fn molecule_has_dirty(&self, molecule_uuid: &str) -> bool {
        self.dirty
            .read()
            .expect("resident dirty lock")
            .iter()
            .any(|key| match key {
                DirtyKey::MoleculeTip {
                    molecule_uuid: m, ..
                }
                | DirtyKey::MoleculeKeyIndex {
                    molecule_uuid: m, ..
                }
                | DirtyKey::MoleculeKeyTombstone {
                    molecule_uuid: m, ..
                } => m == molecule_uuid,
                _ => false,
            })
    }

    /// Snapshot the resident tips for one molecule while persistence drains.
    ///
    /// Point reads can resolve a known slot directly from [`Self::resolve_tip`],
    /// but enumeration first discovers its key set from durable `mk:` records.
    /// During deferred persistence that durable set is necessarily behind T0.
    /// A batch's tips become clean one by one. Restricting the overlay to tips
    /// whose individual dirty bit is still set creates a mixed-generation
    /// window: an already-persisted primary tip can disappear from one field's
    /// durable scan while a secondary field is still draining. Callers use
    /// this snapshot only while the graph has pending dirty state, and the
    /// resident budget bounds the set independently of molecule size.
    pub fn tips_for_molecule(&self, molecule_uuid: &str) -> Vec<ResidentTip> {
        let tips = self.tips.read().expect("tips lock");
        let mut matches: Vec<_> = tips
            .values()
            .filter(|tip| tip.molecule_uuid == molecule_uuid)
            .cloned()
            .collect();

        matches.sort_by(|a, b| (&a.hash, &a.range).cmp(&(&b.hash, &b.range)));
        matches
    }

    // ── Schema ──────────────────────────────────────────────────────────
}
