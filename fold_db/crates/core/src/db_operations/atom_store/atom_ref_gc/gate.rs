//! The per-atom grace and audit gate that the cap-armed delete pass shares with
//! the reaper, and the batch reads it needs.

use super::*;

impl AtomGcCandidate {
    /// True when the verdict on this row was recorded by the audit named
    /// `audit_id`. An older lap's verdict says nothing about references that
    /// appeared since.
    pub(super) fn audit_is_from(&self, audit_id: &str) -> bool {
        self.audit
            .as_ref()
            .is_some_and(|audit| audit.audit_id == audit_id)
    }

    /// Whether a cap-armed delete may take this atom: the zero epoch is older
    /// than `grace_window` and the audit named `audit_id` cleared that epoch.
    #[must_use]
    pub fn grace_gate_verdict(
        &self,
        audit_id: &str,
        now_unix_nanos: u64,
        grace_window: Duration,
    ) -> GraceGateVerdict {
        if now_unix_nanos < self.eligible_at_unix_nanos(grace_window) {
            GraceGateVerdict::WithinGrace
        } else if self.audit_is_from(audit_id) && self.audit_clears_delete() {
            GraceGateVerdict::Clear
        } else {
            GraceGateVerdict::AuditNotClear
        }
    }
}

/// Outcome of the per-atom grace and audit gate (see [`AtomGcCandidate::grace_gate_verdict`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraceGateVerdict {
    Clear,
    WithinGrace,
    AuditNotClear,
}

impl AtomStore {
    /// Read the candidate rows for many atoms in one keyed batch.
    pub(crate) async fn load_atom_gc_candidates(
        &self,
        atom_uuids: &[String],
        storage_prefix: Option<&str>,
    ) -> Result<Vec<Option<AtomGcCandidate>>, SchemaError> {
        let keys: Vec<String> = atom_uuids
            .iter()
            .map(|uuid| atom_gc_candidate_key(uuid, storage_prefix))
            .collect();
        self.main_store
            .get_items::<AtomGcCandidate>(&keys)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("load atom GC candidates in batch: {error}"))
            })
    }

    /// Storage keys that must leave with an atom the delete pass removes: the
    /// candidate row, its queue row and the live-count row.
    pub(crate) fn atom_gc_delete_pass_keys(
        candidate: &AtomGcCandidate,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<String>, SchemaError> {
        Ok(vec![
            atom_gc_candidate_key(&candidate.atom_uuid, storage_prefix),
            candidate.queue_key.clone(),
            super::super::atom_ref_edges::atom_live_ref_count_key(
                &candidate.atom_uuid,
                storage_prefix,
            )?,
        ])
    }
}
