//! Name a molecule to the drain by hand.
//!
//! The candidate rows come from the unbind that takes `bindings` to zero. A
//! molecule that lost its last binding before this step existed, or whose
//! candidate row was lost, has none. An operator names it here. The call reads
//! the molecule inside the write gate, writes a candidate row only for a molecule
//! that the drain may work on or must report (dead or refused), and returns the
//! verdict. It deletes nothing: the drain task does that, batch by batch, with
//! the same checks.

use super::candidate::Candidate;
use super::judge::{judge, Verdict};
use crate::db_operations::atom_store::counter::TipChangeSet;
use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;
use serde::{Deserialize, Serialize};

/// What the drain thinks of one molecule now.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoleculeDrainVerdict {
    pub molecule: String,
    /// The storage scope, empty for the personal scope.
    pub scope: String,
    /// `dead`, `bound`, `refused` or `absent`.
    pub state: String,
    pub slots: u64,
    pub records: u64,
    /// Why the drain refuses the molecule, when the state is `refused`.
    pub refusal: Option<String>,
    /// A candidate row was written, so the drain task works on the molecule.
    pub queued: bool,
}

impl AtomStore {
    /// Refuse a storage scope prefix that no key can carry. A mistyped scope
    /// reads an empty range and looks like a clean scope, so the verbs that take
    /// a scope check it before they read.
    pub fn validate_scope(scope: Option<&str>) -> Result<(), SchemaError> {
        match scope {
            Some(scope) => crate::storage::current_keys::validate_scope(scope)
                .map_err(|error| SchemaError::InvalidData(format!("invalid scope: {error}"))),
            None => Ok(()),
        }
    }

    /// Read the molecule inside the write gate and queue it for the drain when it
    /// is dead or refused. Wakes the drain task after the batch commits.
    pub async fn molecule_drain_enqueue(
        &self,
        scope: Option<&str>,
        molecule: &str,
    ) -> Result<MoleculeDrainVerdict, SchemaError> {
        if !crate::atom::molecule_uuid::is_canonical_molecule_uuid(molecule) {
            return Err(SchemaError::InvalidData(
                "molecule must be a canonical molecule uuid".into(),
            ));
        }
        Self::validate_scope(scope)?;
        let candidate = Candidate::new(scope, molecule);
        let verdict = self
            .in_write_scope(async {
                let verdict = judge(&self.read_molecule_reads(&candidate).await?);
                let queue = matches!(verdict, Verdict::Dead { .. } | Verdict::Refused(_));
                if queue {
                    self.commit_tip_changes(TipChangeSet {
                        rows: vec![candidate.row_json()],
                        ..TipChangeSet::default()
                    })
                    .await?;
                    self.wake_drain_after_commit();
                }
                Ok(report(&candidate, verdict, queue))
            })
            .await?;
        Ok(verdict)
    }
}

impl AtomStore {
    /// The candidate rows now, counted up to `cap` with bounded range reads (a
    /// deleted key does not make the read fail). The second value is true when the
    /// count stopped at the cap.
    pub async fn molecule_drain_candidates(&self, cap: usize) -> Result<(u64, bool), SchemaError> {
        let (start, end) = super::candidate::list_bounds();
        let mut walk = crate::db_operations::atom_store::RangeWalk::new(&start, &end, 256);
        let mut count = 0u64;
        while !walk.done() {
            let step = self.walk_step(&mut walk).await?;
            count += step.rows.len() as u64;
            if count >= cap as u64 {
                return Ok((count, true));
            }
        }
        Ok((count, false))
    }
}

fn report(candidate: &Candidate, verdict: Verdict, queued: bool) -> MoleculeDrainVerdict {
    let (state, slots, records, refusal) = match verdict {
        Verdict::Dead { slots, records } => ("dead", slots, records, None),
        Verdict::Bound => ("bound", 0, 0, None),
        Verdict::Refused(why) => ("refused", 0, 0, Some(why.name().to_string())),
        Verdict::Absent => ("absent", 0, 0, None),
    };
    MoleculeDrainVerdict {
        molecule: candidate.molecule.clone(),
        scope: candidate.scope.clone().unwrap_or_default(),
        state: state.to_string(),
        slots,
        records,
        refusal,
        queued,
    }
}
