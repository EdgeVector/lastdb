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
use super::judge::{judge, Refusal, Verdict};
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
    /// Check the scope that a verb names. The only scopes a verb may name are the
    /// personal scope (no value) and a database scope: exactly 64 lowercase hex
    /// characters. A mistyped scope reads an empty range and looks like a clean
    /// scope, so the verbs that take a scope check it before they read. The check is
    /// narrower than the key validation of the storage layer on purpose: a verb
    /// never needs a `cloud:` or `from:` scope or a reserved name.
    pub fn validate_scope(scope: Option<&str>) -> Result<(), SchemaError> {
        let Some(scope) = scope else {
            return Ok(());
        };
        let lower_hex = scope.len() == 64
            && scope
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'));
        if lower_hex {
            return Ok(());
        }
        let shown: String = scope.chars().take(80).collect();
        Err(SchemaError::InvalidData(format!(
            "invalid scope {shown:?}: a scope is the personal scope (leave the value out) \
             or a database scope of exactly 64 lowercase hex characters"
        )))
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
                let mut verdict = judge(&self.read_molecule_reads(&candidate).await?);
                if verdict == Verdict::Absent && self.tip_present(&candidate).await? {
                    // Tip records and no header: the molecule has no counters.
                    verdict = Verdict::Refused(Refusal::CountersUnset);
                }
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
    /// The molecules of `molecules` that the drain still removes in `scope`: each
    /// has a candidate row and reads dead. An install of a schema that binds such a
    /// molecule again would stop the drain with part of the keys erased, so the
    /// install is refused (`SchemaError::SchemaBeingDropped`). The caller holds the
    /// write gate, so a batch of the drain cannot run between this read and the
    /// bind. A refused molecule (a counter fault) is not on the list: the drain does
    /// not remove it. One bounded range read for each molecule, all at once.
    pub(crate) async fn molecules_draining(
        &self,
        scope: Option<&str>,
        molecules: &[String],
    ) -> Result<Vec<String>, SchemaError> {
        let checks = molecules.iter().map(|molecule| async move {
            let candidate = Candidate::new(scope, molecule);
            let (start, end) = crate::kind_partition::exact_prefix_bounds(&candidate.key());
            let mut walk = crate::db_operations::atom_store::RangeWalk::new(
                start.as_bytes(),
                end.as_bytes(),
                1,
            );
            if self.walk_step(&mut walk).await?.rows.is_empty() {
                return Ok::<bool, SchemaError>(false);
            }
            let reads = self.read_molecule_reads(&candidate).await?;
            Ok(matches!(judge(&reads), Verdict::Dead { .. }))
        });
        let dead = futures::future::try_join_all(checks).await?;
        Ok(molecules
            .iter()
            .zip(dead)
            .filter(|(_, dead)| *dead)
            .map(|(molecule, _)| molecule.clone())
            .collect())
    }

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
