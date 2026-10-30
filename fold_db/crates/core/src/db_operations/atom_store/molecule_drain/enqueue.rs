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
use super::family::{read_molecule_reads, tip_present};
use super::judge::{judge, Refusal, Verdict};
use super::MoleculeDrain;
use crate::db_operations::atom_store::counter::TipChangeSet;
use crate::db_operations::atom_store::{AtomStore, RangeWalk};
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

/// The molecules an install would bind that the drain still removes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DrainingMolecules {
    /// Molecules whose tips, rows or header the drain still erases.
    pub erasing: u64,
    /// Molecules with no tip that wait for the drain to release kept conflict records.
    pub waiting_records: u64,
}

impl MoleculeDrain {
    /// Read the molecule inside the write gate of `store` and queue it for the
    /// drain when it is dead or refused. Wakes the drain task after the batch
    /// commits.
    pub async fn enqueue(
        &self,
        store: &AtomStore,
        scope: Option<&str>,
        molecule: &str,
    ) -> Result<MoleculeDrainVerdict, SchemaError> {
        if !crate::atom::molecule_uuid::is_canonical_molecule_uuid(molecule) {
            return Err(SchemaError::InvalidData(
                "molecule must be a canonical molecule uuid".into(),
            ));
        }
        AtomStore::validate_scope(scope)?;
        let candidate = Candidate::new(scope, molecule);
        let verdict = store
            .in_write_scope(async {
                let mut verdict = judge(&read_molecule_reads(store, &candidate).await?);
                if verdict == Verdict::Absent && tip_present(store, &candidate).await? {
                    // Tip records and no header: the molecule has no counters.
                    verdict = Verdict::Refused(Refusal::CountersUnset);
                }
                let queue = matches!(verdict, Verdict::Dead { .. } | Verdict::Refused(_));
                if queue {
                    store
                        .commit_tip_changes(TipChangeSet {
                            rows: vec![candidate.row_json()],
                            ..TipChangeSet::default()
                        })
                        .await?;
                    self.wake_after_commit(store);
                }
                Ok(report(&candidate, verdict, queue))
            })
            .await?;
        Ok(verdict)
    }

    /// The molecules of `molecules` that the drain still removes in `scope`: each
    /// has a candidate row and reads dead. An install of a schema that binds such a
    /// molecule again would stop the drain with part of the keys erased, so the
    /// install is refused (`SchemaError::SchemaBeingDropped`). The caller holds the
    /// write gate, so a batch of the drain cannot run between this read and the
    /// bind. A refused molecule (a counter fault) is not counted: the drain does not
    /// remove it. A molecule that has no tip and only kept conflict records that
    /// the slot directory does not name is not counted either: no drain step
    /// releases them, and they end with their own window. One bounded range read
    /// for each molecule, all at once.
    pub(crate) async fn draining(
        &self,
        store: &AtomStore,
        scope: Option<&str>,
        molecules: &[String],
    ) -> Result<DrainingMolecules, SchemaError> {
        let checks = molecules.iter().map(|molecule| async move {
            let candidate = Candidate::new(scope, molecule);
            if !rows_under(store, &candidate.key()).await? {
                return Ok::<Option<bool>, SchemaError>(None);
            }
            let reads = read_molecule_reads(store, &candidate).await?;
            let Verdict::Dead { slots, records } = judge(&reads) else {
                return Ok(None);
            };
            if slots > 0 || records == 0 {
                return Ok(Some(false));
            }
            // Only kept records are left: count the molecule while the directory
            // names a record that the drain will release.
            Ok(
                rows_under(store, &super::family::records_prefix(&candidate))
                    .await?
                    .then_some(true),
            )
        });
        let mut found = DrainingMolecules::default();
        for kind in futures::future::try_join_all(checks)
            .await?
            .into_iter()
            .flatten()
        {
            if kind {
                found.waiting_records += 1;
            } else {
                found.erasing += 1;
            }
        }
        Ok(found)
    }

    /// The candidate rows of `store` now, counted up to `cap` with bounded range
    /// reads (a deleted key does not make the read fail). The second value is true
    /// when the count stopped at the cap.
    pub async fn candidates(
        &self,
        store: &AtomStore,
        cap: usize,
    ) -> Result<(u64, bool), SchemaError> {
        let (start, end) = super::candidate::list_bounds();
        let mut walk = RangeWalk::new(&start, &end, 256);
        let mut count = 0u64;
        while !walk.done() {
            let step = store.walk_step(&mut walk).await?;
            count += step.rows.len() as u64;
            if count >= cap as u64 {
                return Ok((count, true));
            }
        }
        Ok((count, false))
    }
}

/// Whether at least one row begins with `prefix`: a range read of one row.
async fn rows_under(store: &AtomStore, prefix: &str) -> Result<bool, SchemaError> {
    let (start, end) = crate::kind_partition::exact_prefix_bounds(prefix);
    let mut walk = RangeWalk::new(start.as_bytes(), end.as_bytes(), 1);
    Ok(!store.walk_step(&mut walk).await?.rows.is_empty())
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
