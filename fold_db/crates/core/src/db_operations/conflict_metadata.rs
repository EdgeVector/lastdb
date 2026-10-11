//! Bounded, read-only conflict metadata for selected field molecules.

use serde::Serialize;

/// A clean result requires a complete authoritative read. Unknown is not clean.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MoleculeConflictState {
    Clean,
    Conflict,
    Unknown,
}

/// Most distinct field molecules one metadata request may read.
pub const SELECTED_CONFLICT_MAX_MOLECULES: usize = 64;

mod read {
    use super::MoleculeConflictState;
    use crate::db_operations::DbOperations;
    use futures::StreamExt;

    const MAX_CONFLICT_ROWS: usize = 128;
    const MAX_CONFLICT_BYTES: usize = 4096;
    const READ_CONCURRENCY: usize = 4;

    // Atom bodies and winner/loser identifiers are not needed for this read.
    #[derive(serde::Deserialize)]
    struct ConflictMetadata {
        id: String,
        molecule_uuid: String,
        resolved: bool,
    }

    impl DbOperations {
        /// The caller holds the serving mutation fence for the complete batch.
        /// Do not trust or repair the advisory mcc list on this path.
        pub(crate) async fn read_selected_conflict_metadata(
            &self,
            molecules: &[String],
        ) -> Vec<MoleculeConflictState> {
            let mut results: Vec<_> = futures::stream::iter(molecules.iter().cloned().enumerate())
                .map(|(index, molecule)| async move {
                    (index, self.read_molecule_conflict_metadata(&molecule).await)
                })
                .buffer_unordered(READ_CONCURRENCY)
                .collect()
                .await;
            results.sort_unstable_by_key(|(index, _)| *index);
            results.into_iter().map(|(_, state)| state).collect()
        }

        async fn read_molecule_conflict_metadata(&self, molecule: &str) -> MoleculeConflictState {
            use MoleculeConflictState::{Clean, Conflict, Unknown};
            if molecule.is_empty() || molecule.len() > 128 || molecule.contains([':', '\0']) {
                return Unknown;
            }
            let prefix = crate::kind_partition::anchored("conflict", &format!("{molecule}:"));
            let mut forms = vec![prefix.clone()];
            if let Some(twin) = crate::kind_partition::form_twin(&prefix) {
                forms.push(twin);
            }
            let reads = forms.iter().map(|form| {
                self.atoms()
                    .raw()
                    .inner()
                    .scan_prefix_paged(form.as_bytes(), MAX_CONFLICT_ROWS + 1)
            });
            let Ok(pages) = futures::future::try_join_all(reads).await else {
                return Unknown;
            };
            let mut conflicted = false;
            for (form, rows) in forms.iter().zip(pages) {
                if rows.len() > MAX_CONFLICT_ROWS {
                    return Unknown;
                }
                for (key, bytes) in rows {
                    if !key.starts_with(form.as_bytes()) || bytes.len() > MAX_CONFLICT_BYTES {
                        return Unknown;
                    }
                    let Ok(row) = serde_json::from_slice::<ConflictMetadata>(&bytes) else {
                        return Unknown;
                    };
                    if row.molecule_uuid != molecule || !valid_conflict_id(&row.id, molecule) {
                        return Unknown;
                    }
                    let Some(stamp) = row.id.get(molecule.len() + 1..) else {
                        return Unknown;
                    };
                    if key.get(form.len()..) != Some(stamp.as_bytes()) {
                        return Unknown;
                    }
                    conflicted |= !row.resolved;
                }
            }
            if conflicted {
                Conflict
            } else {
                Clean
            }
        }
    }

    fn valid_conflict_id(id: &str, molecule: &str) -> bool {
        id.strip_prefix(molecule)
            .and_then(|suffix| suffix.strip_prefix(':'))
            .is_some_and(|stamp| {
                !stamp.is_empty() && stamp.len() <= 20 && stamp.bytes().all(|b| b.is_ascii_digit())
            })
    }
}
