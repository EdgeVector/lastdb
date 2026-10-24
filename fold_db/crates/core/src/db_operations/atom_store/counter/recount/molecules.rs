//! Exact molecule-counter repair on the stopped home of the offline recount.

mod apply;
mod tally;

use super::{Pass, RecountReport};
use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// Planned molecule repairs; `RecountReport::executed` identifies written results.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoleculeRecountReport {
    pub ran: bool,
    pub molecules: u64,
    pub headers_rewritten: u64,
    pub bindings_rewritten: u64,
    pub bindings_deleted: u64,
    pub drains_queued: u64,
}

#[derive(Default)]
struct Count {
    molecule: Option<String>,
    slots: u64,
    records: u64,
    bindings: u64,
    header: Option<Value>,
    binding_counter: Option<Vec<u8>>,
}

type Counts = BTreeMap<String, Count>;

impl AtomStore {
    pub(super) async fn recount_molecules(
        &self,
        pass: &Pass<'_>,
        execute: bool,
        report: &mut RecountReport,
    ) -> Result<(), SchemaError> {
        // All source families are complete and validated before the first write.
        let counts = self.tally_molecule_recount(pass).await?;
        report.molecules.ran = true;
        report.molecules.molecules += counts.len() as u64;
        let counts: Vec<_> = counts.into_iter().collect();
        for chunk in counts.chunks(64) {
            self.apply_molecule_recount(chunk, pass.scope, execute, report)
                .await?;
        }
        Ok(())
    }
}

fn invalid(message: impl std::fmt::Display) -> SchemaError {
    SchemaError::InvalidData(format!("molecule recount: {message}"))
}
