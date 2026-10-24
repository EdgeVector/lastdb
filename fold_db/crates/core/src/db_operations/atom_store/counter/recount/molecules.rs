//! Exact molecule-counter repair on the stopped home of the offline recount.

mod apply;
mod tally;

use super::{Pass, RecountOptions, RecountReport};
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
    /// Validate all requested scopes before any recount writes. The actual
    /// repair tallies again in bounded passes; no cross-scope count stays live.
    pub(super) async fn validate_molecule_recount(
        &self,
        options: &RecountOptions,
    ) -> Result<(), SchemaError> {
        for scope in &options.scopes {
            for index in 0..options.shards.max(1) {
                let pass = Pass {
                    scope: scope.as_deref(),
                    index,
                    count: options.shards.max(1),
                };
                self.tally_molecule_recount(&pass).await?;
            }
        }
        Ok(())
    }

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
