//! Explicit-key migration of existing tips into an already bound protein member.

use super::FoldDB;
use crate::schema::types::KeyValue;
use crate::schema::SchemaError;
use serde::Serialize;

mod plan;
mod records;

#[derive(Debug, Serialize)]
pub struct ProteinBackfillReport {
    pub source_schema: String,
    pub target_schema: String,
    pub keys_supplied: usize,
    /// Candidate tips, before existing LWW and Delete barriers select winners.
    pub tips_offered: usize,
    pub durable: bool,
    pub automatic_history_backfill: bool,
}

impl FoldDB {
    /// Backfill only these source keys. No source key or molecule enumeration runs.
    /// The native gate covers definition checks, current source selection, and one
    /// counter-safe target batch. Original atoms, author clocks, and metadata stay
    /// unchanged; newer target tips and Delete barriers retain their normal priority.
    pub async fn backfill_protein_keys(
        &self,
        source: &str,
        target: &str,
        keys: &[KeyValue],
    ) -> Result<ProteinBackfillReport, SchemaError> {
        if keys.is_empty() || keys.len() > 64 {
            return Err(invalid(
                "protein backfill requires between 1 and 64 explicit source keys",
            ));
        }
        let admission = self.acquire_local_mutation_admission().await;
        let atoms = self.db_ops().atoms();
        admission
            .run(atoms.with_protein_membership(async {
                let plan = plan::Plan::new(self, source, target, keys)?;
                plan.verify_membership(atoms).await?;
                let updates = records::prepare(self, &plan).await?;
                let tips_offered = updates.len();
                atoms
                    .store_molecules_changed_keys_batch(&updates, &[], None)
                    .await?;
                Ok(ProteinBackfillReport {
                    source_schema: source.to_owned(),
                    target_schema: target.to_owned(),
                    keys_supplied: keys.len(),
                    tips_offered,
                    durable: true,
                    automatic_history_backfill: false,
                })
            }))
            .await
    }
}

fn invalid(message: &str) -> SchemaError {
    SchemaError::InvalidData(message.to_owned())
}
