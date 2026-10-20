//! The atom counter planner report and the verifier.

use super::*;

impl FoldDB {
    /// Totals of the atom counter planner since this process started, the
    /// recent counter faults, and the verifier summary. A read of in-memory
    /// state; no store access.
    #[must_use]
    pub fn atom_counter_report(&self) -> crate::db_operations::AtomCounterReport {
        let atoms = self.db_ops().atoms();
        let mut report = atoms.counter_report().snapshot();
        report.verify_refs = Some(atoms.verify_refs_summary());
        report
    }

    /// Run one step of the counter verifier (`lastdb db verify-refs`). It
    /// reads and reports; it writes nothing and takes no write gate.
    pub async fn verify_refs_step(
        &self,
        request: crate::db_operations::VerifyRequest,
    ) -> Result<crate::db_operations::VerifyReport, FoldDbError> {
        self.db_ops()
            .atoms()
            .verify_refs_step(request)
            .await
            .map_err(|error| admin_op_error("verify_refs", error))
    }

    /// The last complete verify run and the run in progress, from memory.
    #[must_use]
    pub fn verify_refs_summary(&self) -> crate::db_operations::VerifySummary {
        self.db_ops().atoms().verify_refs_summary()
    }

    /// The storage scope prefixes that the database catalog names, besides the
    /// personal scope. The catalog is small, so listing it is not a scan of the
    /// data. A scope that holds data and has no catalog entry is not listed.
    pub async fn atom_counter_scopes(&self) -> Result<Vec<String>, FoldDbError> {
        use crate::db_operations::db_catalog_store::{
            referenced_instance_ids, UNPREFIXED_INSTANCE_ID,
        };
        let entries = self
            .db_ops()
            .db_catalog()
            .list_entries()
            .await
            .map_err(|error| {
                FoldDbError::Database(format!("list the database catalog: {error}"))
            })?;
        let mut scopes: Vec<String> = referenced_instance_ids(&entries)
            .into_iter()
            .filter(|id| !id.is_empty() && id != UNPREFIXED_INSTANCE_ID)
            .collect();
        scopes.sort();
        Ok(scopes)
    }
}
