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
}
