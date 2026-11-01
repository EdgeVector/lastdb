use super::collector::Collected;
use super::records::record_outcomes;
use super::*;

impl MutationManager {
    /// Data, original outcomes, and the device head enter one native commit.
    /// The request stages each final record, so the staged value is the
    /// certified effect.
    pub(in crate::fold_db_core::mutation_manager) async fn finish_logical_capture(
        &self,
        runtime: &Arc<PackRuntime>,
        identities: &[String],
        collected: Collected,
    ) -> Result<(), SchemaError> {
        if !runtime.logical_capture_enabled() {
            return Ok(());
        }
        let outcomes = record_outcomes(collected.records, identities)?;
        runtime.stage_logical_capture(identities, outcomes).await?;
        Ok(())
    }
}
