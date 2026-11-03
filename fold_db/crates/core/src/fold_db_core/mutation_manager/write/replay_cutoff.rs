//! Every imported author clock observes the home-wide durable replay floor.

use super::*;

impl MutationManager {
    pub(crate) async fn require_replay_cutoff(
        &self,
        mutations: &[Mutation],
    ) -> Result<(), SchemaError> {
        if !mutations.iter().any(|m| m.imported_written_at.is_some()) {
            return Ok(());
        }
        let cutoff_nanos = self.db_ops.records().minimum_replay_time().await?;
        if cutoff_nanos > 0
            && mutations.iter().any(|mutation| {
                mutation
                    .imported_written_at
                    .is_some_and(|time| time <= cutoff_nanos)
            })
        {
            return Err(SchemaError::ReplayBeforeDeleteCutoff { cutoff_nanos });
        }
        Ok(())
    }
}
