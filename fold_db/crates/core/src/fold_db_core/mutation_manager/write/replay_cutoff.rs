//! Every imported author clock observes the home-wide durable replay floor.

use super::*;

impl MutationManager {
    pub(crate) async fn require_replay_cutoff(
        &self,
        mutations: &[Mutation],
    ) -> Result<(), SchemaError> {
        if mutations.iter().any(|mutation| {
            mutation.replay_cutoff_required
                && mutation.erase_tips_before_nanos.is_some()
                && mutation.author_clock_signature_version == 0
        }) {
            return Err(SchemaError::InvalidData(
                "a replayed TTL cutoff requires its author signature".into(),
            ));
        }
        if !mutations
            .iter()
            .any(|m| m.replay_cutoff_required || m.imported_written_at.is_some())
        {
            return Ok(());
        }
        let cutoff_nanos = self.db_ops.records().minimum_replay_time().await?;
        if cutoff_nanos > 0
            && mutations.iter().any(|mutation| {
                (mutation.replay_cutoff_required || mutation.imported_written_at.is_some())
                    && mutation
                        .imported_written_at
                        .is_none_or(|time| time <= cutoff_nanos)
            })
        {
            return Err(SchemaError::ReplayBeforeDeleteCutoff { cutoff_nanos });
        }
        Ok(())
    }
}
