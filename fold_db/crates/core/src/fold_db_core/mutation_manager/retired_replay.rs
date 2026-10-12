//! Native clock acknowledgement for a preserved, unapplied restore intent.

use super::MutationManager;
use crate::schema::types::author_clock::verify_mutation_author_clock;
use crate::schema::types::Mutation;
use crate::schema::SchemaError;

impl MutationManager {
    pub(crate) fn validate_retired_replay_clocks(
        &self,
        mutations: &[Mutation],
    ) -> Result<(), SchemaError> {
        if mutations.is_empty()
            || mutations.iter().any(|mutation| {
                mutation.imported_written_at.is_none() || !verify_mutation_author_clock(mutation)
            })
        {
            return Err(SchemaError::InvalidData(
                "invalid retired intent author-clock signature".into(),
            ));
        }
        Ok(())
    }

    pub(crate) async fn persist_retired_replay_clocks(
        &self,
        mutations: &[Mutation],
    ) -> Result<(), SchemaError> {
        self.validate_retired_replay_clocks(mutations)?;
        let _kv_suppress = self
            .capture_router()
            .map(|router| router.enter_kv_suppress());
        crate::sync::capture::with_capture_suppressed(async {
            if let Some((reservation, state)) = self.observe_replayed_author_clocks(mutations)? {
                reservation.submit_and_wait(state).await?;
            }
            Ok(())
        })
        .await
    }
}
