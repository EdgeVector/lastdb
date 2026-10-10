//! Serving mutation replay retains typed causes until the safe wire boundary.

use super::*;

impl FoldDB {
    pub(super) fn mutation_intent_replay_applier(
        &self,
        materializer: crate::sync::engine::MutationIntentMaterializer,
    ) -> crate::sync::engine::MutationIntentApplier {
        let mutation_manager = Arc::clone(&self.mutation_manager);
        Arc::new(move |envelopes| {
            let mutation_manager = Arc::clone(&mutation_manager);
            let materializer = Arc::clone(&materializer);
            Box::pin(async move {
                let envelopes = materializer(envelopes)
                    .await
                    .map_err(crate::sync::MutationIntentReplayError::materialization)?;
                let (mutations, prefix) =
                    crate::sync::mutation_intent::decode_mutations(&envelopes);
                mutation_manager
                    .apply_replayed_mutations(mutations, prefix.as_deref())
                    .await
                    .map(|_| ())
                    .map_err(crate::sync::MutationIntentReplayError::from)
            })
        })
    }
}
