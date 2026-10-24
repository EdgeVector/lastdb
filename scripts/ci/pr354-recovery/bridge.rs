// CI-only visibility bridge. The production callback and its logic stay unchanged.
impl crate::fold_db_core::FoldDB {
    pub fn pr354_replay_applier(
        &self,
        materializer: crate::sync::engine::MutationIntentMaterializer,
    ) -> crate::sync::engine::MutationIntentApplier {
        self.mutation_intent_replay_applier(materializer)
    }
}
