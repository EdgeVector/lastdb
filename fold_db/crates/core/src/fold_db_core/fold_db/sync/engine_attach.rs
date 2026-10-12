use super::super::FoldDB;
use super::cloud_purge_source::prepare_photograph_cut_components;
use std::sync::Arc;

impl FoldDB {
    pub(super) async fn set_sync_engine_with_capture(
        &self,
        engine: Arc<crate::sync::SyncEngine>,
        capture_enabled: bool,
    ) {
        self.register_sync_schema_reloaders(&engine).await;

        if capture_enabled {
            if let Some(capture) = &self.mutation_log_capture {
                capture
                    .set_engine_after_mutations(Arc::clone(&engine), || {
                        // New writes must see the atom-retention engine slot
                        // before they can enter the shared serving router.
                        self.mutation_manager
                            .set_capture_engine_metadata(Arc::clone(&engine));
                    })
                    .await;
            } else {
                self.mutation_manager
                    .set_capture_engine(Arc::clone(&engine));
            }
        }
        let db_ops = Arc::clone(&self.db_ops);
        let materializer: crate::sync::engine::MutationIntentMaterializer =
            Arc::new(move |envelopes| {
                let db_ops = Arc::clone(&db_ops);
                Box::pin(async move {
                    crate::sync::mutation_intent::materialize_field_values(
                        envelopes,
                        db_ops.atoms(),
                    )
                    .await
                })
            });
        engine
            .set_mutation_intent_materializer(Arc::clone(&materializer))
            .await;
        engine
            .set_automatic_gc_atom_store(self.db_ops.atoms().clone())
            .await;
        let pending_tasks = Arc::clone(&self.pending_tasks);
        let cut_db_ops = Arc::clone(&self.db_ops);
        let cut_capture_router = self.mutation_log_capture.clone();
        let cut_barrier: crate::sync::engine::PhotographCutBarrier = Arc::new(move || {
            let pending_tasks = Arc::clone(&pending_tasks);
            let cut_db_ops = Arc::clone(&cut_db_ops);
            let cut_capture_router = cut_capture_router.clone();
            Box::pin(async move {
                prepare_photograph_cut_components(
                    &pending_tasks,
                    &cut_db_ops,
                    cut_capture_router.clone(),
                )
                .await
            })
        });
        engine.set_photograph_cut_barrier(cut_barrier).await;
        if let Some(router) = self.mutation_log_capture.clone() {
            engine.set_photograph_mutation_router(router).await;
        }
        let restore_db_ops = Arc::clone(&self.db_ops);
        let restore_barrier: crate::sync::engine::PhotographCutBarrier = Arc::new(move || {
            let restore_db_ops = Arc::clone(&restore_db_ops);
            Box::pin(async move {
                restore_db_ops
                    .atoms()
                    .refresh_boot_encoding_after_restore()
                    .await
                    .map_err(|error| error.to_string())
            })
        });
        engine.set_photograph_restore_barrier(restore_barrier).await;
        if self.preserve_retired_restore_intents {
            engine
                .set_retired_intent_preserver(
                    self.retired_intent_preserver(Arc::clone(&materializer)),
                )
                .await;
        }
        engine
            .set_mutation_intent_applier(self.mutation_intent_replay_applier(materializer))
            .await;
        self.sync_coordinator.set_engine(engine);
    }
}
