//! Retain Delete barriers and physical tip locks through purge enqueue.

use super::*;

pub(super) struct DeletePublication {
    _tip_guards: Vec<tokio::sync::OwnedMutexGuard<()>>,
    _publication_guards: Vec<tokio::sync::OwnedMutexGuard<()>>,
}

impl MutationManager {
    pub(super) async fn prepare_delete_publication(
        &self,
        schema: &crate::schema::types::Schema,
        mutations: &[Mutation],
    ) -> Result<DeletePublication, SchemaError> {
        let barriers = super::super::super::purge::plan_normal_delete_barriers(
            &self.db_ops,
            schema,
            mutations,
        )?;
        let keys: Vec<_> = barriers
            .iter()
            .map(|barrier| barrier.mk_key.clone())
            .collect();
        let tip_guards = self.db_ops.atoms().lock_tip_commits(&keys).await;
        let publication_guards = self.db_ops.atoms().lock_tip_publications(&keys).await;
        self.db_ops
            .atoms()
            .register_pending_delete_barriers(barriers);
        Ok(DeletePublication {
            _tip_guards: tip_guards,
            _publication_guards: publication_guards,
        })
    }
}
