//! Preserve complete retired intents only on an exclusive one-shot restore.

use super::FoldDB;
use crate::schema::SchemaError;
use crate::sync::engine::RetiredIntentPreserver;
use crate::sync::engine::RetiredReplaySource;
use crate::sync::log::LogOp;
use std::collections::BTreeSet;
use std::sync::Arc;

impl FoldDB {
    pub(super) fn retired_intent_preserver(
        &self,
        materializer: crate::sync::engine::MutationIntentMaterializer,
    ) -> RetiredIntentPreserver {
        let db_ops = Arc::clone(&self.db_ops);
        let manager = Arc::clone(&self.mutation_manager);
        Arc::new(move |source| {
            let db_ops = Arc::clone(&db_ops);
            let manager = Arc::clone(&manager);
            let materializer = Arc::clone(&materializer);
            Box::pin(async move {
                preserve(&db_ops, &manager, materializer, source)
                    .await
                    .map_err(Into::into)
            })
        })
    }
}

async fn preserve(
    db_ops: &Arc<crate::db_operations::DbOperations>,
    manager: &Arc<crate::fold_db_core::mutation_manager::MutationManager>,
    materializer: crate::sync::engine::MutationIntentMaterializer,
    source: RetiredReplaySource,
) -> Result<Option<usize>, SchemaError> {
    let LogOp::MutationIntent {
        mutations: original,
    } = &source.record.entry.op
    else {
        return Ok(None);
    };
    let names: Vec<_> = original
        .iter()
        .map(|member| member.schema_name.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let proofs = db_ops.schemas().retired_replay_proofs(&names).await?;
    if proofs.is_empty() {
        return Ok(None);
    }
    if proofs.len() != names.len()
        || original.is_empty()
        || original.iter().any(|member| {
            member
                .storage_prefix
                .as_ref()
                .is_some_and(|prefix| !prefix.is_empty())
                || member.aggregate_set.is_some()
        })
    {
        return Err(refused(
            "unsupported mixed, scoped, or aggregate retired envelope",
        ));
    }
    // No permanent root write occurs until materialization and native signature
    // validation succeed. The shared fence prevents reclaim during those reads.
    let mut retention: crate::db_operations::atom_store::RetiredIntentRetentionGuard =
        db_ops.atoms().retired_intent_retention_guard().await?;
    let materialized = materializer(original.clone())
        .await
        .map_err(|_| refused("retired intent materialization failed"))?;
    let (mutations, prefix) = crate::sync::mutation_intent::decode_mutations(&materialized);
    if mutations.len() != original.len() || prefix.as_ref().is_some_and(|prefix| !prefix.is_empty())
    {
        return Err(refused("retired intent native decode differs"));
    }
    manager.validate_retired_replay_clocks(&mutations)?;
    let source_id = crate::db_operations::retired_intents::retired_intent_source_id(&source.record);
    let prepared = crate::db_operations::retired_intents::prepare_retired_intent(
        &db_ops.namespaced_store(),
        &source,
        proofs.into_values().collect(),
    )
    .await?;
    retention.retain(&source_id, original).await?;
    manager.persist_retired_replay_clocks(&mutations).await?;
    prepared.commit().await?;
    // Retain the guard through the journal's exact durable readback.
    drop(retention);
    Ok(Some(original.len()))
}

fn refused(message: &str) -> SchemaError {
    SchemaError::InvalidData(message.into())
}
