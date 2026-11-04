//! Stable request IDs bind complete intents to their original record revisions.
use super::MutationManager;
use crate::schema::types::Mutation;
use crate::schema::SchemaError;
use std::collections::BTreeMap;

pub(super) struct FilteredMutations {
    pub(super) already_seen_ids: Vec<String>,
    pub(super) revisions: BTreeMap<String, String>,
    pub(super) mutations: Vec<Mutation>,
    pub(super) hashes: Vec<String>,
}
impl MutationManager {
    pub(super) async fn filter_idempotent_mutations(
        &self,
        mutations: Vec<Mutation>,
    ) -> Result<FilteredMutations, SchemaError> {
        let keys: Vec<_> = mutations.iter().map(Mutation::retry_key).collect();
        let cached = self
            .db_ops
            .metadata()
            .get_idempotency_ids(&keys)
            .await
            .unwrap_or_else(|_| vec![None; mutations.len()]);
        if cached.len() != mutations.len() {
            return Err(SchemaError::InvalidData(
                "retry receipt batch is incomplete".into(),
            ));
        }
        let mut out = FilteredMutations {
            already_seen_ids: Vec::new(),
            revisions: BTreeMap::new(),
            mutations: Vec::new(),
            hashes: Vec::new(),
        };
        for (mutation, cached) in mutations.into_iter().zip(cached) {
            let hash = mutation.intent_hash();
            if let Some(receipt) = cached {
                if receipt.intent_hash != hash {
                    return Err(SchemaError::InvalidData(
                        "mutation ID was reused for a different complete request".into(),
                    ));
                }
                out.revisions
                    .insert(mutation.uuid.clone(), receipt.revision);
                out.already_seen_ids.push(mutation.uuid);
            } else {
                out.hashes.push(hash);
                out.mutations.push(mutation);
            }
        }
        Ok(out)
    }
}
