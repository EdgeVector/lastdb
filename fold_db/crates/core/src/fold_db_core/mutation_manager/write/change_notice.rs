//! Notices use the same resolved query keys as the committed logical data.

use super::*;
use crate::db_operations::ChangeFeedEvent;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;

impl MutationManager {
    pub(in crate::fold_db_core::mutation_manager) fn prepare_change_notices(
        &self,
        mutations: &[Mutation],
    ) -> Result<Vec<ChangeFeedEvent>, SchemaError> {
        let mut schemas = BTreeMap::new();
        let mut events = Vec::with_capacity(mutations.len());
        for mutation in mutations {
            if mutation.aggregate_derived_internal {
                continue;
            }
            let name = mutation.schema_name.as_str();
            let schema = match schemas.entry(name) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    entry.insert(self.schema_manager.get_schema_metadata(name)?.ok_or_else(
                        || SchemaError::InvalidData(format!("Schema '{name}' not found")),
                    )?)
                }
            };
            let key = Self::resolve_mutation_key_value(name, schema, mutation)?;
            if let Some(event) = ChangeFeedEvent::from_mutation(mutation, &key) {
                events.push(event);
            }
        }
        Ok(events)
    }
}
