//! Read-through lookup for schemas committed by another service instance.

use super::*;
use std::collections::hash_map::Entry;

impl SchemaServiceState {
    /// An exact public repost stays a read when another instance registered it.
    /// Classify durable state on a miss, including expansions absent from cache.
    pub async fn lookup_idempotent_repost(
        &self,
        schema: &Schema,
        mutation_mappers: &HashMap<String, String>,
    ) -> FoldDbResult<Option<Schema>> {
        if let Some(existing) = self.classify_idempotent_repost(schema, mutation_mappers) {
            return Ok(Some(existing));
        }
        let Some(description) = schema
            .descriptive_name
            .as_deref()
            .filter(|name| !name.trim().is_empty())
        else {
            return Ok(None);
        };
        let stored = self.storage.backend().load_all_schemas().await?;
        let mut probe = schema.clone();
        probe.dedup_fields();
        probe.compute_identity_hash();
        let existing = match probe.get_identity_hash().and_then(|id| stored.get(id)) {
            Some(existing) => Some(
                self.resolve_active_schema(existing, &existing.name, &stored)
                    .map_or_else(|| existing.clone(), |(active, _)| active),
            ),
            None => Self::stored_schema_by_description(
                &stored,
                &descriptive_name_key(schema.owner_app_id.as_deref(), description),
            )?,
        };
        let Some(existing) = existing else {
            return Ok(None);
        };
        self.validate_loaded_schemas(&HashMap::from([(existing.name.clone(), existing.clone())]))
            .await?;
        // Do not overwrite concurrent cache entries with a snapshot read. The
        // same read-only checks decide against this fresh durable candidate.
        Ok(self.classify_repost_candidate(schema, &existing))
    }

    /// A warm process can lack an identity that another instance acknowledged.
    /// Consult durable definitions on a miss; never replace concurrent cache entries.
    pub async fn lookup_schema_by_name(&self, name: &str) -> FoldDbResult<Option<Schema>> {
        if let Some(schema) = self.get_schema_by_name(name)? {
            return Ok(Some(schema));
        }
        let stored = self.storage.backend().load_all_schemas().await?;
        let schema = match stored.get(name) {
            Some(schema) => Some(schema.clone()),
            None => Self::stored_schema_by_description(&stored, name)?,
        };
        if let Some(schema) = &schema {
            self.validate_loaded_schemas(&HashMap::from([(schema.name.clone(), schema.clone())]))
                .await?;
            let inserted = {
                let mut schemas = write_lock(&self.schemas, "schemas")?;
                if let Entry::Vacant(entry) = schemas.entry(schema.name.clone()) {
                    entry.insert(schema.clone());
                    self.bump_state_version();
                    true
                } else {
                    false
                }
            };
            if inserted {
                self.rebuild_descriptive_name_index();
            }
        }
        Ok(schema)
    }

    fn stored_schema_by_description(
        stored: &HashMap<String, Schema>,
        name: &str,
    ) -> FoldDbResult<Option<Schema>> {
        let mut matches = stored.values().filter(|schema| {
            schema.superseded_by.is_none()
                && schema
                    .descriptive_name
                    .as_deref()
                    .is_some_and(|description| {
                        descriptive_name_key(schema.owner_app_id.as_deref(), description) == name
                    })
        });
        let schema = matches.next().cloned();
        if matches.next().is_some() {
            return Err(FoldDbError::Config(
                "stored schema descriptive name is ambiguous".to_owned(),
            ));
        }
        Ok(schema)
    }
}
